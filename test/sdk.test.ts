import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { Destination, type Harness, sleep, startPostlater, TOKEN, waitFor } from './support/harness.js';

let h: Harness;
let dest: Destination;

beforeAll(async () => {
	h = await startPostlater();
	dest = await new Destination().start();
});

afterAll(async () => {
	await h?.close();
	await dest?.close();
});

const fromBase64 = (value: string) => Buffer.from(value, 'base64').toString('utf8');

describe('publishing with @upstash/qstash', () => {
	it('delivers a signed message that Receiver.verify accepts', async () => {
		const res = await h.client.publishJSON({
			url: `${dest.url}/hello`,
			body: { hello: 'world' },
			headers: { 'x-custom': 'yes' },
		});
		expect(res.messageId).toMatch(/^msg_/);

		const received = await waitFor(() => dest.to('/hello')[0], 5000, 'delivery');
		expect(received.method).toBe('POST');
		expect(JSON.parse(received.body)).toEqual({ hello: 'world' });
		expect(received.headers['content-type']).toBe('application/json');
		expect(received.headers['x-custom']).toBe('yes');
		expect(received.headers['upstash-message-id']).toBe(res.messageId);
		expect(received.headers['upstash-retried']).toBe('0');

		const signature = received.headers['upstash-signature'] as string;
		await expect(h.receiver.verify({ signature, body: received.body, url: `${dest.url}/hello` })).resolves.toBe(true);
		await expect(
			h.receiver.verify({ signature, body: `${received.body} `, url: `${dest.url}/hello` }),
		).rejects.toThrow();
		await expect(h.receiver.verify({ signature, body: received.body, url: `${dest.url}/other` })).rejects.toThrow();
	});

	it('keeps the destination query string and honours Upstash-Method', async () => {
		await h.client.publish({ url: `${dest.url}/get-me?a=1&b=two`, method: 'GET' });
		const received = await waitFor(() => dest.to('/get-me')[0]);
		expect(received.method).toBe('GET');
		expect(received.path).toBe('/get-me?a=1&b=two');
		expect(received.body).toBe('');
	});

	it('waits out a delay, and the message can be read while it waits', async () => {
		const published = Date.now();
		const { messageId } = await h.client.publishJSON({ url: `${dest.url}/delayed`, body: { n: 1 }, delay: 2 });

		const message = await h.client.messages.get(messageId);
		expect(message.messageId).toBe(messageId);
		expect(message.url).toBe(`${dest.url}/delayed`);
		expect(message.body).toBe('{"n":1}');
		expect(message.notBefore).toBeGreaterThanOrEqual(published + 2000);

		await sleep(1000);
		expect(dest.to('/delayed')).toHaveLength(0);
		const received = await waitFor(() => dest.to('/delayed')[0], 5000);
		expect(received.at - published).toBeGreaterThanOrEqual(1900);
	});

	it('cancels a waiting message', async () => {
		const { messageId } = await h.client.publishJSON({ url: `${dest.url}/cancelled`, body: {}, delay: 2 });
		await h.client.messages.delete(messageId);
		await expect(h.client.messages.get(messageId)).rejects.toThrow(/not found/);
		await sleep(2500);
		expect(dest.to('/cancelled')).toHaveLength(0);
	});

	it('cancels in bulk', async () => {
		const ids = await Promise.all(
			[1, 2, 3].map(
				async () => (await h.client.publishJSON({ url: `${dest.url}/bulk`, body: {}, delay: 60 })).messageId,
			),
		);
		expect(await h.client.messages.deleteMany(ids)).toBe(3);
	});

	it('retries with the retry delay expression until the destination succeeds', async () => {
		dest.on('/flaky', (_req, count) => (count < 3 ? { status: 500, body: 'not yet' } : { status: 200 }));
		const { messageId } = await h.client.publishJSON({
			url: `${dest.url}/flaky`,
			body: {},
			retries: 3,
			retryDelay: '100 * retried',
		});
		await waitFor(() => dest.to('/flaky').length === 3, 5000, 'three attempts');
		expect(dest.to('/flaky').map((r) => r.headers['upstash-retried'])).toEqual(['0', '1', '2']);

		const logs = await waitFor(async () => {
			const { logs } = await h.client.logs({ filter: { messageId } });
			return logs.some((l) => l.state === 'DELIVERED') && logs;
		});
		const states = logs.map((l) => l.state).reverse();
		expect(states).toEqual(['CREATED', 'ACTIVE', 'RETRY', 'ACTIVE', 'RETRY', 'ACTIVE', 'DELIVERED']);
		expect(logs.find((l) => l.state === 'RETRY')?.error).toContain('not yet');
	});

	it('moves a message that never succeeds to the DLQ, and calls the callbacks', async () => {
		dest.on('/broken', () => ({ status: 503, body: 'down for good' }));
		const { messageId } = await h.client.publishJSON({
			url: `${dest.url}/broken`,
			body: { order: 7 },
			retries: 1,
			retryDelay: '0',
			callback: `${dest.url}/broken-callback`,
			failureCallback: `${dest.url}/broken-failure`,
		});

		const failure = await waitFor(() => dest.to('/broken-failure')[0], 8000, 'failure callback');
		const payload = JSON.parse(failure.body);
		expect(payload.status).toBe(503);
		expect(fromBase64(payload.body)).toBe('down for good');
		expect(payload.retried).toBe(1);
		expect(payload.maxRetries).toBe(1);
		expect(payload.sourceMessageId).toBe(messageId);
		expect(fromBase64(payload.sourceBody)).toBe('{"order":7}');
		expect(payload.dlqId).toBeTruthy();
		expect(typeof payload.createdAt).toBe('string');
		await expect(
			h.receiver.verify({
				signature: failure.headers['upstash-signature'] as string,
				body: failure.body,
				url: `${dest.url}/broken-failure`,
			}),
		).resolves.toBe(true);

		// the callback hears of each attempt
		const callbacks = await waitFor(() => dest.to('/broken-callback').length === 2 && dest.to('/broken-callback'));
		expect(callbacks.map((c) => JSON.parse(c.body).retried).sort()).toEqual([0, 1]);

		const { messages } = await h.client.dlq.listMessages({ filter: { messageId } });
		expect(messages).toHaveLength(1);
		const dlq = messages[0] as (typeof messages)[number];
		expect(dlq.dlqId).toBe(payload.dlqId);
		expect(dlq.responseStatus).toBe(503);
		expect(dlq.responseBody).toBe('down for good');
		expect(dlq.url).toBe(`${dest.url}/broken`);
	});

	it('stops at once on 489 with Upstash-NonRetryable-Error', async () => {
		dest.on('/refuses', () => ({ status: 489, headers: { 'Upstash-NonRetryable-Error': 'true' }, body: 'bad input' }));
		const { messageId } = await h.client.publishJSON({ url: `${dest.url}/refuses`, body: {}, retries: 5 });
		await waitFor(async () => (await h.client.dlq.listMessages({ filter: { messageId } })).messages.length === 1);
		expect(dest.to('/refuses')).toHaveLength(1);
	});

	it('publishes DLQ messages again with dlq.retry, and deletes them', async () => {
		let fixed = false;
		dest.on('/later-fixed', () => (fixed ? { status: 200 } : { status: 500 }));
		const { messageId } = await h.client.publishJSON({ url: `${dest.url}/later-fixed`, body: { x: 1 }, retries: 0 });
		const [entry] = await waitFor(async () => {
			const { messages } = await h.client.dlq.listMessages({ filter: { messageId } });
			return messages.length === 1 && messages;
		});

		fixed = true;
		const { responses } = await h.client.dlq.retry([entry?.dlqId as string]);
		expect(responses).toHaveLength(1);
		await waitFor(() => dest.to('/later-fixed').length === 2);
		expect(dest.to('/later-fixed')[1]?.body).toBe('{"x":1}');
		expect((await h.client.dlq.listMessages({ filter: { messageId } })).messages).toHaveLength(0);

		dest.on('/dead', () => ({ status: 500 }));
		const dead = await h.client.publishJSON({ url: `${dest.url}/dead`, body: {}, retries: 0 });
		const [deadEntry] = await waitFor(async () => {
			const { messages } = await h.client.dlq.listMessages({ filter: { messageId: dead.messageId } });
			return messages.length === 1 && messages;
		});
		expect(await h.client.dlq.delete(deadEntry?.dlqId as string)).toEqual({ deleted: 1 });
	});

	it('calls the callback with the response, its own headers and a valid signature', async () => {
		dest.on('/answers', () => ({ status: 201, headers: { 'x-answer': '42' }, body: 'created' }));
		const { messageId } = await h.client.publishJSON({
			url: `${dest.url}/answers`,
			body: {},
			callback: `${dest.url}/answer-callback`,
			headers: { 'Upstash-Callback-Forward-X-Trace': 'abc' },
		});
		const callback = await waitFor(() => dest.to('/answer-callback')[0]);
		const payload = JSON.parse(callback.body);
		expect(payload.status).toBe(201);
		expect(fromBase64(payload.body)).toBe('created');
		expect(payload.header['x-answer']).toEqual(['42']);
		expect(payload.sourceMessageId).toBe(messageId);
		expect(payload.url).toBe(`${dest.url}/answers`);
		expect(callback.headers['x-trace']).toBe('abc');
		await expect(
			h.receiver.verify({
				signature: callback.headers['upstash-signature'] as string,
				body: callback.body,
				url: `${dest.url}/answer-callback`,
			}),
		).resolves.toBe(true);
	});

	it('deduplicates by id and by content', async () => {
		const first = await h.client.publishJSON({
			url: `${dest.url}/once`,
			body: {},
			deduplicationId: 'order-1',
			delay: 60,
		});
		const second = await h.client.publishJSON({
			url: `${dest.url}/once`,
			body: {},
			deduplicationId: 'order-1',
			delay: 60,
		});
		expect(second.messageId).toBe(first.messageId);
		expect(second.deduplicated).toBe(true);

		const a = await h.client.publishJSON({
			url: `${dest.url}/once`,
			body: { v: 1 },
			contentBasedDeduplication: true,
			delay: 60,
		});
		const b = await h.client.publishJSON({
			url: `${dest.url}/once`,
			body: { v: 1 },
			contentBasedDeduplication: true,
			delay: 60,
		});
		const c = await h.client.publishJSON({
			url: `${dest.url}/once`,
			body: { v: 2 },
			contentBasedDeduplication: true,
			delay: 60,
		});
		expect(b.messageId).toBe(a.messageId);
		expect(c.messageId).not.toBe(a.messageId);
	});

	it('publishes batches', async () => {
		const results = await h.client.batchJSON([
			{ url: `${dest.url}/batch-a`, body: { i: 1 } },
			{ url: `${dest.url}/batch-b`, body: { i: 2 }, queueName: 'batch-queue' },
		]);
		expect(results).toHaveLength(2);
		await waitFor(() => dest.to('/batch-a').length === 1 && dest.to('/batch-b').length === 1);
		expect((await h.client.queue({ queueName: 'batch-queue' }).get()).name).toBe('batch-queue');
	});

	it('refuses requests without the token', async () => {
		const res = await fetch(`${h.postlater.url}/v2/publish/${dest.url}/x`, {
			method: 'POST',
			headers: { Authorization: 'Bearer wrong-token-0123456789' },
		});
		expect(res.status).toBe(401);
		const ok = await fetch(`${h.postlater.url}/v2/keys`, { headers: { Authorization: `Bearer ${TOKEN}` } });
		expect(ok.status).toBe(200);
	});

	it('rejects what it cannot deliver', async () => {
		await expect(h.client.publish({ url: 'ftp://example.com/file' } as never)).rejects.toThrow();
		await expect(h.client.publishJSON({ url: `${dest.url}/x`, body: {}, retryDelay: 'eval(1)' })).rejects.toThrow(
			/Upstash-Retry-Delay/,
		);
		await expect(h.client.publishJSON({ urlGroup: 'no-such-group', body: {} })).rejects.toThrow(/url group/);
	});
});

describe('url groups', () => {
	it('fans a message out to every endpoint', async () => {
		await h.client.urlGroups.addEndpoints({
			name: 'orders',
			endpoints: [{ url: `${dest.url}/fan-1`, name: 'one' }, { url: `${dest.url}/fan-2` }],
		});
		const group = await h.client.urlGroups.get('orders');
		expect(group.endpoints).toEqual([{ name: 'one', url: `${dest.url}/fan-1` }, { url: `${dest.url}/fan-2` }]);
		expect((await h.client.urlGroups.list()).map((g) => g.name)).toContain('orders');

		const res = await h.client.publishJSON({ urlGroup: 'orders', body: { id: 1 } });
		expect(res).toHaveLength(2);
		await waitFor(() => dest.to('/fan-1').length === 1 && dest.to('/fan-2').length === 1);
		expect(dest.to('/fan-1')[0]?.headers['upstash-topic-name']).toBe('orders');

		await h.client.urlGroups.removeEndpoints({ name: 'orders', endpoints: [{ url: `${dest.url}/fan-2` }] });
		expect((await h.client.urlGroups.get('orders')).endpoints).toHaveLength(1);
		await h.client.urlGroups.delete('orders');
		await expect(h.client.urlGroups.get('orders')).rejects.toThrow(/not found/);
	});
});

describe('queues', () => {
	it('delivers in order, one at a time, and can be paused', async () => {
		const queue = h.client.queue({ queueName: 'serial' });
		await queue.upsert({ parallelism: 1 });
		dest.on('/serial', () => ({ delayMs: 150 }));

		await queue.pause();
		for (const n of [1, 2, 3, 4]) await queue.enqueueJSON({ url: `${dest.url}/serial`, body: { n } });
		expect((await queue.get()).paused).toBe(true);
		expect((await queue.get()).lag).toBe(4);
		await sleep(300);
		expect(dest.to('/serial')).toHaveLength(0);

		await queue.resume();
		await waitFor(() => dest.to('/serial').length === 4, 8000);
		expect(dest.to('/serial').map((r) => JSON.parse(r.body).n)).toEqual([1, 2, 3, 4]);
		expect(dest.peak.get('/serial')).toBe(1);

		expect((await h.client.queue().list()).map((q) => q.name)).toContain('serial');
		await queue.delete();
		await expect(queue.get()).rejects.toThrow(/not found/);
	});

	it('runs up to its parallelism at once', async () => {
		const queue = h.client.queue({ queueName: 'pair' });
		await queue.upsert({ parallelism: 2 });
		dest.on('/pair', () => ({ delayMs: 300 }));
		for (const n of [1, 2, 3, 4, 5, 6]) await queue.enqueueJSON({ url: `${dest.url}/pair`, body: { n } });
		await waitFor(() => dest.to('/pair').length === 6, 8000);
		expect(dest.peak.get('/pair')).toBe(2);
	});
});

describe('flow control', () => {
	it('limits parallelism per key', async () => {
		dest.on('/flow', () => ({ delayMs: 200 }));
		for (const n of [1, 2, 3, 4]) {
			await h.client.publishJSON({
				url: `${dest.url}/flow`,
				body: { n },
				flowControl: { key: 'tenant-1', parallelism: 1 },
			});
		}
		await waitFor(() => dest.to('/flow').length === 4, 8000);
		expect(dest.peak.get('/flow')).toBe(1);
	});

	it('limits the rate per key', async () => {
		const started = Date.now();
		for (const n of [1, 2, 3, 4, 5, 6]) {
			await h.client.publishJSON({
				url: `${dest.url}/rated`,
				body: { n },
				flowControl: { key: 'rated', rate: 2, period: '1s' },
			});
		}
		await waitFor(() => dest.to('/rated').length === 6, 8000);
		// six at two a second: the last ones start two seconds after the first
		expect((dest.to('/rated')[5]?.at ?? 0) - started).toBeGreaterThanOrEqual(1900);
	});
});

describe('schedules', () => {
	it('publishes on its cron, and can be paused, resumed and deleted', async () => {
		const { scheduleId } = await h.client.schedules.create({
			destination: `${dest.url}/cron`,
			cron: '* * * * * *',
			body: JSON.stringify({ tick: true }),
			headers: { 'x-from': 'schedule' },
		});
		expect(scheduleId).toMatch(/^scd_/);

		const first = await waitFor(() => dest.to('/cron')[0], 5000, 'first scheduled delivery');
		expect(first.headers['upstash-schedule-id']).toBe(scheduleId);
		expect(first.headers['x-from']).toBe('schedule');
		expect(first.body).toBe('{"tick":true}');

		const schedule = await h.client.schedules.get(scheduleId);
		expect(schedule.cron).toBe('* * * * * *');
		expect(schedule.destination).toBe(`${dest.url}/cron`);
		expect(schedule.isPaused).toBe(false);
		expect((await h.client.schedules.list()).map((s) => s.scheduleId)).toContain(scheduleId);

		await h.client.schedules.pause({ schedule: scheduleId });
		expect((await h.client.schedules.get(scheduleId)).isPaused).toBe(true);
		await sleep(300);
		const count = dest.to('/cron').length;
		await sleep(1500);
		expect(dest.to('/cron').length).toBe(count);

		await h.client.schedules.resume({ schedule: scheduleId });
		await waitFor(() => dest.to('/cron').length > count, 4000);

		await h.client.schedules.delete(scheduleId);
		await expect(h.client.schedules.get(scheduleId)).rejects.toThrow(/not found/);
	});

	it('keeps a chosen schedule id, and updates it in place', async () => {
		await h.client.schedules.create({ destination: `${dest.url}/x`, cron: '0 0 1 1 *', scheduleId: 'nightly-report' });
		await h.client.schedules.create({
			destination: `${dest.url}/y`,
			cron: 'CRON_TZ=Europe/Berlin 0 9 * * *',
			scheduleId: 'nightly-report',
		});
		const schedule = await h.client.schedules.get('nightly-report');
		expect(schedule.destination).toBe(`${dest.url}/y`);
		expect(schedule.cron).toBe('CRON_TZ=Europe/Berlin 0 9 * * *');
		await h.client.schedules.delete('nightly-report');
	});

	it('refuses a cron it cannot read', async () => {
		await expect(h.client.schedules.create({ destination: `${dest.url}/x`, cron: 'every tuesday' })).rejects.toThrow(
			/Upstash-Cron/,
		);
	});
});

describe('signing keys', () => {
	it('rotates, and receivers holding both keys keep verifying', async () => {
		const headers = { Authorization: `Bearer ${TOKEN}` };
		const before = (await (await fetch(`${h.postlater.url}/v2/keys`, { headers })).json()) as {
			current: string;
			next: string;
		};
		const after = (await (await fetch(`${h.postlater.url}/v2/keys/rotate`, { method: 'POST', headers })).json()) as {
			current: string;
			next: string;
		};
		expect(after.current).toBe(before.next);
		expect(after.next).not.toBe(before.next);

		// a receiver still holding the old pair verifies with its next key, as QStash intends
		await h.client.publishJSON({ url: `${dest.url}/rotated`, body: {} });
		const received = await waitFor(() => dest.to('/rotated')[0], 12_000);
		await expect(
			h.receiver.verify({
				signature: received.headers['upstash-signature'] as string,
				body: received.body,
				url: `${dest.url}/rotated`,
			}),
		).resolves.toBe(true);
	});
});
