import { timingSafeEqual } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { getConnInfo } from '@hono/node-server/conninfo';
import { type Context, Hono } from 'hono';
import type { Config } from './config.js';
import { PublishError, toHeaderMap } from './publish.js';
import type { Service } from './service.js';

const openapi = readFileSync(new URL('./openapi.json', import.meta.url), 'utf8');

function tokenMatches(given: string, tokens: string[]): boolean {
	const a = Buffer.from(given);
	let match = false;
	for (const token of tokens) {
		const b = Buffer.from(token);
		// compared in full every time, so the time taken says nothing about how much of a token was right
		if (a.length === b.length && timingSafeEqual(a, b)) match = true;
	}
	return match;
}

function bearer(c: Context): string | null {
	const header = c.req.header('authorization') ?? '';
	const match = /^Bearer\s+(.+)$/i.exec(header);
	return match ? (match[1] as string).trim() : null;
}

export function callerIp(c: Context, trustProxy: boolean): string | null {
	if (trustProxy) {
		const forwarded = c.req.header('x-forwarded-for')?.split(',')[0]?.trim();
		if (forwarded) return forwarded;
	}
	try {
		return getConnInfo(c).remote.address ?? null;
	} catch {
		return null;
	}
}

/** What follows a prefix in the path, with the query string, which belongs to the destination URL. */
function rest(c: Context, prefix: RegExp): string {
	const url = new URL(c.req.url);
	const path = decodeURIComponent(url.pathname.replace(prefix, ''));
	return path + url.search;
}

export function createApp(service: Service, config: Config): Hono {
	const app = new Hono();

	app.onError((error, c) => {
		if (error instanceof PublishError) return c.json({ error: error.message }, error.status as 400);
		console.error(error);
		return c.json({ error: 'internal server error' }, 500);
	});
	app.notFound((c) => c.json({ error: 'not found' }, 404));

	app.get('/health', async (c) => {
		await service.sql`SELECT 1`;
		return c.json({ ok: true });
	});
	app.get('/openapi.json', (c) => c.body(openapi, 200, { 'content-type': 'application/json' }));

	app.get('/metrics', async (c) => {
		if (!config.publicMetrics && !tokenMatches(bearer(c) ?? '', config.tokens)) {
			return c.json({ error: 'unauthorized' }, 401);
		}
		const stats = await service.stats();
		const lines = [
			'# HELP postlater_messages Messages waiting or being delivered, by state.',
			'# TYPE postlater_messages gauge',
			...['CREATED', 'ACTIVE', 'RETRY'].map(
				(state) => `postlater_messages{state="${state}"} ${stats.states.find((s) => s.state === state)?.count ?? 0}`,
			),
			'# HELP postlater_dlq_messages Messages in the dead-letter queue.',
			'# TYPE postlater_dlq_messages gauge',
			`postlater_dlq_messages ${stats.dlq}`,
			'# HELP postlater_schedules Schedules.',
			'# TYPE postlater_schedules gauge',
			`postlater_schedules ${stats.schedules}`,
		];
		return c.text(`${lines.join('\n')}\n`, 200, { 'content-type': 'text/plain; version=0.0.4' });
	});

	const v2 = new Hono();
	v2.use(async (c, next) => {
		if (!tokenMatches(bearer(c) ?? '', config.tokens)) return c.json({ error: 'unauthorized' }, 401);
		await next();
	});

	const body = async (c: Context): Promise<Uint8Array> => {
		const bytes = new Uint8Array(await c.req.arrayBuffer());
		if (bytes.length > config.maxBodyBytes) throw new PublishError('body too large', 413);
		return bytes;
	};
	const json = async <T>(c: Context): Promise<T> => {
		try {
			return (await c.req.json()) as T;
		} catch {
			throw new PublishError('the body must be JSON');
		}
	};
	const headers = (c: Context) => toHeaderMap(c.req.raw.headers);
	const ip = (c: Context) => callerIp(c, config.trustProxy);

	// publishing
	v2.post('/publish/*', async (c) => {
		const result = await service.publish(rest(c, /^\/v2\/publish\//), headers(c), await body(c), { callerIp: ip(c) });
		return c.json(result, 201);
	});
	v2.post('/enqueue/:queue/*', async (c) => {
		const queue = c.req.param('queue');
		const destination = rest(
			c,
			new RegExp(`^/v2/enqueue/${encodeURIComponent(queue).replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}/`),
		);
		const result = await service.publish(destination, headers(c), await body(c), { callerIp: ip(c), queueName: queue });
		return c.json(result, 201);
	});
	v2.post('/batch', async (c) => {
		const items = await json<unknown>(c);
		if (!Array.isArray(items) || items.length === 0)
			throw new PublishError('the body must be a non-empty array of messages');
		const parsed = items.map((item, i) => {
			const m = item as { destination?: unknown; headers?: unknown; body?: unknown; queue?: unknown };
			if (typeof m?.destination !== 'string') throw new PublishError(`message ${i} has no destination`);
			return {
				destination: m.destination,
				headers: toHeaderMap((m.headers ?? {}) as Record<string, string>),
				body: new TextEncoder().encode(typeof m.body === 'string' ? m.body : ''),
				queue: typeof m.queue === 'string' && m.queue ? m.queue : null,
			};
		});
		return c.json(await service.batch(parsed, ip(c)), 201);
	});

	// messages
	v2.get('/messages/:id', async (c) => c.json(await service.getMessage(c.req.param('id'))));
	v2.delete('/messages/:id', async (c) => {
		const cancelled = await service.cancelMessages({ ids: [c.req.param('id')] });
		if (cancelled === 0)
			throw new PublishError(`message ${c.req.param('id')} not found, or already being delivered`, 404);
		return c.json({ cancelled });
	});
	v2.delete('/messages', async (c) => {
		const query = new URL(c.req.url).searchParams;
		const ids = query.getAll('messageIds');
		const count = query.get('count');
		const cancelled = await service.cancelMessages(
			ids.length > 0 ? { ids } : { query, ...(count ? { count: Number(count) } : {}) },
		);
		return c.json({ cancelled });
	});
	v2.post('/messages/:id/retry', async (c) => {
		await service.runNow(c.req.param('id'));
		return c.body(null, 200);
	});

	// logs
	v2.get('/events', async (c) => c.json(await service.events(new URL(c.req.url).searchParams)));

	// dead-letter queue
	v2.get('/dlq', async (c) => c.json(await service.listDlq(new URL(c.req.url).searchParams)));
	v2.delete('/dlq', async (c) => c.json(await service.deleteDlq(new URL(c.req.url).searchParams)));
	v2.post('/dlq/retry', async (c) => c.json(await service.retryDlq(new URL(c.req.url).searchParams)));
	v2.post('/dlq/retry/:id', async (c) =>
		c.json(await service.retryDlq(new URLSearchParams([['dlqIds', c.req.param('id')]]))),
	);
	v2.get('/dlq/:id', async (c) => c.json(await service.getDlq(c.req.param('id'))));
	v2.delete('/dlq/:id', async (c) => c.json(await service.deleteDlq(new URLSearchParams(), c.req.param('id'))));

	// queues
	v2.post('/queues', async (c) => {
		const request = await json<{ queueName?: unknown; parallelism?: unknown; paused?: unknown }>(c);
		await service.upsertQueue(
			String(request.queueName ?? ''),
			Number(request.parallelism ?? 1),
			request.paused === true,
		);
		return c.body(null, 200);
	});
	v2.get('/queues', async (c) => c.json(await service.listQueues()));
	v2.get('/queues/:name', async (c) => c.json(await service.getQueue(c.req.param('name'))));
	v2.delete('/queues/:name', async (c) => {
		await service.deleteQueue(c.req.param('name'));
		return c.body(null, 200);
	});
	for (const [action, paused] of [
		['pause', true],
		['resume', false],
	] as const) {
		v2.post(`/queues/:name/${action}`, async (c) => {
			await service.pauseQueue(c.req.param('name'), paused);
			return c.body(null, 200);
		});
	}

	// schedules
	v2.get('/schedules', async (c) => c.json(await service.listSchedules()));
	for (const [action, paused] of [
		['pause', true],
		['resume', false],
	] as const) {
		const handler = async (c: Context) => {
			await service.pauseSchedule(c.req.param('id') as string, paused);
			return c.body(null, 200);
		};
		v2.patch(`/schedules/:id/${action}`, handler);
	}
	v2.get('/schedules/:id', async (c) => c.json(await service.getSchedule(c.req.param('id'))));
	v2.delete('/schedules/:id', async (c) => {
		await service.deleteSchedule(c.req.param('id'));
		return c.body(null, 200);
	});
	v2.post('/schedules/*', async (c) => {
		const result = await service.createSchedule(rest(c, /^\/v2\/schedules\//), headers(c), await body(c), ip(c));
		return c.json(result, 201);
	});

	// url groups
	v2.post('/topics/:name/endpoints', async (c) => {
		const request = await json<{ endpoints?: { url?: unknown; name?: unknown }[] }>(c);
		await service.addEndpoints(c.req.param('name'), request.endpoints ?? []);
		return c.body(null, 200);
	});
	v2.delete('/topics/:name/endpoints', async (c) => {
		const request = await json<{ endpoints?: { url?: unknown; name?: unknown }[] }>(c);
		await service.removeEndpoints(c.req.param('name'), request.endpoints ?? []);
		return c.body(null, 200);
	});
	v2.get('/topics', async (c) => c.json(await service.listTopics()));
	v2.get('/topics/:name', async (c) => c.json(await service.getTopic(c.req.param('name'))));
	v2.delete('/topics/:name', async (c) => {
		await service.deleteTopic(c.req.param('name'));
		return c.body(null, 200);
	});

	// signing keys
	v2.get('/keys', async (c) => c.json(await service.signingKeys()));
	v2.post('/keys/rotate', async (c) => c.json(await service.rotateSigningKeys()));

	// workflows
	v2.delete('/workflows/runs/:id', async (c) => {
		await service.deleteWorkflowRun(c.req.param('id'), c.req.query('cancel') === 'true');
		return c.body(null, 200);
	});

	// flow control
	v2.get('/flowControl', async (c) => c.json(await service.flowControl()));
	v2.get('/flowControl/:key', async (c) => {
		const [info] = await service.flowControl(c.req.param('key'));
		return c.json(info ?? { flowControlKey: c.req.param('key'), waitListSize: 0, activeCount: 0 });
	});

	app.route('/v2', v2);
	return app;
}
