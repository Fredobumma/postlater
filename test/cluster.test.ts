import { randomBytes } from 'node:crypto';
import { Client } from '@upstash/qstash';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { loadConfig } from '../src/config.js';
import { type Postlater, start } from '../src/server.js';
import { Destination, TOKEN, waitFor } from './support/harness.js';

// two servers on one database; the embedded database is one server's alone, so this needs a real Postgres
const url = process.env.TEST_DATABASE_URL;

describe.skipIf(!url)('several servers on one database', () => {
	const servers: Postlater[] = [];
	let dest: Destination;

	beforeAll(async () => {
		const schema = `test_cluster_${randomBytes(6).toString('hex')}`;
		for (let i = 0; i < 2; i++) {
			const config = loadConfig({
				QSTASH_TOKEN: TOKEN,
				DATABASE_URL: url as string,
				DATABASE_SCHEMA: schema,
				PORT: '0',
				HOST: '127.0.0.1',
				POLL_INTERVAL_MS: '20',
				WORKER_CONCURRENCY: '8',
			});
			servers.push(await start(config, { info: () => {}, error: (m) => console.error(m) }));
		}
		dest = await new Destination().start();
	});

	afterAll(async () => {
		for (const server of servers) await server.close();
		await dest?.close();
	});

	it('delivers each message once, whichever server takes it', async () => {
		dest.on('/shared', () => ({ delayMs: 20 }));
		const clients = servers.map(
			(s) => new Client({ baseUrl: s.url, token: TOKEN, retry: false, enableTelemetry: false }),
		);
		const ids: string[] = [];
		for (let n = 0; n < 60; n++) {
			const client = clients[n % 2] as Client;
			ids.push((await client.publishJSON({ url: `${dest.url}/shared`, body: { n } })).messageId);
		}
		await waitFor(() => dest.to('/shared').length >= 60, 20_000, 'all deliveries');
		// a moment more, for any duplicate to show itself
		await new Promise((r) => setTimeout(r, 500));
		const delivered = dest.to('/shared').map((r) => r.headers['upstash-message-id']);
		expect(delivered).toHaveLength(60);
		expect(new Set(delivered)).toEqual(new Set(ids));
	});

	it('keeps a queue to its parallelism across servers', async () => {
		const clients = servers.map(
			(s) => new Client({ baseUrl: s.url, token: TOKEN, retry: false, enableTelemetry: false }),
		);
		await clients[0]?.queue({ queueName: 'cluster-serial' }).upsert({ parallelism: 1 });
		dest.on('/cluster-serial', () => ({ delayMs: 50 }));
		for (let n = 0; n < 12; n++) {
			await clients[n % 2]
				?.queue({ queueName: 'cluster-serial' })
				.enqueueJSON({ url: `${dest.url}/cluster-serial`, body: { n } });
		}
		await waitFor(() => dest.to('/cluster-serial').length === 12, 20_000);
		expect(dest.peak.get('/cluster-serial')).toBe(1);
		expect(dest.to('/cluster-serial').map((r) => JSON.parse(r.body).n)).toEqual([...Array(12).keys()]);
	});
});
