import { randomBytes } from 'node:crypto';
import { createServer, type IncomingHttpHeaders, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { Client, Receiver } from '@upstash/qstash';
import { loadConfig } from '../../src/config.js';
import { type Postlater, start } from '../../src/server.js';

export const TOKEN = 'test-token-0123456789abcdef';

const quiet = { info: () => {}, error: (message: string) => console.error(message) };

export interface Harness {
	postlater: Postlater;
	client: Client;
	receiver: Receiver;
	close(): Promise<void>;
}

export async function startPostlater(env: Record<string, string> = {}): Promise<Harness> {
	// TEST_DATABASE_URL runs the suite on a real Postgres, each server in a schema of its own
	const postgresUrl = process.env.TEST_DATABASE_URL;
	const config = loadConfig({
		QSTASH_TOKEN: TOKEN,
		DATA_DIR: 'memory',
		...(postgresUrl ? { DATABASE_URL: postgresUrl, DATABASE_SCHEMA: `test_${randomBytes(6).toString('hex')}` } : {}),
		PORT: '0',
		HOST: '127.0.0.1',
		POLL_INTERVAL_MS: '50',
		...env,
	});
	const postlater = await start(config, quiet);
	const keys = await postlater.service.signingKeys();
	return {
		postlater,
		client: new Client({ baseUrl: postlater.url, token: TOKEN, retry: false, enableTelemetry: false }),
		receiver: new Receiver({ currentSigningKey: keys.current, nextSigningKey: keys.next }),
		close: () => postlater.close(),
	};
}

export interface Received {
	method: string;
	path: string;
	headers: IncomingHttpHeaders;
	body: string;
	at: number;
}

export interface Reply {
	status?: number;
	headers?: Record<string, string>;
	body?: string;
	delayMs?: number;
}

/** A destination that records every request and answers as told. */
export class Destination {
	readonly received: Received[] = [];
	private server: Server | undefined;
	private handlers = new Map<string, (request: Received, count: number) => Reply>();
	url = '';
	private busy = new Map<string, number>();
	/** The most requests to one path that were being handled at once. */
	readonly peak = new Map<string, number>();

	async start(): Promise<this> {
		const server = createServer((req, res) => {
			const chunks: Buffer[] = [];
			req.on('data', (chunk: Buffer) => chunks.push(chunk));
			req.on('end', async () => {
				const request: Received = {
					method: req.method ?? '',
					path: req.url ?? '',
					headers: req.headers,
					body: Buffer.concat(chunks).toString('utf8'),
					at: Date.now(),
				};
				this.received.push(request);
				const path = request.path.split('?')[0] as string;
				const busy = (this.busy.get(path) ?? 0) + 1;
				this.busy.set(path, busy);
				this.peak.set(path, Math.max(this.peak.get(path) ?? 0, busy));
				const count = this.received.filter((r) => r.path.split('?')[0] === path).length;
				const reply = this.handlers.get(path)?.(request, count) ?? {};
				if (reply.delayMs) await new Promise((r) => setTimeout(r, reply.delayMs));
				this.busy.set(path, (this.busy.get(path) ?? 1) - 1);
				res.writeHead(reply.status ?? 200, reply.headers ?? {});
				res.end(reply.body ?? 'ok');
			});
		});
		this.server = server;
		await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
		this.url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
		return this;
	}

	on(path: string, handler: (request: Received, count: number) => Reply): this {
		this.handlers.set(path, handler);
		return this;
	}

	to(path: string): Received[] {
		return this.received.filter((r) => r.path.split('?')[0] === path);
	}

	async close(): Promise<void> {
		this.server?.closeAllConnections();
		await new Promise<void>((resolve) => this.server?.close(() => resolve()));
	}
}

type Truthy<T> = Exclude<T, false | null | undefined | 0 | ''>;

export async function waitFor<T>(
	check: () => T | Promise<T>,
	timeoutMs = 10_000,
	what = 'condition',
): Promise<Truthy<T>> {
	const end = Date.now() + timeoutMs;
	for (;;) {
		const value = await check();
		if (value) return value as Truthy<T>;
		if (Date.now() > end) throw new Error(`timed out waiting for ${what}`);
		await new Promise((r) => setTimeout(r, 25));
	}
}

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
