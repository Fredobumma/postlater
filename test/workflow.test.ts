import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { Client as QStashClient } from '@upstash/qstash';
import { serve, Client as WorkflowClient } from '@upstash/workflow';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { Destination, type Harness, startPostlater, TOKEN, waitFor } from './support/harness.js';

let h: Harness;
let server: Server;
let api: Destination;
let base = '';
const done: Record<string, unknown> = {};
const ran: string[] = [];

beforeAll(async () => {
	h = await startPostlater();
	api = await new Destination().start();
	api.on('/quote', (req) => ({
		status: 200,
		headers: { 'content-type': 'application/json' },
		body: JSON.stringify({ quote: 'hi', got: req.body }),
	}));
	api.on('/down', () => ({ status: 502, body: 'bad gateway' }));
	const qstashClient = new QStashClient({ baseUrl: h.postlater.url, token: TOKEN, enableTelemetry: false });

	const { handler } = serve<{ name: string }>(
		async (context) => {
			const greeting = await context.run('greet', () => {
				ran.push('greet');
				return `hello ${context.requestPayload.name}`;
			});
			await context.sleep('nap', 1);
			const [a, b] = await Promise.all([
				context.run('left', () => {
					ran.push('left');
					return 1;
				}),
				context.run('right', () => {
					ran.push('right');
					return 2;
				}),
			]);
			const quote = await context.call<{ quote: string; got: string }>('fetch-quote', {
				url: `${api.url}/quote`,
				method: 'POST',
				body: JSON.stringify({ want: 'quote' }),
			});
			const down = await context.call('fetch-down', { url: `${api.url}/down`, method: 'GET', retries: 0 });
			await context.run('finish', () => {
				ran.push('finish');
				done[context.workflowRunId] = {
					greeting,
					sum: a + b,
					quote: quote.status === 200 ? quote.body.quote : null,
					sent: quote.status === 200 ? JSON.parse(quote.body.got) : null,
					down: down.status,
				};
			});
		},
		{ qstashClient, receiver: h.receiver },
	);

	server = createServer(async (req, res) => {
		const chunks: Buffer[] = [];
		for await (const chunk of req) chunks.push(chunk as Buffer);
		const headers = new Headers();
		for (const [name, value] of Object.entries(req.headers)) {
			if (typeof value === 'string') headers.set(name, value);
		}
		const response = await handler(
			new Request(`${base}${req.url}`, {
				method: req.method,
				headers,
				body: req.method === 'GET' || req.method === 'HEAD' ? undefined : Buffer.concat(chunks),
			}),
		);
		res.writeHead(response.status, Object.fromEntries(response.headers.entries()));
		res.end(Buffer.from(await response.arrayBuffer()));
	});
	await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
	base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});

afterAll(async () => {
	await h?.close();
	await api?.close();
	server?.closeAllConnections();
	await new Promise<void>((resolve) => server?.close(() => resolve()));
});

describe('@upstash/workflow', () => {
	it('runs steps, a sleep, parallel steps and calls to other APIs to the end', async () => {
		const client = new WorkflowClient({ baseUrl: h.postlater.url, token: TOKEN });
		const { workflowRunId } = await client.trigger({ url: `${base}/workflow`, body: { name: 'postlater' } });

		const result = await waitFor(() => done[workflowRunId], 20_000, 'the workflow to finish');
		expect(result).toEqual({ greeting: 'hello postlater', sum: 3, quote: 'hi', sent: { want: 'quote' }, down: 502 });
		// each step body runs once, even though the endpoint is called again for every step
		expect(ran.filter((s) => s === 'greet')).toHaveLength(1);
		expect(ran.filter((s) => s === 'finish')).toHaveLength(1);
		expect(ran).toEqual(expect.arrayContaining(['left', 'right']));
	});
});
