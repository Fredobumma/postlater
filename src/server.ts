import { type ServerType, serve } from '@hono/node-server';
import { createApp } from './api.js';
import type { Config } from './config.js';
import { connect, type Database } from './db.js';
import { Service } from './service.js';
import { type Logger, Worker } from './worker.js';

export interface Postlater {
	url: string;
	service: Service;
	worker: Worker;
	db: Database;
	close(): Promise<void>;
}

const consoleLogger: Logger = {
	info: (message) => console.log(`${new Date().toISOString()} ${message}`),
	error: (message) => console.error(`${new Date().toISOString()} ${message}`),
};

/** Starts the API and the worker. With `port: 0`, a free port is chosen; `url` says which. */
export async function start(config: Config, log: Logger = consoleLogger): Promise<Postlater> {
	const db = await connect(config.databaseUrl, config.dataDir, config.databaseSchema);
	const service = new Service(db.sql, config);
	await service.signingKeys();
	const worker = new Worker(service, db.sql, config, log);

	const app = createApp(service, config);
	const server = await new Promise<{ server: ServerType; port: number }>((resolve) => {
		const s = serve({ fetch: app.fetch, hostname: config.host, port: config.port }, (info) =>
			resolve({ server: s, port: info.port }),
		);
	});
	worker.start();

	const host = config.host === '0.0.0.0' || config.host === '::' ? '127.0.0.1' : config.host;
	const url = `http://${host}:${server.port}`;
	log.info(`postlater listening on ${url}, ${db.embedded ? `embedded database in ${config.dataDir}` : 'Postgres'}`);

	return {
		url,
		service,
		worker,
		db,
		async close() {
			await new Promise<void>((resolve) => server.server.close(() => resolve()));
			await worker.stop();
			await db.close();
		},
	};
}
