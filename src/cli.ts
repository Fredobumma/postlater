#!/usr/bin/env node
import { ConfigError, loadConfig } from './config.js';
import { start } from './server.js';

try {
	const postlater = await start(loadConfig());
	let stopping = false;
	const stop = async (signal: string) => {
		if (stopping) return;
		stopping = true;
		console.log(`${new Date().toISOString()} ${signal}: finishing deliveries under way, then stopping`);
		await postlater.close();
		process.exit(0);
	};
	process.on('SIGINT', () => void stop('SIGINT'));
	process.on('SIGTERM', () => void stop('SIGTERM'));
} catch (error) {
	if (error instanceof ConfigError) {
		console.error(`postlater: ${error.message}`);
		process.exit(2);
	}
	throw error;
}
