import { parseDuration } from './duration.js';

export interface Config {
	host: string;
	port: number;
	/** Bearer tokens accepted by the API: `QSTASH_TOKEN`, comma-separated for more than one. */
	tokens: string[];
	/** A Postgres URL, or null for the embedded database. */
	databaseUrl: string | null;
	/** The Postgres schema for Postlater's tables, so it can share a database with an app. */
	databaseSchema: string;
	/** Where the embedded database keeps its files, or `memory` for none. */
	dataDir: string;
	currentSigningKey: string | null;
	nextSigningKey: string | null;
	workerConcurrency: number;
	pollIntervalMs: number;
	defaultRetries: number;
	maxRetries: number;
	defaultTimeoutMs: number;
	maxTimeoutMs: number;
	deduplicationWindowMs: number;
	eventsRetentionMs: number;
	dlqRetentionMs: number;
	/** Take the caller's address from X-Forwarded-For, when behind a proxy. */
	trustProxy: boolean;
	/** Serve `/metrics` without a token. */
	publicMetrics: boolean;
	maxBodyBytes: number;
}

export class ConfigError extends Error {}

function durationMs(env: NodeJS.ProcessEnv, name: string, fallback: string): number {
	const raw = env[name] ?? fallback;
	const ms = parseDuration(raw);
	if (ms === undefined) throw new ConfigError(`${name} is not a duration: ${raw}`);
	return ms;
}

function int(env: NodeJS.ProcessEnv, name: string, fallback: number, min: number): number {
	const raw = env[name];
	if (raw === undefined || raw === '') return fallback;
	if (!/^\d+$/.test(raw) || Number(raw) < min)
		throw new ConfigError(`${name} must be a whole number of at least ${min}`);
	return Number(raw);
}

function schema(value: string | undefined): string {
	const name = value || 'public';
	if (!/^[a-z_][a-z0-9_]{0,62}$/.test(name)) {
		throw new ConfigError('DATABASE_SCHEMA must be lower-case letters, digits and underscores');
	}
	return name;
}

const truthy = (value: string | undefined) => value === '1' || value === 'true';

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
	const tokens = (env.QSTASH_TOKEN ?? '')
		.split(',')
		.map((t) => t.trim())
		.filter(Boolean);
	if (tokens.length === 0) {
		throw new ConfigError(
			'QSTASH_TOKEN is required: the token your apps send, which they already read from QSTASH_TOKEN',
		);
	}
	if (tokens.some((t) => t.length < 16)) throw new ConfigError('each QSTASH_TOKEN must be at least 16 characters');

	return {
		host: env.HOST ?? '0.0.0.0',
		port: int(env, 'PORT', 8080, 0),
		tokens,
		databaseUrl: env.DATABASE_URL || null,
		databaseSchema: schema(env.DATABASE_SCHEMA),
		dataDir: env.DATA_DIR ?? './data',
		currentSigningKey: env.QSTASH_CURRENT_SIGNING_KEY || null,
		nextSigningKey: env.QSTASH_NEXT_SIGNING_KEY || null,
		workerConcurrency: int(env, 'WORKER_CONCURRENCY', 32, 1),
		pollIntervalMs: int(env, 'POLL_INTERVAL_MS', 500, 10),
		defaultRetries: int(env, 'DEFAULT_RETRIES', 3, 0),
		maxRetries: int(env, 'MAX_RETRIES', 20, 0),
		defaultTimeoutMs: durationMs(env, 'DEFAULT_TIMEOUT', '5m'),
		maxTimeoutMs: durationMs(env, 'MAX_TIMEOUT', '2h'),
		deduplicationWindowMs: durationMs(env, 'DEDUPLICATION_WINDOW', '10m'),
		eventsRetentionMs: durationMs(env, 'EVENTS_RETENTION', '7d'),
		dlqRetentionMs: durationMs(env, 'DLQ_RETENTION', '7d'),
		trustProxy: truthy(env.TRUST_PROXY),
		publicMetrics: truthy(env.PUBLIC_METRICS),
		maxBodyBytes: int(env, 'MAX_BODY_BYTES', 10 * 1024 * 1024, 1),
	};
}
