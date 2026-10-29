import { mkdir } from 'node:fs/promises';
import postgres from 'postgres';

/** The root connection or a transaction: whatever queries are run on. */
export type Sql = postgres.Sql | postgres.TransactionSql;
export type Fragment = postgres.PendingQuery<postgres.Row[]>;

export interface Database {
	sql: postgres.Sql;
	/** True for the embedded database, which is a single connection. */
	embedded: boolean;
	close(): Promise<void>;
}

/**
 * Connects to Postgres, or, with no URL, starts the embedded one: PGlite, Postgres compiled to WebAssembly, reached
 * over its socket server, so the same driver and the same SQL serve both. PGlite has one session, so the embedded
 * database gets one connection, and transactions never interleave.
 */
export async function connect(databaseUrl: string | null, dataDir: string, schema = 'public'): Promise<Database> {
	// timestamps are milliseconds in bigint columns, well inside what a number holds exactly
	const options = {
		onnotice: () => {},
		types: { bigint: { to: 20, from: [20], parse: (x: string) => Number(x), serialize: (x: number) => String(x) } },
	};

	if (databaseUrl) {
		if (schema !== 'public') {
			const setup = postgres(databaseUrl, { ...options, max: 1 });
			await setup`CREATE SCHEMA IF NOT EXISTS ${setup(schema)}`;
			await setup.end();
		}
		const sql = postgres(databaseUrl, { ...options, max: 10, connection: { search_path: schema } });
		await migrate(sql);
		return { sql, embedded: false, close: () => sql.end({ timeout: 5 }) };
	}

	const { PGlite } = await import('@electric-sql/pglite');
	const { PGLiteSocketServer } = await import('@electric-sql/pglite-socket');
	if (dataDir !== 'memory') await mkdir(dataDir, { recursive: true });
	const db = await PGlite.create(dataDir === 'memory' ? undefined : dataDir);
	const server = new PGLiteSocketServer({ db, host: '127.0.0.1', port: 0 });
	await server.start();
	const address = server.getServerConn();
	const sql = postgres(`postgres://postgres:postgres@${address}/postgres`, { ...options, max: 1 });
	await migrate(sql);
	return {
		sql,
		embedded: true,
		async close() {
			await sql.end({ timeout: 5 });
			await server.stop();
			await db.close();
		},
	};
}

const MIGRATIONS: string[] = [
	`
	CREATE TABLE postlater_settings (key text PRIMARY KEY, value text NOT NULL);

	CREATE TABLE postlater_messages (
		id text PRIMARY KEY,
		seq bigserial,
		kind text NOT NULL,
		url text NOT NULL,
		topic_name text,
		endpoint_name text,
		method text NOT NULL,
		headers jsonb NOT NULL,
		body bytea NOT NULL,
		max_retries integer NOT NULL,
		retry_delay text,
		timeout_ms integer NOT NULL,
		callback text,
		failure_callback text,
		callback_headers jsonb NOT NULL,
		failure_callback_headers jsonb NOT NULL,
		not_before bigint,
		created_at bigint NOT NULL,
		queue_name text,
		schedule_id text,
		caller_ip text,
		fc_key text,
		fc_parallelism integer,
		fc_rate integer,
		fc_period integer,
		labels text[] NOT NULL,
		state text NOT NULL,
		retried integer NOT NULL DEFAULT 0,
		next_delivery_at bigint NOT NULL,
		lease_until bigint
	);
	CREATE INDEX postlater_messages_due ON postlater_messages (next_delivery_at) WHERE state IN ('CREATED', 'RETRY');
	CREATE INDEX postlater_messages_queue ON postlater_messages (queue_name, seq) WHERE queue_name IS NOT NULL;
	CREATE INDEX postlater_messages_flow ON postlater_messages (fc_key) WHERE fc_key IS NOT NULL;
	CREATE INDEX postlater_messages_active ON postlater_messages (lease_until) WHERE state = 'ACTIVE';

	CREATE TABLE postlater_dlq (
		dlq_id text PRIMARY KEY,
		seq bigserial,
		message_id text NOT NULL,
		message jsonb NOT NULL,
		source jsonb NOT NULL,
		body bytea NOT NULL,
		response_status integer,
		response_headers jsonb,
		response_body bytea,
		created_at bigint NOT NULL
	);
	CREATE INDEX postlater_dlq_seq ON postlater_dlq (seq);

	CREATE TABLE postlater_events (
		id bigserial PRIMARY KEY,
		time bigint NOT NULL,
		message_id text NOT NULL,
		state text NOT NULL,
		url text NOT NULL,
		topic_name text,
		endpoint_name text,
		queue_name text,
		schedule_id text,
		caller_ip text,
		fc_key text,
		labels text[] NOT NULL,
		error text,
		next_delivery_time bigint,
		header jsonb,
		body text,
		response_status integer
	);
	CREATE INDEX postlater_events_message ON postlater_events (message_id);
	CREATE INDEX postlater_events_time ON postlater_events (time);

	CREATE TABLE postlater_queues (
		name text PRIMARY KEY,
		parallelism integer NOT NULL,
		paused boolean NOT NULL DEFAULT false,
		created_at bigint NOT NULL,
		updated_at bigint NOT NULL
	);

	CREATE TABLE postlater_schedules (
		id text PRIMARY KEY,
		cron text NOT NULL,
		destination text NOT NULL,
		headers jsonb NOT NULL,
		body bytea NOT NULL,
		caller_ip text,
		paused boolean NOT NULL DEFAULT false,
		created_at bigint NOT NULL,
		next_run_at bigint NOT NULL,
		last_run_at bigint,
		last_message_ids text[]
	);
	CREATE INDEX postlater_schedules_due ON postlater_schedules (next_run_at) WHERE NOT paused;

	CREATE TABLE postlater_topics (name text PRIMARY KEY, created_at bigint NOT NULL, updated_at bigint NOT NULL);
	CREATE TABLE postlater_topic_endpoints (
		topic text NOT NULL REFERENCES postlater_topics (name) ON DELETE CASCADE,
		position serial,
		name text,
		url text NOT NULL,
		PRIMARY KEY (topic, url)
	);

	CREATE TABLE postlater_deduplication (key text PRIMARY KEY, response jsonb NOT NULL, expires_at bigint NOT NULL);

	CREATE TABLE postlater_flow_activations (key text NOT NULL, at bigint NOT NULL);
	CREATE INDEX postlater_flow_activations_key ON postlater_flow_activations (key, at);

	CREATE TABLE postlater_workflow_steps (
		run_id text NOT NULL,
		seq bigserial,
		message_id text NOT NULL,
		body bytea NOT NULL,
		created_at bigint NOT NULL
	);
	CREATE INDEX postlater_workflow_steps_run ON postlater_workflow_steps (run_id, seq);
	CREATE INDEX postlater_workflow_steps_message ON postlater_workflow_steps (message_id);
	`,
];

export async function migrate(sql: postgres.Sql): Promise<void> {
	await sql`CREATE TABLE IF NOT EXISTS postlater_migrations (version integer PRIMARY KEY, applied_at bigint NOT NULL)`;
	await sql.begin(async (tx) => {
		// one server migrates at a time; the others wait, then find nothing left to do
		await tx`SELECT pg_advisory_xact_lock(7203214)`;
		const [row] = await tx<{ version: number | null }[]>`SELECT max(version) AS version FROM postlater_migrations`;
		const applied = row?.version ?? 0;
		for (let version = applied + 1; version <= MIGRATIONS.length; version++) {
			await tx.unsafe(MIGRATIONS[version - 1] as string);
			await tx`INSERT INTO postlater_migrations (version, applied_at) VALUES (${version}, ${Date.now()})`;
		}
	});
}
