import { createHash } from 'node:crypto';
import { Cron } from 'croner';
import type postgres from 'postgres';
import type { Config } from './config.js';
import type { Fragment, Sql } from './db.js';
import { messageId, scheduleId as newScheduleId, signingKey } from './ids.js';
import {
	bodyFields,
	type EventRow,
	formatEvent,
	formatMessage,
	type MessageKind,
	type MessageRow,
	recordEvent,
} from './messages.js';
import {
	checkUrl,
	type HeaderMap,
	PublishError,
	type PublishLimits,
	type PublishOptions,
	parsePublishHeaders,
} from './publish.js';

export class NotFoundError extends PublishError {
	constructor(message: string) {
		super(message, 404);
	}
}

export interface PublishContext {
	callerIp: string | null;
	queueName?: string | null;
	scheduleId?: string | null;
	kind?: MessageKind;
}

export interface PublishedMessage {
	messageId: string;
	url: string;
	deduplicated?: boolean;
}

export type PublishResult = PublishedMessage | PublishedMessage[];

export interface Target {
	url: string;
	topicName: string | null;
	endpointName: string | null;
}

/** Splits `CRON_TZ=Europe/Berlin 0 9 * * *` into its timezone and pattern. Without CRON_TZ, crons run in UTC, as on QStash. */
export function parseCron(expression: string): { pattern: string; timezone: string } {
	const match = /^CRON_TZ=(\S+)\s+(.+)$/.exec(expression.trim());
	return match
		? { pattern: (match[2] as string).trim(), timezone: match[1] as string }
		: { pattern: expression.trim(), timezone: 'UTC' };
}

export function nextCronRun(expression: string, after: number): number | null {
	const { pattern, timezone } = parseCron(expression);
	const next = new Cron(pattern, { timezone, paused: true }).nextRun(new Date(after));
	return next ? next.getTime() : null;
}

const DESTINATION_IS_URL = /^https?:\/\//i;

export class Service {
	/** Called after messages are created, so the worker looks at once rather than on its next poll. */
	onPublish: () => void = () => {};

	constructor(
		readonly sql: postgres.Sql,
		readonly config: Config,
	) {}

	get limits(): PublishLimits {
		return {
			defaultRetries: this.config.defaultRetries,
			maxRetries: this.config.maxRetries,
			defaultTimeoutMs: this.config.defaultTimeoutMs,
			maxTimeoutMs: this.config.maxTimeoutMs,
		};
	}

	// ---- publishing -------------------------------------------------------------------------------------------

	async resolveTargets(sql: Sql, destination: string): Promise<{ targets: Target[]; topic: boolean }> {
		if (DESTINATION_IS_URL.test(destination)) {
			return {
				targets: [{ url: checkUrl(destination, 'destination'), topicName: null, endpointName: null }],
				topic: false,
			};
		}
		if (destination.startsWith('api/')) {
			throw new PublishError(`API destinations such as ${destination} are not supported; publish to a URL instead`);
		}
		const endpoints = await sql<{ name: string | null; url: string }[]>`
			SELECT e.name, e.url FROM postlater_topic_endpoints e WHERE e.topic = ${destination} ORDER BY e.position
		`;
		if (endpoints.length === 0) {
			const [topic] = await sql`SELECT 1 FROM postlater_topics WHERE name = ${destination}`;
			throw topic
				? new PublishError(`url group ${destination} has no endpoints`)
				: new NotFoundError(`destination is neither a URL nor an existing url group: ${destination}`);
		}
		return {
			targets: endpoints.map((e) => ({ url: e.url, topicName: destination, endpointName: e.name })),
			topic: true,
		};
	}

	/**
	 * Publishes to a URL or to every endpoint of a URL group: the core of publish, enqueue, batch, schedules and
	 * callbacks. Runs inside the caller's transaction.
	 */
	async publishIn(
		sql: Sql,
		destination: string,
		headers: HeaderMap,
		body: Uint8Array,
		context: PublishContext,
		parsed?: PublishOptions,
	): Promise<PublishResult> {
		const options = parsed ?? parsePublishHeaders(headers, this.limits);
		const queueName = context.queueName ?? null;
		const { targets, topic } = await this.resolveTargets(sql, destination);
		const now = Date.now();

		const dedupKey = options.deduplicationId
			? `id:${destination}:${options.deduplicationId}`
			: options.contentBasedDeduplication
				? `content:${createHash('sha256')
						.update(JSON.stringify([destination, queueName, options.method, options.forward]))
						.update(body)
						.digest('hex')}`
				: null;
		if (dedupKey) {
			const [existing] = await sql<{ response: PublishResult }[]>`
				SELECT response FROM postlater_deduplication WHERE key = ${dedupKey} AND expires_at > ${now}
			`;
			if (existing) {
				const mark = (m: PublishedMessage) => ({ ...m, deduplicated: true });
				return Array.isArray(existing.response) ? existing.response.map(mark) : mark(existing.response);
			}
		}

		if (queueName) {
			await sql`
				INSERT INTO postlater_queues (name, parallelism, paused, created_at, updated_at)
				VALUES (${queueName}, 1, false, ${now}, ${now}) ON CONFLICT (name) DO NOTHING
			`;
		}

		const nextDeliveryAt = Math.max(now + options.delayMs, options.notBefore ?? 0);
		const published: PublishedMessage[] = [];
		for (const target of targets) {
			const row = await this.insertMessage(sql, target, options, body, {
				...context,
				queueName,
				createdAt: now,
				nextDeliveryAt,
			});
			await recordEvent(sql, row, 'CREATED', { withPayload: true });
			await recordWorkflowStep(sql, row);
			published.push({ messageId: row.id, url: row.url });
		}

		const result: PublishResult = topic ? published : (published[0] as PublishedMessage);
		if (dedupKey) {
			await sql`
				INSERT INTO postlater_deduplication (key, response, expires_at)
				VALUES (${dedupKey}, ${sql.json(result as never)}, ${now + this.config.deduplicationWindowMs})
				ON CONFLICT (key) DO UPDATE SET response = excluded.response, expires_at = excluded.expires_at
			`;
		}
		return result;
	}

	async insertMessage(
		sql: Sql,
		target: Target,
		options: PublishOptions,
		body: Uint8Array,
		context: PublishContext & { createdAt: number; nextDeliveryAt: number },
	): Promise<MessageRow> {
		const flow = options.flowControl;
		const [row] = await sql<MessageRow[]>`
			INSERT INTO postlater_messages (id, kind, url, topic_name, endpoint_name, method, headers, body, max_retries,
				retry_delay, timeout_ms, callback, failure_callback, callback_headers, failure_callback_headers, not_before,
				created_at, queue_name, schedule_id, caller_ip, fc_key, fc_parallelism, fc_rate, fc_period, labels, state,
				next_delivery_at)
			VALUES (${messageId()}, ${context.kind ?? 'message'}, ${target.url}, ${target.topicName}, ${target.endpointName},
				${options.method}, ${sql.json(options.forward)}, ${Buffer.from(body)}, ${options.retries},
				${options.retryDelay}, ${options.timeoutMs}, ${options.callback}, ${options.failureCallback},
				${sql.json(options.callbackHeaders)}, ${sql.json(options.failureCallbackHeaders)}, ${options.notBefore},
				${context.createdAt}, ${context.queueName ?? null}, ${context.scheduleId ?? null}, ${context.callerIp},
				${flow?.key ?? null}, ${flow?.parallelism ?? null}, ${flow?.rate ?? null}, ${flow ? flow.periodSeconds : null},
				${options.labels}, 'CREATED', ${context.nextDeliveryAt})
			RETURNING *
		`;
		return row as MessageRow;
	}

	async publish(
		destination: string,
		headers: HeaderMap,
		body: Uint8Array,
		context: PublishContext,
	): Promise<PublishResult> {
		const options = parsePublishHeaders(headers, this.limits);
		if (options.cron) throw new PublishError('Upstash-Cron belongs on /v2/schedules, not /v2/publish');
		const result = await this.sql.begin((tx) => this.publishIn(tx, destination, headers, body, context, options));
		this.onPublish();
		return result;
	}

	async batch(
		items: { destination: string; headers: HeaderMap; body: Uint8Array; queue: string | null }[],
		callerIp: string | null,
	): Promise<PublishResult[]> {
		// checked before anything is written, so a bad item leaves nothing half-published
		const parsed = items.map((item) => parsePublishHeaders(item.headers, this.limits));
		const results = await this.sql.begin(async (tx) => {
			const out: PublishResult[] = [];
			for (const [i, item] of items.entries()) {
				out.push(
					await this.publishIn(
						tx,
						item.destination,
						item.headers,
						item.body,
						{ callerIp, queueName: item.queue },
						parsed[i],
					),
				);
			}
			return out;
		});
		this.onPublish();
		return results;
	}

	// ---- messages ---------------------------------------------------------------------------------------------

	async getMessage(id: string): Promise<Record<string, unknown>> {
		const [row] = await this.sql<MessageRow[]>`SELECT * FROM postlater_messages WHERE id = ${id}`;
		if (!row) throw new NotFoundError(`message ${id} not found`);
		return formatMessage(row);
	}

	/** Cancels messages that are waiting; one already being delivered finishes its attempt but is not retried. */
	async cancelMessages(where: MessageFilter): Promise<number> {
		return await this.sql.begin(async (tx) => {
			const rows = await tx<MessageRow[]>`
				DELETE FROM postlater_messages
				WHERE id IN (
					SELECT id FROM postlater_messages WHERE ${messageFilterSql(tx, where)}
					ORDER BY seq LIMIT ${where.count ?? 1_000_000} FOR UPDATE SKIP LOCKED
				)
				RETURNING *
			`;
			for (const row of rows) await recordEvent(tx, row, 'CANCELED');
			return rows.length;
		});
	}

	async runNow(id: string): Promise<void> {
		const rows = await this.sql`
			UPDATE postlater_messages SET next_delivery_at = ${Date.now()}
			WHERE id = ${id} AND state IN ('CREATED', 'RETRY') RETURNING id
		`;
		if (rows.length === 0) throw new NotFoundError(`message ${id} not found, or already being delivered`);
		this.onPublish();
	}

	// ---- logs -------------------------------------------------------------------------------------------------

	async events(query: URLSearchParams): Promise<{ cursor?: string; events: Record<string, unknown>[] }> {
		const count = Math.min(Number(query.get('count') ?? 100) || 100, 1000);
		const cursor = query.get('cursor');
		const sql = this.sql;
		const filters = [
			cursor ? sql`id < ${Number(cursor)}` : sql`true`,
			listFilter(sql, 'message_id', [...query.getAll('messageId'), ...query.getAll('messageIds')]),
			listFilter(sql, 'state', query.getAll('state')),
			listFilter(sql, 'url', query.getAll('url')),
			listFilter(sql, 'topic_name', query.getAll('topicName')),
			listFilter(sql, 'schedule_id', query.getAll('scheduleId')),
			listFilter(sql, 'queue_name', query.getAll('queueName')),
			listFilter(sql, 'caller_ip', query.getAll('callerIp')),
			listFilter(sql, 'fc_key', query.getAll('flowControlKey')),
			labelFilter(sql, query.getAll('label')),
			dateFilter(sql, 'time', query.get('fromDate'), query.get('toDate')),
		];
		const rows = await sql<EventRow[]>`
			SELECT * FROM postlater_events WHERE ${and(sql, filters)} ORDER BY id DESC LIMIT ${count + 1}
		`;
		const page = rows.slice(0, count);
		return {
			...(rows.length > count ? { cursor: String((page[page.length - 1] as EventRow).id) } : {}),
			events: page.map(formatEvent),
		};
	}

	// ---- queues -----------------------------------------------------------------------------------------------

	async upsertQueue(name: string, parallelism: number, paused: boolean): Promise<void> {
		if (!name) throw new PublishError('queueName is required');
		if (!Number.isInteger(parallelism) || parallelism < 1) throw new PublishError('parallelism must be at least 1');
		const now = Date.now();
		await this.sql`
			INSERT INTO postlater_queues (name, parallelism, paused, created_at, updated_at)
			VALUES (${name}, ${parallelism}, ${paused}, ${now}, ${now})
			ON CONFLICT (name) DO UPDATE SET parallelism = excluded.parallelism, paused = excluded.paused, updated_at = ${now}
		`;
		this.onPublish();
	}

	async listQueues(name?: string): Promise<Record<string, unknown>[]> {
		const rows = await this.sql<
			{ name: string; parallelism: number; paused: boolean; created_at: number; updated_at: number; lag: number }[]
		>`
			SELECT q.*, (SELECT count(*)::int FROM postlater_messages m WHERE m.queue_name = q.name) AS lag
			FROM postlater_queues q ${name === undefined ? this.sql`` : this.sql`WHERE q.name = ${name}`}
			ORDER BY q.created_at
		`;
		return rows.map((q) => ({
			createdAt: q.created_at,
			updatedAt: q.updated_at,
			name: q.name,
			parallelism: q.parallelism,
			lag: q.lag,
			paused: q.paused,
		}));
	}

	async getQueue(name: string): Promise<Record<string, unknown>> {
		const [queue] = await this.listQueues(name);
		if (!queue) throw new NotFoundError(`queue ${name} not found`);
		return queue;
	}

	/** Deletes a queue and cancels the messages still waiting in it. */
	async deleteQueue(name: string): Promise<void> {
		await this.sql.begin(async (tx) => {
			const deleted = await tx`DELETE FROM postlater_queues WHERE name = ${name} RETURNING name`;
			if (deleted.length === 0) throw new NotFoundError(`queue ${name} not found`);
			const rows = await tx<MessageRow[]>`
				DELETE FROM postlater_messages WHERE queue_name = ${name} AND state <> 'ACTIVE' RETURNING *
			`;
			for (const row of rows) await recordEvent(tx, row, 'CANCELED');
		});
	}

	async pauseQueue(name: string, paused: boolean): Promise<void> {
		const rows = await this.sql`
			UPDATE postlater_queues SET paused = ${paused}, updated_at = ${Date.now()} WHERE name = ${name} RETURNING name
		`;
		if (rows.length === 0) throw new NotFoundError(`queue ${name} not found`);
		if (!paused) this.onPublish();
	}

	// ---- schedules --------------------------------------------------------------------------------------------

	async createSchedule(
		destination: string,
		headers: HeaderMap,
		body: Uint8Array,
		callerIp: string | null,
	): Promise<{ scheduleId: string }> {
		const options = parsePublishHeaders(headers, this.limits);
		if (!options.cron) throw new PublishError('Upstash-Cron is required');
		let next: number | null;
		try {
			next = nextCronRun(options.cron, Date.now());
		} catch (error) {
			throw new PublishError(`invalid Upstash-Cron: ${(error as Error).message}`);
		}
		if (next === null) throw new PublishError(`Upstash-Cron never runs: ${options.cron}`);

		// checked now, so a mistake shows at creation, not at the first run
		await this.resolveTargets(this.sql, destination);

		const id = options.scheduleId ?? newScheduleId();
		const stored: HeaderMap = {};
		for (const [name, values] of Object.entries(headers)) {
			if (name === 'upstash-cron' || name === 'upstash-schedule-id' || name === 'authorization') continue;
			if (name.startsWith('upstash-telemetry-')) continue;
			stored[name] = values;
		}
		const now = Date.now();
		await this.sql`
			INSERT INTO postlater_schedules (id, cron, destination, headers, body, caller_ip, paused, created_at, next_run_at)
			VALUES (${id}, ${options.cron}, ${destination}, ${this.sql.json(stored)}, ${Buffer.from(body)}, ${callerIp}, false,
				${now}, ${next})
			ON CONFLICT (id) DO UPDATE SET cron = excluded.cron, destination = excluded.destination,
				headers = excluded.headers, body = excluded.body, caller_ip = excluded.caller_ip, next_run_at = excluded.next_run_at
		`;
		return { scheduleId: id };
	}

	formatSchedule(row: ScheduleRow): Record<string, unknown> {
		const options = parsePublishHeaders(row.headers, this.limits);
		const flow = options.flowControl;
		return {
			scheduleId: row.id,
			cron: row.cron,
			createdAt: row.created_at,
			destination: row.destination,
			method: options.method,
			header: options.forward,
			...bodyFields(row.body),
			retries: options.retries,
			...(options.delayMs ? { delay: Math.round(options.delayMs / 1000) } : {}),
			...(options.callback ? { callback: options.callback } : {}),
			...(options.failureCallback ? { failureCallback: options.failureCallback } : {}),
			...(row.caller_ip ? { callerIp: row.caller_ip } : {}),
			isPaused: row.paused,
			...(options.queueName ? { queueName: options.queueName } : {}),
			...(flow
				? {
						flowControlKey: flow.key,
						...(flow.parallelism ? { parallelism: flow.parallelism } : {}),
						...(flow.rate ? { rate: flow.rate } : {}),
						period: flow.periodSeconds,
					}
				: {}),
			...(options.retryDelay ? { retryDelayExpression: options.retryDelay } : {}),
			...(options.labels.length > 0 ? { label: options.labels[0], labels: options.labels } : {}),
			...(row.last_run_at ? { lastScheduleTime: row.last_run_at } : {}),
			nextScheduleTime: row.next_run_at,
			...(row.last_message_ids?.length
				? { lastScheduleStates: Object.fromEntries(row.last_message_ids.map((id) => [id, 'CREATED'])) }
				: {}),
		};
	}

	async listSchedules(): Promise<Record<string, unknown>[]> {
		const rows = await this.sql<ScheduleRow[]>`SELECT * FROM postlater_schedules ORDER BY created_at`;
		return rows.map((row) => this.formatSchedule(row));
	}

	async getSchedule(id: string): Promise<Record<string, unknown>> {
		const [row] = await this.sql<ScheduleRow[]>`SELECT * FROM postlater_schedules WHERE id = ${id}`;
		if (!row) throw new NotFoundError(`schedule ${id} not found`);
		return this.formatSchedule(row);
	}

	async deleteSchedule(id: string): Promise<void> {
		const rows = await this.sql`DELETE FROM postlater_schedules WHERE id = ${id} RETURNING id`;
		if (rows.length === 0) throw new NotFoundError(`schedule ${id} not found`);
	}

	async pauseSchedule(id: string, paused: boolean): Promise<void> {
		const [row] = await this.sql<ScheduleRow[]>`SELECT * FROM postlater_schedules WHERE id = ${id}`;
		if (!row) throw new NotFoundError(`schedule ${id} not found`);
		// resuming starts from now, rather than running every time that was missed while paused
		const next = paused ? row.next_run_at : (nextCronRun(row.cron, Date.now()) ?? row.next_run_at);
		await this.sql`UPDATE postlater_schedules SET paused = ${paused}, next_run_at = ${next} WHERE id = ${id}`;
	}

	// ---- url groups -------------------------------------------------------------------------------------------

	async addEndpoints(topic: string, endpoints: { url?: unknown; name?: unknown }[]): Promise<void> {
		if (!Array.isArray(endpoints) || endpoints.length === 0) throw new PublishError('endpoints are required');
		for (const endpoint of endpoints) checkUrl(String(endpoint?.url ?? ''), 'endpoint url');
		const now = Date.now();
		await this.sql.begin(async (tx) => {
			await tx`
				INSERT INTO postlater_topics (name, created_at, updated_at) VALUES (${topic}, ${now}, ${now})
				ON CONFLICT (name) DO UPDATE SET updated_at = ${now}
			`;
			for (const endpoint of endpoints) {
				const name = typeof endpoint.name === 'string' && endpoint.name ? endpoint.name : null;
				await tx`
					INSERT INTO postlater_topic_endpoints (topic, name, url) VALUES (${topic}, ${name}, ${String(endpoint.url)})
					ON CONFLICT (topic, url) DO UPDATE SET name = excluded.name
				`;
			}
		});
	}

	async removeEndpoints(topic: string, endpoints: { url?: unknown; name?: unknown }[]): Promise<void> {
		if (!Array.isArray(endpoints) || endpoints.length === 0) throw new PublishError('endpoints are required');
		await this.sql.begin(async (tx) => {
			const [exists] = await tx`SELECT 1 FROM postlater_topics WHERE name = ${topic}`;
			if (!exists) throw new NotFoundError(`url group ${topic} not found`);
			for (const endpoint of endpoints) {
				if (typeof endpoint.url === 'string') {
					await tx`DELETE FROM postlater_topic_endpoints WHERE topic = ${topic} AND url = ${endpoint.url}`;
				} else if (typeof endpoint.name === 'string') {
					await tx`DELETE FROM postlater_topic_endpoints WHERE topic = ${topic} AND name = ${endpoint.name}`;
				}
			}
			await tx`UPDATE postlater_topics SET updated_at = ${Date.now()} WHERE name = ${topic}`;
		});
	}

	async listTopics(name?: string): Promise<Record<string, unknown>[]> {
		const topics = await this.sql<{ name: string; created_at: number; updated_at: number }[]>`
			SELECT * FROM postlater_topics ${name === undefined ? this.sql`` : this.sql`WHERE name = ${name}`} ORDER BY created_at
		`;
		const endpoints = await this.sql<{ topic: string; name: string | null; url: string }[]>`
			SELECT topic, name, url FROM postlater_topic_endpoints ORDER BY position
		`;
		return topics.map((t) => ({
			createdAt: t.created_at,
			updatedAt: t.updated_at,
			name: t.name,
			endpoints: endpoints
				.filter((e) => e.topic === t.name)
				.map((e) => ({ ...(e.name ? { name: e.name } : {}), url: e.url })),
		}));
	}

	async getTopic(name: string): Promise<Record<string, unknown>> {
		const [topic] = await this.listTopics(name);
		if (!topic) throw new NotFoundError(`url group ${name} not found`);
		return topic;
	}

	async deleteTopic(name: string): Promise<void> {
		const rows = await this.sql`DELETE FROM postlater_topics WHERE name = ${name} RETURNING name`;
		if (rows.length === 0) throw new NotFoundError(`url group ${name} not found`);
	}

	// ---- dead-letter queue ------------------------------------------------------------------------------------

	formatDlq(row: DlqRow): Record<string, unknown> {
		return {
			...row.message,
			dlqId: row.dlq_id,
			...(row.response_status === null ? {} : { responseStatus: row.response_status }),
			...(row.response_headers ? { responseHeader: row.response_headers } : {}),
			...(row.response_body && row.response_body.length > 0
				? (() => {
						const body = bodyFields(row.response_body);
						return body.body === undefined ? { responseBodyBase64: body.bodyBase64 } : { responseBody: body.body };
					})()
				: {}),
		};
	}

	async listDlq(query: URLSearchParams): Promise<{ cursor?: string; messages: Record<string, unknown>[] }> {
		const count = Math.min(Number(query.get('count') ?? 100) || 100, 1000);
		const filter = dlqFilter(this.sql, query);
		const rows = await this.sql<DlqRow[]>`
			SELECT * FROM postlater_dlq WHERE ${filter} ORDER BY seq DESC LIMIT ${count + 1}
		`;
		const page = rows.slice(0, count);
		return {
			messages: page.map((row) => this.formatDlq(row)),
			...(rows.length > count ? { cursor: String((page[page.length - 1] as DlqRow).seq) } : {}),
		};
	}

	async getDlq(id: string): Promise<Record<string, unknown>> {
		const [row] = await this.sql<DlqRow[]>`SELECT * FROM postlater_dlq WHERE dlq_id = ${id}`;
		if (!row) throw new NotFoundError(`dlq message ${id} not found`);
		return this.formatDlq(row);
	}

	async deleteDlq(query: URLSearchParams, id?: string): Promise<{ deleted: number; cursor?: string }> {
		if (id) {
			const rows = await this.sql`DELETE FROM postlater_dlq WHERE dlq_id = ${id} RETURNING dlq_id`;
			if (rows.length === 0) throw new NotFoundError(`dlq message ${id} not found`);
			return { deleted: 1 };
		}
		const count = Math.min(Number(query.get('count') ?? 100) || 100, 1000);
		const rows = await this.sql`
			DELETE FROM postlater_dlq WHERE dlq_id IN (
				SELECT dlq_id FROM postlater_dlq WHERE ${dlqFilter(this.sql, query)} ORDER BY seq DESC LIMIT ${count}
			) RETURNING dlq_id
		`;
		return { deleted: rows.length };
	}

	/** Publishes DLQ messages again, as new messages to the same destination, and removes them from the DLQ. */
	async retryDlq(query: URLSearchParams): Promise<{ responses: PublishedMessage[]; cursor?: string }> {
		const count = Math.min(Number(query.get('count') ?? 100) || 100, 1000);
		const responses = await this.sql.begin(async (tx) => {
			const rows = await tx<DlqRow[]>`
				DELETE FROM postlater_dlq WHERE dlq_id IN (
					SELECT dlq_id FROM postlater_dlq WHERE ${dlqFilter(tx, query)} ORDER BY seq DESC LIMIT ${count}
					FOR UPDATE SKIP LOCKED
				) RETURNING *
			`;
			const out: PublishedMessage[] = [];
			for (const dlq of rows) {
				const row = dlq.source;
				const now = Date.now();
				const [copy] = await tx<MessageRow[]>`
					INSERT INTO postlater_messages (id, kind, url, topic_name, endpoint_name, method, headers, body, max_retries,
						retry_delay, timeout_ms, callback, failure_callback, callback_headers, failure_callback_headers, not_before,
						created_at, queue_name, schedule_id, caller_ip, fc_key, fc_parallelism, fc_rate, fc_period, labels, state,
						next_delivery_at)
					VALUES (${messageId()}, ${row.kind}, ${row.url}, ${row.topic_name}, ${row.endpoint_name}, ${row.method},
						${tx.json(row.headers)}, ${dlq.body}, ${row.max_retries}, ${row.retry_delay}, ${row.timeout_ms},
						${row.callback}, ${row.failure_callback}, ${tx.json(row.callback_headers)},
						${tx.json(row.failure_callback_headers)}, null, ${now}, ${row.queue_name}, ${row.schedule_id},
						${row.caller_ip}, ${row.fc_key}, ${row.fc_parallelism}, ${row.fc_rate}, ${row.fc_period}, ${row.labels},
						'CREATED', ${now})
					RETURNING *
				`;
				await recordEvent(tx, copy as MessageRow, 'CREATED', { withPayload: true });
				out.push({ messageId: (copy as MessageRow).id, url: row.url });
			}
			return out;
		});
		this.onPublish();
		return { responses };
	}

	// ---- signing keys -----------------------------------------------------------------------------------------

	async signingKeys(): Promise<{ current: string; next: string }> {
		if (this.config.currentSigningKey && this.config.nextSigningKey) {
			return { current: this.config.currentSigningKey, next: this.config.nextSigningKey };
		}
		return await this.sql.begin(async (tx) => {
			const rows = await tx<{ key: string; value: string }[]>`
				SELECT key, value FROM postlater_settings WHERE key IN ('current_signing_key', 'next_signing_key') FOR UPDATE
			`;
			const stored = Object.fromEntries(rows.map((r) => [r.key, r.value]));
			let current = stored.current_signing_key;
			let next = stored.next_signing_key;
			if (!current || !next) {
				current = signingKey();
				next = signingKey();
				await tx`
					INSERT INTO postlater_settings (key, value) VALUES ('current_signing_key', ${current}), ('next_signing_key', ${next})
					ON CONFLICT (key) DO NOTHING
				`;
			}
			return { current, next };
		});
	}

	/** The next key becomes current, and a new next key is made. Receivers accept both, so nothing is rejected. */
	async rotateSigningKeys(): Promise<{ current: string; next: string }> {
		if (this.config.currentSigningKey) {
			throw new PublishError(
				'signing keys are set with QSTASH_CURRENT_SIGNING_KEY and QSTASH_NEXT_SIGNING_KEY; rotate them there',
			);
		}
		const { next } = await this.signingKeys();
		const fresh = signingKey();
		await this.sql`
			INSERT INTO postlater_settings (key, value) VALUES ('current_signing_key', ${next}), ('next_signing_key', ${fresh})
			ON CONFLICT (key) DO UPDATE SET value = excluded.value
		`;
		return { current: next, next: fresh };
	}

	// ---- workflows --------------------------------------------------------------------------------------------

	/** Ends a workflow run: forgets its steps, and with `cancel`, cancels the messages it still has waiting. */
	async deleteWorkflowRun(runId: string, cancel: boolean): Promise<void> {
		await this.sql.begin(async (tx) => {
			await tx`DELETE FROM postlater_workflow_steps WHERE run_id = ${runId}`;
			if (cancel) {
				const rows = await tx<MessageRow[]>`
					DELETE FROM postlater_messages
					WHERE headers -> 'upstash-workflow-runid' ? ${runId} AND state <> 'ACTIVE' RETURNING *
				`;
				for (const row of rows) await recordEvent(tx, row, 'CANCELED');
			}
		});
	}

	// ---- flow control -----------------------------------------------------------------------------------------

	async flowControl(key?: string): Promise<Record<string, unknown>[]> {
		const rows = await this.sql<{ fc_key: string; waiting: number; active: number }[]>`
			SELECT fc_key, count(*) FILTER (WHERE state <> 'ACTIVE')::int AS waiting,
				count(*) FILTER (WHERE state = 'ACTIVE')::int AS active
			FROM postlater_messages WHERE fc_key IS NOT NULL ${key === undefined ? this.sql`` : this.sql`AND fc_key = ${key}`}
			GROUP BY fc_key ORDER BY fc_key
		`;
		return rows.map((r) => ({ flowControlKey: r.fc_key, waitListSize: r.waiting, activeCount: r.active }));
	}

	// ---- metrics ----------------------------------------------------------------------------------------------

	async stats(): Promise<{ states: { state: string; count: number }[]; dlq: number; schedules: number }> {
		const states = await this.sql<{ state: string; count: number }[]>`
			SELECT state, count(*)::int AS count FROM postlater_messages GROUP BY state
		`;
		const [dlq] = await this.sql<{ count: number }[]>`SELECT count(*)::int AS count FROM postlater_dlq`;
		const [schedules] = await this.sql<{ count: number }[]>`SELECT count(*)::int AS count FROM postlater_schedules`;
		return { states: [...states], dlq: dlq?.count ?? 0, schedules: schedules?.count ?? 0 };
	}
}

const header = (row: MessageRow, name: string) => row.headers[name]?.[0];

/**
 * Upstash Workflow keeps a run's history on the server: the payload that started it, and the result of each step.
 * Those are recorded here as they are published, and handed back with every later call of the run.
 */
async function recordWorkflowStep(sql: Sql, row: MessageRow): Promise<void> {
	const runId = header(row, 'upstash-workflow-runid');
	if (!runId) return;
	const start = header(row, 'upstash-workflow-init') === 'true';
	const step = (header(row, 'upstash-workflow-calltype') ?? 'step') === 'step';
	if (!start && !step) return;
	await sql`
		INSERT INTO postlater_workflow_steps (run_id, message_id, body, created_at)
		VALUES (${runId}, ${row.id}, ${row.body}, ${row.created_at})
	`;
}

/**
 * The body a workflow endpoint expects: every step of the run up to and including this message, the starting payload
 * first, each in base64, as a JSON array. The SDK reads its version header as the sign of this format, the first call
 * of a run included. Null for a message that is not a workflow call.
 */
export async function workflowBody(sql: Sql, row: MessageRow): Promise<Buffer | null> {
	const runId = header(row, 'upstash-workflow-runid');
	if (!runId || !header(row, 'upstash-workflow-sdk-version')) return null;
	if ((header(row, 'upstash-workflow-calltype') ?? 'step') !== 'step') return null;
	const steps = await sql<{ message_id: string; body: Buffer }[]>`
		SELECT message_id, body FROM postlater_workflow_steps
		WHERE run_id = ${runId}
			AND seq <= (SELECT max(seq) FROM postlater_workflow_steps WHERE message_id = ${row.id})
		ORDER BY seq
	`;
	if (steps.length === 0) return null;
	return Buffer.from(
		JSON.stringify(steps.map((s) => ({ messageId: s.message_id, body: s.body.toString('base64'), callType: 'step' }))),
	);
}

export interface ScheduleRow {
	id: string;
	cron: string;
	destination: string;
	headers: HeaderMap;
	body: Buffer;
	caller_ip: string | null;
	paused: boolean;
	created_at: number;
	next_run_at: number;
	last_run_at: number | null;
	last_message_ids: string[] | null;
}

export interface DlqRow {
	dlq_id: string;
	seq: number;
	message_id: string;
	message: Record<string, unknown>;
	/** The message's own row, without its body, to publish it again from. */
	source: Omit<MessageRow, 'body'>;
	body: Buffer;
	response_status: number | null;
	response_headers: Record<string, string[]> | null;
	response_body: Buffer | null;
	created_at: number;
}

export interface MessageFilter {
	ids?: string[];
	query?: URLSearchParams;
	count?: number;
}

function and(sql: Sql, parts: Fragment[]): Fragment {
	return parts.reduce((acc, part) => sql`${acc} AND ${part}`);
}

function listFilter(sql: Sql, column: string, values: string[]): Fragment {
	return values.length === 0 ? sql`true` : sql`${sql(column)} IN ${sql(values)}`;
}

function labelFilter(sql: Sql, labels: string[]): Fragment {
	return labels.length === 0 ? sql`true` : sql`labels && ${labels}::text[]`;
}

function dateFilter(sql: Sql, column: string, from: string | null, to: string | null): Fragment {
	return and(sql, [
		from ? sql`${sql(column)} >= ${Number(from)}` : sql`true`,
		to ? sql`${sql(column)} <= ${Number(to)}` : sql`true`,
	]);
}

function messageFilterSql(sql: Sql, where: MessageFilter): Fragment {
	if (where.ids) return where.ids.length === 0 ? sql`false` : sql`id IN ${sql(where.ids)}`;
	const q = where.query ?? new URLSearchParams();
	return and(sql, [
		listFilter(sql, 'url', q.getAll('url')),
		listFilter(sql, 'topic_name', q.getAll('topicName')),
		listFilter(sql, 'schedule_id', q.getAll('scheduleId')),
		listFilter(sql, 'queue_name', q.getAll('queueName')),
		listFilter(sql, 'caller_ip', q.getAll('callerIp')),
		listFilter(sql, 'fc_key', q.getAll('flowControlKey')),
		labelFilter(sql, q.getAll('label')),
		dateFilter(sql, 'created_at', q.get('fromDate'), q.get('toDate')),
		sql`state <> 'ACTIVE'`,
	]);
}

function dlqFilter(sql: Sql, q: URLSearchParams): Fragment {
	const ids = q.getAll('dlqIds');
	if (ids.length > 0) return sql`dlq_id IN ${sql(ids)}`;
	const cursor = q.get('cursor');
	const field = (name: string, values: string[]) =>
		values.length === 0 ? sql`true` : sql`message ->> ${name} IN ${sql(values)}`;
	const statuses = q.getAll('responseStatus').map(Number);
	return and(sql, [
		cursor ? sql`seq < ${Number(cursor)}` : sql`true`,
		field('messageId', q.getAll('messageId')),
		field('url', q.getAll('url')),
		field('topicName', q.getAll('topicName')),
		field('scheduleId', q.getAll('scheduleId')),
		field('queueName', q.getAll('queueName')),
		field('callerIp', q.getAll('callerIp')),
		field('flowControlKey', q.getAll('flowControlKey')),
		statuses.length === 0 ? sql`true` : sql`response_status IN ${sql(statuses)}`,
		dateFilter(sql, 'created_at', q.get('fromDate'), q.get('toDate')),
	]);
}

export { messageFilterSql };
