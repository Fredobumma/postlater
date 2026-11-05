import type postgres from 'postgres';
import type { Config } from './config.js';
import type { Sql } from './db.js';
import { dlqId } from './ids.js';
import { formatMessage, type MessageKind, type MessageRow, recordEvent } from './messages.js';
import { type HeaderMap, parsePublishHeaders, toHeaderMap, unprefixCallbackHeaders } from './publish.js';
import { retryAfterMs, retryDelayMs } from './retry-delay.js';
import { nextCronRun, type ScheduleRow, type Service, workflowBody } from './service.js';
import { signDelivery } from './signing.js';

export interface Logger {
	info(message: string): void;
	error(message: string): void;
}

interface Response {
	status: number;
	headers: HeaderMap;
	body: Buffer;
}

// a lease outlives the attempt by this much; a message whose lease runs out was lost with its server, and is retried
const LEASE_GRACE_MS = 60_000;
const MAX_STORED_RESPONSE_BYTES = 1024 * 1024;
const MAINTENANCE_INTERVAL_MS = 60_000;
const FLOW_ACTIVATION_RETENTION_MS = 7 * 86_400_000;
const WITHOUT_BODY = new Set(['GET', 'HEAD']);

const base64 = (bytes: Uint8Array | string) => Buffer.from(bytes).toString('base64');

export class Worker {
	private active = 0;
	private timer: NodeJS.Timeout | undefined;
	private running = false;
	private ticking = false;
	private again = false;
	private lastMaintenance = 0;
	private keys: { current: string; next: string; at: number } | undefined;
	private readonly inflight = new Set<Promise<void>>();

	constructor(
		private readonly service: Service,
		private readonly sql: postgres.Sql,
		private readonly config: Config,
		private readonly log: Logger,
	) {}

	start(): void {
		this.running = true;
		this.service.onPublish = () => this.kick();
		this.timer = setInterval(() => this.kick(), this.config.pollIntervalMs);
		this.kick();
	}

	/** Stops claiming, and waits for the deliveries under way to finish. */
	async stop(): Promise<void> {
		this.running = false;
		clearInterval(this.timer);
		while (this.ticking) await new Promise((r) => setTimeout(r, 10));
		await Promise.allSettled([...this.inflight]);
	}

	/** Looks for work now. Calls that arrive during a tick run one more tick after it. */
	kick(): void {
		if (!this.running) return;
		if (this.ticking) {
			this.again = true;
			return;
		}
		this.ticking = true;
		this.tick()
			.catch((error) => this.log.error(`worker: ${(error as Error).message}`))
			.finally(() => {
				this.ticking = false;
				if (this.again) {
					this.again = false;
					this.kick();
				}
			});
	}

	private async tick(): Promise<void> {
		const now = Date.now();
		if (now - this.lastMaintenance >= MAINTENANCE_INTERVAL_MS) {
			this.lastMaintenance = now;
			await this.maintain(now);
		}
		await this.runSchedules();

		const free = this.config.workerConcurrency - this.active;
		if (free <= 0) return;
		for (const row of await this.claim(free)) {
			this.active++;
			const delivery = this.deliver(row)
				.catch((error) => this.log.error(`delivery of ${row.id}: ${(error as Error).message}`))
				.finally(() => {
					this.active--;
					this.inflight.delete(delivery);
					this.kick();
				});
			this.inflight.add(delivery);
		}
	}

	// ---- claiming ---------------------------------------------------------------------------------------------

	/**
	 * Marks up to `limit` due messages as being delivered, and returns them. Several servers can share a database:
	 * rows are claimed with SKIP LOCKED, and each queue and flow control key is claimed by one server at a time.
	 */
	async claim(limit: number): Promise<MessageRow[]> {
		const now = Date.now();
		const rows = await this.sql.begin(async (tx): Promise<MessageRow[]> => {
			const ids: string[] = [];

			const plain = await tx<{ id: string }[]>`
				SELECT id FROM postlater_messages
				WHERE state IN ('CREATED', 'RETRY') AND next_delivery_at <= ${now} AND queue_name IS NULL AND fc_key IS NULL
				ORDER BY next_delivery_at LIMIT ${limit} FOR UPDATE SKIP LOCKED
			`;
			ids.push(...plain.map((r) => r.id));

			// a queue delivers in order: only its first `parallelism` messages may be under way, and a message that is
			// waiting to be retried holds back the ones behind it
			if (ids.length < limit) {
				const queues = await tx<{ name: string; parallelism: number }[]>`
					SELECT DISTINCT m.queue_name AS name, coalesce(q.parallelism, 1) AS parallelism
					FROM postlater_messages m LEFT JOIN postlater_queues q ON q.name = m.queue_name
					WHERE m.queue_name IS NOT NULL AND m.state IN ('CREATED', 'RETRY') AND m.next_delivery_at <= ${now}
						AND NOT coalesce(q.paused, false)
				`;
				for (const queue of queues) {
					if (ids.length >= limit) break;
					if (!(await this.lock(tx, `queue:${queue.name}`))) continue;
					const heads = await tx<{ id: string; state: string; next_delivery_at: number }[]>`
						SELECT id, state, next_delivery_at FROM postlater_messages WHERE queue_name = ${queue.name}
						ORDER BY seq LIMIT ${queue.parallelism}
					`;
					for (const head of heads) {
						if (head.state !== 'ACTIVE' && head.next_delivery_at <= now && ids.length < limit) ids.push(head.id);
					}
				}
			}

			// flow control: at most `parallelism` under way, and at most `rate` started per period, for each key
			if (ids.length < limit) {
				const keys = await tx<{ key: string; parallelism: number | null; rate: number | null; period: number }[]>`
					SELECT fc_key AS key, max(fc_parallelism) AS parallelism, max(fc_rate) AS rate, max(fc_period) AS period
					FROM postlater_messages
					WHERE fc_key IS NOT NULL AND queue_name IS NULL AND state IN ('CREATED', 'RETRY') AND next_delivery_at <= ${now}
					GROUP BY fc_key
				`;
				for (const flow of keys) {
					if (ids.length >= limit) break;
					if (!(await this.lock(tx, `flow:${flow.key}`))) continue;
					const [counts] = await tx<{ active: number; recent: number }[]>`
						SELECT
							(SELECT count(*)::int FROM postlater_messages WHERE fc_key = ${flow.key} AND state = 'ACTIVE') AS active,
							(SELECT count(*)::int FROM postlater_flow_activations
								WHERE key = ${flow.key} AND at > ${now - flow.period * 1000}) AS recent
					`;
					let slots = limit - ids.length;
					if (flow.parallelism !== null) slots = Math.min(slots, flow.parallelism - (counts?.active ?? 0));
					if (flow.rate !== null) slots = Math.min(slots, flow.rate - (counts?.recent ?? 0));
					if (slots <= 0) continue;
					const ready = await tx<{ id: string }[]>`
						SELECT id FROM postlater_messages
						WHERE fc_key = ${flow.key} AND queue_name IS NULL AND state IN ('CREATED', 'RETRY') AND next_delivery_at <= ${now}
						ORDER BY next_delivery_at, seq LIMIT ${slots} FOR UPDATE SKIP LOCKED
					`;
					for (const { id } of ready) {
						ids.push(id);
						await tx`INSERT INTO postlater_flow_activations (key, at) VALUES (${flow.key}, ${now})`;
					}
				}
			}

			if (ids.length === 0) return [];
			const claimed = await tx<MessageRow[]>`
				UPDATE postlater_messages SET state = 'ACTIVE', lease_until = ${now + LEASE_GRACE_MS}::bigint + timeout_ms
				WHERE id IN ${tx(ids)} RETURNING *
			`;
			for (const row of claimed) await recordEvent(tx, row, 'ACTIVE');
			return claimed;
		});
		return [...rows];
	}

	private async lock(tx: Sql, name: string): Promise<boolean> {
		const [row] = await tx<{ ok: boolean }[]>`SELECT pg_try_advisory_xact_lock(hashtext(${`postlater:${name}`})) AS ok`;
		return row?.ok === true;
	}

	// ---- delivering -------------------------------------------------------------------------------------------

	private async signingKey(): Promise<string> {
		if (!this.keys || Date.now() - this.keys.at > 10_000) {
			this.keys = { ...(await this.service.signingKeys()), at: Date.now() };
		}
		return this.keys.current;
	}

	async deliver(row: MessageRow): Promise<void> {
		const headers = new Headers();
		for (const [name, values] of Object.entries(row.headers)) for (const value of values) headers.append(name, value);
		headers.set('Upstash-Message-Id', row.id);
		headers.set('Upstash-Retried', String(row.retried));
		if (row.schedule_id) headers.set('Upstash-Schedule-Id', row.schedule_id);
		if (row.topic_name) headers.set('Upstash-Topic-Name', row.topic_name);
		if (row.caller_ip) headers.set('Upstash-Caller-Ip', row.caller_ip);
		headers.set('User-Agent', 'Upstash-QStash');
		const payload = (await workflowBody(this.sql, row)) ?? row.body;
		headers.set('Upstash-Signature', await signDelivery(await this.signingKey(), row.url, payload));

		let response: Response | null = null;
		let error: string | null = null;
		try {
			const res = await fetch(row.url, {
				method: row.method,
				headers,
				body: WITHOUT_BODY.has(row.method) ? undefined : payload,
				signal: AbortSignal.timeout(row.timeout_ms),
			});
			const body = Buffer.from(await res.arrayBuffer());
			response = {
				status: res.status,
				headers: toHeaderMap(res.headers),
				body: body.length > MAX_STORED_RESPONSE_BYTES ? body.subarray(0, MAX_STORED_RESPONSE_BYTES) : body,
			};
		} catch (e) {
			const err = e as Error & { cause?: { code?: string; message?: string } };
			error =
				err.name === 'TimeoutError'
					? `timed out after ${row.timeout_ms / 1000}s`
					: `${err.cause?.code ?? err.name}: ${err.cause?.message ?? err.message}`;
		}
		await this.finish(row, response, error);
	}

	private async finish(row: MessageRow, response: Response | null, networkError: string | null): Promise<void> {
		const now = Date.now();
		const ok = response !== null && response.status >= 200 && response.status < 300;
		const nonRetryable = response?.status === 489 && response.headers['upstash-nonretryable-error']?.[0] === 'true';
		const final = ok || nonRetryable || row.retried >= row.max_retries;

		await this.sql.begin(async (tx) => {
			// the message may have been deleted with its queue while it was under way
			const [still] = await tx`SELECT 1 FROM postlater_messages WHERE id = ${row.id} FOR UPDATE`;
			if (!still) return;

			// the callback hears of every attempt; retried and maxRetries tell it whether another is coming
			if (row.callback) await this.publishCallback(tx, row, 'callback', response, networkError);

			if (ok) {
				await tx`DELETE FROM postlater_messages WHERE id = ${row.id}`;
				await recordEvent(tx, row, 'DELIVERED', { responseStatus: response.status });
				return;
			}

			const error =
				networkError ?? `${response?.status}: ${response?.body.subarray(0, 500).toString('utf8') ?? ''}`.trim();
			if (final) {
				const id = dlqId();
				const { body: _body, ...source } = row;
				await tx`DELETE FROM postlater_messages WHERE id = ${row.id}`;
				await tx`
					INSERT INTO postlater_dlq (dlq_id, message_id, message, source, body, response_status, response_headers,
						response_body, created_at)
					VALUES (${id}, ${row.id}, ${tx.json(formatMessage(row) as never)}, ${tx.json(source as never)}, ${row.body},
						${response?.status ?? null}, ${response ? tx.json(response.headers) : null}, ${response?.body ?? null}, ${now})
				`;
				await recordEvent(tx, row, 'FAILED', { error, responseStatus: response?.status ?? null });
				if (row.failure_callback) await this.publishCallback(tx, row, 'failure_callback', response, networkError, id);
				return;
			}

			const delay = (response && retryAfterMs(response.headers)) ?? retryDelayMs(row.retry_delay, row.retried + 1);
			await tx`
				UPDATE postlater_messages
				SET state = 'RETRY', retried = retried + 1, next_delivery_at = ${now + delay}, lease_until = NULL
				WHERE id = ${row.id}
			`;
			await recordEvent(tx, row, 'RETRY', {
				error,
				nextDeliveryTime: now + delay,
				responseStatus: response?.status ?? null,
			});
		});
	}

	private async publishCallback(
		tx: postgres.TransactionSql,
		row: MessageRow,
		kind: Exclude<MessageKind, 'message'>,
		response: Response | null,
		networkError: string | null,
		dlq?: string,
	): Promise<void> {
		const url = (kind === 'callback' ? row.callback : row.failure_callback) as string;
		const payload = {
			status: response?.status ?? 0,
			header: response?.headers ?? {},
			body: base64(response ? response.body : (networkError ?? '')),
			retried: row.retried,
			maxRetries: row.max_retries,
			sourceMessageId: row.id,
			...(row.topic_name ? { topicName: row.topic_name } : {}),
			...(row.endpoint_name ? { endpointName: row.endpoint_name } : {}),
			url: row.url,
			method: row.method,
			sourceHeader: row.headers,
			sourceBody: base64(row.body),
			notBefore: String(row.not_before ?? row.created_at),
			createdAt: String(row.created_at),
			...(row.schedule_id ? { scheduleId: row.schedule_id } : {}),
			...(row.caller_ip ? { callerIP: row.caller_ip } : {}),
			...(dlq ? { dlqId: dlq } : {}),
		};
		const headers = unprefixCallbackHeaders(kind === 'callback' ? row.callback_headers : row.failure_callback_headers);
		try {
			await tx.savepoint((sp) =>
				this.service.publishIn(sp, url, headers, Buffer.from(JSON.stringify(payload)), {
					callerIp: row.caller_ip,
					kind,
				}),
			);
		} catch (error) {
			// a bad callback setting must not lose the message's own outcome
			this.log.error(`could not publish the ${kind.replace('_', ' ')} of ${row.id}: ${(error as Error).message}`);
		}
	}

	// ---- schedules --------------------------------------------------------------------------------------------

	async runSchedules(): Promise<void> {
		const now = Date.now();
		const ran = await this.sql.begin(async (tx) => {
			const due = await tx<ScheduleRow[]>`
				SELECT * FROM postlater_schedules WHERE NOT paused AND next_run_at <= ${now}
				ORDER BY next_run_at LIMIT 100 FOR UPDATE SKIP LOCKED
			`;
			for (const schedule of due) {
				let ids: string[] = [];
				try {
					const options = parsePublishHeaders(schedule.headers, this.service.limits);
					const result = await tx.savepoint((sp) =>
						this.service.publishIn(
							sp,
							schedule.destination,
							schedule.headers,
							schedule.body,
							{ callerIp: schedule.caller_ip, scheduleId: schedule.id, queueName: options.queueName },
							options,
						),
					);
					ids = (Array.isArray(result) ? result : [result]).map((m) => m.messageId);
				} catch (error) {
					this.log.error(`schedule ${schedule.id}: ${(error as Error).message}`);
				}
				// a server that was down runs a missed schedule once, not once for every time it missed
				let next: number | null = null;
				try {
					next = nextCronRun(schedule.cron, now);
				} catch {}
				await tx`
					UPDATE postlater_schedules
					SET last_run_at = ${now}, last_message_ids = ${ids}, next_run_at = ${next ?? Number.MAX_SAFE_INTEGER}
					WHERE id = ${schedule.id}
				`;
			}
			return due.length;
		});
		if (ran > 0) this.again = true;
	}

	// ---- upkeep -----------------------------------------------------------------------------------------------

	async maintain(now: number): Promise<void> {
		// messages whose server stopped mid-delivery are delivered again: at least once, never lost
		const lost = await this.sql<MessageRow[]>`
			UPDATE postlater_messages SET state = 'RETRY', lease_until = NULL, next_delivery_at = ${now}
			WHERE state = 'ACTIVE' AND lease_until < ${now} RETURNING *
		`;
		for (const row of lost)
			await recordEvent(this.sql, row, 'RETRY', { error: 'delivery interrupted', nextDeliveryTime: now });

		await this.sql`DELETE FROM postlater_events WHERE time < ${now - this.config.eventsRetentionMs}`;
		await this.sql`DELETE FROM postlater_dlq WHERE created_at < ${now - this.config.dlqRetentionMs}`;
		await this.sql`DELETE FROM postlater_deduplication WHERE expires_at < ${now}`;
		await this.sql`DELETE FROM postlater_flow_activations WHERE at < ${now - FLOW_ACTIVATION_RETENTION_MS}`;
		// runs the SDK never ended, such as ones that failed for good, are forgotten with the rest of the history
		await this.sql`DELETE FROM postlater_workflow_steps WHERE created_at < ${now - this.config.eventsRetentionMs}`;
	}
}
