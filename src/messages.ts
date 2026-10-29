import type { Sql } from './db.js';
import type { HeaderMap } from './publish.js';

export type MessageKind = 'message' | 'callback' | 'failure_callback';
export type State = 'CREATED' | 'ACTIVE' | 'RETRY' | 'DELIVERED' | 'ERROR' | 'FAILED' | 'CANCELED';

export interface MessageRow {
	id: string;
	seq: number;
	kind: MessageKind;
	url: string;
	topic_name: string | null;
	endpoint_name: string | null;
	method: string;
	headers: Record<string, string[]>;
	body: Buffer;
	max_retries: number;
	retry_delay: string | null;
	timeout_ms: number;
	callback: string | null;
	failure_callback: string | null;
	callback_headers: HeaderMap;
	failure_callback_headers: HeaderMap;
	not_before: number | null;
	created_at: number;
	queue_name: string | null;
	schedule_id: string | null;
	caller_ip: string | null;
	fc_key: string | null;
	fc_parallelism: number | null;
	fc_rate: number | null;
	fc_period: number | null;
	labels: string[];
	state: State;
	retried: number;
	next_delivery_at: number;
	lease_until: number | null;
}

const utf8 = new TextDecoder('utf-8', { fatal: true });

/** A body as QStash reports it: as text when it is text, otherwise in base64. */
export function bodyFields(body: Uint8Array): { body?: string; bodyBase64?: string } {
	if (body.length === 0) return {};
	try {
		return { body: utf8.decode(body) };
	} catch {
		return { bodyBase64: Buffer.from(body).toString('base64') };
	}
}

const optional = <T>(name: string, value: T | null | undefined) =>
	value === null || value === undefined ? {} : { [name]: value };

/** A message as `GET /v2/messages/{id}` returns it. */
export function formatMessage(row: MessageRow): Record<string, unknown> {
	return {
		messageId: row.id,
		...optional('topicName', row.topic_name),
		...optional('endpointName', row.endpoint_name),
		url: row.url,
		method: row.method,
		header: row.headers,
		...bodyFields(row.body),
		maxRetries: row.max_retries,
		...optional('retryDelayExpression', row.retry_delay),
		...optional('notBefore', row.not_before ?? row.next_delivery_at),
		createdAt: row.created_at,
		...optional('callback', row.callback),
		...optional('failureCallback', row.failure_callback),
		...optional('queueName', row.queue_name),
		...optional('scheduleId', row.schedule_id),
		...optional('callerIp', row.caller_ip),
		...optional('flowControlKey', row.fc_key),
		...optional('parallelism', row.fc_parallelism),
		...optional('rate', row.fc_rate),
		...optional('period', row.fc_key ? row.fc_period : null),
		...(row.labels.length > 0 ? { label: row.labels[0], labels: row.labels } : {}),
	};
}

export interface EventDetails {
	error?: string | null;
	nextDeliveryTime?: number | null;
	responseStatus?: number | null;
	withPayload?: boolean;
}

export async function recordEvent(sql: Sql, row: MessageRow, state: State, details: EventDetails = {}): Promise<void> {
	await sql`
		INSERT INTO postlater_events (time, message_id, state, url, topic_name, endpoint_name, queue_name, schedule_id,
			caller_ip, fc_key, labels, error, next_delivery_time, header, body, response_status)
		VALUES (${Date.now()}, ${row.id}, ${state}, ${row.url}, ${row.topic_name}, ${row.endpoint_name}, ${row.queue_name},
			${row.schedule_id}, ${row.caller_ip}, ${row.fc_key}, ${row.labels}, ${details.error ?? null},
			${details.nextDeliveryTime ?? null}, ${details.withPayload ? sql.json(row.headers) : null},
			${details.withPayload && row.body.length > 0 ? row.body.toString('base64') : null},
			${details.responseStatus ?? null})
	`;
}

export interface EventRow {
	id: number;
	time: number;
	message_id: string;
	state: State;
	url: string;
	topic_name: string | null;
	endpoint_name: string | null;
	queue_name: string | null;
	schedule_id: string | null;
	caller_ip: string | null;
	fc_key: string | null;
	labels: string[];
	error: string | null;
	next_delivery_time: number | null;
	header: Record<string, string[]> | null;
	body: string | null;
	response_status: number | null;
}

/** An event as `GET /v2/events` returns it. */
export function formatEvent(row: EventRow): Record<string, unknown> {
	return {
		time: row.time,
		state: row.state,
		messageId: row.message_id,
		...optional('nextDeliveryTime', row.next_delivery_time),
		...optional('error', row.error),
		url: row.url,
		...optional('topicName', row.topic_name),
		...optional('endpointName', row.endpoint_name),
		...optional('queueName', row.queue_name),
		...optional('scheduleId', row.schedule_id),
		...optional('callerIp', row.caller_ip),
		...optional('flowControlKey', row.fc_key),
		...optional('header', row.header),
		...optional('body', row.body),
		...optional('responseStatus', row.response_status),
		...(row.labels.length > 0 ? { label: row.labels[0], labels: row.labels } : {}),
	};
}
