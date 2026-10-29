import { parseDuration } from './duration.js';
import { parseRetryDelay, RetryDelayError } from './retry-delay.js';

/** Header names lower-cased, each with its values. */
export type HeaderMap = Record<string, string[]>;

export class PublishError extends Error {
	constructor(
		message: string,
		readonly status = 400,
	) {
		super(message);
	}
}

export interface FlowControl {
	key: string;
	parallelism: number | null;
	rate: number | null;
	periodSeconds: number;
}

/** Everything a publish request says about the messages it creates. */
export interface PublishOptions {
	method: string;
	/** What reaches the destination: Content-Type, `Upstash-Forward-*` without the prefix, and `Upstash-Workflow-*`. */
	forward: Record<string, string[]>;
	delayMs: number;
	notBefore: number | null;
	retries: number;
	retryDelay: string | null;
	timeoutMs: number;
	callback: string | null;
	failureCallback: string | null;
	/** `Upstash-Callback-*` and `Upstash-Failure-Callback-*`, kept to publish the callbacks with. */
	callbackHeaders: HeaderMap;
	failureCallbackHeaders: HeaderMap;
	deduplicationId: string | null;
	contentBasedDeduplication: boolean;
	flowControl: FlowControl | null;
	labels: string[];
	queueName: string | null;
	cron: string | null;
	scheduleId: string | null;
}

export interface PublishLimits {
	defaultRetries: number;
	maxRetries: number;
	defaultTimeoutMs: number;
	maxTimeoutMs: number;
}

const METHODS = new Set(['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'HEAD']);
// what fetch sets itself, or must not be forwarded
const UNFORWARDABLE = new Set(['host', 'content-length', 'connection', 'transfer-encoding', 'keep-alive', 'upgrade']);

export function toHeaderMap(headers: Headers | Record<string, string | string[] | undefined>): HeaderMap {
	const map: HeaderMap = {};
	const add = (name: string, value: string) => {
		const key = name.toLowerCase();
		const values = map[key] ?? [];
		values.push(value);
		map[key] = values;
	};
	if (headers instanceof Headers) {
		headers.forEach((value, name) => {
			add(name, value);
		});
	} else {
		for (const [name, value] of Object.entries(headers)) {
			if (value === undefined) continue;
			for (const v of Array.isArray(value) ? value : [value]) add(name, String(v));
		}
	}
	return map;
}

const first = (headers: HeaderMap, name: string) => headers[name]?.[0];

function integer(headers: HeaderMap, name: string, min: number, max: number): number | null {
	const value = first(headers, name);
	if (value === undefined) return null;
	if (!/^\d+$/.test(value.trim())) throw new PublishError(`invalid ${name}: ${value}`);
	const number = Number(value);
	if (number < min || number > max) throw new PublishError(`${name} must be between ${min} and ${max}`);
	return number;
}

function duration(headers: HeaderMap, name: string): number | null {
	const value = first(headers, name);
	if (value === undefined) return null;
	const ms = parseDuration(value);
	if (ms === undefined) throw new PublishError(`invalid ${name}: ${value}`);
	return ms;
}

export function checkUrl(value: string, what: string): string {
	let url: URL;
	try {
		url = new URL(value);
	} catch {
		throw new PublishError(`invalid ${what}: ${value}`);
	}
	if (url.protocol !== 'http:' && url.protocol !== 'https:') {
		throw new PublishError(`${what} must be an http or https URL: ${value}`);
	}
	return value;
}

function prefixed(headers: HeaderMap, prefix: string): HeaderMap {
	const out: HeaderMap = {};
	for (const [name, values] of Object.entries(headers)) {
		if (name.startsWith(prefix) && name.length > prefix.length) out[name.slice(prefix.length)] = values;
	}
	return out;
}

function parseFlowControl(headers: HeaderMap): FlowControl | null {
	const key = first(headers, 'upstash-flow-control-key');
	if (key === undefined) return null;
	const flow: FlowControl = { key, parallelism: null, rate: null, periodSeconds: 1 };
	for (const part of (first(headers, 'upstash-flow-control-value') ?? '').split(',')) {
		const [name, raw] = part.split('=').map((s) => s.trim());
		if (!name) continue;
		if (name === 'parallelism' || name === 'rate') {
			if (!raw || !/^\d+$/.test(raw) || Number(raw) < 1) throw new PublishError(`invalid flow control ${name}: ${raw}`);
			flow[name] = Number(raw);
		} else if (name === 'period') {
			const ms = raw === undefined ? undefined : parseDuration(raw);
			if (ms === undefined || ms < 1000) throw new PublishError(`invalid flow control period: ${raw}`);
			flow.periodSeconds = Math.round(ms / 1000);
		} else {
			throw new PublishError(`unknown flow control setting: ${name}`);
		}
	}
	if (flow.parallelism === null && flow.rate === null) {
		throw new PublishError('flow control needs parallelism, rate or both');
	}
	return flow;
}

export function parsePublishHeaders(headers: HeaderMap, limits: PublishLimits): PublishOptions {
	const method = (first(headers, 'upstash-method') ?? 'POST').toUpperCase();
	if (!METHODS.has(method)) throw new PublishError(`unsupported Upstash-Method: ${method}`);

	// Upstash-Workflow-* headers pass through as they are; an Upstash-Forward-* header of the same name replaces one
	const forward: Record<string, string[]> = {};
	for (const [name, values] of Object.entries(headers)) {
		if (name === 'content-type' || name.startsWith('upstash-workflow-')) forward[name] = values;
	}
	for (const [name, values] of Object.entries(headers)) {
		if (name.startsWith('upstash-forward-')) forward[name.slice('upstash-forward-'.length)] = values;
	}
	for (const name of UNFORWARDABLE) delete forward[name];

	const retryDelay = first(headers, 'upstash-retry-delay') ?? null;
	if (retryDelay !== null) {
		try {
			parseRetryDelay(retryDelay);
		} catch (error) {
			if (error instanceof RetryDelayError) throw new PublishError(`invalid Upstash-Retry-Delay: ${error.message}`);
			throw error;
		}
	}

	const notBefore = integer(headers, 'upstash-not-before', 0, Number.MAX_SAFE_INTEGER);
	const timeoutMs = duration(headers, 'upstash-timeout') ?? limits.defaultTimeoutMs;
	if (timeoutMs < 1000 || timeoutMs > limits.maxTimeoutMs) {
		throw new PublishError(`Upstash-Timeout must be between 1s and ${limits.maxTimeoutMs / 1000}s`);
	}

	const callback = first(headers, 'upstash-callback');
	const failureCallback = first(headers, 'upstash-failure-callback');

	return {
		method,
		forward,
		delayMs: duration(headers, 'upstash-delay') ?? 0,
		notBefore: notBefore === null ? null : notBefore * 1000,
		retries: integer(headers, 'upstash-retries', 0, limits.maxRetries) ?? limits.defaultRetries,
		retryDelay,
		timeoutMs,
		callback: callback === undefined ? null : checkUrl(callback, 'Upstash-Callback'),
		failureCallback: failureCallback === undefined ? null : checkUrl(failureCallback, 'Upstash-Failure-Callback'),
		callbackHeaders: prefixed(headers, 'upstash-callback-'),
		failureCallbackHeaders: prefixed(headers, 'upstash-failure-callback-'),
		deduplicationId: first(headers, 'upstash-deduplication-id') ?? null,
		contentBasedDeduplication: first(headers, 'upstash-content-based-deduplication') === 'true',
		flowControl: parseFlowControl(headers),
		labels: (first(headers, 'upstash-label') ?? '')
			.split(',')
			.map((label) => label.trim())
			.filter(Boolean),
		queueName: first(headers, 'upstash-queue-name') ?? null,
		cron: first(headers, 'upstash-cron') ?? null,
		scheduleId: first(headers, 'upstash-schedule-id') ?? null,
	};
}

/**
 * The headers to publish a callback with: `Upstash-Callback-Retries` becomes `Upstash-Retries`,
 * `Upstash-Callback-Forward-X` becomes `Upstash-Forward-X`, and so on, so the callback is published like any message.
 */
export function unprefixCallbackHeaders(callbackHeaders: HeaderMap): HeaderMap {
	const headers: HeaderMap = { 'content-type': ['application/json'] };
	for (const [name, values] of Object.entries(callbackHeaders)) headers[`upstash-${name}`] = values;
	return headers;
}
