/**
 * The retry delay expressions of `Upstash-Retry-Delay`, such as `pow(2, retried) * 1000`: arithmetic on the number of
 * times the message has been retried, giving the delay in milliseconds. Parsed into a tree once, at publish time, and
 * evaluated without `eval`.
 */

import { parseDuration } from './duration.js';
import type { HeaderMap } from './publish.js';

type Node =
	| { kind: 'number'; value: number }
	| { kind: 'retried' }
	| { kind: 'negate'; operand: Node }
	| { kind: 'binary'; op: '+' | '-' | '*' | '/' | '%'; left: Node; right: Node }
	| { kind: 'call'; name: string; args: Node[] };

const FUNCTIONS: Record<string, { arity: [number, number]; fn: (...args: number[]) => number }> = {
	pow: { arity: [2, 2], fn: Math.pow },
	sqrt: { arity: [1, 1], fn: Math.sqrt },
	abs: { arity: [1, 1], fn: Math.abs },
	exp: { arity: [1, 1], fn: Math.exp },
	floor: { arity: [1, 1], fn: Math.floor },
	ceil: { arity: [1, 1], fn: Math.ceil },
	round: { arity: [1, 1], fn: Math.round },
	min: { arity: [2, Number.POSITIVE_INFINITY], fn: Math.min },
	max: { arity: [2, Number.POSITIVE_INFINITY], fn: Math.max },
};

const MAX_LENGTH = 256;

export class RetryDelayError extends Error {}

export function parseRetryDelay(expression: string): Node {
	if (expression.length > MAX_LENGTH) throw new RetryDelayError(`longer than ${MAX_LENGTH} characters`);
	const tokens = expression.match(/\d+(?:\.\d+)?|[A-Za-z_]+|[-+*/%(),]|\S/g) ?? [];
	let at = 0;

	const peek = () => tokens[at];
	const take = (expected?: string) => {
		const token = tokens[at];
		if (token === undefined || (expected !== undefined && token !== expected)) {
			throw new RetryDelayError(`expected ${expected ?? 'more'} at "${token ?? 'end'}"`);
		}
		at++;
		return token;
	};

	const primary = (): Node => {
		const token = take();
		if (/^\d/.test(token)) return { kind: 'number', value: Number(token) };
		if (token === '(') {
			const inner = sum();
			take(')');
			return inner;
		}
		if (token === '-') return { kind: 'negate', operand: primary() };
		if (token === 'retried') return { kind: 'retried' };

		const fn = FUNCTIONS[token];
		if (!fn) throw new RetryDelayError(`unknown name "${token}"`);
		take('(');
		const args = [sum()];
		while (peek() === ',') {
			take(',');
			args.push(sum());
		}
		take(')');
		if (args.length < fn.arity[0] || args.length > fn.arity[1]) {
			throw new RetryDelayError(`wrong number of arguments for ${token}`);
		}
		return { kind: 'call', name: token, args };
	};

	const product = (): Node => {
		let node = primary();
		while (peek() === '*' || peek() === '/' || peek() === '%') {
			const op = take() as '*' | '/' | '%';
			node = { kind: 'binary', op, left: node, right: primary() };
		}
		return node;
	};

	const sum = (): Node => {
		let node = product();
		while (peek() === '+' || peek() === '-') {
			const op = take() as '+' | '-';
			node = { kind: 'binary', op, left: node, right: product() };
		}
		return node;
	};

	const tree = sum();
	if (at !== tokens.length) throw new RetryDelayError(`unexpected "${tokens[at]}"`);
	return tree;
}

function evaluate(node: Node, retried: number): number {
	switch (node.kind) {
		case 'number':
			return node.value;
		case 'retried':
			return retried;
		case 'negate':
			return -evaluate(node.operand, retried);
		case 'call':
			return (FUNCTIONS[node.name] as (typeof FUNCTIONS)[string]).fn(...node.args.map((arg) => evaluate(arg, retried)));
		case 'binary': {
			const left = evaluate(node.left, retried);
			const right = evaluate(node.right, retried);
			if (node.op === '+') return left + right;
			if (node.op === '-') return left - right;
			if (node.op === '*') return left * right;
			if (node.op === '/') return left / right;
			return left % right;
		}
	}
}

const DAY_MS = 86_400_000;

/** The delay before the next attempt, in milliseconds, after `retried` failed attempts. */
export function retryDelayMs(expression: string | null | undefined, retried: number): number {
	if (!expression) {
		// QStash's own default: e^(2.5 × retried) seconds, at most a day
		return Math.min(DAY_MS, Math.round(Math.exp(2.5 * retried) * 1000));
	}
	const delay = evaluate(parseRetryDelay(expression), retried);
	if (!Number.isFinite(delay) || delay < 0) return 0;
	return Math.min(DAY_MS, Math.round(delay));
}

// the answer headers that say when to try again, as QStash reads them
const RETRY_AFTER = ['upstash-retry-after', 'retry-after'];
const RATE_LIMIT_RESET = ['x-ratelimit-reset', 'x-ratelimit-reset-requests', 'x-ratelimit-reset-tokens'];

/**
 * How long the destination asked to wait before the next attempt, in milliseconds, from `Retry-After` or a rate limit's
 * reset headers on any failed answer: seconds (`10`), a duration (`6m5s`) or a date. When several reset headers are
 * set, the longest wins, so the next attempt doesn't run into the other limit. Null when the answer doesn't say, or asks
 * for more than a day, which leaves the attempt to the usual backoff.
 */
export function retryAfterMs(headers: HeaderMap, now = Date.now()): number | null {
	const read = (name: string) => {
		const value = headers[name]?.[0];
		return value === undefined ? null : untilMs(value, now);
	};
	let wait = RETRY_AFTER.map(read).find((ms) => ms !== null) ?? null;
	if (wait === null) {
		const resets = RATE_LIMIT_RESET.map(read).filter((ms) => ms !== null);
		wait = resets.length > 0 ? Math.max(...resets) : null;
	}
	return wait !== null && wait <= DAY_MS ? wait : null;
}

function untilMs(value: string, now: number): number | null {
	const ms = parseDuration(value);
	if (ms !== undefined) return ms;
	const date = Date.parse(value);
	return Number.isNaN(date) ? null : Math.max(0, date - now);
}
