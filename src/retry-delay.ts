/**
 * The retry delay expressions of `Upstash-Retry-Delay`, such as `pow(2, retried) * 1000`: arithmetic on the number of
 * times the message has been retried, giving the delay in milliseconds. Parsed into a tree once, at publish time, and
 * evaluated without `eval`.
 */

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
