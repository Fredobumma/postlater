import { describe, expect, it } from 'vitest';
import { ConfigError, loadConfig } from '../src/config.js';
import { parseDuration } from '../src/duration.js';
import { PublishError, parsePublishHeaders, toHeaderMap, unprefixCallbackHeaders } from '../src/publish.js';
import { parseRetryDelay, RetryDelayError, retryDelayMs } from '../src/retry-delay.js';
import { nextCronRun, parseCron } from '../src/service.js';

const limits = { defaultRetries: 3, maxRetries: 20, defaultTimeoutMs: 300_000, maxTimeoutMs: 7_200_000 };
const parse = (headers: Record<string, string>) => parsePublishHeaders(toHeaderMap(headers), limits);

describe('durations', () => {
	it('reads what QStash takes', () => {
		expect(parseDuration('10s')).toBe(10_000);
		expect(parseDuration('5m')).toBe(300_000);
		expect(parseDuration('1h30m')).toBe(5_400_000);
		expect(parseDuration('2d')).toBe(172_800_000);
		expect(parseDuration('250ms')).toBe(250);
		expect(parseDuration('30')).toBe(30_000);
	});

	it('refuses anything else', () => {
		for (const value of ['', 'ten seconds', '10x', '5m garbage', '-5s']) {
			expect(parseDuration(value), value).toBeUndefined();
		}
	});
});

describe('retry delay expressions', () => {
	it('evaluates arithmetic on retried', () => {
		expect(retryDelayMs('1000', 1)).toBe(1000);
		expect(retryDelayMs('pow(2, retried) * 1000', 3)).toBe(8000);
		expect(retryDelayMs('max(500, 100 * retried)', 2)).toBe(500);
		expect(retryDelayMs('min(1000, 100 * retried + 50)', 4)).toBe(450);
		expect(retryDelayMs('(retried + 1) * -1 + 10', 1)).toBe(8);
		expect(retryDelayMs('floor(sqrt(retried * 100))', 2)).toBe(14);
	});

	it('keeps delays between zero and a day', () => {
		expect(retryDelayMs('0 - 5000', 1)).toBe(0);
		expect(retryDelayMs('1 / 0', 1)).toBe(0);
		expect(retryDelayMs('pow(10, 12)', 1)).toBe(86_400_000);
	});

	it('defaults to QStash backoff', () => {
		expect(retryDelayMs(null, 1)).toBe(Math.round(Math.exp(2.5) * 1000));
		expect(retryDelayMs(null, 20)).toBe(86_400_000);
	});

	it('refuses anything that is not arithmetic on retried', () => {
		for (const expression of ['process.exit()', 'retried; 1', 'eval(1)', 'pow(2)', '1 +', '(1', 'x'.repeat(300)]) {
			expect(() => parseRetryDelay(expression), expression).toThrow(RetryDelayError);
		}
	});
});

describe('publish headers', () => {
	it('reads every setting', () => {
		const options = parse({
			'Content-Type': 'application/json',
			'Upstash-Method': 'put',
			'Upstash-Delay': '30s',
			'Upstash-Not-Before': '1800000000',
			'Upstash-Retries': '5',
			'Upstash-Retry-Delay': '1000 * retried',
			'Upstash-Timeout': '45s',
			'Upstash-Callback': 'https://example.com/cb',
			'Upstash-Callback-Retries': '1',
			'Upstash-Failure-Callback': 'https://example.com/failed',
			'Upstash-Failure-Callback-Forward-X-Id': '7',
			'Upstash-Forward-Authorization': 'Bearer abc',
			'Upstash-Deduplication-Id': 'order-1',
			'Upstash-Flow-Control-Key': 'tenant',
			'Upstash-Flow-Control-Value': 'parallelism=2, rate=10, period=1m',
			'Upstash-Label': 'billing,eu',
			'X-Not-Forwarded': 'no',
		});
		expect(options).toMatchObject({
			method: 'PUT',
			delayMs: 30_000,
			notBefore: 1_800_000_000_000,
			retries: 5,
			retryDelay: '1000 * retried',
			timeoutMs: 45_000,
			callback: 'https://example.com/cb',
			failureCallback: 'https://example.com/failed',
			callbackHeaders: { retries: ['1'] },
			failureCallbackHeaders: { 'forward-x-id': ['7'] },
			deduplicationId: 'order-1',
			flowControl: { key: 'tenant', parallelism: 2, rate: 10, periodSeconds: 60 },
			labels: ['billing', 'eu'],
		});
		expect(options.forward).toEqual({ 'content-type': ['application/json'], authorization: ['Bearer abc'] });
	});

	it('uses the defaults', () => {
		const options = parse({});
		expect(options).toMatchObject({ method: 'POST', delayMs: 0, notBefore: null, retries: 3, timeoutMs: 300_000 });
		expect(options.forward).toEqual({});
	});

	it('passes workflow headers through, and lets a forwarded one replace its twin', () => {
		const options = parse({
			'Upstash-Workflow-RunId': 'wfr_1',
			'Upstash-Workflow-Sdk-Version': '1',
			'Upstash-Forward-Upstash-Workflow-Sdk-Version': '1',
		});
		expect(options.forward).toEqual({ 'upstash-workflow-runid': ['wfr_1'], 'upstash-workflow-sdk-version': ['1'] });
	});

	it('never forwards headers fetch manages', () => {
		expect(parse({ 'Upstash-Forward-Host': 'evil.example', 'Upstash-Forward-Content-Length': '5' }).forward).toEqual(
			{},
		);
	});

	it('refuses bad values', () => {
		const bad: Record<string, string>[] = [
			{ 'Upstash-Method': 'CONNECT' },
			{ 'Upstash-Retries': 'many' },
			{ 'Upstash-Retries': '21' },
			{ 'Upstash-Delay': 'soon' },
			{ 'Upstash-Timeout': '3h' },
			{ 'Upstash-Callback': 'not a url' },
			{ 'Upstash-Callback': 'file:///etc/passwd' },
			{ 'Upstash-Flow-Control-Key': 'k' },
			{ 'Upstash-Flow-Control-Key': 'k', 'Upstash-Flow-Control-Value': 'speed=fast' },
			{ 'Upstash-Retry-Delay': 'require("fs")' },
		];
		for (const headers of bad) {
			expect(() => parse(headers), JSON.stringify(headers)).toThrow(PublishError);
		}
	});

	it('turns callback headers into publish headers', () => {
		expect(unprefixCallbackHeaders({ retries: ['2'], 'forward-x-a': ['b'], 'workflow-runid': ['wfr_1'] })).toEqual({
			'content-type': ['application/json'],
			'upstash-retries': ['2'],
			'upstash-forward-x-a': ['b'],
			'upstash-workflow-runid': ['wfr_1'],
		});
	});
});

describe('cron', () => {
	it('reads CRON_TZ', () => {
		expect(parseCron('CRON_TZ=Europe/Berlin 0 9 * * *')).toEqual({ pattern: '0 9 * * *', timezone: 'Europe/Berlin' });
		expect(parseCron('*/5 * * * *')).toEqual({ pattern: '*/5 * * * *', timezone: 'UTC' });
	});

	it('finds the next run, in UTC unless told otherwise', () => {
		const after = Date.UTC(2026, 9, 29, 8, 30);
		expect(nextCronRun('0 9 * * *', after)).toBe(Date.UTC(2026, 9, 29, 9, 0));
		// 09:00 in Berlin is 08:00 UTC at the end of October, once summer time has ended
		expect(nextCronRun('CRON_TZ=Europe/Berlin 0 9 * * *', after)).toBe(Date.UTC(2026, 9, 30, 8, 0));
	});
});

describe('config', () => {
	it('needs a token', () => {
		expect(() => loadConfig({})).toThrow(ConfigError);
		expect(() => loadConfig({ QSTASH_TOKEN: 'short' })).toThrow(/16 characters/);
	});

	it('reads the environment', () => {
		const config = loadConfig({
			QSTASH_TOKEN: 'a-long-enough-token-1, a-long-enough-token-2',
			PORT: '9000',
			DEFAULT_TIMEOUT: '30s',
			TRUST_PROXY: 'true',
		});
		expect(config.tokens).toEqual(['a-long-enough-token-1', 'a-long-enough-token-2']);
		expect(config.port).toBe(9000);
		expect(config.defaultTimeoutMs).toBe(30_000);
		expect(config.trustProxy).toBe(true);
		expect(config.databaseUrl).toBeNull();
	});

	it('refuses nonsense', () => {
		expect(() => loadConfig({ QSTASH_TOKEN: 'a-long-enough-token-1', PORT: 'eighty' })).toThrow(ConfigError);
		expect(() => loadConfig({ QSTASH_TOKEN: 'a-long-enough-token-1', DEFAULT_TIMEOUT: 'later' })).toThrow(ConfigError);
	});
});
