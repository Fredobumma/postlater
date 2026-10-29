import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import pkg from '../package.json' with { type: 'json' };
import { createApp } from '../src/api.js';
import { loadConfig } from '../src/config.js';
import type { Service } from '../src/service.js';

const spec = JSON.parse(readFileSync(new URL('../src/openapi.json', import.meta.url), 'utf8'));

/** A path with its parameters blanked, so `/v2/queues/:name` and `/v2/queues/{queueName}` compare equal. */
const shape = (path: string) => path.replace(/\{[^}]+\}|:[A-Za-z]+|\*/g, '{}');

describe('the OpenAPI description', () => {
	it('documents every route, and nothing that is not one', () => {
		const app = createApp({} as Service, loadConfig({ QSTASH_TOKEN: 'a-long-enough-token-1' }));
		const routes = new Set(
			app.routes.filter((r) => r.method !== 'ALL').map((r) => `${r.method.toLowerCase()} ${shape(r.path)}`),
		);
		const documented = new Set(
			Object.entries(spec.paths as Record<string, Record<string, unknown>>).flatMap(([path, operations]) =>
				Object.keys(operations).map((method) => `${method} ${shape(path)}`),
			),
		);
		expect([...routes].filter((r) => !documented.has(r))).toEqual([]);
		expect([...documented].filter((r) => !routes.has(r))).toEqual([]);
	});

	it('has the package version', () => {
		expect(spec.info.version).toBe(pkg.version);
	});
});
