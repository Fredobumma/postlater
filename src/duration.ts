const UNITS: Record<string, number> = {
	ns: 1e-6,
	us: 1e-3,
	µs: 1e-3,
	μs: 1e-3,
	ms: 1,
	s: 1000,
	m: 60_000,
	h: 3_600_000,
	d: 86_400_000,
};

const PART = /(\d+(?:\.\d+)?)(ns|us|µs|μs|ms|s|m|h|d)/y;

/**
 * Reads a duration as QStash takes it, such as `10s`, `5m`, `6m5s`, `250ms` or `2d`, or a bare number of seconds, and
 * returns it in milliseconds. Returns undefined for anything else.
 */
export function parseDuration(value: string): number | undefined {
	const text = value.trim();
	if (/^\d+(\.\d+)?$/.test(text)) return Math.round(Number(text) * 1000);

	let total = 0;
	PART.lastIndex = 0;
	while (PART.lastIndex < text.length) {
		const start = PART.lastIndex;
		const match = PART.exec(text);
		if (!match || match.index !== start) return undefined;
		total += Number(match[1]) * (UNITS[match[2] as string] as number);
	}
	return text.length > 0 ? Math.round(total) : undefined;
}
