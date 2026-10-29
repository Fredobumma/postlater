import { randomBytes } from 'node:crypto';

const ALPHABET = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';

/** A random id in base62, with QStash's prefixes: `msg_` for messages, `scd_` for schedules, `sig_` for keys. */
export function randomId(prefix: string, length = 24): string {
	const bytes = randomBytes(length);
	let id = '';
	for (const byte of bytes) id += ALPHABET[byte % 62];
	return prefix + id;
}

export const messageId = () => randomId('msg_');
export const scheduleId = () => randomId('scd_');
export const dlqId = () => randomId('dlq_');
export const signingKey = () => randomId('sig_', 32);
