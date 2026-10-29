import { createHash, randomBytes } from 'node:crypto';
import { base64url, SignJWT } from 'jose';

/**
 * Signs a delivery the way QStash does, so `Receiver.verify` from `@upstash/qstash`, and every framework helper built
 * on it, accepts it unchanged: an HS256 JWT keyed with the signing key's text, issued by "Upstash", whose subject is
 * the destination URL and whose `body` claim is the SHA-256 of the body, in base64url.
 */
export async function signDelivery(key: string, url: string, body: Uint8Array): Promise<string> {
	const now = Math.floor(Date.now() / 1000);
	return await new SignJWT({ body: base64url.encode(createHash('sha256').update(body).digest()) })
		.setProtectedHeader({ alg: 'HS256', typ: 'JWT' })
		.setIssuer('Upstash')
		.setSubject(url)
		.setIssuedAt(now)
		.setNotBefore(now)
		.setExpirationTime(now + 300)
		.setJti(`jwt_${randomBytes(16).toString('hex')}`)
		.sign(new TextEncoder().encode(key));
}
