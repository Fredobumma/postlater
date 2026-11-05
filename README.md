# Postlater

A self-hosted message server that speaks the QStash API. Publish an HTTP message now, and Postlater delivers it later: after a delay, on a cron schedule, in order through a queue, retried until it gets through, with callbacks and a dead-letter queue when it doesn't. Messages are kept in your own Postgres.

Apps built on [`@upstash/qstash`](https://github.com/upstash/qstash-js) and [`@upstash/workflow`](https://github.com/upstash/workflow-js) work against it unchanged: set four environment variables and they talk to Postlater instead of Upstash.

## Why

QStash is Upstash's hosted service for background jobs over HTTP, and a number of popular open-source apps are built on it, among them [Dub](https://github.com/dubinc/dub), [Papermark](https://github.com/papermark/papermark), [LobeHub](https://github.com/lobehub/lobehub), [Inbox Zero](https://github.com/elie222/inbox-zero) and [openstatus](https://github.com/openstatusHQ/openstatus). Self-hosting them still meant an Upstash account, and a server that Upstash can reach from the internet to call back. Upstash's local server doesn't fill the gap: it keeps everything in memory, and its licence is for development and testing only.

Postlater is an open-source server for the same API, so those apps can run entirely on your own machines.

- **Nothing leaves your network.** Payloads such as emails, documents and chat messages stay on your servers.
- **Private addresses work.** Postlater can deliver to `localhost` and internal hosts that a hosted service can't reach.
- **Real persistence for development and CI**, with no in-memory server and no account.
- **No bill per message.**

## Quick start

```sh
QSTASH_TOKEN=$(openssl rand -hex 24) npx postlater
```

With no `DATABASE_URL`, Postlater keeps its data in `./data` using an embedded Postgres ([PGlite](https://pglite.dev)), which is enough for development and small installs. For production, give it a Postgres:

```sh
docker build -t postlater .
docker run -p 8080:8080 \
  -e QSTASH_TOKEN=your-long-random-token \
  -e DATABASE_URL=postgres://user:password@db:5432/postlater \
  postlater
```

### Point your app at it

Every app built on the QStash SDKs reads the same variables:

```sh
QSTASH_URL=http://postlater:8080
QSTASH_TOKEN=your-long-random-token
QSTASH_CURRENT_SIGNING_KEY=…
QSTASH_NEXT_SIGNING_KEY=…
```

Postlater makes the signing keys on first start. Read them with the token:

```sh
curl -H "Authorization: Bearer $QSTASH_TOKEN" http://localhost:8080/v2/keys
```

Or choose them yourself, with `QSTASH_CURRENT_SIGNING_KEY` and `QSTASH_NEXT_SIGNING_KEY` on Postlater too.

## What works

Tested against the official SDKs, `@upstash/qstash` 2.12 and `@upstash/workflow` 1.5, which run as the clients in Postlater's test suite. Every delivery is checked with the SDK's own `Receiver.verify`.

|                   |                                                                                                                                                                                                  |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Publishing        | `publish`, `publishJSON`, `batch`, `batchJSON`; to a URL or a URL group                                                                                                                          |
| Delivery settings | method, delay, not-before, retries, retry delay expressions, timeout, forwarded headers, labels                                                                                                  |
| Retries           | QStash's default backoff or your own expression; `Retry-After` and the `X-RateLimit-Reset` headers are honoured on any error, up to a day; `489` with `Upstash-NonRetryable-Error` stops at once |
| Callbacks         | callback after each attempt, failure callback after the last, both configurable with `Upstash-Callback-*` headers                                                                                |
| Deduplication     | by id or by content                                                                                                                                                                              |
| Queues            | in order, with parallelism; pause, resume, lag                                                                                                                                                   |
| Flow control      | parallelism and rate per key                                                                                                                                                                     |
| Schedules         | cron with `CRON_TZ`, chosen ids, pause and resume                                                                                                                                                |
| URL groups        | add and remove endpoints; fan-out publishing                                                                                                                                                     |
| Dead-letter queue | list, filter, get, delete, publish again                                                                                                                                                         |
| Messages and logs | get, cancel (one or in bulk), deliver now; logs with filters and paging                                                                                                                          |
| Signing           | `Upstash-Signature` JWTs; key rotation                                                                                                                                                           |
| Upstash Workflow  | `context.run`, `context.sleep`, parallel steps and `context.call`                                                                                                                                |

### Not yet

- **Upstash Workflow:** `waitForEvent` and `notify`, the workflow logs and workflow DLQ endpoints, and fetching steps lazily. A workflow that uses these fails with an error rather than quietly.
- **API destinations,** such as publishing to an LLM provider or to Resend through QStash. Publish to the provider's URL instead.
- **Flow control management:** pausing, pinning and resetting a key. Listing keys works.
- **Redaction** (`Upstash-Redact-Fields`) is accepted and ignored.

## Configuration

| Variable                                                | Default           |                                                                                             |
| ------------------------------------------------------- | ----------------- | ------------------------------------------------------------------------------------------- |
| `QSTASH_TOKEN`                                          | required          | The token apps send. Several, separated by commas, for rotating it. At least 16 characters. |
| `DATABASE_URL`                                          | none              | A Postgres URL. Without one, data goes in the embedded database in `DATA_DIR`.              |
| `DATABASE_SCHEMA`                                       | `public`          | Keeps Postlater's tables apart when it shares a database with an app.                       |
| `DATA_DIR`                                              | `./data`          | The embedded database's folder, or `memory`.                                                |
| `QSTASH_CURRENT_SIGNING_KEY`, `QSTASH_NEXT_SIGNING_KEY` | generated         | Signing keys, if you'd rather set them than have them made.                                 |
| `PORT`, `HOST`                                          | `8080`, `0.0.0.0` |                                                                                             |
| `WORKER_CONCURRENCY`                                    | `32`              | Deliveries under way at once, per server.                                                   |
| `DEFAULT_RETRIES`, `MAX_RETRIES`                        | `3`, `20`         |                                                                                             |
| `DEFAULT_TIMEOUT`, `MAX_TIMEOUT`                        | `5m`, `2h`        | How long one delivery attempt may take.                                                     |
| `DEDUPLICATION_WINDOW`                                  | `10m`             |                                                                                             |
| `EVENTS_RETENTION`, `DLQ_RETENTION`                     | `7d`, `7d`        | How long logs and dead-letter entries are kept.                                             |
| `TRUST_PROXY`                                           | `false`           | Take the caller's address from `X-Forwarded-For`.                                           |
| `PUBLIC_METRICS`                                        | `false`           | Serve `/metrics` without the token.                                                         |
| `MAX_BODY_BYTES`                                        | 10 MB             |                                                                                             |

## How it works

Published messages are rows in Postgres. A worker in each Postlater server claims due messages with `SELECT … FOR UPDATE SKIP LOCKED`, delivers them, and records the outcome. Several servers can share one database: each message is delivered once, and each queue and flow control key is claimed by one server at a time. The test suite runs two servers on one database to check this.

Delivery is at least once. A server that stops in the middle of a delivery leaves a lease on the message; when the lease runs out, after the message's timeout plus a minute, another server or the same one after a restart delivers it again. Receivers should be idempotent, as with QStash; `Upstash-Message-Id` is there for that.

Schedules run in UTC unless they start with `CRON_TZ=`. A schedule missed while every server was down runs once when one comes back, not once for every time it missed.

`GET /health` checks the server and its database. `GET /metrics` gives Prometheus metrics for messages by state, the DLQ and schedules. `GET /openapi.json` describes the whole API.

## Security

- **The token is the key to everything.** Anyone with it can publish, and Postlater will make requests on their behalf to any address it can reach, internal ones included, since reaching them is part of the point. Keep the token secret, and don't expose Postlater beyond the apps that use it.
- **Use TLS in front of it** when apps reach it over a network you don't trust. Postlater serves plain HTTP.
- **Deliveries are signed.** Receivers that verify `Upstash-Signature`, as the SDK's framework helpers do, reject requests that didn't come from Postlater.

## Development

```sh
pnpm install
pnpm check                # types, lint and tests, on the embedded database
TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres pnpm test   # on a real Postgres too
```

## License

MIT. Postlater is not affiliated with Upstash. QStash is a trademark of Upstash, used here to say what Postlater is compatible with.
