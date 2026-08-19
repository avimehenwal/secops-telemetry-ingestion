# telemetryprocessor

The microservice that receives records from the `telemetryingestor` CLI,
enriches each one via the **Enrichment Service**, and forwards the enriched
events to the rate-limited **Analytics Service** (see
[docs/openapi.json](../../docs/openapi.json)).

## Endpoints

| Method | Path       | Purpose                                                   |
| ------ | ---------- | --------------------------------------------------------- |
| `POST` | `/ingest`  | The single endpoint the CLI calls. Body: `{"records":[…]}`. |
| `GET`  | `/health`  | Liveness/readiness probe.                                 |

`POST /ingest` returns a JSON summary so the CLI can give the operator
actionable feedback:

```json
{
  "received": 20, "enriched": 19, "ingested": 19,
  "failedEnrichment": 0, "failedCategory": 1, "failedAnalytics": 0,
  "recordErrors": [{ "id": 993425, "stage": "category", "reason": "unrecognised category \"xyz\"" }]
}
```

- **200** — the run completed (fully or partially); the counts tell the story.
  This includes a payload whose records were *all* rejected for an unrecognised
  category: that is a problem with the CSV, not with a dependency.
- **502** — records actually reached an upstream service and **none** of them
  landed (e.g. Enrichment Service down, or Analytics rejecting everything); the
  CLI exits non-zero. Records dropped at the category stage are excluded from
  this judgement, so a bad CSV never sends the operator hunting a healthy service.
- **400** — malformed request body. **405** — non-POST.

`/ingest` is unauthenticated: per the assessment brief, authn/authz of our own
API is out of scope. In production this would sit behind the same gateway auth
as the rest of the estate.

```bash
# normal ingest
curl -sS -X POST http://localhost:8080/ingest \
  -H 'Content-Type: application/json' \
  -d '{"records":[{"id":1,"asset":"a","ip":"1.2.3.4","category":"phising"}]}'

# liveness/readiness probe
curl -sS http://localhost:8080/health
```

### Dry-run

Add `?dry-run=true` to `/ingest` to **validate records without calling the
Enrichment or Analytics microservices**. Categories are still normalised
(so bad categories are still reported), but nothing is enriched or forwarded
downstream — useful for checking a payload, smoke-testing a deploy, or
exercising the ingest path with zero side effects.

```bash
# dry-run: validate only, no downstream calls
curl -sS -X POST 'http://localhost:8080/ingest?dry-run=true' \
  -H 'Content-Type: application/json' \
  -d '{"records":[{"id":1,"asset":"a","ip":"1.2.3.4","category":"phising"}]}'
```

Accepted truthy values: `1`, `true` (any case), `yes`. The response echoes the
mode and reports how many records *would* have been sent as `ingested`
(`enriched` stays `0`, and a zero-enriched dry-run stays **200**, never `502`):

```json
{ "received": 1, "enriched": 0, "ingested": 1, "failedCategory": 0, "dryRun": true }
```

## Processing pipeline

For each record:

1. **Normalise category.** The CSV `category` is free text with inconsistent
   casing, punctuation and typos (`phising`, `explaoit-public facing`,
   `valida_accounts`, `compromise (driveby)`). It is canonicalised to the
   Enrichment enum (see `libs/category`, shared with the CLI). Unrecognised values are reported
   and skipped rather than sent (which would 400).
2. **Enrich.** Call the Enrichment Service. Its ASN, MITRE category handle and
   correlation id are required to build the Analytics event, so a record that
   cannot be enriched cannot be ingested — it is reported per-record.
3. **Forward.** Enriched events are batched and sent to the Analytics Service.

### Resilience

- **Enrichment** is known to be intermittently unreliable, so calls are retried
  with exponential backoff (network errors, 429, 5xx) and fronted by a
  **circuit breaker** that fails fast once the service is clearly down. A `400`
  is treated as a permanent per-record error and is not retried.
- **Analytics** is rate limited to **1 request / 10 s** and accepts at most 20
  items per request (see `/analytics` in `docs/openapi.json`). Because a
  request costs a whole window however full it is, the batch size *is* the
  throughput — so a single process-wide **`analytics.Batcher`** collects
  records from every in-flight ingest request and emits full 20-item batches.
  A partial batch is flushed after `ANALYTICS_BATCH_DELAY` rather than being
  stranded. Every *request* — including 429 retries — is gated through a
  **shared token-bucket limiter**. `429` responses are retried, honouring
  `Retry-After` (the live service returns `Retry-After: 10`).
- **A 200 from Analytics is not a receipt.** The response carries an
  `itemsIngested` count that is allowed to be lower than what we sent, and a
  `status` that can be `error`. Both are treated as a per-record failure for the
  shortfall rather than being rounded up to success — otherwise a
  mission-critical pipeline reports "ingested 999" while quietly losing records.
  The service says *how many* landed, never *which*, so the shortfall is
  attributed to the tail of the batch; the count is what reaches the operator.

## Configuration

Everything is overridable from environment variables (env var > default). The
process fails fast at startup on a malformed value.

| Env var                       | Default                                | Description                                        |
| ----------------------------- | -------------------------------------- | -------------------------------------------------- |
| `PROCESSOR_ADDR`              | `:8080`                                | Listen address.                                    |
| `ENRICHMENT_URL`              | `https://api.heyering.com/enrichment`  | Enrichment Service endpoint.                       |
| `ANALYTICS_URL`               | `https://api.heyering.com/analytics`   | Analytics Service endpoint.                        |
| `EYE_API_KEY`                 | `eye-am-hiring`                        | `Authorization` header sent to both services.      |
| `ENRICHMENT_TIMEOUT`          | `10s`                                  | Per-attempt HTTP timeout.                          |
| `ENRICHMENT_MAX_ATTEMPTS`     | `4`                                    | Total attempts (1 = no retry).                     |
| `ENRICHMENT_BACKOFF_BASE`     | `200ms`                                | First retry delay, doubled each attempt.           |
| `ENRICHMENT_BACKOFF_MAX`      | `5s`                                   | Cap on a single backoff sleep.                     |
| `ENRICHMENT_BREAKER_TRIP`     | `5`                                    | Consecutive failures before the breaker opens (0 disables). |
| `ENRICHMENT_BREAKER_COOLDOWN` | `15s`                                  | How long the breaker stays open.                   |
| `ANALYTICS_TIMEOUT`           | `10s`                                  | Per-attempt HTTP timeout.                          |
| `ANALYTICS_BATCH_SIZE`        | `20`                                   | Events per upstream request (1–20).                |
| `ANALYTICS_BATCH_DELAY`       | `2s`                                   | How long a partial batch waits for company.        |
| `ANALYTICS_QUEUE_SIZE`        | `200`                                  | Events buffered before ingest requests block.      |
| `ANALYTICS_BATCH_GRACE`       | `30s`                                  | Budget for delivering queued events at shutdown.   |
| `ANALYTICS_RATE_REQUESTS`     | `1`                                    | Requests permitted per window (upstream allows 1). |
| `ANALYTICS_RATE_WINDOW`       | `10s`                                  | The rate-limit window.                             |
| `ANALYTICS_MAX_RETRIES`       | `5`                                    | Retries on HTTP 429.                               |

```bash
mise run run-processor          # or: go run ./apps/telemetryprocessor/cmd/server
mise run dev-processor          # hot reload via air

# example: point at a local mock and loosen the rate limit for testing
ENRICHMENT_URL=http://localhost:8091/enrichment \
ANALYTICS_URL=http://localhost:8091/analytics \
ANALYTICS_RATE_REQUESTS=50 ANALYTICS_RATE_WINDOW=1s \
  go run ./apps/telemetryprocessor/cmd/server
```

The server shuts down gracefully on `SIGINT`/`SIGTERM`, draining in-flight
ingests (which can be slow under the Analytics rate limit) for up to 30 s.

## Throughput note (rate limit ↔ CLI)

The Analytics limit (1 request / 10 s, ≤ 20 items each) is the pipeline's hard
bottleneck: **20 records per 10 s, ~8.5 minutes for a 1000-row file**, and no
amount of client-side concurrency changes that. A single `/ingest` call
carrying *N* records blocks for roughly `(ceil(N/20) - 1) × 10s`.

Batching happens **once, process-wide**, not per HTTP request. That matters:
if each request batched its own records, two clients posting 25 each would burn
four windows on 50 records instead of three, and every short trailing batch
would waste a full window. With the shared batcher the client's request size is
irrelevant to upstream efficiency — which is why the CLI no longer chunks at
all and simply posts the whole file.

Backpressure falls out of the same design: `Batcher.Submit` blocks once the
queue is full, so a large ingest is throttled by the rate limit rather than
piling up in memory.

The honest next step for production is to stop modelling this as a synchronous
call: have `/ingest` return `202` with a job id and drain the queue in the
background, so an 8-minute file is not one hanging request.
