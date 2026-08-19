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
- **502** — records were supplied but nothing could be enriched/forwarded
  (e.g. Enrichment Service down); the CLI exits non-zero.
- **400** — malformed request body. **405** — non-POST.

## Processing pipeline

For each record:

1. **Normalise category.** The CSV `category` is free text with inconsistent
   casing, punctuation and typos (`phising`, `explaoit-public facing`,
   `valida_accounts`, `compromise (driveby)`). It is canonicalised to the
   Enrichment enum (see `internal/category`). Unrecognised values are reported
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
- **Analytics** is rate limited (20 messages / 10 s) and accepts at most 20
  items per request. The client batches to ≤ 20 and gates every event through a
  **shared token-bucket limiter**, so the process stays within quota *even
  across concurrent ingest requests*. `429` responses are retried, honouring
  `Retry-After`.

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
| `ANALYTICS_BATCH_SIZE`        | `20`                                   | Max events per request (1–20).                     |
| `ANALYTICS_RATE_LIMIT`        | `20`                                   | Messages permitted per window.                     |
| `ANALYTICS_RATE_WINDOW`       | `10s`                                  | The rate-limit window.                             |
| `ANALYTICS_MAX_RETRIES`       | `5`                                    | Retries on HTTP 429.                               |

```bash
mise run run-processor          # or: go run ./apps/telemetryprocessor/cmd/server
mise run dev-processor          # hot reload via air

# example: point at a local mock and loosen the rate limit for testing
ENRICHMENT_URL=http://localhost:8091/enrichment \
ANALYTICS_URL=http://localhost:8091/analytics \
ANALYTICS_RATE_LIMIT=50 ANALYTICS_RATE_WINDOW=1s \
  go run ./apps/telemetryprocessor/cmd/server
```

The server shuts down gracefully on `SIGINT`/`SIGTERM`, draining in-flight
ingests (which can be slow under the Analytics rate limit) for up to 30 s.

## Throughput note (rate limit ↔ CLI)

The Analytics limit (20 messages / 10 s) is the pipeline's bottleneck: a single
`/ingest` call carrying *N* records blocks for roughly `N/20 × 10s` while the
limiter drains. Because the limiter is **shared**, splitting a large file into
many small CLI requests does **not** exceed the global quota — so for big
ingests prefer a small `-chunk-size` (e.g. `20`) on the CLI, which keeps each
HTTP request short and within the CLI's client timeout while overall throughput
stays capped at the mandated rate.
