# SecOps Telemetry Ingestion

Go monorepo for ingesting SecOps malicious-activity CSV logs into the Analytics
Service

```
apps/
  telemetryingestor/     CLI — reads the CSV file, calls telemetryprocessor
  telemetryprocessor/    microservice — enriches records, forwards to Analytics Service
```

## Toolchain

Go version is pinned via [mise](https://mise.jdx.dev) in
[.mise.toml](.mise.toml).

NOTE: If you don't have mise installed yet:

```bash
curl https://mise.run | sh

# Alternatively
brew install mise
```

```bash
mise install                 # installs the pinned Go version (and air, for hot reload)
mise run build-cli           # builds bin/telemetryingestor
mise run build-processor     # builds bin/telemetryprocessor
mise run run-processor       # runs the microservice on :8080
mise run dev-processor       # runs the microservice with hot reload (air), see .air.toml
mise run test                # go test across both modules
```

Without mise, the equivalent plain `go` commands work too, e.g.:

```bash
go run ./apps/telemetryingestor ingest -file docs/example_data_2.csv
go run ./apps/telemetryprocessor/cmd/server
```

## CLI (`telemetryingestor`)

The CLI has `ingest`, `validate`, and `filter` subcommands. Full usage, flags,
the filter language, configuration precedence, and stated assumptions are
documented in
[apps/telemetryingestor/README.md](apps/telemetryingestor/README.md).

## Microservice (`telemetryprocessor`)

The processor exposes a single `POST /ingest` endpoint. For each record it
normalises the free-text category, enriches it via the Enrichment Service
(retries + circuit breaker for the known intermittent failures), and forwards
the result to the rate-limited Analytics Service (batching + a shared
token-bucket limiter, 20 messages/10s). Endpoints, the API key, and all
retry/rate-limit tuning are overridable from environment variables — see
[apps/telemetryprocessor/README.md](apps/telemetryprocessor/README.md).
