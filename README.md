# secops-telemetry-ingestion

Go monorepo for ingesting SecOps malicious-activity CSV logs into the Analytics
Service, per the Jira ticket in
[docs/TechAssessment.pdf](docs/TechAssessment.pdf).

Two apps, one `go.work` workspace:

```
apps/
  telemetryingestor/     CLI — reads the CSV file, calls telemetryprocessor
  telemetryprocessor/    microservice — enriches records, forwards to Analytics Service
```

## Toolchain

Go version is pinned via [mise](https://mise.jdx.dev) in
[.mise.toml](.mise.toml).

```bash
mise install                 # installs the pinned Go version
mise run build-cli           # builds bin/telemetryingestor
mise run build-processor     # builds bin/telemetryprocessor
mise run run-processor       # runs the microservice on :8080
mise run test                # go test across both modules
```

Without mise, the equivalent plain `go` commands work too, e.g.:

```bash
go run ./apps/telemetryingestor -file docs/example_data_2.csv
go run ./apps/telemetryprocessor/cmd/server
```

Note: because this is a multi-module `go.work` workspace, `go build ./...` from
the repo root does not expand across modules — use
`./apps/telemetryingestor/...` / `./apps/telemetryprocessor/...` (or the mise
tasks above, which already do this).
