# telemetryingestor

CLI that ingests a secOps telemetry CSV and forwards the records to the
`telemetryprocessor` microservice.

## Build & run

```bash
mise run build-cli                       # -> bin/telemetryingestor
# or, without mise:
go build -o bin/telemetryingestor ./apps/telemetryingestor
go run ./apps/telemetryingestor <command> [flags]
```

## Commands

The CLI is organised into subcommands:

```bash
telemetryingestor ingest   -file <path> [-endpoint <url>] [-timeout <dur>] [-where <expr>] [-dry-run] [-skip-preflight]
telemetryingestor validate -file <path> [-quiet]
telemetryingestor filter   -file <path> -where <expr> [-where <expr> ...]
telemetryingestor help
```

Run `telemetryingestor <command> -h` for command-specific flags.

### `ingest`

Reads the CSV, applies any filters, and POSTs the records to the processor as a
single request, then prints an end-of-run summary.

The CLI does **not** chunk. Splitting the file up would only move batching to
the wrong place: the processor already coalesces records from every client into
full 20-item upstream batches, and a client-side split just makes those batches
worse. See [Throughput](#throughput).

| Flag          | Env var                  | Default                        | Description                                       |
| ------------- | ------------------------ | ------------------------------ | ------------------------------------------------- |
| `-file`       | —                        | — (required)                   | Path to the CSV file to ingest.                   |
| `-endpoint`   | `EYESECURITY_ENDPOINT`   | `http://localhost:8080/ingest` | Processor ingest endpoint.                        |
| `-timeout`    | `EYESECURITY_TIMEOUT`    | derived from the record count  | HTTP timeout. The default allows for the Analytics rate limit. |
| `-where`      | —                        | none                           | Filter expression (repeatable, ANDed). See below. |
| `-dry-run`    | —                        | `false`                        | Parse and filter but send nothing.                |
| `-skip-preflight` | —                    | `false`                        | Skip the health check against the processor before sending. |

```bash
# ingest everything on the default endpoint
telemetryingestor ingest -file docs/example_data_2.csv

# only Defender phishing events
telemetryingestor ingest -file docs/example_data_2.csv \
  -where source=defender -where category~phis

# preview what would be sent, without hitting the network
telemetryingestor ingest -file docs/example_data_2.csv -where category~phis -dry-run
```

Example output:

```
Ingesting docs/example_data_2.csv
  endpoint:   http://localhost:8080/ingest
  filter:     category~phis
  processor:  reachable and healthy
  records:    196
  estimated:  1m30s at 20 records / 10s (upstream rate limit)
  timeout:    2m22.5s

  sending...
  [ 20/196]  10.2% | enriched 36 | elapsed 12s | eta 1m45s
  [ 40/196]  20.4% | enriched 56 | elapsed 22s | eta 1m26s
  [ 60/196]  30.6% | enriched 76 | elapsed 32s | eta 1m13s
  ...
  done

Summary:
  rows read:            999
  filtered out:         802
  skipped (malformed):  1
  sent:                 196
  ingested:             196
```

### Preflight check

Before sending (and unless `-dry-run` or `-skip-preflight` is set), `ingest`
`GET`s the processor's `/health` endpoint, derived from `-endpoint` by
replacing the trailing `/ingest` path. This turns "processor is down" or
"wrong `-endpoint`" into an immediate, actionable error instead of a hung
request that eventually times out:

```
error: processor pre-check failed for http://localhost:8080/health: dial tcp ...

Nothing is listening at http://localhost:8080/ingest. Start the processor, for example:

  go run ./apps/telemetryprocessor/cmd/server

then re-run this command. Use -endpoint or EYESECURITY_ENDPOINT to point at a different
instance, or -dry-run to parse the CSV without sending anything.
```

The hint is tailored to the failure: a DNS error points at `-endpoint`/
`EYESECURITY_ENDPOINT`, a timeout suggests `-skip-preflight` (the processor
may just be slow to start), and a reachable-but-unhealthy response points at
the processor's own logs and its Enrichment/Analytics dependencies.

### Progress reporting

A large ingest is one long request — 998 records take about 8 minutes — so the
processor streams progress back as newline-delimited JSON and the CLI prints a
line per upstream batch (roughly one per 10 s rate-limit window).

`ingested` is the headline number because it is the only one that means a record
reached the Analytics Service. `enriched` is shown next to it on purpose: the
gap between the two **is** the processor's queue depth, so you can watch
backpressure open up and then close as the run drains.

The `eta` is extrapolated from the observed rate rather than the up-front
estimate, so it self-corrects — the estimate assumes perfectly full batches,
which a run that drops records never achieves.

If the stream stops before the final result — the processor died, or something
cut the connection — the CLI reports **`outcome unknown`** rather than
summarising a partial run as success. Records already forwarded upstream stay
forwarded, so this is the same situation as a timeout and gets the same
treatment.

`sent` is what the processor accepted; `ingested` is what it confirms reached
the Analytics Service, read from the response body. They differ when records
are dropped upstream, and the summary breaks the difference down
(`dropped (category|enrichment|analytics)`). The exit code is non-zero whenever
they differ.

A request that times out is reported as **`outcome unknown`**, not as failed:
the processor may well have completed the work after the CLI hung up, so
calling it a failure would be wrong and a blind retry would duplicate records
upstream. Re-run deliberately, with a longer `-timeout`.

### Interrupting a run (Ctrl-C)

`SIGINT`/`SIGTERM` are caught rather than killing the process outright. Where
that lands depends on how far the run got:

- **Before anything was sent** (still reading/filtering the CSV): the CLI
  prints `interrupted before sending; nothing was ingested` and exits `1`. No
  network request was made, so there is nothing to reconcile.
- **Mid-send**: the counts shown are the last ones the processor confirmed,
  reported as `outcome unknown` and flagged `interrupted` in the summary —
  a few records already queued upstream (in the processor's batcher) may
  still land after the CLI exits. As with a timeout, re-running will
  duplicate whatever did land.

### Throughput

The Analytics Service allows 1 request of ≤ 20 items per 10 s, so the pipeline
tops out at **20 records / 10 s** — about 8.5 minutes for the 999-row sample
file. Nothing client-side changes that number, which is why the CLI does not
try: it sends everything in one request and lets the processor's shared batcher
keep every upstream batch full.

`-timeout` therefore defaults to `floor + 25% + 30s`, where
`floor = (ceil(records/20) - 1) × 10s`, so a run is never cancelled merely for
waiting its turn. The CLI prints the estimate before it starts.

The proportional part matters: `floor` assumes *perfectly full* batches, and a
real run does not manage that. One enrichment retry ladder can stall a batch
past the batcher's flush delay, and every short batch costs another whole
window. With a flat 30 s — 6% of an 8-minute run — a handful of short batches
made the CLI hang up on a run that was about to succeed and report `outcome
unknown`, which is precisely the state that invites a duplicate-producing
re-run.

### `validate`

Parses the CSV and reports problems **without ingesting**. Issues are split by
severity:

- **error** (blocks ingestion, exits non-zero): missing/non-integer `id`,
  missing/invalid `ip`, missing `asset_name`, wrong number of fields on a row.
- **warning** (advisory, does not block): empty or unrecognised `category`
  (the latter flagged explicitly as "the processor will drop this record"),
  duplicate `id`, `null`/empty `source`, unparseable `created_utc`.

```bash
telemetryingestor validate -file docs/example_data_2.csv
telemetryingestor validate -file docs/example_data_2.csv -quiet   # summary only
```

```
line 342 [error] id: id "notanint" is not an integer
line 511 [warning] category: empty category

Validated 999 rows: 2 error(s), 6 warning(s).
CSV has blocking errors; fix them before ingesting.
```

Exit codes: `0` = no errors, `1` = blocking errors found, `2` = usage error.

### `filter`

Applies a filter and prints matching rows as CSV to **stdout** (feedback goes to
stderr), so it composes with pipes and can be saved and re-ingested. This is the
standalone form of the same `-where` engine used by `ingest`.

```bash
telemetryingestor filter -file docs/example_data_2.csv -where category~phis > phishing.csv
telemetryingestor filter -file docs/example_data_2.csv -where source=defender -where category=phising
```

## Filter language

`-where` accepts two operators, both **case-insensitive**, and is repeatable —
multiple predicates are ANDed:

| Form            | Meaning              | Example              |
| --------------- | -------------------- | -------------------- |
| `column=value`  | exact match          | `category=phising`   |
| `column~value`  | substring / contains | `category~phis`      |

Case-insensitivity is intentional: the sample data has inconsistent casing
(`phising` vs `Phising`). Valid columns:
`id, asset_name, ip, created_utc, source, category`. An unknown column is
rejected up front with a clear error rather than silently matching nothing.

## Configuration precedence

For settings that have both a flag and an env var, the order is:

**CLI flag > environment variable > built-in default.**

A flag left at its default does not override the env var — only a flag the user
actually passes wins.

## CSV format

The reader expects a `;`-delimited file with this exact header:

```
id;asset_name;ip;created_utc;source;category
```

`created_utc` uses the `DD/MM/YYYY HH:MM` layout (e.g. `27/02/2024 00:00`).

## Assumptions

- `category` is forwarded **verbatim**; normalising the free-text values to the
  Enrichment Service's enum is the processor's responsibility.
- `source` and `created_utc` are **not** part of the processor's ingest
  contract and are dropped when sending.
- Malformed rows (wrong field count, non-integer `id`) are **skipped and
  counted** in the feedback rather than aborting the whole run.
- Batching and rate limiting are entirely the processor's concern; the CLI has
  no knowledge of the 20-item upstream limit beyond estimating how long a run
  will take.
- The whole filtered file is held in memory before sending. At ~60 bytes per
  record that is negligible next to the hours the rate limit would need for a
  file large enough to matter.
- Records are **not** idempotent end to end: the CLI never retries on its own,
  because re-sending would duplicate anything the processor had already
  forwarded. De-duplicating on `id` upstream is the prerequisite for automatic
  retries.
