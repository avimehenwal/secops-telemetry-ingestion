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
telemetryingestor ingest   -file <path> [-endpoint <url>] [-chunk-size <n>] [-timeout <dur>] [-where <expr>] [-dry-run]
telemetryingestor validate -file <path> [-quiet]
telemetryingestor filter   -file <path> -where <expr> [-where <expr> ...]
telemetryingestor help
```

Run `telemetryingestor <command> -h` for command-specific flags.

### `ingest`

Streams the CSV, applies any filters, and POSTs the records to the processor in
chunks, printing per-chunk progress and an end-of-run summary.

| Flag          | Env var                  | Default                        | Description                                       |
| ------------- | ------------------------ | ------------------------------ | ------------------------------------------------- |
| `-file`       | —                        | — (required)                   | Path to the CSV file to ingest.                   |
| `-endpoint`   | `EYESECURITY_ENDPOINT`   | `http://localhost:8080/ingest` | Processor ingest endpoint.                        |
| `-chunk-size` | `EYESECURITY_CHUNK_SIZE` | `20`                           | Records to POST per HTTP request. 20 = one upstream Analytics request. |
| `-timeout`    | `EYESECURITY_TIMEOUT`    | derived from `-chunk-size`     | Per-chunk HTTP timeout. Default allows for the Analytics rate limit. |
| `-where`      | —                        | none                           | Filter expression (repeatable, ANDed). See below. |
| `-dry-run`    | —                        | `false`                        | Parse and filter but send nothing.                |

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
  chunk size: 20
  timeout:    30s per chunk
  filter:     category~phis
  processor:  reachable and healthy

  chunk 1: sent 20 records, 20 ingested (20 total)
  ...
  chunk 10: sent 16 records, 16 ingested (196 total)

Summary:
  rows read:            998
  filtered out:         802
  skipped (malformed):  1
  sent:                 196
  ingested:             196
```

`sent` is what the processor accepted; `ingested` is what it confirms reached
the Analytics Service, read from the response body. They differ when records
are dropped upstream, and the summary breaks the difference down
(`dropped (category|enrichment|analytics)`). The exit code is non-zero whenever
they differ.

A chunk whose HTTP request times out is reported as **`outcome unknown`**, not
as failed: the processor may well have completed the work after the CLI hung
up, so calling it a failure would be wrong and a blind retry would duplicate
records upstream. Re-run such chunks deliberately, with a longer `-timeout` or
smaller `-chunk-size`.

### Throughput

The Analytics Service allows 1 request of ≤ 20 items per 10 s, so the pipeline
tops out at **20 records / 10 s** — about 8.5 minutes for the 999-row sample
file. The default `-chunk-size` of 20 maps one chunk onto one upstream request,
and `-timeout` defaults to `(ceil(chunk/20) - 1) × 10s + 30s` so a chunk is
never cancelled merely for waiting its turn.

### `validate`

Parses the CSV and reports problems **without ingesting**. Issues are split by
severity:

- **error** (blocks ingestion, exits non-zero): missing/non-integer `id`,
  missing/invalid `ip`, missing `asset_name`, wrong number of fields on a row.
- **warning** (advisory, does not block): empty `category`, `null`/empty
  `source`, unparseable `created_utc`.

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
- `chunk-size` controls only the CLI→processor request size; the downstream
  20-item Analytics batching and rate limiting are the processor's concern.
  The default is aligned to 20 so the two agree, but any positive size works.
- Records are **not** idempotent end to end: the CLI never retries a chunk on
  its own, because re-sending would duplicate anything the processor had
  already forwarded. De-duplicating on `id` upstream is the prerequisite for
  automatic retries.
