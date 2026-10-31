# Authz persona performance harness

Standalone benchmark tool for the `authz-perf-medium` datapack. Measures frontend GraphQL authz latency for 17 test personas. **Not** part of smoke-test or `datahub-dev test`.

## Setup

```bash
perf-test/authz-perf/setup.sh
source perf-test/authz-perf/venv/bin/activate

# Optional: editable SDK when developing unreleased datapack CLI changes in-tree
pip install -e ../../metadata-ingestion
```

`setup.sh` creates an isolated venv and installs deps from `requirements.txt` (`acryl-datahub>=1.6.0,<2.0.0` includes `graphql_query_adapter` for schema-aware query generation). Benchmark GraphQL workloads are declared in `fixture/benchmarks.json` under `query_specs` (field paths + privilege type expansion). At run time the harness introspects GMS and **generates** valid operation documents—no GraphQL strings or symlinks in the repo.

**Layout:** ingestible MCP data is published via the datapack registry (same hosting model as `showcase-ecommerce`, in the `datahub-project/static-assets` repository under `datapacks/authz-perf-medium/`). Harness fixtures live in `perf-test/authz-perf/fixture/`:

- **`benchmarks.json`** — perf oracle: `query_specs` plus per-persona scenarios (`expected_status_code`, optional field assertions with `--full-correctness`).
- **`personas.json`** — persona catalog (URN, stress labels, membership counts). Policy match/deny fields are reference notes for fixture authors only; runtime expectations come from `benchmarks.json`.

With `static-assets` checked out as a sibling of this repo, the harness auto-detects the local datapack.

**Every run** — local quickstart included — introspects the target GMS schema, generates queries from specs, and caches documents per target. Timed requests still POST to frontend `/api/v2/graphql`.

## Quick start

```bash
datahub init --username datahub --password datahub   # once; writes ~/.datahubenv
scripts/dev/datahub-dev.sh start

source perf-test/authz-perf/venv/bin/activate
python perf-test/authz-perf/run.py \
  --warmup 5 --iterations 25 \
  --output ~/.datahub/authz-perf-results/run.jsonl
```

Coordinator auth (datapack probe/load) is resolved from `~/.datahubenv` by default. Alternatively pass `--username` / `--password`, set `DATAHUB_GMS_TOKEN`, or run interactively to be prompted. Persona benchmarks always log in as each test user (`password = username` on local quickstart). Smoke init requires `me.corpUser.urn` to survive schema adaptation.

On **remote** instances (or when using `--env-file`), the harness also admin-resets each persona password using the coordinator token from the env file (`createNativeUserResetToken` + `resetNativeUserCredentials`) before frontend login. Passwords are **not** `password=username` on remotes — they are derived deterministically from a local seed file at `~/.datahub/authz-perf-results/.persona-password-seed` (or `AUTHZ_PERF_PERSONA_PASSWORD_SEED`), the machine hostname, the remote GMS host, and the persona name.

## Multi-remote and multi-variant runs

Use `--output-dir` to write **one JSONL per (target × variant)** plus a `manifest.json`. Results from different remotes, git commits, or docker tags are never merged into one file.

### Alternate env files (like `~/.datahubenv`)

```bash
cp ~/.datahubenv ~/.datahub/staging.datahubenv   # edit gms.server/token
cp ~/.datahubenv ~/.datahub/prod.datahubenv

python perf-test/authz-perf/run.py \
  --env-file ~/.datahub/staging.datahubenv \
  --env-file ~/.datahub/prod.datahubenv \
  --output-dir ~/.datahub/authz-perf-results/matrix-$(date +%F)/
```

Target name defaults to the env file stem (`staging`, `prod`). Override with repeatable `--env-file-name`.

### Explicit targets

```bash
python perf-test/authz-perf/run.py \
  --target name=staging,gms_url=http://staging:8080,frontend_url=http://staging:9002 \
  --target name=prod,gms_url=http://prod:8080 \
  --output-dir ~/.datahub/authz-perf-results/matrix/
```

### Build variants (tags / commits)

```bash
# Labeled variants
python perf-test/authz-perf/run.py \
  --gms-url http://localhost:8080 \
  --run-label v0.14.0 --run-label v0.15.0 \
  --output-dir ~/.datahub/authz-perf-results/tags/

# Bisect-style: one file per git commit under output dir
python perf-test/authz-perf/run.py \
  --gms-url http://localhost:8080 \
  --output-dir ~/.datahub/authz-perf-results/bisect-2025-06-23/
# → commit-<short-sha>.jsonl (from git metadata)
```

Each JSONL row includes live GMS metadata from `GET /config`: `deployment.gms_version`, `deployment.gms_commit`, `deployment.gms_host`, and `deployment.target_name`.

## CLI

| Flag                           | Default                                                         | Description                                                    |
| ------------------------------ | --------------------------------------------------------------- | -------------------------------------------------------------- |
| `--gms-url`                    | `~/.datahubenv` / localhost                                     | GMS base URL (single-target or local target in `--output-dir`) |
| `--env-file`                   | —                                                               | Repeatable alternate `~/.datahubenv` YAML                      |
| `--target`                     | —                                                               | Repeatable `name=...,gms_url=...` target                       |
| `--output`                     | —                                                               | Single JSONL path (exclusive with `--output-dir`)              |
| `--output-dir`                 | —                                                               | Per-(target×variant) JSONL + `manifest.json`                   |
| `--run-label`                  | —                                                               | Repeatable build variant label                                 |
| `--docker-tag`                 | —                                                               | Repeatable docker tag variant (metadata)                       |
| `--fail-fast`                  | off                                                             | Stop matrix on first cell failure                              |
| `--username` / `--password`    | from env or prompt                                              | Mint coordinator token when not in datahubenv                  |
| `--personas`                   | all 17                                                          | Comma-separated subset                                         |
| `--parallel-personas`          | 1                                                               | Concurrent persona workers                                     |
| `--cache-phases`               | `warm`                                                          | `warm`, `cold`, or both                                        |
| `--full-correctness`           | off                                                             | Assert privilege booleans before timing                        |
| `--force-reload`               | off                                                             | Always reload datapack                                         |
| `--skip-load`                  | off                                                             | Never load; probe sentinel only                                |
| `--fixture-dir`                | `perf-test/authz-perf/fixture/`                                 | Persona catalog + benchmark scenarios (not ingested)           |
| `--datapack-dir`               | sibling `static-assets/datapacks/authz-perf-medium/` if present | Local pack for `file://` load; else registry URL               |
| `--set-persona-passwords`      | auto on remotes / `--env-file`                                  | Admin-reset persona passwords via env-file token before login  |
| `--skip-set-persona-passwords` | off                                                             | Skip admin password reset even on remotes                      |
| `--es-jitter`                  | off                                                             | Rotate search `start` offset                                   |

Timed samples use **frontend GraphQL only** (`/api/v2/graphql`). Each scenario in `fixture/benchmarks.json` declares `expected_status_code` (default `200`; use `403` for denied authz). The harness maps HTTP 200 + GraphQL `errors` to semantic **403** when matching expectations. Timeouts, network errors, and status mismatches abort the run.

Before benchmarks, the harness reads GMS `system-info` (`authorization.view.enabled`). Fixture deny scenarios (`getDomain` expected **403**) assume view authorization is enabled; on hosts where it is disabled, effective expectations become **200** and metrics are emitted under separate names (`getDomain@expect200`, profile `allow_without_view_auth`) instead of `getDomain@expect403` (`authz_deny`). Multi-target runs warn when auth settings differ. JSONL rows include `metric_key`, `performance_profile`, and `deployment.authorization.view_enabled`. `compare.py` matches rows by `metric_key` and warns on unmatched profiles across runs.

Setup (datapack load, sentinel probe) may use OpenAPI/SDK/CLI; non-404 OpenAPI errors fail the run.

## Compare runs

```bash
python perf-test/authz-perf/compare.py \
  ~/.datahub/authz-perf-results/baseline.jsonl \
  ~/.datahub/authz-perf-results/run.jsonl \
  --threshold-p95-ratio 1.25 \
  --persona persona-p90-groups --operation getMe
```

`compare.py` matches rows by `metric_key` (e.g. `getDomain@expect403`), warns when baseline/candidate deployment or auth settings differ, and lists metrics present in only one run when performance profiles diverge.

## Git bisect

```bash
perf-test/authz-perf/bisect.sh \
  --good v0.14.0 --bad HEAD \
  --persona persona-p90-groups \
  --threshold-p95-ms 200
```

Bisect writes separate `commit-<short-sha>.jsonl` files under `--results-dir` via `--output-dir`.

## Unit tests

```bash
source perf-test/authz-perf/venv/bin/activate
ruff check perf-test/authz-perf --exclude perf-test/authz-perf/venv
ruff format perf-test/authz-perf --exclude perf-test/authz-perf/venv --check
python -m pytest perf-test/authz-perf/tests/ -v
```

## Results

Append-only JSONL under `~/.datahub/authz-perf-results/` (or `--output-dir`). Each row includes p50/p95/max, `max_to_p50_ratio`, git/deployment metadata (`gms_host`, `gms_version`, `target_name`), `request_health`, `response_size` (avg/min/max HTTP body size in KB on timed iterations), and `execution_mode` (`isolated` vs `concurrent`).

Multi-run orchestrations also write `manifest.json` listing every target×variant cell, output paths, and observed GMS version.
