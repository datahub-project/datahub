# DataHub Performance Tests

Load and stress tests for DataHub GMS. Two load-test implementations share a common datapack seeding layer.

|                       | Implementation 1                  | Implementation 2                      |
| --------------------- | --------------------------------- | ------------------------------------- |
| **Runner**            | `locust` CLI                      | `pytest` (via locust-grasshopper)     |
| **Config**            | CLI flags                         | YAML scenario files + CLI override    |
| **Assertions**        | HTTP/GQL failure marking          | `check()` — named metrics, historical |
| **Multi-step timing** | Not built-in                      | `@custom_trend` decorator             |
| **Performance gates** | Manual CSV inspection             | Thresholds → non-zero exit on breach  |
| **Metrics export**    | CSV / Locust web UI               | InfluxDB → Grafana (optional)         |
| **Best for**          | Exploratory runs, quick iteration | CI gating, release qualification      |

See [GRASSHOPPER_ANALYSIS.md](GRASSHOPPER_ANALYSIS.md) for a full feature comparison and adoption roadmap.

---

## Directory Structure

```
e2e-test/perf/
├── GRASSHOPPER_ANALYSIS.md            # Analysis: locust-grasshopper vs plain Locust
│
├── datapacks/                         # Synthetic data seeders — shared by both impls
│   ├── base_seeder.py                 # Shared: _Stats, _worker, progress reporter, run_seed()
│   ├── seed_search.py                 # Step 1: datasets + search aspects
│   ├── seed_lineage.py                # Step 2: UpstreamLineage + DataFlow/DataJob pipelines
│   └── vocab/
│       ├── __init__.py
│       └── words.py                   # Vocabulary: platforms, columns, tags, owners, etc.
│
├── seed-output/                       # Written by seeders (gitignored)
│   ├── manifest.json                  # URN sample + vocabulary (seed_search.py output)
│   └── lineage-manifest.json          # Lineage graph stats (seed_lineage.py output)
│
├── implementation_1/                  # Plain Locust (no extra dependencies)
│   ├── requirements.txt
│   └── tests/
│       ├── framework/
│       │   └── base_perf_test.py      # DataHubUser base class (auth, GraphQL helper)
│       └── search/
│           └── graphql_search_ramp.py # Global search ramp scenario
│
└── implementation_2/                  # locust-grasshopper (structured observability)
    ├── requirements.txt
    ├── conftest.py                    # pytest fixtures (extra env vars, sys.path)
    ├── scenarios/
    │   └── search_scenarios.yaml     # Smoke / ramp / stress scenario definitions
    ├── framework/
    │   └── datahub_journey.py        # DataHubJourney base (auth, graphql + checks)
    ├── journeys/
    │   └── search_journey.py         # GlobalSearchJourney + custom_trend
    └── tests/
        └── test_search_load.py       # pytest entry point
```

---

## Authentication

All seeders and load tests read `DATAHUB_GMS_TOKEN`. When absent they fall back to the actor-spoof header for local no-auth dev instances:

```bash
export DATAHUB_GMS_TOKEN=<your-PAT>
```

---

## Datapacks — Synthetic Data Seeding

Datapacks are standalone Python scripts that populate GMS with deterministic synthetic data. They are independent of the load-test implementations and compose in sequence: each seeder reads the manifest written by the previous step.

### Architecture

```
base_seeder.py          ← shared threading/stats infrastructure
    ↑
seed_search.py          ← Step 1: creates base dataset entities
    │  writes manifest.json
    ↓
seed_lineage.py         ← Step 2: adds UpstreamLineage + DataFlow/DataJob on top
    │  writes lineage-manifest.json
    ↓
seed_governance.py      ← Step 3 (future): glossary terms, domains, policies
seed_dashboards.py      ← Step 4 (future): Dashboard/Chart entities
```

### Seeder Composition

| Step | Seeder                          | Entities created                                              | MCPs per 1 M datasets   | Depends on      |
| ---- | ------------------------------- | ------------------------------------------------------------- | ----------------------- | --------------- |
| 1    | `seed_search.py`                | Dataset (properties, schema, tags, ownership, status, browse) | 6 M                     | —               |
| 2    | `seed_lineage.py`               | UpstreamLineage + DataFlow + DataJob + DataJobInputOutput     | ~3× downstream datasets | `manifest.json` |
| 3    | `seed_governance.py` _(future)_ | GlossaryTerm, Domain, DataHubPolicy, Assertion                | TBD                     | `manifest.json` |
| 4    | `seed_dashboards.py` _(future)_ | Dashboard, Chart + upstream lineage to datasets               | TBD                     | `manifest.json` |

All seeders are deterministic: the same `--seed` value always produces the same output.

---

### Step 1 — `seed_search.py`

Seeds GMS with synthetic datasets optimised for search performance testing.

```bash
python e2e-test/perf/datapacks/seed_search.py \
    --count    1000000 \
    --host     http://localhost:8080 \
    [--token   <PAT>] \
    [--workers  8] \
    [--batch-size 200] \
    [--seed    42] \
    [--output  e2e-test/perf/seed-output]
```

| Flag           | Default                     | Description                   |
| -------------- | --------------------------- | ----------------------------- |
| `--count`      | 1,000,000                   | Datasets to generate          |
| `--host`       | `http://localhost:8080`     | GMS base URL                  |
| `--token`      | `$DATAHUB_GMS_TOKEN`        | PAT (omit for no-auth)        |
| `--workers`    | 8                           | Parallel emitter threads      |
| `--batch-size` | 200                         | MCPs per batch                |
| `--seed`       | 42                          | RNG seed for reproducibility  |
| `--output`     | `e2e-test/perf/seed-output` | Directory for `manifest.json` |
| `--min-cols`   | 10                          | Min columns per dataset       |
| `--max-cols`   | 30                          | Max columns per dataset       |

**Per dataset:** 6 MCPs — DatasetProperties, SchemaMetadata, GlobalTags, Ownership, Status, BrowsePaths.

**Throughput expectations (single GMS pod):**

| Workers | MCP/s  | Time for 1 M datasets |
| ------- | ------ | --------------------- |
| 4       | ~600   | ~166 min              |
| 8       | ~1,200 | ~83 min               |
| 16      | ~2,000 | ~50 min               |

**Output:** `seed-output/manifest.json` — contains 1,000 sample URNs and the full vocabulary. Both load-test implementations read this file at startup to issue realistic queries against real data.

**Cleanup:**

```bash
datahub delete by-filter \
    --query "urn:li:dataset:(urn:li:dataPlatform:*,perf_search_*,*)" \
    --batch-size 1000 --workers 4 --hard
```

---

### Step 2 — `seed_lineage.py`

Reads `manifest.json` from step 1 and adds multi-hop lineage across the schema tier hierarchy.

**What it creates:**

- **`UpstreamLineage`** on each downstream dataset (1–3 upstream datasets per node)
- **`DataFlowInfo`** — one Airflow-style pipeline entity per `(platform, db)` pair
- **`DataJobInfo`** — one job per tier transition (e.g. `raw_to_staging`)
- **`DataJobInputOutput`** — connects each job to its input and output dataset lists

**Lineage topology (`--hops 3`):**

```
raw ──────► staging ──────► trusted ──────► curated
 (90)          (90)            (90)            (90)

Each arrow = UpstreamLineage + DataJob + DataJobInputOutput
Each (platform, db) pair = one DataFlow pipeline entity
```

```bash
python e2e-test/perf/datapacks/seed_lineage.py \
    --manifest e2e-test/perf/seed-output/manifest.json \
    --host     http://localhost:8080 \
    [--token   <PAT>] \
    [--workers  4] \
    [--hops    3] \
    [--output  e2e-test/perf/seed-output]
```

| Flag           | Default                                   | Description                                                         |
| -------------- | ----------------------------------------- | ------------------------------------------------------------------- |
| `--manifest`   | `e2e-test/perf/seed-output/manifest.json` | Manifest from `seed_search.py`                                      |
| `--host`       | `http://localhost:8080`                   | GMS base URL                                                        |
| `--token`      | `$DATAHUB_GMS_TOKEN`                      | PAT (omit for no-auth)                                              |
| `--workers`    | 4                                         | Parallel emitter threads                                            |
| `--batch-size` | 100                                       | MCPs per batch                                                      |
| `--seed`       | 42                                        | RNG seed                                                            |
| `--hops`       | 3                                         | Lineage chain depth (tier transitions from the lowest tier present) |
| `--output`     | `e2e-test/perf/seed-output`               | Directory for `lineage-manifest.json`                               |

**`--hops` explained:**

`hops` controls how many consecutive tier transitions are created, counting from the lowest tier present in the seeded data. Tier order: `raw → staging → trusted → curated → analytics → mart → reporting`.

| `--hops` | Chain created                     | Downstream datasets (from 500 seeded) |
| -------- | --------------------------------- | ------------------------------------- |
| 1        | raw → staging                     | ~90                                   |
| 2        | raw → staging → trusted           | ~180                                  |
| 3        | raw → staging → trusted → curated | ~270                                  |
| 5        | … → analytics                     | ~450                                  |

**Output:** `seed-output/lineage-manifest.json` — downstream URN sample and graph stats.

**Cleanup:**

```bash
# Remove lineage aspects from datasets
datahub delete by-filter \
    --query "urn:li:dataset:(urn:li:dataPlatform:*,perf_search_*,*)" \
    --aspect upstreamLineage --hard

# Remove pipeline entities
datahub delete by-filter \
    --query "urn:li:dataFlow:(airflow,*_etl,PROD)" --hard
datahub delete by-filter \
    --query "urn:li:dataJob:(*,*_to_*)" --hard
```

---

### Full Seeding Workflow

```bash
# Step 1: seed 1 M datasets (search-optimised)
python e2e-test/perf/datapacks/seed_search.py \
    --count 1000000 --host http://localhost:8080 --workers 8

# Step 2: add 3-hop lineage on top of the seeded datasets
python e2e-test/perf/datapacks/seed_lineage.py \
    --manifest e2e-test/perf/seed-output/manifest.json \
    --host http://localhost:8080 --hops 3
```

For a quick local smoke (500 datasets, fast):

```bash
python e2e-test/perf/datapacks/seed_search.py \
    --count 500 --host http://localhost:8080 --workers 4

python e2e-test/perf/datapacks/seed_lineage.py \
    --manifest e2e-test/perf/seed-output/manifest.json \
    --host http://localhost:8080 --hops 3
```

---

### Adding a New Datapack

Each domain seeder follows the same pattern:

1. Create `datapacks/seed_<domain>.py`
2. Import `run_seed` from `datapacks.base_seeder`
3. Read `manifest.json` to get seeded URNs (the entities you'll annotate)
4. Write a generator that yields `MetadataChangeProposalWrapper` objects
5. Call `run_seed(host, token, workers, batch_size, mcp_iter, total_mcps)`
6. Write a `<domain>-manifest.json` with stats and sample entity URNs

```python
from datapacks.base_seeder import run_seed

def generate_governance_mcps(urns: list[str]) -> Iterator[MetadataChangeProposalWrapper]:
    for urn in urns:
        yield MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=GlossaryTermsClass(terms=[...]),
        )

stats = run_seed(
    host=cfg.host, token=cfg.token,
    workers=cfg.workers, batch_size=cfg.batch_size,
    mcp_iter=generate_governance_mcps(urns),
    total_mcps=len(urns),
)
```

---

## Implementation 1 — Plain Locust

### Install

```bash
pip install -r e2e-test/perf/implementation_1/requirements.txt
```

### Run — from `e2e-test/perf/implementation_1/`

```bash
# Web UI (recommended for initial exploration)
locust -f tests/search/graphql_search_ramp.py -H http://localhost:8080

# Headless — ramp to 200 users over 10 min, save CSV baselines
locust -f tests/search/graphql_search_ramp.py \
    --headless -H http://localhost:8080 \
    -u 200 -r 10 -t 10m \
    --csv ../baselines/search-ramp-$(date +%Y%m%d)
```

### Available Tests

#### `tests/search/graphql_search_ramp.py` — Global Search Ramp

Simulates users performing global search to find the throughput knee as concurrent users increase from 50 to 200.

**Task mix:**

| Weight | Task                 | Description                                                       |
| ------ | -------------------- | ----------------------------------------------------------------- |
| 5      | `search_keyword`     | Bare keyword search, `skipAggregates: true` — lightest path       |
| 3      | `search_typed`       | Keyword + entity type filter, paginated across first 4 pages      |
| 2      | `search_with_facets` | Full search with aggregation facets — mirrors the UI results page |
| 1      | `autocomplete`       | 3-char prefix typeahead — `autoCompleteForMultiple`               |

### Framework: `tests/framework/base_perf_test.py`

All test files subclass `DataHubUser`, which provides:

- **Auth injection** — `Authorization: Bearer <token>` or `X-DataHub-Actor` header
- **`graphql()` helper** — POSTs to `/api/graphql`, marks failed on HTTP errors or non-null `errors` array
- **`random_dataset_urn()`** — fallback URN generator when no manifest is present

### Adding New Tests (Impl 1)

1. Create `tests/<domain>/` with an `__init__.py`
2. Subclass `DataHubUser` from `tests.framework.base_perf_test`
3. Define `@task` methods; use `self.graphql()` for GraphQL or `self.client` for REST
4. Add a domain seeder to `datapacks/` if the test needs pre-seeded data

---

## Implementation 2 — locust-grasshopper

Grasshopper wraps Locust with pytest, YAML-driven scenario management, named assertion metrics (`check()`), composite journey timing (`@custom_trend`), and configurable p90 performance gates (thresholds).

### Install

```bash
pip install -r e2e-test/perf/implementation_2/requirements.txt
```

### Run — from `e2e-test/perf/`

```bash
# Smoke test (5 users, 60 s)
pytest implementation_2/tests/test_search_load.py \
    --scenario_file=implementation_2/scenarios/search_scenarios.yaml \
    --scenario_name=search_smoke \
    -H http://localhost:8080

# Ramp test (200 users, 10 min)
pytest implementation_2/tests/test_search_load.py \
    --scenario_file=implementation_2/scenarios/search_scenarios.yaml \
    --scenario_name=search_ramp \
    -H http://localhost:8080

# Stress test (500 users, 20 min)
pytest implementation_2/tests/test_search_load.py \
    --scenario_file=implementation_2/scenarios/search_scenarios.yaml \
    --scenario_name=search_stress \
    -H http://localhost:8080

# Run all scenarios tagged "smoke"
pytest implementation_2/tests/test_search_load.py \
    --scenario_file=implementation_2/scenarios/search_scenarios.yaml \
    --tags=smoke \
    -H http://localhost:8080
```

Override any YAML value on the CLI without editing files:

```bash
# Quick 5-user validation against a staging host
pytest implementation_2/tests/test_search_load.py \
    --scenario_file=implementation_2/scenarios/search_scenarios.yaml \
    --scenario_name=search_ramp \
    -H https://staging.datahub.example.com \
    --users=5 --runtime=30
```

### YAML Scenario Files

Scenarios are defined in `implementation_2/scenarios/search_scenarios.yaml`. Each scenario specifies:

- `test_run` — user count, spawn rate, duration
- `scenario` — journey-level config, including **thresholds** (p90 gates per request + custom trend)

```yaml
scenarios:
  - name: search_ramp
    tags: [ramp, search]
    test_run:
      users: 200
      spawn_rate: 10
      runtime: 600
    scenario:
      thresholds:
        "graphql/searchAcrossEntities [keyword]":
          type: post
          limit: 2000 # p90 must be < 2000 ms; test exits non-zero if exceeded
        search_flow:
          type: custom
          limit: 10000 # end-to-end multi-step journey must complete in < 10 s p90
```

### Available Tests

#### `tests/test_search_load.py` → `GlobalSearchJourney`

Same search task mix as Implementation 1, plus:

| Extra                       | Description                                                                     |
| --------------------------- | ------------------------------------------------------------------------------- |
| `search_flow` (weight 1)    | **Custom trend** — keyword → faceted → autocomplete in one timed session        |
| `check()` on every response | Named pass/fail assertions, visible in console + InfluxDB `locust_checks` table |
| Thresholds                  | p90 gates on all requests + `search_flow` trend; non-zero exit on breach        |

**Task mix:**

| Weight | Task                 | Description                                               |
| ------ | -------------------- | --------------------------------------------------------- |
| 5      | `search_keyword`     | Bare keyword search — lightest path                       |
| 3      | `search_typed`       | Keyword + type filter, paginated                          |
| 2      | `search_with_facets` | Full aggregations — heaviest read path                    |
| 1      | `autocomplete`       | 3-char prefix typeahead                                   |
| 1      | `search_flow`        | Multi-step custom trend (keyword → facets → autocomplete) |

### Framework: `framework/datahub_journey.py`

All impl-2 journeys subclass `DataHubJourney`, which provides:

- **Auth injection** — same `DATAHUB_GMS_TOKEN` → Bearer / actor-spoof fallback as impl 1
- **`graphql()` helper** — HTTP + GQL error detection + `check()` recording
- **Manifest URN pool** — `_manifest_urns`, `_manifest_tags`, `_manifest_platforms` class attributes loaded at import time
- **`scenario_args`** — Grasshopper injects YAML scenario config per-journey instance

### InfluxDB + Grafana (Optional)

When `--influx_host` is set, Grasshopper streams all metrics to InfluxDB automatically:

```bash
# Start the local observability stack
cd e2e-test/perf/implementation_2/observability_infrastructure
docker-compose up -d
# Grafana at http://localhost:3000  (admin/admin)

# Run with metrics export
pytest implementation_2/tests/test_search_load.py \
    --scenario_file=implementation_2/scenarios/search_scenarios.yaml \
    --scenario_name=search_ramp \
    -H http://localhost:8080 \
    --influx_host=localhost
```

**Tables written automatically:**

| Table               | Contents                                                              |
| ------------------- | --------------------------------------------------------------------- |
| `locust_requests`   | Per-request stats + custom trend timings (p50/p90/p99, RPS, failures) |
| `locust_checks`     | Named check pass/fail counts with tags                                |
| `locust_events`     | Test lifecycle events (started, stopped, user ramp)                   |
| `locust_exceptions` | Exception type, message, count                                        |

### Configuration Precedence (Impl 2)

```
CLI flag (--users=5)
  → DATAHUB_GMS_TOKEN env var
    → GH_* prefixed env vars
      → scenario: section in YAML
        → test_run: section in YAML
          → grasshopper: section in YAML
            → built-in defaults
```

### Adding New Tests (Impl 2)

1. Run the relevant datapack seeder(s) to populate GMS
2. Create `implementation_2/journeys/<domain>_journey.py`
3. Subclass `DataHubJourney` from `framework.datahub_journey`
4. Define `@task` methods; use `self.graphql()` + `check()` for GraphQL calls
5. Wrap multi-step flows with `@custom_trend("name", extra_tag_keys=[...])`
6. Add a scenario block to `implementation_2/scenarios/<domain>_scenarios.yaml` with thresholds
7. Add a `test_<domain>_load` function in `implementation_2/tests/`

---

## Synthetic Data Design

### Vocabulary (`datapacks/vocab/words.py`)

| Dimension      | Count | Examples                                                       |
| -------------- | ----- | -------------------------------------------------------------- |
| Platforms      | 6     | Snowflake, BigQuery, Redshift, MySQL, PostgreSQL, Hive         |
| Databases      | 15    | `sales_db`, `analytics_db`, `finance_db`, …                    |
| Schema layers  | 10    | `raw`, `staging`, `trusted`, `curated`, `analytics`, `mart`, … |
| Table prefixes | 30    | `orders`, `customers`, `sessions`, `events`, …                 |
| Table suffixes | 10    | `_fact`, `_dim`, `_staging`, `_snapshot`, …                    |
| Column names   | ~478  | Semantic groups: identity, financial, temporal, ML, governance |
| Tags           | 30    | `pii`, `certified`, `gdpr`, `ml-feature`, …                    |
| Domains        | 10    | Sales, Marketing, Finance, Engineering, …                      |
| Owners         | 50    | `alice.wang`, `bob.chen`, …                                    |

Topology: 6 × 15 × 10 = **900 topology slots** → ~1,111 tables per slot at 1 M datasets.

### Lineage Topology (`seed_lineage.py`)

Schema layers map to tier numbers in `TIER_ORDER`. `seed_lineage.py` walks these tiers bottom-up, connecting each non-raw dataset to 1–3 upstream datasets from the tier below within the same `(platform, db)` when possible.

```
Tier 0   Tier 2     Tier 4     Tier 6     Tier 7     Tier 8
 raw  →  staging  →  trusted  →  curated  →  analytics  →  mart
                                                           reporting
```

For each tier transition a `DataFlow` pipeline entity and a `DataJob` transform entity are created, giving a complete provenance chain from raw source to analytics-ready tables.
