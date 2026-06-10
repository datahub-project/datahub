# locust-grasshopper: Technical Analysis

**Library:** [alteryx/locust-grasshopper](https://github.com/alteryx/locust-grasshopper)  
**Install:** `pip install locust-grasshopper`  
**Analysed against:** DataHub Implementation 1 (plain Locust + locust-plugins)

---

## 1. What It Is

locust-grasshopper is a thin opinionated layer on top of Locust that wires together three components that every serious load-test project eventually builds by hand:

| Component           | Plain Locust                             | Grasshopper                                                |
| ------------------- | ---------------------------------------- | ---------------------------------------------------------- |
| Test runner         | `locust` CLI                             | `pytest` (locust runs inside a pytest test function)       |
| Configuration       | CLI flags only                           | CLI → env vars → YAML → defaults (precedence chain)        |
| Assertions          | None (failures are silent stats entries) | `check()` — assertions recorded as named, taggable metrics |
| Multi-step timing   | Not built-in                             | `@custom_trend()` decorator                                |
| Performance gates   | Not built-in                             | `thresholds` — p90 limits per request or custom trend      |
| Metrics export      | CSV / web UI                             | InfluxDB → Grafana (plug-and-play Docker Compose stack)    |
| Scenario management | Not built-in                             | YAML scenario files with tag-based filtering               |

The net effect is a shift from an _ad-hoc_ load runner to an _observable, assertion-driven_ performance regression framework.

---

## 2. Architecture

```
pytest invocation
    └─ pytest-grasshopper plugin (auto-registered on install)
           ├─ provides complete_configuration fixture
           │      │
           │      └─ merges: CLI args → GH_* env vars → YAML scenario → defaults
           │
           └─ conftest.py (user-provided)
                  └─ extra_env_var_keys fixture (extend env var list)

test_my_load.py
    └─ def test_foo(complete_configuration):
           ├─ MyJourney.update_incoming_scenario_args(complete_configuration)
           └─ Grasshopper.launch_test(MyJourney, **complete_configuration)
                  │
                  └─ Locust runner
                         ├─ spawns MyJourney instances
                         ├─ collects HTTP stats (locust_requests table)
                         ├─ collects check results (locust_checks table)
                         ├─ collects custom trend timings (locust_requests CUSTOM)
                         └─ streams all to InfluxDB (when configured)
```

### Journey class hierarchy

```
locust.HttpUser
    └─ grasshopper.lib.journeys.base_journey.BaseJourney
           ├─ scenario_args  — per-journey immutable config dict
           ├─ log_prefix     — structured logger
           └─ DataHubJourney (our custom base, in grasshopper_tests/framework/)
                  ├─ _headers       — auth injection
                  ├─ graphql()      — GraphQL POST with check recording
                  └─ GlobalSearchJourney (in grasshopper_tests/journeys/)
                         └─ @task methods
```

---

## 3. Feature Deep-Dive

### 3.1 Configuration Hierarchy

Configuration is resolved in this precedence order (highest first):

```
1. --key=value          CLI argument
2. KEY=value            Uppercase env var (plain or GH_KEY=value prefix)
3. scenario: key: val   YAML scenario file → scenario section
4. test_run: key: val   YAML scenario file → test_run section
5. grasshopper: key:    YAML scenario file → grasshopper section
6. built-in defaults    (runtime=120, users=1, spawn_rate=1, …)
```

This lets you set a stable YAML baseline and override per-run via env vars (CI) or CLI (local).

```yaml
# search_scenarios.yaml
test_run:
  users: 200
  spawn_rate: 10
  runtime: 600
scenario:
  thresholds:
    graphql/searchAcrossEntities [keyword]:
      type: post
      limit: 2000 # p90 must be < 2000 ms
```

```bash
# Override users for a quick smoke run without editing YAML
pytest tests/ --scenario_name=search_ramp --users=5 --runtime=30
```

Compare to **Implementation 1**, which has no concept of scenario files — all parameters are CLI flags every time, with no persistence or defaults.

---

### 3.2 Checks — Assertions as Metrics

```python
from grasshopper.lib.util.utils import check

response = self.client.post("/api/graphql", json=payload, name="search")
check(
    "search returns results",
    response.json().get("data", {}).get("searchAcrossEntities", {}).get("total", 0) > 0,
    env=self.environment,
    tags={"query_type": "keyword"},
)
```

- Checks are **pass/fail counters** recorded alongside HTTP stats.
- They appear in the console summary **and** in the `locust_checks` InfluxDB table.
- Under load, a check failure rate > 0% is immediately visible in Grafana — you see _correctness degrading_ as concurrency increases, not just latency increasing.

**Implementation 1 gap:** `resp.failure()` marks a request as failed in Locust stats but there is no way to record semantic validation failures separately, query them historically, or threshold them.

---

### 3.3 Custom Trends — Multi-step Timing

```python
from grasshopper.lib.util.utils import custom_trend
from locust import task

@task(1)
@custom_trend("search_flow", extra_tag_keys=["entity_types"])
def search_flow(self) -> None:
    # Step 1 — keyword search
    self.client.post("/api/graphql", json=step1, name="search [keyword]")
    # Step 2 — faceted refinement
    self.client.post("/api/graphql", json=step2, name="search [facets]")
    # Step 3 — autocomplete
    self.client.post("/api/graphql", json=step3, name="autocomplete")
```

The decorator wraps the entire method and records its wall-clock time as a `CUSTOM/search_flow` entry in `locust_requests`. This gives you:

- End-to-end user-journey timing (not just individual request timing)
- Threshold enforcement on the composite workflow
- Tags from `scenario_args` (e.g., `entity_types=DATASET`) appear on every metric row

**Implementation 1 gap:** No mechanism to measure multi-step flows. You can see each request individually in stats but not the aggregate user journey time.

---

### 3.4 Thresholds — Automated Performance Gates

```python
GlobalSearchJourney.update_incoming_scenario_args({
    "thresholds": {
        "graphql/searchAcrossEntities [keyword]": {"type": "post", "limit": 2000},
        "search_flow": {"type": "custom", "limit": 10000},
    }
})
```

- Thresholds default to **p90** (90th percentile).
- After the test run, grasshopper compares observed p90 against each limit.
- If any threshold is exceeded, the test exits non-zero → CI fails the perf job.

This transforms perf tests from _observational_ to _gating_: a PR that degrades search p90 above 2 seconds fails automatically.

**Implementation 1 gap:** No thresholds. Every run produces CSV and web UI stats but nothing asserts regression.

---

### 3.5 YAML Scenario Files

```yaml
scenarios:
  - name: search_smoke
    tags: [smoke, search]
    test_run:
      users: 5
      spawn_rate: 1
      runtime: 60
    scenario:
      thresholds: { ... }

  - name: search_ramp
    tags: [ramp, search]
    test_run:
      users: 200
      spawn_rate: 10
      runtime: 600
```

```bash
# Run all smoke-tagged scenarios
pytest grasshopper_tests/ --scenario_file=scenarios/search_scenarios.yaml --tags=smoke

# Run a single named scenario
pytest grasshopper_tests/ --scenario_file=scenarios/search_scenarios.yaml \
    --scenario_name=search_ramp -H http://localhost:8080
```

Tags use the `tag-matcher` package, supporting `AND`/`OR` expressions:

```bash
--tags="smoke AND search"
--tags="search OR ingestion"
```

**Implementation 1 gap:** Scenario definition is entirely in CLI flags — no source-of-truth file, no tagging, no multi-scenario runs.

---

### 3.6 InfluxDB + Grafana Observability Stack

Grasshopper ships a Docker Compose template:

```bash
cd observability_infrastructure/
docker-compose up -d
# Grafana at localhost:3000  (admin/admin)
# InfluxDB at localhost:8086
```

```bash
pytest grasshopper_tests/ --influx_host=localhost --influx_port=8086
```

**Tables written automatically:**

| Table               | Contents                                                                 |
| ------------------- | ------------------------------------------------------------------------ |
| `locust_requests`   | Every HTTP request + custom trends (method, name, p50/p90/p99, failures) |
| `locust_checks`     | Check name, pass/fail count, tags                                        |
| `locust_events`     | Test lifecycle (started, stopped, user counts)                           |
| `locust_exceptions` | Exception type, message, count                                           |

Pre-built Grafana dashboards provide:

- RPS and latency over time
- Error rate heatmap
- Check pass/fail ratio
- Custom trend timelines
- Threshold breach markers

**Implementation 1 gap:** CSV export only. No historical comparison, no dashboards, no check visibility.

---

## 4. Comparison: Implementation 1 vs Implementation 2

| Dimension                | Impl 1 (Plain Locust)                           | Impl 2 (Grasshopper)                                    |
| ------------------------ | ----------------------------------------------- | ------------------------------------------------------- |
| **Boilerplate per test** | ~20 lines: HttpUser subclass, on_start, headers | ~10 lines: BaseJourney subclass + defaults dict         |
| **Config management**    | CLI flags repeated per run                      | YAML file + CLI override, env var support               |
| **Auth injection**       | Custom `on_start` in DataHubUser                | Inherited from DataHubJourney                           |
| **GraphQL helper**       | Yes (DataHubUser.graphql())                     | Yes (DataHubJourney.graphql()) + check recording        |
| **Assertion tracking**   | resp.failure() only (HTTP/GQL errors)           | check() + HTTP/GQL errors (named, taggable, historical) |
| **Multi-step timing**    | Not available                                   | @custom_trend decorator                                 |
| **Performance gates**    | Manual CSV inspection                           | Thresholds → non-zero exit on breach                    |
| **Scenario management**  | CLI flags only                                  | YAML scenarios with tag filtering                       |
| **Metrics storage**      | CSV files                                       | InfluxDB (optional)                                     |
| **Visualization**        | Locust web UI (ephemeral)                       | Grafana dashboards (persistent)                         |
| **CI integration**       | Requires manual CSV diffing                     | Non-zero exit on threshold breach                       |
| **Learning curve**       | Low (standard Locust)                           | Medium (pytest idioms + grasshopper fixtures)           |
| **Dependency footprint** | locust, locust-plugins                          | + locust-grasshopper, pytest                            |
| **Test discovery**       | locust -f flag                                  | pytest discovery (standard)                             |

---

## 5. Key Trade-offs

### Advantages of Grasshopper for DataHub

1. **CI regressions are automatic.** Thresholds mean a PR cannot accidentally regress search p90 without explicitly bumping the limit. This is the single most valuable property for a release qualification framework.

2. **Correctness visibility under load.** `check()` lets us assert `total > 0` on search results — catching empty-result bugs that only appear at high concurrency (cache stampedes, ES query timeouts returning empty).

3. **Historical comparison.** InfluxDB + Grafana lets you overlay this week's search ramp against last month's release — critical for release qualification.

4. **Multi-step journey timing.** `@custom_trend("search_flow")` captures the real user experience: keyword → facets → autocomplete as a single metric, not just three separate request histograms.

5. **YAML scenario catalog.** Smoke/ramp/stress scenarios are checked into source control. Any engineer can reproduce a specific load profile with one command.

### Limitations

1. **pytest wrapper friction.** Engineers familiar with `locust -f test.py` now need to use `pytest test.py` with grasshopper fixtures. The mental model shift is small but real.

2. **No distributed mode documentation.** Grasshopper's docs don't explicitly cover `locust --master/--worker` distributed mode. For very high load (>1000 users), plain Locust distributed mode may be needed.

3. **InfluxDB is optional but needed for value.** The threshold/check features work without InfluxDB, but historical comparison requires it. Running InfluxDB in CI adds infra overhead.

4. **Fixture magic.** `complete_configuration` arrives via pytest plugin. Debugging configuration issues requires understanding the grasshopper plugin chain, which is non-obvious.

---

## 6. Integration Strategy for DataHub

### Recommended adoption path

```
Phase 1: Drop-in replacement (this PR)
  - DataHubJourney extends BaseJourney (mirrors existing DataHubUser)
  - GlobalSearchJourney re-implements existing tasks with checks + custom_trend
  - YAML scenarios for smoke/ramp/stress
  - No InfluxDB dependency yet (thresholds work without it)

Phase 2: CI integration
  - Add perf smoke job to GitHub Actions (search_smoke scenario, 5 users, 60s)
  - Threshold failures block merge
  - Use --csv for CSV baseline if InfluxDB not yet available

Phase 3: Observability stack
  - Deploy InfluxDB + Grafana (Docker Compose in perf environment)
  - Add --influx_host to CI job
  - Import pre-built Grafana dashboards
  - Add check pass-rate panels to release qualification dashboard

Phase 4: Domain expansion
  - Ingest journey: MCP write throughput + check consistency
  - Lineage journey: getLineage multi-hop timing
  - Governance journey: policy eval under concurrent writes
  - Each domain adds a YAML scenario and @custom_trend wrapping its critical path
```

### Compatibility with Implementation 1

Implementations 1 and 2 share:

- The `datapacks/` seeder (unchanged — YAML scenarios point to the same seed-output/manifest.json)
- The `DATAHUB_GMS_TOKEN` env var convention
- The same GraphQL queries (copied into journey files)

They are completely independent — no code dependencies between `tests/` and `grasshopper_tests/`.

---

## 7. Verdict

Grasshopper is a **well-scoped, low-ceremony upgrade** to plain Locust. The core value is turning load tests from observation tools into regression gates via thresholds + checks. For a project like DataHub with a release qualification requirement, the automatic CI gating alone justifies the adoption.

The additional investment is modest: ~50 lines of boilerplate per new domain, one YAML scenario file, and an optional InfluxDB deployment. The payoff is a framework where `pytest --tags=smoke` is a complete pass/fail signal, not a stat dump requiring manual inspection.

**Recommendation: adopt as Implementation 2, targeting CI integration in Phase 2.**
