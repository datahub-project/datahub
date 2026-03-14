# k3d Resource Optimizer — Implementation Plan

## Goal

Create a script that finds the minimum memory and CPU for each DataHub
component (GMS, frontend, actions) that still passes the full smoke test suite.
Uses binary search per component with per-trial checkpointing so it can be
resumed after interruption.

## Design Decisions (already approved)

- **Scope**: DataHub components only (GMS, frontend, actions). Infra (MySQL, Kafka, ES) stays fixed.
- **Dimensions**: Memory (request + limit) and CPU (request). JVM heap is coupled to memory limit (heap ≤ 75% of limit).
- **Strategy**: Binary search per component, one at a time. High = current working value, low = floor.
- **Checkpointing**: Per-trial granularity. JSON state file records every deploy+test cycle.
- **Pass criteria**: Pods healthy within 5 min + pytest passes (with flaky test handling).
- **Flaky test handling**: On test failure, re-run failed tests up to 2 times. A trial only counts as "failed" if the same tests fail on all retries OR if pods OOM/crash (a resource issue, not flakiness). Known-flaky tests can be excluded via a skip list.

## File to create

`k3d/optimize-resources.py` — single self-contained Python script (no new deps beyond what k3d/ already has: click, subprocess, json, pathlib).

## Component Search Space

```
Component   | Dimension       | Current  | Floor   | Step
------------|-----------------|----------|---------|------
GMS         | memory_limit    | 1536Mi   | 512Mi   | 64Mi
GMS         | memory_request  | 512Mi    | 256Mi   | 64Mi
GMS         | cpu_request     | 200m     | 50m     | 25m
GMS         | jvm_heap        | 512m     | 256m    | 64m  (derived: min(floor, 75% of memory_limit))
Frontend    | memory_limit    | 768Mi    | 256Mi   | 64Mi
Frontend    | memory_request  | 256Mi    | 128Mi   | 64Mi
Frontend    | cpu_request     | 100m     | 25m     | 25m
Frontend    | jvm_heap        | 256m     | 128m    | 64m
Actions     | memory_limit    | 384Mi    | 128Mi   | 64Mi
Actions     | memory_request  | 128Mi    | 64Mi    | 64Mi
Actions     | cpu_request     | 50m      | 25m     | 25m
```

## Implementation Steps

### Step 1: Define data structures and constants

Create the search space configuration as Python dicts:

```python
COMPONENTS = {
    "gms": {
        "helm_prefix": "datahub-gms",
        "has_jvm": True,
        "jvm_env_key": "JAVA_OPTS",
        "dimensions": {
            "memory_limit":   {"current": 1536, "floor": 512,  "step": 64, "unit": "Mi"},
            "memory_request": {"current": 512,  "floor": 256,  "step": 64, "unit": "Mi"},
            "cpu_request":    {"current": 200,  "floor": 50,   "step": 25, "unit": "m"},
        },
    },
    "frontend": {
        "helm_prefix": "datahub-frontend",
        "has_jvm": True,
        "jvm_env_key": "JAVA_OPTS",
        "dimensions": {
            "memory_limit":   {"current": 768,  "floor": 256,  "step": 64, "unit": "Mi"},
            "memory_request": {"current": 256,  "floor": 128,  "step": 64, "unit": "Mi"},
            "cpu_request":    {"current": 100,  "floor": 25,   "step": 25, "unit": "m"},
        },
    },
    "actions": {
        "helm_prefix": "acryl-datahub-actions",
        "has_jvm": False,
        "dimensions": {
            "memory_limit":   {"current": 384,  "floor": 128,  "step": 64, "unit": "Mi"},
            "memory_request": {"current": 128,  "floor": 64,   "step": 64, "unit": "Mi"},
            "cpu_request":    {"current": 50,   "floor": 25,   "step": 25, "unit": "m"},
        },
    },
}
```

### Step 2: Checkpoint file read/write

State file: `k3d/resource-optimization-state.json`

```python
@dataclass
class Trial:
    id: int
    component: str
    dimension: str
    value: int
    unit: str
    all_overrides: dict[str, str]  # full set of --set args used
    test_passed: bool | None       # None = in progress
    duration_s: float
    timestamp: str
    pod_events: list[str]          # OOM, CrashLoop, etc.

@dataclass
class SearchState:
    low: int
    high: int
    step: int

@dataclass
class OptimizerState:
    started_at: str
    components_order: list[str]          # ["gms", "frontend", "actions"]
    current_component: str | None
    current_dimension: str | None
    search_state: SearchState | None
    trials: list[Trial]
    results: dict[str, dict[str, int]]   # component -> {dim: optimal_value}
```

Functions:
- `load_state(path) -> OptimizerState | None` — returns None if no file
- `save_state(state, path)` — atomic write (write to .tmp, rename)
- On resume: find last incomplete trial or next untested component/dimension

### Step 3: Helm override generation

Given the current best-known values for all components plus the trial value for
the dimension being tested, generate `--set` args:

```python
def build_helm_overrides(state: OptimizerState, trial_component: str,
                         trial_dimension: str, trial_value: int) -> list[str]:
```

Mapping from dimensions to Helm paths:
- `memory_limit`   → `{prefix}.resources.limits.memory={value}Mi`
- `memory_request` → `{prefix}.resources.requests.memory={value}Mi`
- `cpu_request`    → `{prefix}.resources.requests.cpu={value}m`

For JVM heap (GMS/frontend): when `memory_limit` changes, derive heap as
`min(current_heap, int(memory_limit * 0.75))` rounded down to nearest 64m.
JVM heap is set via `{prefix}.extraEnvs` which is an array — need to use
Helm `--set` array syntax or generate a small values override file instead.

**Decision**: Generate a temporary values YAML file for each trial rather than
using `--set` for complex nested structures like `extraEnvs`. Write it to
`/tmp/optimize-trial-{id}.yaml` and pass via `-f`.

### Step 4: Deploy function

```python
def deploy_trial(overrides_file: str) -> bool:
    """Run helm upgrade with the trial overrides. Returns True if pods become healthy."""
```

Uses `k3d/dh deploy --local -f {overrides_file}` or direct `helm upgrade` with
the override file appended. The script must:

1. Run the deploy command
2. Wait for pods (timeout 5 min) — check for OOM (exit 137), CrashLoopBackOff
3. Return pass/fail

If pods never become Ready, record failure and move on.

### Step 5: Test runner function with flaky test handling

```python
def run_smoke_tests(max_retries: int = 2) -> tuple[bool, float, list[str]]:
    """Run pytest, return (passed, duration_seconds, failed_tests)."""
```

Runs from `smoke-test/` directory with env vars:
- `DATAHUB_GMS_URL` / `DATAHUB_FRONTEND_URL` — resolved via `k3d/dh status -o json`
- `ADMIN_USERNAME` / `ADMIN_PASSWORD` — parsed from `datahub-frontend/conf/user.props`
- `USE_STATIC_SLEEP=true`

**Flaky test strategy**:

1. First run: `pytest tests/ --tb=line -q --junitxml=/tmp/optimize-results.xml`
2. If all pass → trial passes
3. If some fail, check if pods are OOM/CrashLoopBackOff:
   - If yes → trial fails (resource issue, not flakiness)
   - If no → re-run only the failed tests (up to `max_retries` times)
4. If re-runs pass → trial passes (original failures were flaky)
5. If re-runs still fail → trial fails

Additionally, a `--known-flaky` file can list test node IDs to always ignore:
```
# Tests known to be flaky or incompatible with k3d
tests/openapi/v1/test_tracking.py::test_tracking_api_kafka
tests/tokens/session_access_token_test.py::test_01_soft_delete
```

Parse failed tests from junit XML or pytest output to build the re-run list.

**Distinguishing resource failures from flaky tests**:
- OOM kill (exit 137) or CrashLoopBackOff → always a resource failure
- Connection refused / timeout to GMS → likely resource failure (GMS crashed)
- Assertion errors with healthy pods → likely flaky test, worth retrying

### Step 6: Binary search loop

For each component, for each dimension:

```python
def optimize_dimension(state, component, dimension):
    dim_cfg = COMPONENTS[component]["dimensions"][dimension]
    low, high, step = dim_cfg["floor"], dim_cfg["current"], dim_cfg["step"]

    # Resume from checkpoint if available
    if state.search_state:
        low, high, step = state.search_state.low, state.search_state.high, state.search_state.step

    while high - low >= step:
        mid = round_to_step((low + high) // 2, step)
        # Save state before trial
        state.search_state = SearchState(low=low, high=high, step=step)
        save_state(state)

        trial = run_trial(state, component, dimension, mid)

        if trial.test_passed:
            high = mid  # can go lower
        else:
            low = mid + step  # need more

    return high  # last known good
```

Order of optimization within each component: `memory_limit` first (since it
constrains JVM heap), then `memory_request`, then `cpu_request`.

### Step 7: CLI interface

```python
@click.command()
@click.option("--state-file", default="k3d/resource-optimization-state.json")
@click.option("--resume/--no-resume", default=True)
@click.option("--components", default="gms,frontend,actions")
@click.option("--dry-run", is_flag=True, help="Show what would be tested without deploying")
def main(state_file, resume, components, dry_run):
```

### Step 8: Output

When all components are optimized:

1. Print summary table:
```
Component  | Dimension       | Current | Optimal | Saved
-----------|-----------------|---------|---------|------
GMS        | memory_limit    | 1536Mi  | 768Mi   | 768Mi
GMS        | memory_request  | 512Mi   | 384Mi   | 128Mi
...
```

2. Write `k3d/values/optimized-resources.yaml` — a Helm values override file
   that can be used with `k3d/dh deploy -f k3d/values/optimized-resources.yaml`.

3. Print total resource savings.

## Testing

No unit tests for this script — it's an operational tool that requires a live
cluster. Test manually by running with `--dry-run` first.

## Estimated trials

Per component: 3 dimensions × ~4 binary search steps each = ~12 trials.
3 components = ~36 total trials.
Each trial: ~2 min deploy + ~10 min tests = ~12 min.
Total: ~7 hours worst case (many trials will fail fast on OOM → < 1 min).

## Resume behavior

On `--resume` (default):
1. Load state file
2. If last trial has `test_passed: None` → re-run it (was interrupted)
3. If current_dimension has search_state → continue binary search
4. If current_component is done → move to next component
5. If all components done → print results and exit
