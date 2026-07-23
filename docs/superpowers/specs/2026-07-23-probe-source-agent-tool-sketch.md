# Sketch: wiring the recipe probes into the Cloud Ingestion Agent

**Date:** 2026-07-23
**Status:** Follow-up design sketch (not scheduled)
**Context:** The `datahub recipe probe` capability (this branch) is the executor-side
implementation of the "probe the source" capability the Ingestion Agent
(`agents/ingestion/`, in `datahub-fork`) does not yet have.

## The gap (verified in the fork)

The Ingestion Agent's tools (`agents/ingestion/backend/tools.py`) cover
`get_recipe_schema`, `validate_recipe`, `test_recipe_connection`, source-code
reads, and source/execution management — but there is **no tool to enumerate a
live source's schemas/tables/topics** so it can write `*_pattern` filters. Grep of
`agents/`, `datahub-executor/`, `datahub-integrations-service/` confirms no
probe / list-schemas path exists. The Notion caveat ("a first run can report
SUCCESS but ingest 0 entities — e.g. Unity Catalog scoping") is exactly what a
post-connection probe prevents: confirm the filters match real entities before
the first run.

## Why it must run executor-side (not in the agent)

`test_recipe_connection`'s own docstring states the constraint: the
integrations-service (agent host) carries only a thin dep subset; the **executor**
(customer VPC) has every connector's deps + network reach + resolves recipe
secrets from the GMS Secrets Store. So the agent delegates connectivity to the
executor via `createTestConnectionRequest` and polls the `executionRequest`. The
probe follows the identical path. This maps 1:1 onto this branch's design
principle ("credential boundary = execution boundary"): the click-free core
(`datahub.ingestion.agent` + `ProbeCapableConfig` on configs) is what ships in the
`acryl-datahub` wheel the executor runs; the `datahub recipe` CLI, `probe_api`,
and the local env-var `SecretResolver` do NOT port (dev-only surfaces).

## Three pieces (each mirrors existing test-connection plumbing)

### 1. Executor dispatch — `createProbeSourceRequest` (GMS + executor)

Add a GraphQL mutation `createProbeSourceRequest(input: {recipe, level, parentPath, limit, version})`
returning an executionRequest URN, sibling to `createTestConnectionRequest`.
The executor task handler:

1. Parses the recipe → `source.type` + `source.config`.
2. Resolves `${secretName}` refs from the GMS Secrets Store (the executor already
   does this for RUN_INGEST / test-connection — the probe gets resolved config for
   free; this is the deferred `GmsSecretsStoreResolver`, now satisfied by the host).
3. Calls the framework entrypoint already in `acryl-datahub`:
   `datahub.ingestion.agent.probe.probe(source_type, resolved_config, parent_path, limit)`.
4. Serializes `result.to_dict()` into the execution `structuredReport.serializedValue`.

**Lighter alternative:** route through the existing generic `RUN_TASK` verb with a
probe payload — no new GMS mutation, but less UI-aligned and no first-class
execution-request surface. Recommend the dedicated mutation for parity with
test-connection and future UI reuse ("show me what this recipe would ingest").

### 2. Agent tool — `probe_recipe_source` (near-copy of `test_recipe_connection`)

```python
@tool
def probe_recipe_source(
    yaml_text: str,
    parent_path: list[str] | None = None,   # [] top level; ["db"] its tables; ...
    limit: int = 200,
    version: str | None = None,
    timeout_seconds: int = 30,
    poll_interval_seconds: float = 2.0,
) -> dict[str, Any]:
    """List a live source's children (schemas/tables/topics/…) for the recipe,
    by submitting a probe request to the executor. Names/counts only — never row
    data. Each node reports `pattern_field` (which *_pattern to edit) and
    `included`/`excluded_by` (what the recipe's filters would actually keep)."""
```

Behavior: submit `createProbeSourceRequest` → poll `executionRequest` until
terminal (same 2s cadence, same worker-thread note) → parse `structuredReport`
as `ProbeResult` and return `data.nodes` / `data.supported` / `data.fallback`.
An unsupported source (`supported=false`) is a graceful, non-error result.

To discover levels first, the agent calls it with `parent_path=[]`; the returned
node kinds + `pattern_field`s tell it what the next level is. (Optionally add a
tiny `get_probe_hierarchy(connector)` tool backed by the connection-free
`probe_hierarchy(source_type)` so the agent can plan the descent without a round
trip — this needs no executor call and could even run in the agent host if the
connector's config class imports cheaply.)

### 3. Secrets & redaction

The executor resolves secrets in the VPC; the agent never sees them. The probe's
output is names/types/verdicts only. Keep this branch's output redactor as
defense-in-depth on the executor→cloud return leg (strip any resolved-secret
substring that a driver error might embed in a node/error field).

## What already exists vs. what's net-new

| Piece | Status |
|---|---|
| `probe(source_type, config, parent_path, limit)` framework entrypoint | **Done** (this branch, in `acryl-datahub`) |
| `ProbeCapableConfig` on ~16 connectors + `ClientProbe` | **Done** (this branch) |
| Secret resolution in executor | **Exists** (GMS Secrets Store, used by test-connection) |
| Redaction of resolved secrets from output | **Done** (this branch) |
| `createProbeSourceRequest` GraphQL mutation (GMS) | Net-new (mirror `createTestConnectionRequest`) |
| Executor probe task handler | Net-new (thin: resolve → `probe()` → structuredReport) |
| `probe_recipe_source` agent tool | Net-new (copy of `test_recipe_connection`) |

## Recommended agent workflow addition

Insert probing into the onboard flow, after test-connection passes and before
`create_ingestion_source`:
`describe/scaffold → validate_recipe → test_recipe_connection → probe_recipe_source (confirm filters match real entities, iterate on *_pattern) → create_ingestion_source`.
This directly closes the "SUCCESS but 0 entities" caveat.

## Open questions

- Cost/scale guardrails on `probe` for huge catalogs (reuse `limit` + the
  executor's per-task quotas; the essay's two-lane metadata-vs-data gate is
  overkill here since the probe is metadata-plane only).
- Whether `probe_hierarchy` runs agent-side (needs the connector config class
  importable in the integrations-service — often true, since it's pure pydantic)
  or always via the executor.
- Non-SQL sources with no `ProbeCapableConfig` yet return `supported=false`; the
  agent falls back to `test-connection` + docs (acceptable, matches today).
