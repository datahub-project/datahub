# LangSmith Connector - Planning Document

**Status:** v1.2 implemented - branch `feat/langsmith-source`
**Source type:** API (ML platform)
**Base class:** `StatefulIngestionSourceBase`

---

## Source Information

- **System:** LangSmith - LangChain's observability and evaluation platform
- **API:** REST at `api.smith.langchain.com` (US) / `eu.api.smith.langchain.com` (EU)
- **SDK:** `langsmith` Python package (`langsmith.Client`)
- **Auth:** API key via `X-Api-Key` header (PAT or Service Key)
- **Docs:** https://docs.smith.langchain.com/reference/python/reference

---

## Entity Mapping

| LangSmith Concept           | DataHub Entity                                 | SubType             | Rationale                                                               |
| --------------------------- | ---------------------------------------------- | ------------------- | ----------------------------------------------------------------------- |
| **Project** (TracerSession) | `Container`                                    | "LangSmith Project" | Top-level org unit; direct parallel to MLflow Experiment -> Container   |
| **Trace** (root Run)        | `DataProcessInstance`                          | "LLM Trace"         | Full LLM invocation; root-only avoids exploding volume from child spans |
| **Dataset**                 | `Dataset`                                      | n/a                 | Evaluation datasets map naturally; platform="langsmith"                 |
| **Feedback**                | Properties on DPI                              | (embedded)          | Aggregated per-key scores as custom properties; not standalone entities |
| **Token usage**             | `MLTrainingRunPropertiesClass.trainingMetrics` | n/a                 | prompt/completion/total tokens as `MLMetricClass` entries               |
| **Run tags**                | `GlobalTagsClass`                              | n/a                 | String tags on root runs -> DataHub tag entities                        |

### Why root runs only for traces

LangSmith runs form trees - a single user-facing LLM call may produce 5-20 child
spans (retriever calls, tool calls, LLM sub-calls). Ingesting all runs would
produce enormous volume with limited governance value. Root runs (`is_root=True`)
represent complete end-to-end invocations - the right granularity for cataloging.

Child span ingestion is the primary v2 candidate if users need deeper lineage.

---

## Architecture Decisions

| Decision      | Choice                                     | Rationale                                                         |
| ------------- | ------------------------------------------ | ----------------------------------------------------------------- |
| Base class    | `StatefulIngestionSourceBase`              | API-based ML platform; golden standard recommendation             |
| File layout   | Single file `langsmith.py`                 | 3 entity types; follows MLflow pattern (~280 lines)               |
| API client    | `langsmith` SDK directly                   | LangChain-maintained; handles auth, pagination, retries           |
| Emission      | MCP (`MetadataChangeProposalWrapper`)      | Modern pattern; matches MLflow and VertexAI                       |
| Stale removal | `StaleEntityRemovalHandler`                | Standard deletion detection; soft-deletes removed entities        |
| Container key | `ExperimentKey(platform, id=project.name)` | Reuses existing mcp_builder key; deterministic URN from name      |
| DPI identity  | `DataProcessInstance(id=str(run.id))`      | Run UUID is stable and unique                                     |
| External URL  | `run.app_path` field                       | LangSmith populates this on API responses; avoids org UUID lookup |

### Reference connectors consulted

- `mlflow.py` - primary structural reference (single file, MCP, DPI for runs, Container for experiments)
- `vertexai/` - reference for modular extractor pattern (deferred; overkill for v1 scope)

---

## Configuration

```python
class LangSmithConfig(StatefulIngestionConfigBase, EnvConfigMixin):
    api_key: SecretStr                          # required
    api_url: str = "https://api.smith.langchain.com"
    project_pattern: AllowDenyPattern           # filter by project name
    max_traces_per_project: Optional[int] = 1000
    trace_start_time: Optional[datetime]        # incremental window
    include_datasets: bool = True
    include_feedback_on_traces: bool = True
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig]
```

---

## v1 Scope (implemented)

- Projects as Containers with custom properties
- Traces (root runs) as DataProcessInstances with token metrics, feedback, tags, run event
- Datasets as Dataset entities
- Stateful stale entity removal
- 23 unit tests, 1 golden-file integration test
- Docs: `langsmith_pre.md`, `langsmith_recipe.yml`

## v1.1 Scope (implemented) - Child span ingestion

Opt-in via `include_child_spans: true` in the recipe.

- Child spans (retriever, llm, tool, chain, parser) are ingested as nested
  DataProcessInstances with subtype "LLM Span"
- Each child span carries a `dataProcessInstanceRelationships` aspect with
  `parentInstance` pointing to its direct parent's DPI URN
- Multi-level nesting (root -> tool call -> sub-chain -> LLM) works naturally:
  each child's `parent_run_id` is used to compute the parent's DPI URN
  deterministically, with no caching required
- LangSmith API call: `list_runs(trace_id=root_run.id, is_root=False)` - one
  call per root trace to fetch all child spans as a flat list
- New config field: `include_child_spans: bool = False`
- New report field: `spans_ingested: int`
- New subtype: `MLAssetSubTypes.LANGSMITH_SPAN = "LLM Span"`
- 30 unit tests (7 new), golden file updated (45 MCPs, 3 relationships aspects)

## Lineage in v1.1

Child spans produce `ChildOf` edges in DataHub's lineage graph. The execution
tree (root trace -> child spans) is visible in DataHub. However, there are still
no upstream dataset edges - the retriever span exists but has no data source
reference attached.

One level of execution hierarchy remains absent:

- **Pipeline template** - the RAG chain or agent definition has no DataHub
  representation. A fuller model would be:
  `DataFlow` (project) -> `DataJob` (pipeline definition) -> `DataProcessInstance`
  (run), with each DPI carrying a `parentTemplate` edge back to its DataJob.

The full lineage story (retriever span -> vector store -> source dataset) requires
either (a) the seed script using a named/persistent vector store (e.g., Chroma),
or (b) the connector detecting retriever spans and emitting stub Dataset entities
for the referenced data sources.

## v1.2 Scope (implemented) - MLModel lineage for LLM spans

Emit stub `MLModel` entities for LLM child spans, creating lineage:
`[Dataset] -> [rag-run-XX] <- [MLModel]`

- Model metadata (`ls_model_name`, `ls_provider`, `ls_model_type`) comes from
  `run.extra["metadata"]`, populated automatically by LangChain's
  `BaseChatModel._get_ls_params()` - no seed script changes needed.
- MLModel URN: `urn:li:mlModel:(urn:li:dataPlatform:{provider},{model_name},{env})`
  e.g. `urn:li:mlModel:(urn:li:dataPlatform:anthropic,claude-haiku-4-5-20251001,PROD)`
- New methods: `_build_model_stub(run)`, `_build_retriever_stub(project, run)`,
  `_collect_run_edges(project, run, input_edge_urns)`.
- Edge accumulator pattern in `_get_trace_workunits`: all dataset + model input edges
  for a root trace are collected across all child spans and emitted as a SINGLE
  `DataProcessInstanceInputClass` aspect, avoiding UPSERT collision.
- Deduplication: multiple LLM calls to the same model within one trace produce one edge.
- New report field: `models_emitted: int`
- New config: none (uses existing `include_child_spans`)
- 42 unit tests (5 new), golden file updated (~57 MCPs)

| LangSmith Concept                  | DataHub Entity | SubType | Rationale                                |
| ---------------------------------- | -------------- | ------- | ---------------------------------------- |
| **LLM model** (from span metadata) | `MLModel`      | n/a     | Stub entity; platform from `ls_provider` |

## v1.3 Scope (implemented) - Assertions from feedback scores

Emit DataHub Assertion entities from LangSmith feedback scores on traces.

- One `Assertion` entity per unique feedback key (e.g. `correctness`, `helpfulness`)
- Linked to the eval dataset via `CustomAssertionInfo.entity` (PDL constraint: must be `dataset`)
- One `AssertionRunEvent` per trace with feedback for that key
- Pass/fail determined by `assertion_score_threshold` (default 0.7)
- `AssertionResult.actualAggValue` carries the avg score float
- Dataset ingested before projects in `get_workunits_internal` so `_dataset_urns` is
  populated for assertion target resolution before trace processing
- Assertion URN: `make_assertion_urn(datahub_guid({"platform", "feedback_key", "dataset"}))`
  - Deterministic and stable across ingestions
- Auto-detects assertion target from ingested datasets; configurable override via
  `assertion_dataset_urn`
- New config: `include_assertions: bool = False`, `assertion_score_threshold: float = 0.7`,
  `assertion_dataset_urn: Optional[str] = None`
- New report fields: `assertions_emitted`, `assertion_run_events_emitted`,
  `assertions_skipped_no_dataset`
- Seed script updated: LLM-as-judge evaluation feedback on RAG traces
  (`correctness` + `helpfulness`), status-based feedback on agent traces
  (`task_completion`)
- 56 unit tests (10 new), golden file updated (62 MCPs: +8 assertion MCPs,
  dataset MCPs now precede project MCPs)

### Live testing note: async Kafka reliability in quickstart

When running against a local DataHub quickstart, use `mode: SYNC` in the recipe
sink config. The default `datahub-rest` sink uses `ASYNC_BATCH` mode, which
queues regular aspects (e.g. `assertionInfo`, `dataPlatformInstance`) on the
Kafka MCP topic. If the Kafka group coordinator is rebalancing at ingest time,
those proposals are silently dropped. Timeseries aspects (`AssertionRunEvent`)
are written synchronously by GMS regardless of mode and are not affected.

```yaml
sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
    mode: SYNC
```

The recipe comment in `langsmith_recipe.yml` documents this.

## v1.4 Scope (implemented) - Per-run-name assertion identity

Changed assertion identity from one-per-feedback-key to one-per-(feedback_key, run_name).

### Problem

The DataHub Quality tab shows only the latest run event's status per assertion.
With one assertion per feedback key and 72 traces flowing through it, only the
last trace's result was visible -- 16 correctness failures (on rag-run-01 and
rag-run-06) were buried in run history and invisible. As observed: "like saying
all planes in the fleet are airworthy because the last one inspected passed."

### Fix

Include `run.name` in the `datahub_guid` dict for assertion URN generation:

```python
# Before
datahub_guid({"platform": ..., "feedback_key": key, "dataset": dataset_urn})

# After
datahub_guid({"platform": ..., "feedback_key": key, "dataset": dataset_urn, "run_name": run_name})
```

Each named scenario now has its own assertion entity with its own current status.
The description and `customProperties` also include `run_name`.

- `assertions_emitted` count increases: from 3 to ~26 in demo data
  (8 run names x 2 RAG keys + 10 run names x 1 agent key)
- All existing assertion URNs changed -- stale entity removal soft-deletes old ones
- No new config flag; this is the default behavior

### Architecture note: name cardinality

If run names are dynamic (timestamps, user IDs, etc.), assertion entity count
could grow unboundedly. Unlikely for LangSmith eval projects where run names
reflect chain/agent definitions. Revisit if users report high cardinality --
potential mitigation: config option to normalize names or revert to per-key-only.

### Changes

- `langsmith.py:_emit_assertion_workunits` -- added `run_name` to guid dict,
  description, and customProperties; updated `include_assertions` field description
- Unit tests: renamed dedup test, added 3 new tests (per-name, dedup-same-name,
  description, customProperties)
- Golden file: regenerated (assertion URNs + description + customProperties changed)
- `langsmith_pre.md`: updated entity mapping table and assertions section

| LangSmith Concept              | DataHub Entity              | Notes                                          |
| ------------------------------ | --------------------------- | ---------------------------------------------- |
| **Feedback score on trace**    | `Assertion` + `AssertionRunEvent` | Custom assertion type; one per feedback key |

## v2 Candidates (not implemented)

- **DataJob as pipeline template** - emit a `DataJob` per distinct run name/type
  within a project; DPIs carry `parentTemplate` -> DataJob. Gives execution history
  per pipeline definition, not just per project.
- **Prompt Hub ingestion** - versioned prompts as `MLModel`/`MLModelGroup`; maps
  naturally to DataHub's version model
- **Experiment ingestion** - evaluation runs (one per dataset example) as Containers
  or DPIs with evaluator feedback
- **Dataset -> Trace lineage** - when a trace `reference_example_id` links to a
  dataset example, emit an upstream lineage edge
- **Assertions from LangSmith evaluations** - map LangSmith evaluator results to
  DataHub `AssertionInfo` + `AssertionRunEvent`. Each evaluator (correctness,
  faithfulness, etc.) becomes an assertion; each evaluation experiment run emits
  an `AssertionRunEvent` (PASS/FAIL + numeric score). Surfaces health indicators
  on project Containers in the DataHub UI without requiring lineage to be in place.
  Narrower audience than trace ingestion (requires structured LangSmith evals).
- **Incremental ingestion** - time-windowed using `trace_start_time`; needed for
  large workspaces
- **AllowDenyPattern on run_type** - filter to only ingest chain/llm/tool runs

## Prioritisation: lineage vs observability

Lineage should come before assertions for one reason: lineage makes observability
meaningful in context. The interesting signal is not "error rate this week" (visible
in LangSmith directly) but "RAG quality dropped and the upstream dataset it retrieves
from was last refreshed 47 days ago" - that causal chain requires the lineage edge
to exist first.

Assertions from evaluations have strong standalone ROI and do not depend on lineage,
but their audience is narrower (teams running structured LangSmith evaluations with
configured evaluators and thresholds, not all LangSmith users).

A third direction worth noting: DataHub health signals as inputs TO LangSmith context
(e.g. tagging traces with "upstream-data-stale" when a source dataset has a failing
freshness assertion). This is DataHub-side work but represents the integration moving
from passive catalog to active governance layer.

Practical order:

1. Child span lineage + upstream dataset edges (foundational; unlocks governance narrative)
2. Assertions from LangSmith evaluation results (standalone ROI; high UI visibility)
3. DataJob as pipeline template (execution history per pipeline definition)
4. Dataset -> Trace lineage via reference_example_id (narrower, evaluation-focused)

---

## Live Testing

### Seed data

Before running a live ingestion, populate LangSmith with demo-worthy traces using
the seed script at `~/src/langchain/seed_langsmith_demo.py`.

```bash
cd ~/src/langchain
mise install                         # sets up Python 3.13.2 + .venv
pip install -r requirements.txt      # langsmith, langgraph, langchain-anthropic

export LANGSMITH_API_KEY="lsv2_pt_..."
export ANTHROPIC_API_KEY="sk-ant-..."

python seed_langsmith_demo.py
```

The script creates two projects and one evaluation dataset:

| LangSmith asset          | What it is                                             | DataHub entity after ingestion      |
| ------------------------ | ------------------------------------------------------ | ----------------------------------- |
| `langsmith-demo-agent`   | LangGraph ReAct agent - 10 traces, 2-4 tool calls each | Container + 10 DataProcessInstances |
| `langsmith-demo-rag`     | RAG pipeline - 8 traces with retriever + LLM steps     | Container + 8 DataProcessInstances  |
| `langsmith-demo-qa-eval` | Q&A eval dataset, 8 examples                           | Dataset                             |

Cost: ~$0.03 using Claude Haiku 4.5. No API keys beyond Anthropic + LangSmith.

### Run the connector

```bash
# Start DataHub locally
datahub docker quickstart

# Ingest LangSmith metadata
export LANGSMITH_API_KEY="lsv2_pt_..."
datahub ingest -c docs/sources/langsmith/langsmith_recipe.yml
```

Verify in the DataHub UI: search for "langsmith" to find the two project Containers,
their trace DataProcessInstances (with token metrics and tags), and the eval Dataset.

---

## v1.2 - MLModel Lineage Nodes (implemented)

### What changed

Added `MLModel` entity stubs for LLM child spans, creating upstream lineage edges
from each root trace DPI to both the retriever Dataset and the LLM model:

```
[Dataset]  ->  rag-run-XX  <-  [MLModel]
```

### Key design decisions

**Edge accumulator pattern** - DataHub MCPs are UPSERT by entity+aspect. A DPI can
have only one `DataProcessInstanceInput` aspect. Previously, the retriever edge was
emitted inside `_emit_run` for retriever child spans. Emitting a second aspect for
the model would have overwritten it. Fix: collect ALL input edges (dataset + model)
from all child spans into a shared `input_edge_urns: Dict[str, EdgeClass]` dict,
then emit a single `DataProcessInstanceInputClass` on the root trace DPI after all
children are processed.

**MLModel URN** - `urn:li:mlModel:(urn:li:dataPlatform:{provider},{model_name},{env})`

- Platform: `ls_provider` from run metadata if present; otherwise inferred from
  model name prefix via `_infer_provider()` (`claude` -> `anthropic`, `gpt-`/`o1-`/
  `o3-`/`o4-` -> `openai`, `gemini` -> `google`, etc.)
- Name: `ls_model_name` from run metadata, with fallbacks to
  `invocation_params["model"]` and `metadata["model"]`

**Deduplication** - same model called multiple times within a trace yields one edge
(dict keyed by URN). `report.models_emitted` counts call-sites, not unique models.

### New methods

| Method                                              | Purpose                                                                   |
| --------------------------------------------------- | ------------------------------------------------------------------------- |
| `_build_model_stub(run)`                            | Build MLModel stub + return (urn, workunits); None if no model metadata   |
| `_build_retriever_stub(project, run)`               | Extracted from old `_emit_retriever_input_edge`; returns (urn, workunits) |
| `_collect_run_edges(project, run, input_edge_urns)` | Unified edge collector; populates shared dict, yields stubs               |
| `_infer_provider(model_name)`                       | Static; infers DataHub platform from model name prefix                    |

### Test coverage

44 unit tests (up from 23). New tests cover: `_build_model_stub` with all three
fallback paths, provider inference, combined retriever+LLM edges, deduplication.
Integration golden file regenerated with 2-edge `dataProcessInstanceInput` aspects
and MLModel entity MCPs.

---

## Files

| Path                                                     | Purpose                                                 |
| -------------------------------------------------------- | ------------------------------------------------------- |
| `src/datahub/ingestion/source/langsmith.py`              | Main connector                                          |
| `src/datahub/ingestion/source/common/subtypes.py`        | Added `LANGSMITH_PROJECT`, `LANGSMITH_TRACE`            |
| `setup.py` + `pyproject.toml`                            | `langsmith` extras, entry point                         |
| `tests/unit/test_langsmith_source.py`                    | 23 unit tests                                           |
| `tests/integration/langsmith/test_langsmith_source.py`   | Integration test                                        |
| `tests/integration/langsmith/langsmith_mcps_golden.json` | Golden file (regenerated for v1.2)                      |
| `docs/sources/langsmith/langsmith_pre.md`                | User setup docs                                         |
| `docs/sources/langsmith/langsmith_recipe.yml`            | Example recipe                                          |
| `~/src/langchain/seed_langsmith_demo.py`                 | Demo seed script (LangGraph agent + RAG + eval dataset) |
