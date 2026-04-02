## Setup

### Prerequisites

1. **LangSmith account** - Sign up for a free Developer plan (5,000 traces/month) at
   [smith.langchain.com](https://smith.langchain.com).

2. **API key** - In LangSmith, navigate to **Settings > API Keys** and create a
   Personal Access Token (PAT) or Service Key. Copy the key (shown only once).

3. **Install the plugin:**
   ```shell
   pip install 'acryl-datahub[langsmith]'
   ```

### Concepts

| LangSmith Concept    | DataHub Entity                           | Notes                                                |
| -------------------- | ---------------------------------------- | ---------------------------------------------------- |
| **Project**          | Container (subtype: LangSmith Project)   | Top-level grouping of traces                         |
| **Trace** (root run) | DataProcessInstance (subtype: LLM Trace) | Full end-to-end LLM invocation                       |
| **Child span**       | DataProcessInstance (subtype: LLM Span)  | Retriever, LLM, tool, chain spans; opt-in            |
| **Dataset**          | Dataset                                  | Evaluation datasets for LLM testing                  |
| **LLM model**        | MLModel                                  | Stub entity from span metadata; upstream lineage     |
| **Feedback score**   | Assertion + AssertionRunEvent            | One assertion per (feedback key, run name); pass/fail by threshold; opt-in |
| Feedback (inline)    | Properties on DataProcessInstance        | Aggregated scores embedded as custom properties      |
| **Span execution flow** | DataProcessInstanceInput (inputEdges) | DPI-to-DPI lineage edges showing call graph; opt-in via include_child_spans |

Token usage (prompt/completion/total tokens) is captured as ML metrics on each trace,
enabling cost and usage tracking within DataHub.

### Span Execution Lineage

When `include_child_spans: true` is set, the connector computes an execution flow
graph from the span timestamps and emits `DataProcessInstanceInput.inputEdges` on
each span DPI. This makes the full call graph visible in the DataHub **Lineage** tab.

Execution order among sibling spans is inferred from `start_time`/`end_time`.
Non-overlapping siblings are treated as sequential steps; overlapping siblings are
treated as a parallel group (fan-in). For typical synchronous LangChain pipelines
(e.g. a RAG chain with a retriever, prompt, LLM, and parser), the ordering is
unambiguous and matches the actual call sequence.

Asset edges (Dataset for retrievers, MLModel for LLM calls) are attached to the
span that owns them rather than to the root trace, so the lineage graph shows
`[Dataset] -> [Retriever span] -> ... -> [Root trace]` rather than a flat star.

### Assertions from Feedback Scores

When `include_assertions: true` is set, the connector maps LangSmith feedback scores
to DataHub Assertion entities. Each unique (feedback key, run name) pair becomes one
Assertion entity -- for example, `correctness | rag-run-01` and
`correctness | rag-run-06` are separate assertions, each with their own current
pass/fail status. This means each named evaluation scenario is independently
monitored: failing scenarios appear as failing assertions in the Quality tab
immediately, rather than being hidden by later passing runs.

Each trace emits an `AssertionRunEvent` with a SUCCESS or FAILURE result, determined
by `assertion_score_threshold` (default: 0.7).

Assertions are linked to the eval dataset (auto-detected from ingested LangSmith
datasets, or set explicitly via `assertion_dataset_urn`). The Quality tab on the
dataset entity in DataHub shows each scenario's current pass/fail status and
numeric score.

To generate feedback scores, use `langsmith.Client.create_feedback()` in your
application code, or run LangSmith evaluators against your traces.

### Required Permissions

The API key must belong to a workspace member with at least **Viewer** access to the
projects you want to ingest.

### EU Region

If your LangSmith workspace is in the EU region, set `api_url` to
`https://eu.api.smith.langchain.com`.
