### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Deletion Detection

Deletion detection is enabled, but scoped narrowly and intentionally:

- **Prompts** are fully enumerated on every run, so stale-entity soft deletion is safe and correctly removes Prompt versions that no longer exist in Langfuse.
- **Traces and Generations are excluded from deletion detection.** They are retrieved through the rolling time window above, not fully enumerated, so comparing "entities seen this run" against "entities seen last run" would incorrectly flag Traces/Generations that simply aged out of the window as deleted. This connector never soft-deletes a Trace or Generation, even if it is later removed from Langfuse.

#### Scores

Langfuse Scores are attached as `MLMetric` entries on the Trace or Generation they were recorded against. The original Langfuse `dataType` (`NUMERIC`, `BOOLEAN`, `CATEGORICAL`, etc.) is preserved in the metric's description, since DataHub's `MLMetric.value` field is a plain string.

Session-level and Dataset-Run-level Scores have no corresponding DataHub entity in this connector version and are dropped; the ingestion report includes a count of dropped scores.

#### Prompts

Every version of every Prompt is ingested as its own `Dataset` entity, with all versions of the same Prompt name linked together via a shared Version Set (the same mechanism DataHub uses for versioned ML models). Prompt labels (e.g. `production`, `latest`) become version aliases.

### Limitations

- **Datasets, Dataset Items, Dataset Runs/Experiments, and Dataset&harr;Trace lineage are not extracted.** Only Traces, Generations, Scores, and Prompts are in scope for this connector version.
- **Only `generation`-type Observations are emitted as individual entities.** Every other Observation type — `span`, `event`, `agent`, `tool`, `chain`, `retriever`, `embedding`, `guardrail`, etc. — is not emitted individually; it is only summarized as a count (`non_generation_observation_count`) on the parent Trace's custom properties, to bound entity cardinality for high-volume traces. In practice, applications instrumented primarily with agent/tool-call frameworks may have zero `generation`-type Observations, in which case Traces from that application will show zero nested Observation entities even though real LLM-call-shaped work happened underneath.
- **Sessions and Users are not emitted as separate entities.** Their IDs are recorded as custom properties on the Trace.
- **No lineage to conventional catalogued datasets.** Langfuse does not expose which upstream tables or warehouses an LLM application actually read from or wrote to.
- **A Trace whose root Observation started before the configured window, but which has a `generation` Observation that started inside it, is still ingested** using a partial, synthesized Trace record (flagged via `customProperties["partial_trace"] = "true"`) rather than being dropped.

### Troubleshooting

If ingestion fails, first verify:

- The configured `connection.host` is reachable and correct (no trailing slash).
- `connection.public_key` / `connection.secret_key` are valid for the target project — test with `datahub ingest -c recipe.yml --test-source-connection`.
- The target Langfuse deployment is self-hosted v4+ or Langfuse Cloud. If the connection test reports a v2/v3 API error, the deployment may be running an unsupported, older API generation.

Then review ingestion logs for source-specific warnings (e.g., a specific Prompt version failing to fetch) and adjust `trace_name_pattern` / `prompt_pattern` or the time `window` as needed.
