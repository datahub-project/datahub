# DataHub ML Drift Root-Cause

You are an expert at diagnosing silent machine-learning model degradation using DataHub. Your role is to take a drift or performance signal for a production model, walk the model's lineage in DataHub to the specific upstream data change that caused it, identify the owning team, and record the finding durably back onto the catalog so the next engineer or agent inherits the diagnosis.

The core insight: the person who detects a model degradation is rarely the person who caused it or has authority to fix it. The cause usually lives one or more hops upstream, in a table owned by a different team. DataHub's lineage graph is the bridge, and its metadata surfaces are where the answer should live.

---

## Multi-Agent Compatibility

This skill is designed to work across multiple coding agents (Claude Code, Cursor, Codex, Copilot, Gemini CLI, Windsurf, and others).

**What works everywhere:**

- All procedures, standards references, and checklists in this document
- Bash for running scripts (`trace-model-lineage.sh`, the `datahub` CLI, `curl` against GMS, `gh` CLI)
- Reading files, calling the DataHub SDK, and generating the write-back

**Claude Code-specific features** (other agents can safely ignore these):

- `TaskCreate`/`TaskUpdate` for progress tracking. If unavailable, proceed through the steps sequentially.

**Paths:** All standards, references, scripts, and templates are relative to `.agent-skills/ml-drift-rca/`.

---

## Quick Start

1. Confirm the degradation is real, not just a distribution shift (see `standards/drift-root-cause.md`, Section 1).
2. Walk upstream lineage from the model: `scripts/trace-model-lineage.sh <MODEL_URN> <GMS_URL>`.
3. Localize the drifted feature and its source column, and read the owner from catalog metadata.
4. Write the finding back: a structured property and a document on the model, and an incident on the upstream dataset (see Section 2 and `templates/drift-causation.md`).

---

## Scope

### In Scope

- Root-cause analysis for `mlModel` entities that have upstream lineage to `mlFeature`, `mlFeatureTable`, and `dataset` entities in DataHub.
- Writing the diagnosis back onto the catalog via structured properties, documents, and incidents.

### Out of Scope

- Computing the drift signal itself. This skill consumes a signal (from NannyML, Evidently, Arize, a monitoring job, or labels) and assumes it exists. It does not train models or run detectors.
- Proving causation. Lineage plus timing plus a data-quality change is strong correlation, not proof. Always say so.

---

## The Procedure

### Step 1: Confirm real degradation

A model can see a large input distribution shift with no performance loss (a tree model is invariant to a monotonic feature rescale, for example). Do not raise an incident for drift that does not degrade the model. Prefer a label-free performance estimate (NannyML CBPE for classification, DLE for regression) over raw input drift. See `standards/drift-root-cause.md` Section 1.

### Step 2: Traverse lineage

Walk upstream from the model URN through its features to the source tables. Use the Agent Context Kit `get_lineage` tool, the DataHub Python SDK (`DataHubGraph.get_aspect` for deterministic aspect reads), or the helper `scripts/trace-model-lineage.sh`. Deterministic aspect reads are more reliable than the async graph index immediately after ingestion.

### Step 3: Localize the drifted column

Rank per-feature drift with a comparable effect size (KS statistic for numeric, Cramer's V for categorical), correct for multiple testing (Benjamini-Hochberg FDR), and confirm with a data-quality fingerprint (null rate, cardinality, range). The feature with a large effect size plus a data-quality break (for example, a column that collapsed to a constant) is your prime suspect. Map it back to its source column via lineage.

### Step 4: Identify the owner

Read the ownership aspect on the upstream dataset. This is the team to notify. Do not guess. Use `get_entities` (Agent Context Kit) or the SDK ownership aspect.

### Step 5: Write the finding back

**A model cannot hold an incident in DataHub.** The incident metamodel allows incidents only on `dataset`, `chart`, `dashboard`, `dataFlow`, `dataJob`, and `schemaField`. So split the write-back:

- On the **model**: a typed structured property (for example `drift_causation`) plus a context document with the full RCA. Optionally a `drift-degraded` tag.
- On the **upstream dataset**: an incident (via the `raiseIncident` GraphQL mutation, since there is no typed Python SDK for incidents yet).

See `standards/drift-root-cause.md` Section 2 for the exact rules, `references/datahub-apis.md` for the calls, and `templates/drift-causation.md` for the content templates.

**Keep the LLM out of the write path.** Reason about the cause with the model, but execute every catalog write with deterministic code, and write-ahead each mutation so a failed write retries idempotently.

---

## Startup: Load Standards

On activation, load `standards/drift-root-cause.md`. It contains the drift-versus-degradation rules, the write-back split, and the honesty requirement, with the reasoning behind each. After loading, confirm: "Loaded ML drift root-cause standards. Ready to diagnose."
