# ML Drift Root-Cause Standards

Rules for diagnosing silent model degradation with DataHub, and for writing the diagnosis back. Each rule states the reasoning so agents apply it in situations this document does not enumerate.

---

## 1. Drift is not degradation

### 1.1 Only alarm on real performance loss

An input distribution can shift a lot without the model getting worse, and it can shift a little while the model falls apart. Raw input drift (a KS test firing on a feature) is a diagnostic clue, not an incident on its own.

- A monotonic rescale of a feature (for example, a unit change from dollars to cents) moves the distribution but leaves a tree model's predictions unchanged. This is benign. Do not raise an incident.
- A feature that regresses to a constant default (for example, an upstream job coalescing nulls to 0) removes signal and degrades the model. This is harmful. Raise an incident.

### 1.2 Prefer label-free performance estimation

In production, ground-truth labels usually arrive late or never. Estimate performance without labels:

- Classification: NannyML CBPE (Confidence-Based Performance Estimation). Valid under covariate shift with calibrated probabilities.
- Regression: NannyML DLE (Direct Loss Estimation).

Report the estimate as directional. CBPE is not valid under concept drift (when the relationship between features and target changes), so say "estimated" and note the assumption.

### 1.3 Rank features with a comparable effect size

Different tests produce non-comparable statistics (a KS statistic and a Chi-squared statistic are not on the same scale). To rank suspects, use a comparable effect size: the KS statistic for numeric features, Cramer's V for categorical features. Correct p-values for multiple testing with Benjamini-Hochberg FDR, since testing many features inflates false positives. Confirm the top suspect with a data-quality fingerprint (null rate, cardinality, min/max range) before acting.

---

## 2. The write-back split

### 2.1 A model cannot hold an incident

DataHub's incident metamodel allows incidents on `dataset`, `chart`, `dashboard`, `dataFlow`, `dataJob`, and `schemaField` only. It does not allow incidents on `mlModel`. Attempting to raise one on a model fails.

Therefore split the write-back:

| Target | What to write | Mechanism |
| --- | --- | --- |
| The model (`mlModel`) | A typed structured property (for example `drift_causation`) | `add_structured_properties` (MCP) or the SDK structured-property API |
| The model (`mlModel`) | A context document with the full RCA | `save_document` (MCP) or the Document SDK |
| The model (`mlModel`) | An optional `drift-degraded` tag | GlobalTags aspect |
| The upstream `dataset` | An incident routed to the owner | `raiseIncident` GraphQL mutation |

### 2.2 Use the open-source-available write surfaces

On self-hosted DataHub Core with mutations enabled, `add_structured_properties` and `save_document` are available. Do not use `update_description` in the open-source path; it is Cloud-only in recent MCP versions. Incidents have no typed Python SDK yet, so call the `raiseIncident` GraphQL mutation directly.

### 2.3 Write-ahead every mutation

Before each catalog write, append the intended mutation to a local write-ahead log keyed by a deterministic id. On retry, skip anything already recorded as done. A partial failure mid-write must not double-write or corrupt the record.

---

## 3. Honesty

### 3.1 Correlation, not causation

Lineage plus timing plus a data-quality change is a strong correlation. It is not proof that the upstream change caused the degradation. State this in the RCA and recommend the human confirm the upstream commit history before rolling anything back.

### 3.2 The model reasons, code writes

Use the language model to synthesize the narrative and rank suspects. Execute every catalog mutation with deterministic code. This keeps the write path stable and auditable, and it means a demo or a production run behaves identically regardless of what the model says.
