## Overview

[Langfuse](https://langfuse.com/) is an open-source LLM observability and evaluation platform. Applications send LLM traces to Langfuse via its SDKs or OpenTelemetry, and Langfuse records the resulting Traces, Observations (spans/generations/events), Scores, and versioned Prompts, all scoped to a Project.

The DataHub integration for Langfuse catalogs a Langfuse project's versioned Prompts as first-class, stale-tracked entities, and surfaces recent Traces and their LLM-call ("generation") Observations as execution records within a configurable rolling time window, with Scores attached as metrics. Datasets, Dataset Runs/Experiments, and Dataset&harr;Trace lineage are not covered by this connector version.

## Concept Mapping

| Source Concept                             | DataHub Concept                                                        | Notes                                                                                                              |
| ------------------------------------------ | ---------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| `"langfuse"`                               | [Data Platform](../../metamodel/entities/dataPlatform.md)              |                                                                                                                    |
| Langfuse Project                           | [Container](../../metamodel/entities/container.md)                     | Subtype `Langfuse Project`                                                                                         |
| Langfuse Trace                             | [DataProcessInstance](../../metamodel/entities/dataProcessInstance.md) | Subtype `Langfuse Trace`; retrieved within the configured rolling time window, not fully enumerated                |
| Langfuse Observation (type `generation`)   | [DataProcessInstance](../../metamodel/entities/dataProcessInstance.md) | Subtype `Langfuse Generation`; nested under its parent Trace via `parentInstance`                                  |
| Langfuse Observation (type `span`/`event`) | _(not emitted individually)_                                           | Summarized as a count on the parent Trace's custom properties                                                      |
| Langfuse Score                             | `MLMetric` on the target Trace/Generation                              | Attached to Trace- and Observation-level scores only; Session- and Dataset-Run-level scores are dropped (reported) |
| Langfuse Session / User                    | Custom properties on the Trace                                         | Not emitted as separate entities in this version                                                                   |
| Langfuse Prompt (per version)              | [Dataset](../../metamodel/entities/dataset.md)                         | Subtype `Langfuse Prompt`; versions of the same prompt are linked via a shared Version Set                         |
