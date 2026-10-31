


# Langfuse

## Overview

[Langfuse](https://langfuse.com/) is an open-source LLM observability and evaluation platform. Applications send LLM traces to Langfuse via its SDKs or OpenTelemetry, and Langfuse records the resulting Traces, Observations (spans/generations/events), Scores, and versioned Prompts, all scoped to a Project.

The DataHub integration for Langfuse catalogs a Langfuse project's versioned Prompts as first-class, stale-tracked entities, and surfaces recent Traces and their LLM-call ("generation") Observations as execution records within a configurable rolling time window, with Scores attached as metrics. Datasets, Dataset Runs/Experiments, and Dataset&harr;Trace lineage are not covered by this connector version.

## Concept Mapping

| Source Concept                             | DataHub Concept                                                        | Notes                                                                                                                          |
| ------------------------------------------ | ---------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `"langfuse"`                               | [Data Platform](../../metamodel/entities/dataPlatform.md)              |                                                                                                                                |
| Langfuse Project                           | [Container](../../metamodel/entities/container.md)                     | Subtype `Langfuse Project`                                                                                                     |
| Langfuse Trace                             | [DataProcessInstance](../../metamodel/entities/dataProcessInstance.md) | Subtype `Langfuse Trace`; retrieved within the configured rolling time window, not fully enumerated                            |
| Langfuse Observation (type `generation`)   | [DataProcessInstance](../../metamodel/entities/dataProcessInstance.md) | Subtype `Langfuse Generation`; nested under its parent Trace via `parentInstance`; input lineage to the Prompt version it used |
| Langfuse Observation (type `span`/`event`) | _(not emitted)_                                                        | Filtered out on the Langfuse server; only root and `generation` Observations are retrieved                                     |
| Langfuse Score                             | `MLMetric` on the target Trace/Generation                              | Attached to Trace- and Observation-level scores only; Session- and Dataset-Run-level scores are dropped (reported)             |
| Langfuse Session / User                    | Custom properties on the Trace                                         | Not emitted as separate entities in this version                                                                               |
| Langfuse Prompt (per version)              | [Dataset](../../metamodel/entities/dataset.md)                         | Subtype `Langfuse Prompt`; versions of the same prompt are linked via a shared Version Set                                     |


## Module `langfuse`
![Alpha](https://img.shields.io/badge/support%20status-Alpha-lightgrey)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Enabled by default (Project container). |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled for Prompts only (fully enumerated each run). NOT enabled for Traces/Generations, which are retrieved through a rolling time window and are excluded from stale-entity tracking to avoid incorrectly soft-deleting entities that simply aged out of the window. |
| Table-Level Lineage | ✅ | Enabled by default: Generation -> Prompt version lineage via DataProcessInstanceInput. |

### Overview

The `langfuse` module ingests metadata from a single Langfuse project into DataHub.

#### Compatibility

This connector targets the Langfuse Public API v2/v3 surface (`/api/public/v2/observations`, `/api/public/v2/prompts`, `/api/public/v3/scores`). It requires **self-hosted Langfuse v4 or newer**, or Langfuse Cloud. Some self-hosted v4 deployments run in "events_only" mode, which disables the legacy, unversioned `/api/public/traces`-style endpoints entirely; this connector never uses those endpoints, so it works correctly against both events_only and non-events_only v4 deployments. Older self-hosted deployments (pre-v4) that only expose the legacy v1 API are not supported.

#### Extracted Metadata Scope

The connector extracts, on every run:

- The Project as a container
- All Prompts and every version of each Prompt, linked via a shared Version Set

The connector extracts, within a configurable rolling time window (default: the last 7 days):

- Traces, and their `generation`-type Observations (LLM calls)
- Scores attached to those Traces/Observations

### Prerequisites

1. A running Langfuse instance (self-hosted v4+, or Langfuse Cloud).
2. A Langfuse **Public Key** and **Secret Key** for the project you want to ingest. Create these in the Langfuse UI under **Project Settings > API Keys**. The key pair is scoped to exactly one project; ingesting multiple projects requires one recipe per project.

No special role beyond the default API key permissions is required — the API key's Basic Auth credentials already scope every request to the correct project.


### Install the Plugin
```shell
pip install 'acryl-datahub[langfuse]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: langfuse
  config:
    # Coordinates
    platform_instance: production # optional
    env: PROD # optional

    connection:
      host: http://localhost:3000 # or your self-hosted URL, e.g. http://localhost:3000
      public_key: ${LANGFUSE_PUBLIC_KEY}
      secret_key: ${LANGFUSE_SECRET_KEY}

    # Rolling time window for Trace/Observation/Score retrieval (default: last 7 days)
    window:
      start_time: "-7d" # optional, also accepts an absolute ISO 8601 datetime
      # end_time: "2026-01-01T00:00:00Z"  # optional, defaults to now

    # Feature toggles
    include_traces: true # optional, default true - extract Traces and generation Observations
    include_scores: true # optional, default true - attach Scores as MLMetric; requires include_traces
    include_sessions: true # optional, default true - record session/user IDs as custom properties
    include_prompts: true # optional, default true - extract Prompts as versioned Datasets

    # Filtering
    trace_name_pattern:
      deny:
        - "^healthcheck.*" # example: exclude internal healthcheck traces
    prompt_pattern:
      allow:
        - ".*" # default: allow all prompts
sink:
  # sink configs

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">connection</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">LangfuseConnectionConfig</span></div> | Connection configuration for a single Langfuse project. <br />  <br /> Langfuse Basic Auth resolves to exactly one project per key pair, so one <br /> recipe ingests exactly one project. Users with multiple Langfuse projects <br /> run one recipe per project.  |
| <div className="path-line"><span className="path-prefix">connection.</span><span className="path-main">host</span>&nbsp;<abbr title="Required if connection is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Langfuse base URL, e.g. 'http://localhost:3000' for a self-hosted instance, or the Langfuse Cloud region URL (e.g. 'https://cloud.langfuse.com'). No trailing slash.  |
| <div className="path-line"><span className="path-prefix">connection.</span><span className="path-main">public_key</span>&nbsp;<abbr title="Required if connection is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Langfuse project Public Key (Basic Auth username). Find it in Project Settings > API Keys.  |
| <div className="path-line"><span className="path-prefix">connection.</span><span className="path-main">secret_key</span>&nbsp;<abbr title="Required if connection is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string(password)</span></div> | Langfuse project Secret Key (Basic Auth password). Find it in Project Settings > API Keys. Only shown once at creation time in the Langfuse UI.  |
| <div className="path-line"><span className="path-main">include_prompts</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Extract Prompts as versioned Dataset entities, one per prompt version, linked together via a shared VersionSet. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_scores</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Attach Langfuse Scores as MLMetric entries on the Trace or Generation they were recorded against. Session- and dataset-run-level scores are not attached in this version (Sessions and Dataset Runs are not emitted as entities) and are dropped with a reported count. Requires include_traces=True. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_sessions</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Record each Trace's Langfuse sessionId and userId as customProperties on the Trace's DataProcessInstance. Sessions and Users are not emitted as separate DataHub entities in this version. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_traces</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Extract Traces, and their 'generation'-type Observations, within the configured time window as DataProcessInstance entities nested under a Project container. Non-generation Observations (plain spans/events) are summarized as counts on the parent Trace rather than emitted individually. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">page_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of items to fetch per API call when paginating through observations, scores, and prompts. Langfuse's v2/v3 APIs cap this at 100-1000 depending on endpoint; values above the endpoint's cap are rejected by the Langfuse server. <div className="default-line default-line-with-docs">Default: <span className="default-value">50</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">prompt_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes. <br />  <br /> Patterns are matched against the start of the string only, not the entire <br /> string - a pattern does not need to match to the end to be considered a match. <br /> For example, the pattern "prod" matches "prod", "prod_east", and "production". <br /> To require an exact match, anchor your pattern explicitly, e.g. "^prod$".  |
| <div className="path-line"><span className="path-prefix">prompt_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">trace_name_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes. <br />  <br /> Patterns are matched against the start of the string only, not the entire <br /> string - a pattern does not need to match to the end to be considered a match. <br /> For example, the pattern "prod" matches "prod", "prod_east", and "production". <br /> To require an exact match, anchor your pattern explicitly, e.g. "^prod$".  |
| <div className="path-line"><span className="path-prefix">trace_name_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">window</span></div> <div className="type-name-line"><span className="type-name">BaseTimeWindowConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">window.</span><span className="path-main">bucket_duration</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "DAY", "HOUR"  |
| <div className="path-line"><span className="path-prefix">window.</span><span className="path-main">end_time</span></div> <div className="type-name-line"><span className="type-name">string(date-time)</span></div> | Latest date of lineage/usage to consider. Default: Current time in UTC  |
| <div className="path-line"><span className="path-prefix">window.</span><span className="path-main">start_time</span></div> <div className="type-name-line"><span className="type-name">One of string(date-time), string</span></div> | Earliest date of lineage/usage to consider. Default: Last full day in UTC (or hour, depending on `bucket_duration`). You can also specify relative time with respect to end_time such as '-7 days' Or '-7d'. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Stateful ingestion configuration for stale-entity soft deletion. Applies only to Prompt entities, which are fully enumerated on every run. It does NOT apply to Trace/Generation entities: those are retrieved through the rolling time window above, so a naive full-history comparison would incorrectly flag entities that simply aged out of the window as deleted. Trace/Generation workunits are emitted with is_primary_source=False specifically to exclude them from this comparison. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">fail_safe_threshold</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'. <div className="default-line default-line-with-docs">Default: <span className="default-value">75.0</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |

</div>




#### Schema


The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.


```javascript
{
  "$defs": {
    "AllowDenyPattern": {
      "additionalProperties": false,
      "description": "A class to store allow deny regexes.\n\nPatterns are matched against the start of the string only, not the entire\nstring - a pattern does not need to match to the end to be considered a match.\nFor example, the pattern \"prod\" matches \"prod\", \"prod_east\", and \"production\".\nTo require an exact match, anchor your pattern explicitly, e.g. \"^prod$\".",
      "properties": {
        "allow": {
          "default": [
            ".*"
          ],
          "description": "List of regex patterns to include in ingestion. Patterns match from the start of the string only, not the entire string - anchor with '^...$' for an exact match, e.g. '^prod$'.",
          "items": {
            "type": "string"
          },
          "title": "Allow",
          "type": "array"
        },
        "deny": {
          "default": [],
          "description": "List of regex patterns to exclude from ingestion. Patterns match from the start of the string only, not the entire string - anchor with '^...$' for an exact match, e.g. '^prod$'.",
          "items": {
            "type": "string"
          },
          "title": "Deny",
          "type": "array"
        },
        "ignoreCase": {
          "anyOf": [
            {
              "type": "boolean"
            },
            {
              "type": "null"
            }
          ],
          "default": true,
          "description": "Whether to ignore case sensitivity during pattern matching.",
          "title": "Ignorecase"
        }
      },
      "title": "AllowDenyPattern",
      "type": "object"
    },
    "BaseTimeWindowConfig": {
      "additionalProperties": false,
      "properties": {
        "bucket_duration": {
          "$ref": "#/$defs/BucketDuration",
          "default": "DAY",
          "description": "Size of the time window to aggregate usage stats."
        },
        "end_time": {
          "description": "Latest date of lineage/usage to consider. Default: Current time in UTC",
          "format": "date-time",
          "title": "End Time",
          "type": "string"
        },
        "start_time": {
          "anyOf": [
            {
              "format": "date-time",
              "type": "string"
            },
            {
              "pattern": "^\\s*-\\s*\\d+(\\.\\d+)?\\s*(ms|milliseconds?|s|secs?|seconds?|m|mins?|minutes?|h|hrs?|hours?|d|days?|w|weeks?|y|years?)\\s*$",
              "type": "string"
            }
          ],
          "default": null,
          "description": "Earliest date of lineage/usage to consider. Default: Last full day in UTC (or hour, depending on `bucket_duration`). You can also specify relative time with respect to end_time such as '-7 days' Or '-7d'.",
          "title": "Start Time"
        }
      },
      "title": "BaseTimeWindowConfig",
      "type": "object"
    },
    "BucketDuration": {
      "enum": [
        "DAY",
        "HOUR"
      ],
      "title": "BucketDuration",
      "type": "string"
    },
    "LangfuseConnectionConfig": {
      "additionalProperties": false,
      "description": "Connection configuration for a single Langfuse project.\n\nLangfuse Basic Auth resolves to exactly one project per key pair, so one\nrecipe ingests exactly one project. Users with multiple Langfuse projects\nrun one recipe per project.",
      "properties": {
        "host": {
          "description": "Langfuse base URL, e.g. 'http://localhost:3000' for a self-hosted instance, or the Langfuse Cloud region URL (e.g. 'https://cloud.langfuse.com'). No trailing slash.",
          "title": "Host",
          "type": "string"
        },
        "public_key": {
          "description": "Langfuse project Public Key (Basic Auth username). Find it in Project Settings > API Keys.",
          "title": "Public Key",
          "type": "string"
        },
        "secret_key": {
          "description": "Langfuse project Secret Key (Basic Auth password). Find it in Project Settings > API Keys. Only shown once at creation time in the Langfuse UI.",
          "format": "password",
          "title": "Secret Key",
          "type": "string",
          "writeOnly": true
        }
      },
      "required": [
        "host",
        "public_key",
        "secret_key"
      ],
      "title": "LangfuseConnectionConfig",
      "type": "object"
    },
    "StatefulStaleMetadataRemovalConfig": {
      "additionalProperties": false,
      "description": "Base specialized config for Stateful Ingestion with stale metadata removal capability.",
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False",
          "title": "Enabled",
          "type": "boolean"
        },
        "remove_stale_metadata": {
          "default": true,
          "description": "Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled.",
          "title": "Remove Stale Metadata",
          "type": "boolean"
        },
        "fail_safe_threshold": {
          "default": 75.0,
          "description": "Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'.",
          "maximum": 100.0,
          "minimum": 0.0,
          "title": "Fail Safe Threshold",
          "type": "number"
        }
      },
      "title": "StatefulStaleMetadataRemovalConfig",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
    "env": {
      "default": "PROD",
      "description": "The environment that all assets produced by this connector belong to",
      "title": "Env",
      "type": "string"
    },
    "platform_instance": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details.",
      "title": "Platform Instance"
    },
    "stateful_ingestion": {
      "anyOf": [
        {
          "$ref": "#/$defs/StatefulStaleMetadataRemovalConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Stateful ingestion configuration for stale-entity soft deletion. Applies only to Prompt entities, which are fully enumerated on every run. It does NOT apply to Trace/Generation entities: those are retrieved through the rolling time window above, so a naive full-history comparison would incorrectly flag entities that simply aged out of the window as deleted. Trace/Generation workunits are emitted with is_primary_source=False specifically to exclude them from this comparison."
    },
    "connection": {
      "$ref": "#/$defs/LangfuseConnectionConfig",
      "description": "Connection details for the Langfuse project to ingest from."
    },
    "window": {
      "$ref": "#/$defs/BaseTimeWindowConfig",
      "description": "Time window for Trace/Observation/Score retrieval. Defaults to the last 7 days. Langfuse's Observations API v2 is optimized for bounded, recent time ranges; querying the full history on every run is neither necessary nor efficient for this use case."
    },
    "include_traces": {
      "default": true,
      "description": "Extract Traces, and their 'generation'-type Observations, within the configured time window as DataProcessInstance entities nested under a Project container. Non-generation Observations (plain spans/events) are summarized as counts on the parent Trace rather than emitted individually.",
      "title": "Include Traces",
      "type": "boolean"
    },
    "include_scores": {
      "default": true,
      "description": "Attach Langfuse Scores as MLMetric entries on the Trace or Generation they were recorded against. Session- and dataset-run-level scores are not attached in this version (Sessions and Dataset Runs are not emitted as entities) and are dropped with a reported count. Requires include_traces=True.",
      "title": "Include Scores",
      "type": "boolean"
    },
    "include_sessions": {
      "default": true,
      "description": "Record each Trace's Langfuse sessionId and userId as customProperties on the Trace's DataProcessInstance. Sessions and Users are not emitted as separate DataHub entities in this version.",
      "title": "Include Sessions",
      "type": "boolean"
    },
    "include_prompts": {
      "default": true,
      "description": "Extract Prompts as versioned Dataset entities, one per prompt version, linked together via a shared VersionSet.",
      "title": "Include Prompts",
      "type": "boolean"
    },
    "trace_name_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "description": "Regex patterns to filter Traces to ingest, by trace name."
    },
    "prompt_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "description": "Regex patterns to filter Prompts to ingest, by prompt name."
    },
    "page_size": {
      "default": 50,
      "description": "Number of items to fetch per API call when paginating through observations, scores, and prompts. Langfuse's v2/v3 APIs cap this at 100-1000 depending on endpoint; values above the endpoint's cap are rejected by the Langfuse server.",
      "title": "Page Size",
      "type": "integer"
    }
  },
  "required": [
    "connection"
  ],
  "title": "LangfuseSourceConfig",
  "type": "object"
}
```





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
- **Only `generation`-type Observations are emitted as individual entities.** Only root and `generation` Observations are retrieved (filtered on the Langfuse server); every other Observation type — `span`, `event`, `agent`, `tool`, `chain`, `retriever`, `embedding`, `guardrail`, etc. — is not ingested, to bound API volume and entity cardinality for high-volume traces. In practice, applications instrumented primarily with agent/tool-call frameworks may have zero `generation`-type Observations, in which case Traces from that application will show zero nested Observation entities even though real LLM-call-shaped work happened underneath.
- **Sessions and Users are not emitted as separate entities.** Their IDs are recorded as custom properties on the Trace.
- **No lineage to conventional catalogued datasets.** Langfuse does not expose which upstream tables or warehouses an LLM application actually read from or wrote to.
- **A Trace whose root Observation started before the configured window, but which has a `generation` Observation that started inside it, is ingested partially.** Its generations are emitted and linked to the Trace, but the Trace's own properties, run events, and Scores are not emitted from that run, so a full Trace record written by an earlier run is never overwritten. The ingestion report counts these as `partial_traces`.

### Troubleshooting

If ingestion fails, first verify:

- The configured `connection.host` is reachable and correct (no trailing slash).
- `connection.public_key` / `connection.secret_key` are valid for the target project — test with `datahub ingest -c recipe.yml --test-source-connection`.
- The target Langfuse deployment is self-hosted v4+ or Langfuse Cloud. If the connection test reports a v2/v3 API error, the deployment may be running an unsupported, older API generation.

Then review ingestion logs for source-specific warnings (e.g., a specific Prompt version failing to fetch) and adjust `trace_name_pattern` / `prompt_pattern` or the time `window` as needed.


### Code Coordinates
- Class Name: `datahub.ingestion.source.langfuse.langfuse.LangfuseSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/langfuse/langfuse.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Langfuse, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
