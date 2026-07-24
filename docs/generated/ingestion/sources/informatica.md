


# Informatica

## Overview

Informatica Intelligent Data Management Cloud (IDMC) is a cloud-native data integration and management platform. Learn more in the [official Informatica documentation](https://docs.informatica.com/integration-cloud.html).

The DataHub integration for Informatica covers projects and folders as containers; Mapping Tasks as DataFlows with a `transform` DataJob per task; Taskflows as DataFlows with a single `orchestrate` DataJob that chains the step order via `inputDatajobs`; and resolves table-level lineage across the data estate from mapping source/target connections. It also supports ownership extraction and stateful deletion detection.

## Concept Mapping

| Source Concept  | DataHub Concept                                                                                                     | Notes                                                                                                                             |
| --------------- | ------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `"informatica"` | [Data Platform](../../metamodel/entities/dataPlatform.md)                                                           |                                                                                                                                   |
| Project         | [Container](../../metamodel/entities/container.md)                                                                  | SubType `"Project"`                                                                                                               |
| Folder          | [Container](../../metamodel/entities/container.md)                                                                  | SubType `"Folder"`                                                                                                                |
| Taskflow        | [DataFlow](../../metamodel/entities/dataFlow.md) + one `orchestrate` [DataJob](../../metamodel/entities/dataJob.md) | SubTypes `"Taskflow"` / `"Taskflow Orchestration"`; the orchestrate sits at the end of the chain with `inputDatajobs = [last MT]` |
| Mapping Task    | [DataFlow](../../metamodel/entities/dataFlow.md) + inner `transform` [DataJob](../../metamodel/entities/dataJob.md) | SubTypes `"Mapping Task"` / `"Task Logic"`; MTs chain to each other via `inputDatajobs` in Taskflow step order                    |
| Mapping         | _not emitted as a standalone entity_                                                                                | Only Mapping Tasks (runnable schedules) are emitted; the Mapping reference is surfaced via customProperties on the Task           |
| Mapplet         | _not emitted_                                                                                                       | Internal sub-mappings included in other mappings; skipped                                                                         |
| Source/Target   | [Dataset](../../metamodel/entities/dataset.md)                                                                      | Upstream/downstream lineage; external dataset URNs receive a minimal `Status` stub so they resolve in lineage search              |


## Module `informatica`
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Projects and folders as containers. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Via stateful ingestion. |
| Extract Ownership | ✅ | From IDMC object createdBy/updatedBy. |
| Extract Tags | ✅ | IDMC object tags emitted as DataHub GlobalTags. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Table-Level Lineage | ✅ | Table-level lineage via v3 Export API. |
| Test Connection | ✅ | Enabled by default. |

### Overview

The `informatica` module ingests metadata from Informatica Cloud (IDMC) into DataHub. It extracts projects, folders, Mapping Tasks, and Taskflows, and resolves table-level lineage from the Mapping each Task references. Standalone Mappings (ones without a Mapping Task) and Mapplets are not emitted.

:::tip Quick Start

1. **Create a service account** — Use a dedicated IDMC user with minimum permissions (see [Required Permissions](#required-permissions))
2. **Identify your pod URL** — Determine the IDMC regional login URL (US, US2, EMEA, or APAC)
3. **Configure recipe** — Use `informatica_recipe.yml` as a template
4. **Run ingestion** — Execute `datahub ingest -c informatica_recipe.yml`

:::

#### Key Features

- Projects and folders as Containers
- Mapping Tasks as DataFlows with a `transform` DataJob each; Taskflows as DataFlows with one `orchestrate` DataJob that chains the MTs in step order
- Table-level lineage (source → mapping → target) resolved via the v3 Export API and connection metadata; Mapping Tasks chain to each other in Taskflow step order and the Taskflow `orchestrate` DataJob anchors the end of the chain
- Three-layer filtering: tag-based (recommended for large orgs), project/folder pattern, and mapping/taskflow name pattern
- Cross-source lineage to datasets ingested by other connectors (Snowflake, Oracle, BigQuery, etc.) via connection type mapping
- Manual connection type overrides for unusual or custom connectors
- Stateful ingestion for stale entity removal
- Ownership extraction from `createdBy`/`updatedBy`

#### Concept Mapping

| IDMC concept  | DataHub entity                             | Subtype                               |
| ------------- | ------------------------------------------ | ------------------------------------- |
| Project       | Container                                  | `Project`                             |
| Folder        | Container                                  | `Folder`                              |
| Taskflow      | DataFlow **and** one `orchestrate` DataJob | `Taskflow` / `Taskflow Orchestration` |
| Mapping Task  | DataFlow **and** one `transform` DataJob   | `Mapping Task` / `Task Logic`         |
| Mapping       | _not emitted_ — see notes                  | —                                     |
| Mapplet       | _not emitted_ — see notes                  | —                                     |
| Source/target | Dataset (upstream/downstream lineage)      | —                                     |

Mapping Tasks are the runnable schedules in IDMC, and that's what we emit as
first-class entities. Each MT's inner `transform` DataJob carries the
`dataJobInputOutput` aspect with the source/target tables resolved from the
Mapping it references — so cross-source lineage lands on the thing users
actually schedule and operate.

**Mappings without a Mapping Task are not emitted** (they're not runnable on
their own). **Mapplets are not emitted either** — they're internal sub-mappings
included in other mappings. The referenced Mapping's friendly name, v2 id,
and v3 GUID are still surfaced as `customProperties.mappingName` /
`mappingId` / `mappingV3Id` on every MT so you can cross-reference back to
IDMC without leaving DataHub.

#### Taskflow step DAG

The Taskflow step order is resolved from the v3 Export API (`.TASKFLOW.xml`),
parsed from the IDMC `taskflowModel` `<eventContainer>` / `<service>` /
`<link>` graph. All Taskflow GUIDs for a single ingestion run are submitted
as **one** export job for efficiency.

Rather than emitting a separate DataJob per step, the connector collapses
step references into the MT they run and chains the MT `transform` DataJobs
directly via `dataJobInputOutput.inputDatajobs`. A single `orchestrate`
DataJob is emitted per Taskflow and anchored at the end of the chain:
`inputDatajobs = [last MT]`, `outputDatasets` mirrors the last MT's outputs.

The resulting Taskflow lineage reads cleanly end to end:

```
input_dataset → MT1.transform → MT2.transform → … → MTn.transform → orchestrate → output_dataset
```

Non-data steps (command / decision / notification / …) don't participate in
the chain but are summarized in `customProperties.stepSummary` on the
orchestrate DataJob for auditing.

### Prerequisites

#### Required Permissions

| Capability                        | IDMC privilege                        | Notes                                                            |
| --------------------------------- | ------------------------------------- | ---------------------------------------------------------------- |
| Authenticate                      | Any active IDMC user                  | Uses the v2 login endpoint                                       |
| List projects, folders, taskflows | `Asset - read` (or the Observer role) | Needed for all container/flow emission                           |
| List mappings / mapping tasks     | `Asset - read`                        | Mapping Tasks are optional and skipped with a warning if 403     |
| Extract table-level lineage       | `Asset - export`                      | Submits v3 export jobs; skip by setting `extract_lineage: false` |
| List connections                  | `Connection - read`                   | Needed for lineage to resolve to dataset URNs                    |

#### Regional login URLs

Set `login_url` to your IDMC pod's regional URL (not the API runtime URL — the connector discovers that from the login response):

| Region | `login_url`                           |
| ------ | ------------------------------------- |
| US     | `https://dm-us.informaticacloud.com`  |
| US2    | `https://dm2-us.informaticacloud.com` |
| EMEA   | `https://dm-em.informaticacloud.com`  |
| APAC   | `https://dm-ap.informaticacloud.com`  |

#### References

- [Informatica IDMC REST API](https://docs.informatica.com/integration-cloud/data-integration/current-version/rest-api-reference.html)
- [IDMC v3 Objects API](https://docs.informatica.com/integration-cloud/data-integration/current-version/rest-api-reference/platform-rest-api-version-3-resources/objects.html)
- [IDMC Export API](https://docs.informatica.com/integration-cloud/data-integration/current-version/rest-api-reference/platform-rest-api-version-3-resources/export.html)


### Install the Plugin
```shell
pip install 'acryl-datahub[informatica]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: informatica
  config:
    # -------------------------------------------------------------------------
    # Connection
    # -------------------------------------------------------------------------

    # Regional login URL for your IDMC pod. The connector discovers the runtime
    # API URL from the login response. Common values:
    #   US     https://dm-us.informaticacloud.com
    #   US2    https://dm2-us.informaticacloud.com
    #   EMEA   https://dm-em.informaticacloud.com
    #   APAC   https://dm-ap.informaticacloud.com
    login_url: "https://dm-us.informaticacloud.com"

    # IDMC service account. Prefer a dedicated user with the Observer role
    # plus "Asset - export" (required for lineage, see README).
    username: "${IDMC_USERNAME}"
    password: "${IDMC_PASSWORD}"

    # Optional: group entities into a platform instance if you ingest more than
    # one IDMC org/pod into the same DataHub instance.
    # platform_instance: "idmc_prod"
    # env: "PROD"

    # -------------------------------------------------------------------------
    # Filtering — combine any or all three layers
    # -------------------------------------------------------------------------

    # Layer 1 (recommended for large orgs): only ingest objects tagged in IDMC
    # with at least one of these names. Tags are matched exactly.
    # Applies to Projects, Folders, Taskflows, and Mapping Tasks only —
    # Mappings and Connections are always fetched in full regardless of this filter.
    # tag_filter_names: ["datahub", "critical"]

    # Layer 2: filter by project/folder name (regex).
    # project_pattern:
    #   allow:
    #     - "^Production_.*"
    #   deny:
    #     - ".*_sandbox$"
    # folder_pattern:
    #   allow:
    #     - ".*"

    # Layer 3: filter by mapping/taskflow name (regex, applied across all matches).
    # mapping_pattern:
    #   allow:
    #     - ".*"
    # taskflow_pattern:
    #   allow:
    #     - ".*"

    # -------------------------------------------------------------------------
    # Features
    # -------------------------------------------------------------------------

    # Requires the "Asset - export" privilege on the service account.
    extract_lineage: true

    # Derives owners from IDMC createdBy/updatedBy fields.
    extract_ownership: true

    # Emits IDMC object tags as DataHub GlobalTags on Projects, Folders,
    # Taskflows, and Mapping Tasks. Defaults to true — tags will be ingested
    # even if this field is not specified.
    extract_tags: true

    # -------------------------------------------------------------------------
    # Connection → platform overrides
    # -------------------------------------------------------------------------

    # Use when IDMC reports a connection type the connector doesn't know about.
    # Keys are IDMC connection IDs; values are DataHub platform names.
    # connection_type_overrides:
    #   "01DM180B000000000008": "snowflake"

    # -------------------------------------------------------------------------
    # Performance (tune for large orgs)
    # -------------------------------------------------------------------------

    # page_size: 200                 # v3 objects per page (max 200)
    # export_batch_size: 1000        # mappings per export job (max 1000)
    # export_poll_timeout_secs: 300  # seconds to wait for an export job
    # export_poll_interval_secs: 5   # seconds between export polls

    # -------------------------------------------------------------------------
    # Stateful ingestion — recommended for automatic stale-entity removal
    # -------------------------------------------------------------------------

    stateful_ingestion:
      enabled: true

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">password</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string(password)</span></div> | Informatica Cloud password.  |
| <div className="path-line"><span className="path-main">username</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Informatica Cloud username (email or service account name).  |
| <div className="path-line"><span className="path-main">connection_to_platform_instance</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div> |   |
| <div className="path-line"><span className="path-main">connection_type_overrides</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div> |   |
| <div className="path-line"><span className="path-main">connection_type_platform_map</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div> |   |
| <div className="path-line"><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Lowercase the dataset qualifier in emitted upstream URNs to match the default behavior of the Snowflake, Postgres, and BigQuery sources (which lowercase by default). Set to False only if you've disabled lowercasing on every source this connector produces lineage to. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">export_batch_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of mappings per v3 export batch job (max 1000). <div className="default-line default-line-with-docs">Default: <span className="default-value">1000</span></div> |
| <div className="path-line"><span className="path-main">export_poll_interval_secs</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Interval in seconds between export job status polls. <div className="default-line default-line-with-docs">Default: <span className="default-value">5</span></div> |
| <div className="path-line"><span className="path-main">export_poll_timeout_secs</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Timeout in seconds for polling export job completion. <div className="default-line default-line-with-docs">Default: <span className="default-value">300</span></div> |
| <div className="path-line"><span className="path-main">extract_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to extract table-level lineage from mapping definitions. Requires the 'Asset - export' privilege on the service account. When enabled, uses the v3 Export API to fetch full mapping definitions. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_ownership</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to extract ownership from IDMC object createdBy/updatedBy fields. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_tags</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit IDMC object tags as DataHub GlobalTags on Projects, Folders, Taskflows, and Mapping Tasks. Set to False to skip tag extraction. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">login_url</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Informatica Cloud login URL. This is the regional pod URL, not the runtime serverUrl. After login, the connector discovers the actual API base URL from the login response. Common values: https://dm-us.informaticacloud.com (US), https://dm2-us.informaticacloud.com (US2), https://dm-em.informaticacloud.com (EMEA), https://dm-ap.informaticacloud.com (APAC). <div className="default-line default-line-with-docs">Default: <span className="default-value">https://dm-us.informaticacloud.com</span></div> |
| <div className="path-line"><span className="path-main">max_concurrent_export_jobs</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of v3 export jobs to run concurrently. Each job covers one batch of mappings. Increase to reduce lineage wall-clock time on large orgs; decrease if hitting IDMC rate limits. <div className="default-line default-line-with-docs">Default: <span className="default-value">4</span></div> |
| <div className="path-line"><span className="path-main">page_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of objects to fetch per API page (max 200 for v3 objects). <div className="default-line default-line-with-docs">Default: <span className="default-value">200</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">request_timeout_secs</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | HTTP timeout in seconds for IDMC API requests. Raise this for large deployments where /api/v2/mapping or /api/v2/connection returns many records and the default 60s is insufficient. <div className="default-line default-line-with-docs">Default: <span className="default-value">60</span></div> |
| <div className="path-line"><span className="path-main">strip_user_email_domain</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Strip the domain from IDMC user identifiers before forming the CorpUser URN (e.g. ``alice@acme.com`` → ``urn:li:corpuser:alice``). Enable when your Okta/AzureAD source ingests users without the email domain so ownership edges align with existing CorpUser URNs. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">folder_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">folder_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">mapping_task_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">mapping_task_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">project_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">project_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">tag_filter_names</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of literal IDMC tag names. When set, only objects tagged with at least one of these tags will be ingested. Tags are matched exactly (not regex). This is the recommended filtering approach for large orgs — IDMC admins tag objects in the UI and the connector picks them up. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">tag_filter_names.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">taskflow_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">taskflow_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Configuration for stateful ingestion and stale entity removal. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
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
      "description": "A class to store allow deny regexes",
      "properties": {
        "allow": {
          "default": [
            ".*"
          ],
          "description": "List of regex patterns to include in ingestion",
          "items": {
            "type": "string"
          },
          "title": "Allow",
          "type": "array"
        },
        "deny": {
          "default": [],
          "description": "List of regex patterns to exclude from ingestion.",
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
  "description": "Configuration for Informatica Cloud (IDMC) ingestion source.",
  "properties": {
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
      "description": "Configuration for stateful ingestion and stale entity removal."
    },
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
    "login_url": {
      "default": "https://dm-us.informaticacloud.com",
      "description": "Informatica Cloud login URL. This is the regional pod URL, not the runtime serverUrl. After login, the connector discovers the actual API base URL from the login response. Common values: https://dm-us.informaticacloud.com (US), https://dm2-us.informaticacloud.com (US2), https://dm-em.informaticacloud.com (EMEA), https://dm-ap.informaticacloud.com (APAC).",
      "title": "Login Url",
      "type": "string"
    },
    "username": {
      "description": "Informatica Cloud username (email or service account name).",
      "title": "Username",
      "type": "string"
    },
    "password": {
      "description": "Informatica Cloud password.",
      "format": "password",
      "title": "Password",
      "type": "string",
      "writeOnly": true
    },
    "project_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns to filter IDMC projects by name. Only projects matching these patterns will be ingested. Example: allow: ['Production.*'], deny: ['.*_sandbox']"
    },
    "folder_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns to filter IDMC folders by name. Only folders matching these patterns will be ingested."
    },
    "mapping_task_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns to filter Mapping Tasks. Matched against '<folder_path>/<name>' so same-named tasks in different folders can be targeted independently (e.g. allow: ['.*/ProjectA/MyTask'])."
    },
    "taskflow_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns to filter Taskflows. Matched against '<folder_path>/<name>' so same-named taskflows in different folders can be targeted independently (e.g. allow: ['.*/ProjectA/MyFlow'])."
    },
    "tag_filter_names": {
      "default": [],
      "description": "List of literal IDMC tag names. When set, only objects tagged with at least one of these tags will be ingested. Tags are matched exactly (not regex). This is the recommended filtering approach for large orgs \u2014 IDMC admins tag objects in the UI and the connector picks them up.",
      "items": {
        "type": "string"
      },
      "title": "Tag Filter Names",
      "type": "array"
    },
    "extract_lineage": {
      "default": true,
      "description": "Whether to extract table-level lineage from mapping definitions. Requires the 'Asset - export' privilege on the service account. When enabled, uses the v3 Export API to fetch full mapping definitions.",
      "title": "Extract Lineage",
      "type": "boolean"
    },
    "extract_ownership": {
      "default": true,
      "description": "Whether to extract ownership from IDMC object createdBy/updatedBy fields.",
      "title": "Extract Ownership",
      "type": "boolean"
    },
    "strip_user_email_domain": {
      "default": false,
      "description": "Strip the domain from IDMC user identifiers before forming the CorpUser URN (e.g. ``alice@acme.com`` \u2192 ``urn:li:corpuser:alice``). Enable when your Okta/AzureAD source ingests users without the email domain so ownership edges align with existing CorpUser URNs.",
      "title": "Strip User Email Domain",
      "type": "boolean"
    },
    "extract_tags": {
      "default": true,
      "description": "Emit IDMC object tags as DataHub GlobalTags on Projects, Folders, Taskflows, and Mapping Tasks. Set to False to skip tag extraction.",
      "title": "Extract Tags",
      "type": "boolean"
    },
    "connection_type_overrides": {
      "additionalProperties": {
        "type": "string"
      },
      "default": {},
      "description": "Per-connection-ID override mapping IDMC connection id \u2192 DataHub platform name. Use when a single connection can't be auto-resolved (e.g., a one-off custom connector). Example: {'01DM180B000000000008': 'snowflake'}. Takes priority over `connection_type_platform_map` and the built-in CONNECTION_TYPE_MAP.",
      "title": "Connection Type Overrides",
      "type": "object"
    },
    "connection_type_platform_map": {
      "additionalProperties": {
        "type": "string"
      },
      "default": {},
      "description": "Extend the built-in connection-type \u2192 DataHub-platform map with custom entries. Keys are the IDMC `connParams[\"Connection Type\"]` string (or the connection's top-level `type` as a fallback), values are DataHub platform names. Useful for new IDMC marketplace connectors that aren't in the built-in map yet. Example: {'MyCustomConnector_v3': 'snowflake', 'CompanyDW': 'postgres'}. Entries here are merged with (and override) CONNECTION_TYPE_MAP.",
      "title": "Connection Type Platform Map",
      "type": "object"
    },
    "convert_urns_to_lowercase": {
      "default": true,
      "description": "Lowercase the dataset qualifier in emitted upstream URNs to match the default behavior of the Snowflake, Postgres, and BigQuery sources (which lowercase by default). Set to False only if you've disabled lowercasing on every source this connector produces lineage to.",
      "title": "Convert Urns To Lowercase",
      "type": "boolean"
    },
    "connection_to_platform_instance": {
      "additionalProperties": {
        "type": "string"
      },
      "default": {},
      "description": "Map IDMC connection ID \u2192 DataHub `platform_instance` to use when building upstream/downstream dataset URNs. Required whenever the target source was ingested with a non-default platform_instance; otherwise lineage edges will point at URNs that don't exist in DataHub. Example: {'01DM180B000000000008': 'prod_sf'}.",
      "title": "Connection To Platform Instance",
      "type": "object"
    },
    "page_size": {
      "default": 200,
      "description": "Number of objects to fetch per API page (max 200 for v3 objects).",
      "maximum": 200,
      "minimum": 1,
      "title": "Page Size",
      "type": "integer"
    },
    "export_batch_size": {
      "default": 1000,
      "description": "Number of mappings per v3 export batch job (max 1000).",
      "maximum": 1000,
      "minimum": 1,
      "title": "Export Batch Size",
      "type": "integer"
    },
    "export_poll_timeout_secs": {
      "default": 300,
      "description": "Timeout in seconds for polling export job completion.",
      "maximum": 3600,
      "minimum": 30,
      "title": "Export Poll Timeout Secs",
      "type": "integer"
    },
    "export_poll_interval_secs": {
      "default": 5,
      "description": "Interval in seconds between export job status polls.",
      "maximum": 600,
      "minimum": 1,
      "title": "Export Poll Interval Secs",
      "type": "integer"
    },
    "request_timeout_secs": {
      "default": 60,
      "description": "HTTP timeout in seconds for IDMC API requests. Raise this for large deployments where /api/v2/mapping or /api/v2/connection returns many records and the default 60s is insufficient.",
      "maximum": 600,
      "minimum": 5,
      "title": "Request Timeout Secs",
      "type": "integer"
    },
    "max_concurrent_export_jobs": {
      "default": 4,
      "description": "Maximum number of v3 export jobs to run concurrently. Each job covers one batch of mappings. Increase to reduce lineage wall-clock time on large orgs; decrease if hitting IDMC rate limits.",
      "maximum": 8,
      "minimum": 1,
      "title": "Max Concurrent Export Jobs",
      "type": "integer"
    }
  },
  "required": [
    "username",
    "password"
  ],
  "title": "InformaticaSourceConfig",
  "type": "object"
}
```





### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Filtering

Three filter layers can be combined, applied in order:

1. **Tag-based** (`tag_filter_names`, recommended for large orgs) — an allowlist of IDMC tags; only tagged objects are ingested.
2. **Path-based** (`project_pattern`, `folder_pattern`) — regex allow/deny on project and folder names.
3. **Name-based** (`mapping_pattern`, `taskflow_pattern`) — regex allow/deny on mapping and taskflow names.

#### Connection Type Mapping

When emitting lineage, each IDMC connection is mapped to a DataHub platform (e.g. `Snowflake_Cloud_Data_Warehouse → snowflake`). The mapping is driven by `connParams["Connection Type"]`. If IDMC returns an unknown type (or a customer-specific connector), set `connection_type_overrides` to map that connection ID to a DataHub platform name. The connector will warn about unknown platforms at config-parse time.

#### External Dataset Stubs

Every input/output dataset URN referenced by mapping lineage receives a minimal `Status` aspect when it is first seen. Without this stub, DataHub treats URNs that no other connector has ingested as non-existent and `searchAcrossLineage` filters them out of results — which would leave the left-side chevron on a Mapping Task's `transform` DataJob unable to expand upstream datasets. The stub is idempotent and does not override Schema, Ownership, or other metadata written by the source platform's own connector when it runs.

### Limitations

- **No column-level lineage** — the v3 export gives us transformation-level source/target tables but not column mappings.
- **No execution history** — the connector does not ingest Activity Monitor runs as DataProcessInstances.
- **Taskflow step DAG requires `Asset - export`** — Taskflow step ordering lives in a `taskflowModel` XML document fetched via the v3 Export API. Ingestion will silently no-op the step chain for Taskflows the user can't export (the Taskflow itself is still emitted as a DataFlow with its `orchestrate` DataJob, but that orchestrate won't have an `inputDatajobs` chain). The report includes `taskflows_with_steps` so you can confirm coverage.
- **Single-user auth only** — service-principal / federated SSO login is not supported; use a native IDMC user.
- **v2 API endpoints are not paginated** — `/api/v2/mapping` and `/api/v2/connection` return all records in a single response; the IDMC v2 API does not honour `limit`, `skip`, or `maxRecordsCount` parameters (verified against a live instance). For orgs with very large numbers of mappings (>10k) or connections (>1k), the single call may exceed `request_timeout_secs` or produce a very large response. Mitigations: raise `request_timeout_secs`, or use `tag_filter_names` to scope ingestion to a tagged subset of objects.

### Troubleshooting

#### `IDMC login failed` at startup

The connector raises this when the v2 login endpoint returns non-200 or a body without `icSessionId`/`serverUrl`. Common causes:

- Wrong `login_url` for your pod (see the region table in the Prerequisites section).
- Service account locked out, MFA-protected, or password-expired. Use a dedicated IDMC user without interactive MFA.
- Firewall blocking egress to `*.informaticacloud.com`.

The raised error includes the HTTP status, a truncated response body, and the `login_url` used.

#### `connections_unresolved` entries in the report

The connector resolves lineage dataset URNs by matching the mapping's `connectionId` (e.g. `saas:@fed-xyz`) against the IDMC connection catalog. If a connection cannot be mapped to a DataHub platform, the lineage edge is dropped and the connection is recorded in `connections_unresolved`. Two typical causes:

1. The connection uses a type not in the built-in `CONNECTION_TYPE_MAP` (e.g. a custom connector). Add it to `connection_type_overrides` with the connection ID → DataHub platform.
2. The `Connection - read` privilege is missing from the service account, so `list_connections` fetches an empty or partial catalog.

#### `Failed to fetch mapping tasks` warning

Mapping Tasks live at `/api/v2/mttask`, which is often restricted to specific roles. The connector treats this as a warning (not a failure) because mapping and lineage ingestion can still complete without it. Grant `Asset - read` on mapping tasks if you need them.

#### Export job timed out

The v3 Export API is asynchronous; for very large orgs, the default `export_poll_timeout_secs: 300` may be too short. Try:

- Reduce `export_batch_size` (default 1000) — smaller batches finish faster individually.
- Raise `export_poll_timeout_secs` (max 3600).
- Use `tag_filter_names` to scope the export to tagged mappings only.

The connector emits a report warning titled "IDMC export job timed out" for each timed-out batch and records it under `export_jobs_failed`.

#### Add-On Bundles showing up

IDMC ships several marketplace bundles (e.g. Cloud Data Integration templates). The connector filters these out automatically by checking `path.startswith("Add-On Bundles/")` or `updated_by == "bundle-license-notifier"`. If you see bundle mappings leaking through, open an issue with the offending object's path.


### Code Coordinates
- Class Name: `datahub.ingestion.source.informatica.source.InformaticaSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/informatica/source.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Informatica, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
