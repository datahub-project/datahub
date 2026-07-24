


# MicroStrategy

## Overview

[MicroStrategy](https://www.microstrategy.com/) is an enterprise business intelligence platform for building governed dossiers, dashboards, reports, cubes, metrics, and attributes over business data.

The DataHub integration for MicroStrategy ingests projects and folders as containers, dossiers/documents as dashboards, visualizations as charts, and embedded dashboard datasets/cubes as datasets with schema fields derived from MicroStrategy metrics, attributes, and attribute forms. It can also ingest MicroStrategy reports as chart entities with report-scoped source datasets when `extract_reports` is enabled. The connector records project source warehouse/source type summaries when datasource APIs are available.

Lineage is emitted from datasets to visualizations and report source datasets to reports through `ChartInfo.inputs` and `ChartInfo.inputEdges` when the connector can resolve bindings from definitions or runtime details. When metadata exposes metric and attribute references, the connector also emits chart `InputFields` for the fields used by each visualization or report. Direct dashboard dataset edges are disabled by default because large BI dashboards can reference many datasets and make lineage graphs noisy; set `emit_dashboard_dataset_edges: true` only when dashboard-level fallback lineage is preferred. Coarse table-level warehouse lineage from SQL-view APIs is also disabled by default; set `extract_warehouse_lineage: true` for dashboard dataset -> warehouse table edges or `extract_report_sql_lineage: true` for report source dataset -> warehouse table edges before field-level metric, attribute, or fact lineage is available.

## Concept Mapping

| MicroStrategy Concept          | DataHub Concept                                 | Notes                                                                                                                  |
| ------------------------------ | ----------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| Project                        | Container (`Project` subtype)                   | Project-scoped APIs require `X-MSTR-ProjectID`.                                                                        |
| Folder                         | Container (`Folder` subtype)                    | Created when search results expose folder or location context.                                                         |
| Dossier / document / dashboard | Dashboard (`Dossier` subtype)                   | Dashboard metadata is read from dossier/document definition APIs.                                                      |
| Visualization                  | Chart (`Visualization` subtype)                 | Chart lineage uses `ChartInfo.inputs`, `inputEdges`, and chart `InputFields` when dataset and field IDs can be mapped. |
| Report                         | Chart (`Report` subtype)                        | Optional via `extract_reports`; report lineage uses a report-scoped source dataset.                                    |
| Dashboard dataset / cube       | Dataset (`MicroStrategy Dataset` subtype)       | Schema fields come from embedded `availableObjects`.                                                                   |
| Report source dataset          | Dataset (`MicroStrategy Dataset` subtype)       | Schema fields come from report definition `availableObjects`.                                                          |
| Source warehouse / datasource  | Custom properties and optional upstream lineage | SQL-view APIs provide opt-in coarse physical table lineage when available.                                             |
| Metric                         | Schema field with `Measure` tag                 | Metric IDs are preserved in field `jsonProps`.                                                                         |
| Attribute / form               | Schema field with `Dimension` tag               | Date/time forms also receive the `Temporal` tag.                                                                       |
| Owner                          | CorpUser ownership                              | Emitted when owner fields are exposed and `ingest_owner` is enabled.                                                   |
| Metric / attribute glossary    | Field glossary terms from explicit config maps  | The connector does not create glossary terms automatically.                                                            |


## Module `microstrategy`
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Projects and folders emit as containers. |
| Column-level Lineage | ✅ | Modeling API field lineage from MicroStrategy metrics and attributes to source warehouse fields. |
| Dataset Usage | ✅ | Dashboard and report usage from the Platform Analytics cube when `extract_usage_statistics` is enabled. |
| Descriptions | ✅ | Enabled by default. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled via stateful ingestion. |
| Extract Ownership | ✅ | Enabled by default via `ingest_owner`. |
| Extract Tags | ✅ | Metric, attribute, and temporal field tags. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Schema Metadata | ✅ | Enabled by default. |
| Table-Level Lineage | ✅ | Visualization and report inputs when resolvable. |
| Test Connection | ✅ | Enabled by default. |

### Overview

[MicroStrategy](https://www.microstrategy.com/) is an enterprise business intelligence platform for building governed dossiers, dashboards, reports, cubes, metrics, and attributes over business data.

This source ingests projects and folders as containers, dossiers/documents as dashboards, visualizations as charts, and embedded dashboard datasets/cubes as datasets with schema fields derived from MicroStrategy metrics, attributes, and attribute forms. It can also ingest MicroStrategy reports as chart entities with report-scoped source datasets when `extract_reports` is enabled, and it records project source warehouse summaries when datasource APIs are available.

Lineage is emitted from datasets to visualizations (and from report source datasets to reports) through `ChartInfo.inputs` and `ChartInfo.inputEdges`, with chart `InputFields` when metric and attribute references are exposed. Optional coarse table-level warehouse lineage from SQL-view APIs is disabled by default; enable `extract_warehouse_lineage` or `extract_report_sql_lineage` to emit dataset-to-warehouse-table edges.

### Prerequisites

Create a MicroStrategy user with read access to the projects, dossiers, documents, and reports you want to ingest. For production metadata extraction, use username/password authentication with access to Library APIs and project-scoped metadata search. Guest authentication is useful for public demo exploration but does not expose all modeling APIs.

For optional upstream warehouse table lineage, the MicroStrategy principal needs access to create a dashboard/dossier instance and read the dataset SQL-view API. This lineage is coarse table-level lineage and is disabled by default until field-level metric, attribute, or fact lineage is available. Modeling privileges such as Architect/editor access can expose additional logical model details, but they are not required for SQL-view physical table lineage when the SQL-view APIs are available.

For optional report ingestion, the principal needs project-scoped report search and report definition access. For optional report SQL lineage, it also needs permission to create report instances and read the report SQL-view API.


### Install the Plugin
```shell
pip install 'acryl-datahub[microstrategy]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: microstrategy
  config:
    base_url: "https://your-company.example.com/MicroStrategyLibrary"

    auth:
      type: password
      username: "${MSTR_USERNAME}"
      password: "${MSTR_PASSWORD}"

    platform_instance: production
    env: PROD

    # Optional regex filters. All projects, dashboards, and reports are
    # ingested by default; scope them like this when needed:
    # project_pattern:
    #   allow:
    #     - "^Finance$"
    # dashboard_pattern:
    #   allow:
    #     - "^Quarterly Business Review$"
    # report_pattern:
    #   deny:
    #     - "^Scratch.*"

    extract_dashboards: true
    extract_charts: true
    extract_reports: false
    extract_report_definitions: true
    extract_cubes: true
    extract_lineage: true
    extract_visualization_details: true
    extract_source_warehouses: true
    extract_dashboard_dependencies: true
    extract_metric_expressions: true
    extract_model_lineage: true

    # Optional coarse table-level lineage from MicroStrategy SQL-view APIs.
    # Keep disabled unless you want dataset -> warehouse table edges without
    # field-level metric / attribute / fact lineage.
    extract_warehouse_lineage: false
    extract_report_sql_lineage: false

    # Map each MicroStrategy datasource / connection name to the platform,
    # platform instance, env, and URN casing it was ingested under, so warehouse
    # lineage URNs match those tables. Keying by connection name lets several
    # connections to the same platform (e.g. prod vs dev Snowflake) resolve
    # independently. Matched against the connection name first, then the
    # datasource name. Datasources with no entry auto-detect their platform and
    # use this connector's env, no platform instance, and lowercase URNs.
    # datasource_platform_mapping:
    #   "Snowflake Prod":
    #     platform: snowflake
    #     platform_instance: my_snowflake_instance
    #     env: PROD
    #     convert_urns_to_lowercase: true

    # Disabled by default to keep DataHub lineage graphs focused on
    # dataset -> visualization edges.
    emit_dashboard_dataset_edges: false

    tag_measures_and_dimensions: true
    ingest_owner: true

    stateful_ingestion:
      enabled: true
      remove_stale_metadata: true

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
| <div className="path-line"><span className="path-main">base_url</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | MicroStrategy Library base URL, for example `https://your-company.example.com/MicroStrategyLibrary`.  |
| <div className="path-line"><span className="path-main">attribute_glossary_term_mapping</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div> |   |
| <div className="path-line"><span className="path-main">emit_dashboard_dataset_edges</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit DashboardInfo.datasetEdges as a fallback. Disabled by default because BI dashboards with many datasets make lineage views noisy. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">extract_charts</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to extract visualizations as DataHub charts. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_cubes</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to extract embedded dashboard datasets as DataHub datasets. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_dashboard_dependencies</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to call metadata search lineage APIs for direct dashboard components such as metrics, attributes, filters, and functions. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_dashboards</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to extract dossiers/documents as DataHub dashboards. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_independent_reports</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to extract reports not referenced by any ingested dashboard. By default only dashboard-linked reports are ingested (`report_pattern` still applies), so scoping dashboards also scopes reports. The linkage comes from `extract_dashboard_dependencies`; when dashboards or dependencies are not extracted, all matching reports are ingested. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">extract_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to emit dataset-to-chart lineage when resolved from definitions. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_metric_expressions</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to fetch metric model definitions with expression tokens and attach expression metadata to metric schema fields when the MicroStrategy principal has access. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_model_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to attempt modeling/table API access needed for logical table and source warehouse lineage. If privileges are missing, the connector reports the failure and continues. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_report_definitions</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to fetch report definitions for source dataset, metric, and attribute details when `extract_reports` is enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_report_sql_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to execute report SQL-view APIs and emit coarse table-level lineage from report source datasets to source warehouse datasets. Disabled by default for the same reason as `extract_warehouse_lineage`. Field-level model lineage for report source datasets also requires this flag, because model lineage only attaches to datasets with known warehouse upstreams. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">extract_reports</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to extract MicroStrategy reports as DataHub charts. Disabled by default because reports can be numerous and are independent from dossier visualization extraction. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">extract_source_warehouses</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to call the MicroStrategy datasource management APIs to discover project source warehouse names, source types, database versions, DBMS names, and connection metadata. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_usage_statistics</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to extract dashboard and report usage statistics (view counts, unique users, per-user counts) by querying the MicroStrategy Platform Analytics telemetry cube. Requires the Platform Analytics project to be enabled on the environment (standard on MicroStrategy Cloud) and readable by the ingestion principal. Disabled by default. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">extract_visualization_details</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to execute dashboards and fetch per-visualization runtime definitions to resolve dataset-to-visualization lineage when the static dashboard definition does not include dataset IDs. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_warehouse_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to execute dashboard/dossier SQL-view APIs and emit upstream coarse table-level lineage from MicroStrategy datasets to source warehouse datasets parsed from SQL. Disabled by default because this is not field-level metric, attribute, or fact lineage. The connector discovers the warehouse platform from MicroStrategy datasource metadata and does not store raw SQL. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_hidden</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to include hidden MicroStrategy objects when APIs support it. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">ingest_owner</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to map API owner fields to DataHub ownership aspects. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">max_retries</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum retry attempts for transient API failures. <div className="default-line default-line-with-docs">Default: <span className="default-value">3</span></div> |
| <div className="path-line"><span className="path-main">metric_glossary_term_mapping</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div> |   |
| <div className="path-line"><span className="path-main">page_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of objects requested per paginated metadata search call. <div className="default-line default-line-with-docs">Default: <span className="default-value">100</span></div> |
| <div className="path-line"><span className="path-main">platform_analytics_project_name</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Name of the MicroStrategy project that hosts Platform Analytics telemetry. Only change this if the environment renamed the standard project. <div className="default-line default-line-with-docs">Default: <span className="default-value">Platform Analytics</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">tag_measures_and_dimensions</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Tag metric fields as Measure, attribute fields as Dimension, and date/time attribute forms as Temporal. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">timeout_seconds</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | HTTP request timeout in seconds. <div className="default-line default-line-with-docs">Default: <span className="default-value">30</span></div> |
| <div className="path-line"><span className="path-main">usage_cube_name</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Name of the Platform Analytics cube to query for usage. The default is the aggregate telemetry cube shipped with Platform Analytics; a custom cube works as long as it exposes Date, Project, Object, and User attributes and an executions metric. <div className="default-line default-line-with-docs">Default: <span className="default-value">Platform Analytics (Agg)</span></div> |
| <div className="path-line"><span className="path-main">usage_lookback_days</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | How many days of usage history to request from the Platform Analytics cube. The shipped aggregate cube typically retains a 14-day rolling window, so larger values only help when the environment retains more history. <div className="default-line default-line-with-docs">Default: <span className="default-value">14</span></div> |
| <div className="path-line"><span className="path-main">usage_query_timeout_seconds</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | HTTP timeout in seconds for Platform Analytics cube query calls. Cube execution is server-side work and can be slower than metadata definition APIs. <div className="default-line default-line-with-docs">Default: <span className="default-value">180</span></div> |
| <div className="path-line"><span className="path-main">verify_ssl</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to verify SSL certificates for MicroStrategy API calls. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">warehouse_lineage_sql_timeout_seconds</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | HTTP timeout in seconds for SQL-view APIs. These calls can be slower than metadata definition APIs because MicroStrategy must create and resolve a dashboard instance. <div className="default-line default-line-with-docs">Default: <span className="default-value">180</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">auth</span></div> <div className="type-name-line"><span className="type-name">One of MicroStrategyGuestAuth, MicroStrategyPasswordAuth</span></div> | Authentication mode. Use `type: guest` for public demo-style access or `type: password` with username/password for authenticated tenants.  |
| <div className="path-line"><span className="path-prefix">auth.</span><span className="path-main">password</span>&nbsp;<abbr title="Required if auth is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string(password)</span></div> | MicroStrategy password.  |
| <div className="path-line"><span className="path-prefix">auth.</span><span className="path-main">username</span>&nbsp;<abbr title="Required if auth is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | MicroStrategy username.  |
| <div className="path-line"><span className="path-prefix">auth.</span><span className="path-main">type</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Const value: guest <div className="default-line default-line-with-docs">Default: <span className="default-value">guest</span></div> |
| <div className="path-line"><span className="path-main">dashboard_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">dashboard_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">datasource_platform_mapping</span></div> <div className="type-name-line"><span className="type-name">map(str,ConnectionPlatformConfig)</span></div> |   |
| <div className="path-line"><span className="path-prefix">datasource_platform_mapping.`key`.</span><span className="path-main">platform</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | DataHub platform name (for example `snowflake`) to override the one auto-detected from the datasource's database type. Set this when the warehouse is custom or its database type is not recognized. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">datasource_platform_mapping.`key`.</span><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Lowercase the upstream warehouse dataset and column URNs. Set to false when this warehouse was ingested with case preserved so lineage URNs match. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">datasource_platform_mapping.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The platform instance the warehouse was ingested under. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">datasource_platform_mapping.`key`.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The environment the warehouse was ingested under. Defaults to this connector's `env` when unset. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">folder_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">folder_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">project_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">project_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">report_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">report_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Stateful ingestion config with stale entity removal support. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
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
    "ConnectionPlatformConfig": {
      "additionalProperties": false,
      "properties": {
        "platform": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "DataHub platform name (for example `snowflake`) to override the one auto-detected from the datasource's database type. Set this when the warehouse is custom or its database type is not recognized.",
          "title": "Platform"
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
          "description": "The platform instance the warehouse was ingested under.",
          "title": "Platform Instance"
        },
        "env": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The environment the warehouse was ingested under. Defaults to this connector's `env` when unset.",
          "title": "Env"
        },
        "convert_urns_to_lowercase": {
          "default": true,
          "description": "Lowercase the upstream warehouse dataset and column URNs. Set to false when this warehouse was ingested with case preserved so lineage URNs match.",
          "title": "Convert Urns To Lowercase",
          "type": "boolean"
        }
      },
      "title": "ConnectionPlatformConfig",
      "type": "object"
    },
    "MicroStrategyGuestAuth": {
      "additionalProperties": false,
      "properties": {
        "type": {
          "const": "guest",
          "default": "guest",
          "title": "Type",
          "type": "string"
        }
      },
      "title": "MicroStrategyGuestAuth",
      "type": "object"
    },
    "MicroStrategyPasswordAuth": {
      "additionalProperties": false,
      "properties": {
        "type": {
          "const": "password",
          "default": "password",
          "title": "Type",
          "type": "string"
        },
        "username": {
          "description": "MicroStrategy username.",
          "title": "Username",
          "type": "string"
        },
        "password": {
          "description": "MicroStrategy password.",
          "format": "password",
          "title": "Password",
          "type": "string",
          "writeOnly": true
        }
      },
      "required": [
        "username",
        "password"
      ],
      "title": "MicroStrategyPasswordAuth",
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
      "description": "Stateful ingestion config with stale entity removal support."
    },
    "base_url": {
      "description": "MicroStrategy Library base URL, for example `https://your-company.example.com/MicroStrategyLibrary`.",
      "title": "Base Url",
      "type": "string"
    },
    "auth": {
      "description": "Authentication mode. Use `type: guest` for public demo-style access or `type: password` with username/password for authenticated tenants.",
      "discriminator": {
        "mapping": {
          "guest": "#/$defs/MicroStrategyGuestAuth",
          "password": "#/$defs/MicroStrategyPasswordAuth"
        },
        "propertyName": "type"
      },
      "oneOf": [
        {
          "$ref": "#/$defs/MicroStrategyGuestAuth"
        },
        {
          "$ref": "#/$defs/MicroStrategyPasswordAuth"
        }
      ],
      "title": "Auth"
    },
    "verify_ssl": {
      "default": true,
      "description": "Whether to verify SSL certificates for MicroStrategy API calls.",
      "title": "Verify Ssl",
      "type": "boolean"
    },
    "timeout_seconds": {
      "default": 30,
      "description": "HTTP request timeout in seconds.",
      "exclusiveMinimum": 0,
      "title": "Timeout Seconds",
      "type": "integer"
    },
    "max_retries": {
      "default": 3,
      "description": "Maximum retry attempts for transient API failures.",
      "minimum": 0,
      "title": "Max Retries",
      "type": "integer"
    },
    "page_size": {
      "default": 100,
      "description": "Number of objects requested per paginated metadata search call.",
      "exclusiveMinimum": 0,
      "title": "Page Size",
      "type": "integer"
    },
    "project_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "description": "Regex patterns to filter MicroStrategy projects by name."
    },
    "dashboard_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "description": "Regex patterns to filter MicroStrategy dossiers/dashboards by name."
    },
    "report_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "description": "Regex patterns to filter MicroStrategy reports by name."
    },
    "folder_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "description": "Regex patterns to filter folder containers by name. When an intermediate folder is denied, its children re-parent to the nearest allowed ancestor rather than being dropped."
    },
    "include_hidden": {
      "default": false,
      "description": "Whether to include hidden MicroStrategy objects when APIs support it.",
      "title": "Include Hidden",
      "type": "boolean"
    },
    "extract_dashboards": {
      "default": true,
      "description": "Whether to extract dossiers/documents as DataHub dashboards.",
      "title": "Extract Dashboards",
      "type": "boolean"
    },
    "extract_charts": {
      "default": true,
      "description": "Whether to extract visualizations as DataHub charts.",
      "title": "Extract Charts",
      "type": "boolean"
    },
    "extract_reports": {
      "default": false,
      "description": "Whether to extract MicroStrategy reports as DataHub charts. Disabled by default because reports can be numerous and are independent from dossier visualization extraction.",
      "title": "Extract Reports",
      "type": "boolean"
    },
    "extract_report_definitions": {
      "default": true,
      "description": "Whether to fetch report definitions for source dataset, metric, and attribute details when `extract_reports` is enabled.",
      "title": "Extract Report Definitions",
      "type": "boolean"
    },
    "extract_independent_reports": {
      "default": false,
      "description": "Whether to extract reports not referenced by any ingested dashboard. By default only dashboard-linked reports are ingested (`report_pattern` still applies), so scoping dashboards also scopes reports. The linkage comes from `extract_dashboard_dependencies`; when dashboards or dependencies are not extracted, all matching reports are ingested.",
      "title": "Extract Independent Reports",
      "type": "boolean"
    },
    "extract_cubes": {
      "default": true,
      "description": "Whether to extract embedded dashboard datasets as DataHub datasets.",
      "title": "Extract Cubes",
      "type": "boolean"
    },
    "extract_lineage": {
      "default": true,
      "description": "Whether to emit dataset-to-chart lineage when resolved from definitions.",
      "title": "Extract Lineage",
      "type": "boolean"
    },
    "extract_visualization_details": {
      "default": true,
      "description": "Whether to execute dashboards and fetch per-visualization runtime definitions to resolve dataset-to-visualization lineage when the static dashboard definition does not include dataset IDs.",
      "title": "Extract Visualization Details",
      "type": "boolean"
    },
    "extract_source_warehouses": {
      "default": true,
      "description": "Whether to call the MicroStrategy datasource management APIs to discover project source warehouse names, source types, database versions, DBMS names, and connection metadata.",
      "title": "Extract Source Warehouses",
      "type": "boolean"
    },
    "extract_dashboard_dependencies": {
      "default": true,
      "description": "Whether to call metadata search lineage APIs for direct dashboard components such as metrics, attributes, filters, and functions.",
      "title": "Extract Dashboard Dependencies",
      "type": "boolean"
    },
    "extract_metric_expressions": {
      "default": true,
      "description": "Whether to fetch metric model definitions with expression tokens and attach expression metadata to metric schema fields when the MicroStrategy principal has access.",
      "title": "Extract Metric Expressions",
      "type": "boolean"
    },
    "extract_model_lineage": {
      "default": true,
      "description": "Whether to attempt modeling/table API access needed for logical table and source warehouse lineage. If privileges are missing, the connector reports the failure and continues.",
      "title": "Extract Model Lineage",
      "type": "boolean"
    },
    "extract_warehouse_lineage": {
      "default": false,
      "description": "Whether to execute dashboard/dossier SQL-view APIs and emit upstream coarse table-level lineage from MicroStrategy datasets to source warehouse datasets parsed from SQL. Disabled by default because this is not field-level metric, attribute, or fact lineage. The connector discovers the warehouse platform from MicroStrategy datasource metadata and does not store raw SQL.",
      "title": "Extract Warehouse Lineage",
      "type": "boolean"
    },
    "extract_report_sql_lineage": {
      "default": false,
      "description": "Whether to execute report SQL-view APIs and emit coarse table-level lineage from report source datasets to source warehouse datasets. Disabled by default for the same reason as `extract_warehouse_lineage`. Field-level model lineage for report source datasets also requires this flag, because model lineage only attaches to datasets with known warehouse upstreams.",
      "title": "Extract Report Sql Lineage",
      "type": "boolean"
    },
    "warehouse_lineage_sql_timeout_seconds": {
      "default": 180,
      "description": "HTTP timeout in seconds for SQL-view APIs. These calls can be slower than metadata definition APIs because MicroStrategy must create and resolve a dashboard instance.",
      "exclusiveMinimum": 0,
      "title": "Warehouse Lineage Sql Timeout Seconds",
      "type": "integer"
    },
    "emit_dashboard_dataset_edges": {
      "default": false,
      "description": "Emit DashboardInfo.datasetEdges as a fallback. Disabled by default because BI dashboards with many datasets make lineage views noisy.",
      "title": "Emit Dashboard Dataset Edges",
      "type": "boolean"
    },
    "extract_usage_statistics": {
      "default": false,
      "description": "Whether to extract dashboard and report usage statistics (view counts, unique users, per-user counts) by querying the MicroStrategy Platform Analytics telemetry cube. Requires the Platform Analytics project to be enabled on the environment (standard on MicroStrategy Cloud) and readable by the ingestion principal. Disabled by default.",
      "title": "Extract Usage Statistics",
      "type": "boolean"
    },
    "usage_lookback_days": {
      "default": 14,
      "description": "How many days of usage history to request from the Platform Analytics cube. The shipped aggregate cube typically retains a 14-day rolling window, so larger values only help when the environment retains more history.",
      "exclusiveMinimum": 0,
      "maximum": 365,
      "title": "Usage Lookback Days",
      "type": "integer"
    },
    "platform_analytics_project_name": {
      "default": "Platform Analytics",
      "description": "Name of the MicroStrategy project that hosts Platform Analytics telemetry. Only change this if the environment renamed the standard project.",
      "title": "Platform Analytics Project Name",
      "type": "string"
    },
    "usage_cube_name": {
      "default": "Platform Analytics (Agg)",
      "description": "Name of the Platform Analytics cube to query for usage. The default is the aggregate telemetry cube shipped with Platform Analytics; a custom cube works as long as it exposes Date, Project, Object, and User attributes and an executions metric.",
      "title": "Usage Cube Name",
      "type": "string"
    },
    "usage_query_timeout_seconds": {
      "default": 180,
      "description": "HTTP timeout in seconds for Platform Analytics cube query calls. Cube execution is server-side work and can be slower than metadata definition APIs.",
      "exclusiveMinimum": 0,
      "title": "Usage Query Timeout Seconds",
      "type": "integer"
    },
    "tag_measures_and_dimensions": {
      "default": true,
      "description": "Tag metric fields as Measure, attribute fields as Dimension, and date/time attribute forms as Temporal.",
      "title": "Tag Measures And Dimensions",
      "type": "boolean"
    },
    "ingest_owner": {
      "default": true,
      "description": "Whether to map API owner fields to DataHub ownership aspects.",
      "title": "Ingest Owner",
      "type": "boolean"
    },
    "datasource_platform_mapping": {
      "additionalProperties": {
        "$ref": "#/$defs/ConnectionPlatformConfig"
      },
      "description": "Optional mapping from MicroStrategy datasource or connection name to the platform, platform instance, environment, and URN casing that warehouse was ingested under. MicroStrategy can hold several connections to the same warehouse platform (for example a prod and a dev Snowflake account), so keying by connection name lets each resolve to the right instance. Entries are matched against the datasource's connection name first, then its datasource name. Datasources with no entry auto-detect their platform and use this connector's `env`, no platform instance, and lowercase URNs.",
      "title": "Datasource Platform Mapping",
      "type": "object"
    },
    "metric_glossary_term_mapping": {
      "additionalProperties": {
        "type": "string"
      },
      "description": "Optional explicit mapping from MicroStrategy metric ID or name to DataHub glossary term URN.",
      "title": "Metric Glossary Term Mapping",
      "type": "object"
    },
    "attribute_glossary_term_mapping": {
      "additionalProperties": {
        "type": "string"
      },
      "description": "Optional explicit mapping from MicroStrategy attribute/form ID or name to DataHub glossary term URN.",
      "title": "Attribute Glossary Term Mapping",
      "type": "object"
    }
  },
  "required": [
    "base_url"
  ],
  "title": "MicroStrategyConfig",
  "type": "object"
}
```





### Capabilities

#### Lineage Behavior

By default, the connector emits lineage from MicroStrategy datasets to visualizations by setting `ChartInfo.inputs` and `ChartInfo.inputEdges`. When visualization metadata exposes metric and attribute references, it also emits chart `InputFields` pointing at the MicroStrategy dataset fields used by each visualization. It does not emit direct dataset-to-dashboard edges because those edges can make DataHub lineage views noisy for large BI dashboards.

Set `emit_dashboard_dataset_edges: true` if you want every dashboard dataset to appear directly upstream of the dashboard as fallback lineage.

The definition APIs do not expose a visualization's dataset binding directly, so the connector resolves it in tiers. First it reads the modeling document API: derived metrics and attributes are scoped to a single dataset, so a visualization grid referencing them identifies its source dataset with certainty. When no dataset-scoped objects are referenced (or modeling access is unavailable), the connector falls back to inferring the binding from shared object references and name tokens. If that inference cannot exclude any dataset (for example, dashboards built from one cube per time period where all cubes share one object catalog), the visualization is treated as unresolved and gets no dataset inputs rather than an all-to-all fan-out; use `emit_dashboard_dataset_edges: true` to keep such dashboards connected to their datasets.

When `extract_visualization_details: true`, the connector creates a dashboard instance and calls the v2 visualization definition endpoint to resolve dataset-to-visualization lineage when the static dashboard definition does not include dataset IDs. Use `dashboard_pattern` to scope live validation runs, for example:

```yaml
dashboard_pattern:
  allow:
    - "^Quarterly Business Review$"
```

#### Reports

Set `extract_reports: true` to ingest MicroStrategy reports as DataHub chart entities with the `Report` subtype. Report extraction is disabled by default because report libraries can be much larger than curated dossiers. Use `report_pattern` to scope report extraction.

When dashboards are also extracted, only reports referenced by an ingested dashboard are ingested by default, so scoping dashboards with `dashboard_pattern` scopes reports too. Linked reports are fetched directly by id rather than by enumerating the project's report library, so large report libraries do not slow down scoped runs. Set `extract_independent_reports: true` to also ingest reports not used by any dashboard (every report matching `report_pattern`); this enumerates the full report library. This scoping relies on `extract_dashboard_dependencies` for the dashboard-to-report linkage; without it, or without dashboard extraction, all matching reports are ingested.

When report definitions expose source and `availableObjects` metadata, the connector emits a report-scoped MicroStrategy source dataset containing the report metrics, attributes, and attribute forms. Report lineage uses `ChartInfo.inputs`, `ChartInfo.inputEdges`, and chart `InputFields` from that report source dataset to the report chart.

If `extract_dashboard_dependencies: true` and `extract_reports: true`, dashboards that expose report dependencies link to the matching report chart entities. Reports are a separate lineage path from dossier visualizations:

```text
Dashboard/Dossier -> Visualization -> MicroStrategy Dataset
Dashboard/Dossier -> Report -> MicroStrategy Report Source Dataset
```

Set `extract_report_sql_lineage: true` only when you also want optional coarse report source dataset -> warehouse table lineage from the report SQL-view API. This setting is disabled by default and does not emit direct warehouse edges to reports or dashboards.

#### Source Warehouses

When `extract_source_warehouses: true`, the connector calls MicroStrategy datasource management APIs for each project and records a source warehouse summary on the project container. The summary includes the datasource count, source database types, datasource types, and DBMS names exposed by MicroStrategy.

If a dashboard dataset payload includes a direct source warehouse reference, the connector also records datasource ID, datasource name, source type, database version, DBMS name, connection ID/name, and available database/schema context as dataset custom properties.

When `extract_warehouse_lineage: true`, the connector executes the dashboard/dossier SQL-view API and emits coarse upstream lineage from each MicroStrategy dataset to the physical warehouse datasets parsed from SQL. When field-level model lineage resolves for a dataset, its table-level upstreams are restricted to the tables evidenced by field lineage — tables the SQL only joins for filtering (dimension lookups, calendar subqueries) are not emitted as upstreams. Datasets without field-level lineage keep the full SQL-derived table set. It does not store raw SQL or connection strings. This setting is disabled by default because SQL-view lineage is table-level and does not prove field-level metric, attribute, or fact lineage. The connector uses dataset-level source warehouse metadata when MicroStrategy provides it. It only falls back to project-level datasource metadata when the project resolves to one unambiguous warehouse context, so multi-source projects do not get broad dataset-to-table edges from an arbitrary datasource. The resulting lineage shape is:

```text
Dashboard/Dossier -> Visualization -> MicroStrategy Dataset -> Warehouse Dataset
```

The connector intentionally keeps direct `DashboardInfo.datasetEdges` disabled by default so dashboards do not draw edges directly to every dataset in DataHub lineage views.

#### Dependency and Model Lineage Enrichment

When `extract_dashboard_dependencies: true`, the connector uses MicroStrategy metadata search lineage APIs to record direct dashboard component dependency summaries, including dependency counts by MicroStrategy object type.

When `extract_metric_expressions: true`, the connector fetches accessible metric model definitions and stores expression token summaries in metric field `jsonProps`.

When `extract_model_lineage: true`, the connector probes modeling table APIs needed for logical table and physical source warehouse lineage. Missing privileges are reported as warnings and counters; the connector continues with dashboard, dataset, metric, and source warehouse metadata.

#### Metric and Attribute Tags

MicroStrategy metrics and attributes are emitted as schema fields on the dashboard dataset/cube. The connector attaches canonical DataHub tags to the fields:

- `urn:li:tag:Measure` for metrics.
- `urn:li:tag:Dimension` for attributes and attribute forms.
- `urn:li:tag:Temporal` for date/time attribute forms.

These tags are written to source-managed `SchemaMetadata` field metadata, not editable schema metadata.

#### Usage Statistics

Set `extract_usage_statistics: true` to emit daily view counts, unique-user counts, and per-user usage for ingested dashboards and reports. MicroStrategy has no per-object usage REST endpoint; the connector queries the Platform Analytics telemetry cube (the `Platform Analytics (Agg)` cube in the `Platform Analytics` project) through the standard cube instance APIs and joins the telemetry rows to ingested entities by object GUID.

Requirements and behavior:

- Platform Analytics must be enabled on the environment (standard on MicroStrategy Cloud) and the ingestion principal needs read access to the Platform Analytics project. When the project or cube is missing, the connector records one warning and continues without usage.
- The cube's attributes and metrics are resolved by name (`Date`, `Project`, `Object`, `User`, and `Num Executions` or `Count Actions`), so renamed or heavily customized telemetry cubes are skipped with a warning explaining what was missing. Use `usage_cube_name` to point at a custom cube that exposes the same objects.
- `usage_lookback_days` bounds the request window (default 14 days — the shipped aggregate cube typically retains a 14-day rolling window). Usage freshness depends on the environment's Platform Analytics cube refresh schedule.
- Usage rows for objects outside the ingested scope are counted in the `usage_objects_unmatched` report counter and skipped.

### Limitations

- Warehouse lineage from the SQL-view APIs is coarse table-level lineage and is disabled by default (`extract_warehouse_lineage` and `extract_report_sql_lineage`). Field-level metric, attribute, or fact lineage to warehouse tables is not available yet.
- Report extraction is disabled by default (`extract_reports`) because report libraries can be much larger than curated dossiers.
- Direct dashboard-to-dataset edges are disabled by default; enable `emit_dashboard_dataset_edges` only if you want dashboard-level fallback lineage, which can make lineage views noisy for large dashboards.
- Modeling APIs (logical tables, metric expressions) may return 403 when the principal lacks modeling privileges. The connector degrades gracefully — missing privileges are reported as warnings and counters, and ingestion continues with dashboard, dataset, metric, and source warehouse metadata.
- Multi-source projects only receive dataset-to-warehouse edges when MicroStrategy exposes dataset-level source warehouse metadata; the connector does not guess a datasource when the project-level warehouse context is ambiguous.
- Dataset `upstreamLineage` is replaced wholesale while any upstream tables remain, but a dataset whose warehouse lineage disappears entirely keeps the previous run's aspect (stale-entity removal deletes entities, not aspects). Remove leftovers with `datahub delete --urn <urn> --aspect upstreamLineage` or a rollback of the earlier run. Avoid pipeline-level incremental-lineage transformers with this source: their patch-add semantics prevent edge reductions from propagating.

### Troubleshooting

#### Missing dataset-to-visualization lineage

If charts do not show upstream datasets, the static dashboard definition may not include dataset IDs. Set `extract_visualization_details: true` so the connector creates a dashboard instance and resolves bindings from the v2 visualization definition endpoint. This requires the principal to have instance-creation privileges; use `dashboard_pattern` to scope the run while validating.

#### 403 errors on modeling or SQL-view APIs

The connector does not fail ingestion on modeling API 403s — it records warnings and counters in the ingestion report and continues. Check the report counters to see which APIs were inaccessible, and grant the principal instance-creation and SQL-view access (for warehouse lineage) or modeling privileges (for `extract_metric_expressions` / `extract_model_lineage`) as needed.

#### Session invalidation mid-run

MicroStrategy can invalidate the session token at any time (idle or absolute timeouts, concurrent-session limits, administrator action). The connector re-authenticates automatically and replays the failed request; the `sessions_reauthenticated` counter in the ingestion report shows when this happened. If re-login itself fails — or the server rejects a request immediately after a successful re-login — the run aborts with a single `MicroStrategy Authentication Lost` failure instead of failing every remaining project. Avoid signing in elsewhere with the ingestion service account while a run is in progress if your tenant enforces concurrent-session limits.

#### Empty or incomplete results

If little or no metadata is ingested, verify that the principal has Library API access and project-scoped metadata search, and check that `project_pattern` (and `dashboard_pattern` / `report_pattern`, if set) are not filtering out the content you expect. Guest authentication works for public demo exploration but does not expose all modeling APIs.


### Code Coordinates
- Class Name: `datahub.ingestion.source.microstrategy.source.MicroStrategySource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/microstrategy/source.py)


:::tip Questions?

If you've got any questions on configuring ingestion for MicroStrategy, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
