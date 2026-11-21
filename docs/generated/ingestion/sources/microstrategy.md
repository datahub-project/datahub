


# MicroStrategy

## Overview

[MicroStrategy](https://www.microstrategy.com/) is an enterprise business intelligence platform for building governed dossiers, dashboards, reports, cubes, metrics, and attributes over business data.

The DataHub integration for MicroStrategy ingests projects and folders as containers, dossiers/documents as dashboards, visualizations as charts, and embedded dashboard datasets/cubes as datasets with schema fields derived from MicroStrategy metrics, attributes, and attribute forms. It can also ingest MicroStrategy reports as chart entities with report-scoped source datasets when `extract_reports` is enabled. The connector records project source warehouse/source type summaries when datasource APIs are available.

Lineage is emitted from datasets to visualizations and report source datasets to reports through `ChartInfo.inputs` and `ChartInfo.inputEdges` when the connector can resolve bindings from definitions or runtime details. When metadata exposes metric and attribute references, the connector also emits chart `InputFields` for the fields used by each visualization or report. Direct dossier dataset edges are disabled by default because large BI dashboards can reference many datasets and make lineage graphs noisy; set `emit_dashboard_dataset_edges: true` only when dashboard-level fallback lineage is preferred. Report Services documents expose no visualizations through the REST API, so they are linked to their datasets directly by default (`emit_document_dataset_edges`). Coarse table-level warehouse lineage from SQL-view APIs is also disabled by default; set `extract_warehouse_lineage: true` for dashboard dataset -> warehouse table edges or `extract_report_sql_lineage: true` for report source dataset -> warehouse table edges before field-level metric, attribute, or fact lineage is available.

## Concept Mapping

| MicroStrategy Concept         | DataHub Concept                                 | Notes                                                                                                                  |
| ----------------------------- | ----------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| Project                       | Container (`Project` subtype)                   | Project-scoped APIs require `X-MSTR-ProjectID`.                                                                        |
| Folder                        | Container (`Folder` subtype)                    | Created when search results expose folder or location context.                                                         |
| Dossier / dashboard           | Dashboard (`Dossier` subtype)                   | Dashboard metadata is read from the dossier definition API.                                                            |
| Report Services document      | Dashboard (`Document` subtype)                  | Object subtype 14081; metadata is read from the document definition API.                                               |
| Visualization                 | Chart (`Visualization` subtype)                 | Chart lineage uses `ChartInfo.inputs`, `inputEdges`, and chart `InputFields` when dataset and field IDs can be mapped. |
| Report                        | Chart (`Report` subtype)                        | Optional via `extract_reports`; report lineage uses a report-scoped source dataset.                                    |
| Dashboard dataset (report)    | Dataset (`MicroStrategy Dataset` subtype)       | Schema fields come from embedded `availableObjects`.                                                                   |
| Dashboard dataset (cube)      | Dataset (`Cube` subtype)                        | Intelligent and super cubes (object subtype 776/779); `MicroStrategy Dataset` when the object info lookup fails.       |
| Report source dataset         | Dataset (`MicroStrategy Dataset` or `Cube`)     | Schema fields come from report definition `availableObjects`; `Cube` when the report reads a cube.                     |
| Source warehouse / datasource | Custom properties and optional upstream lineage | SQL-view APIs provide opt-in coarse physical table lineage when available.                                             |
| Metric                        | Schema field with `Measure` tag                 | Metric IDs are preserved in field `jsonProps`.                                                                         |
| Attribute / form              | Schema field with `Dimension` tag               | Date/time forms also receive the `Temporal` tag.                                                                       |
| Owner                         | CorpUser ownership                              | Emitted when owner fields are exposed and `ingest_owner` is enabled.                                                   |
| Metric / attribute glossary   | Field glossary terms from explicit config maps  | The connector does not create glossary terms automatically.                                                            |


## Module `microstrategy`
![Alpha](https://img.shields.io/badge/support%20status-Alpha-lightgrey)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Projects and folders emit as containers. |
| Column-level Lineage | ✅ | Modeling API field lineage from MicroStrategy metrics and attributes to source warehouse fields. |
| Dataset Usage | ✅ | Dashboard and report usage from the Platform Analytics cube when `extract_usage_statistics` is enabled. |
| Descriptions | ✅ | Enabled by default. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled via stateful ingestion. |
| Extract Ownership | ✅ | Enabled by default via `ingest_owner`. |
| Extract Tags | ✅ | Metric, attribute, temporal, and derived-metric field tags. |
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

Install the connector with its extra, `pip install 'acryl-datahub[microstrategy]'`, so the SQL parsing dependencies used by SQL-view lineage are present. When `extract_lineage` is enabled together with `extract_warehouse_lineage` or `extract_report_sql_lineage`, the connector checks for them at startup and fails the run with a message naming the extra, rather than failing every SQL view.

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

    # Additionally emit each project's schema as a first-class semanticModel
    # (one logical dataset per MicroStrategy logical table, one metric entity
    # per project metric with lineage). Additive; existing dashboard/report
    # datasets are unaffected. Requires a server that registers those entities
    # (Cloud >= 2.1.0, or OSS with METRICS_ENABLED=true).
    # emit_semantic_model_entities: true

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
| <div className="path-line"><span className="path-main">dashboard_instance_resolve_only</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to create dossier and document instances with `resolveOnly: true`, which Strategy documents as resolving the dashboard without executing it. Instances back visualization runtime details and SQL-view warehouse lineage; executing them runs the dashboard's queries against the warehouse and can take minutes per dashboard. Off by default until verified to return the same runtime grid and SQL view as an executed instance. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">dataset_field_order</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "report", "alphabetical" <div className="default-line default-line-with-docs">Default: <span className="default-value">report</span></div> |
| <div className="path-line"><span className="path-main">emit_chapter_page_structured_properties</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Also record each dossier visualization chart's chapter and page as DataHub structured properties, so charts can be filtered by chapter and page in search and the placement is read by AI tools: `io.acryl.microstrategy.chapter` (MicroStrategy Chapter, text), `io.acryl.microstrategy.page` (MicroStrategy Page, text) and `io.acryl.microstrategy.pagePosition` (MicroStrategy Page Position, a number: the page's 1-based position within its chapter, the same value as the `microstrategyPageIndex` custom property). The `microstrategyChapter` and `microstrategyPage` custom properties are emitted regardless. The three property definitions (applicable to charts) are created in DataHub on the first run with this flag on and re-emitted unchanged after that; each run replaces the chart's structured property values with these. A visualization the dossier does not place on a page gets no values. Default is false. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">emit_dashboard_dataset_edges</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit DashboardInfo.datasetEdges as a fallback. Disabled by default because BI dashboards with many datasets make lineage views noisy. Governs dossiers only; see `emit_document_dataset_edges` for Report Services documents. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">emit_document_dataset_edges</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to record a Report Services document's datasets as DashboardInfo.datasetEdges, so the document lists its datasets and lineage reads dataset -> document. The REST API exposes no visualizations for a document (its definition lists layouts as chapters without pages), so unlike a dossier it has no charts to carry dataset lineage; without these edges the document is an empty page. Independent of `emit_dashboard_dataset_edges`, which keeps governing dossiers. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">emit_semantic_model_entities</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If true, additionally emit one `semanticModel` entity per project (logical datasets with subtype `Semantic Model Dataset`, one per MicroStrategy logical table, plus a `metric` entity per project-wide Base/Derived/Consolidated metric with lineage resolved from its expression). This is additive: existing report/dashboard/dataset emission is unaffected. Default is false. Requires a DataHub server that registers semanticModel/metric (Cloud >= 2.1.0, or OSS with `METRICS_ENABLED=true`) and `extract_model_lineage` access to the MicroStrategy modeling APIs. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">extract_charts</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to extract visualizations as DataHub charts. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_cubes</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to extract embedded dashboard datasets as DataHub datasets. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_dashboard_dependencies</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to call metadata search lineage APIs for direct dashboard components such as metrics, attributes, filters, and functions. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_dashboards</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to extract dossiers/documents as DataHub dashboards. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_derived_metrics</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to surface derived metrics (grid columns marked `derived: true`) as schema fields on the dataset backing their column group, tagged `Derived`. When the dataset is a report, its definition is fetched (GET /api/model/reports/{id}, falling back to GET /api/v2/reports/{id}) so report-level derived metrics carry the report's object name and formula; every derived metric the report defines is emitted, not only those a grid displays. Derived metrics no definition exposes are visualization-local and carry provenance but no expression. Adds one or two API calls per distinct report-backed dataset per project. Grid input requires `extract_lineage` and `extract_visualization_details`. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_document_dataset_sql_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to emit dataset-to-warehouse lineage for the datasets of Report Services documents. A document's datasets are grid reports, and the dossier SQL-view API answers a document instance with views that carry no SQL, so each report-backed dataset's SQL is read through a report instance instead (`POST /api/v2/reports/{id}/instances`, `GET /api/v2/reports/{id}/instances/{instance}/sqlView`, then `DELETE` of the instance) and parsed like a dossier SQL view. Creating a report instance may execute the report against the warehouse: one execution per distinct report per run (a report shared by several documents is read once), subject to `warehouse_lineage_sql_timeout_seconds` and `max_consecutive_execution_timeouts`. Cube datasets have no report SQL view and are skipped. Requires `extract_warehouse_lineage`. Off by default, so documents keep their previous behaviour: datasets emitted without warehouse lineage. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">extract_independent_reports</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to extract reports not referenced by any ingested dashboard. By default only dashboard-linked reports are ingested (`report_pattern` still applies), so scoping dashboards also scopes reports. The linkage comes from `extract_dashboard_dependencies`; when dashboards or dependencies are not extracted, all matching reports are ingested. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">extract_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to emit dataset-to-chart lineage when resolved from definitions. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_metric_expressions</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to fetch metric model definitions with expression tokens and attach expression metadata to metric schema fields when the MicroStrategy principal has access. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_metric_formula_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to parse `{Metric Name}` references out of catalog metric expressions (fetched via `extract_metric_expressions`) and emit field-to-field lineage from a metric to the sibling fields it references on the same dataset. Best effort: references to objects that are not fields of the dataset are counted in the report and skipped. Disabled by default. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">extract_model_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to attempt modeling/table API access needed for logical table and source warehouse lineage. If privileges are missing, the connector reports the failure and continues. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_report_definitions</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to fetch report definitions for source dataset, metric, and attribute details when `extract_reports` is enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_report_sql_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to execute report SQL-view APIs and emit coarse table-level lineage from report source datasets to source warehouse datasets. Disabled by default for the same reason as `extract_warehouse_lineage`. Field-level model lineage for report source datasets also requires this flag, because model lineage only attaches to datasets with known warehouse upstreams. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">extract_reports</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to extract MicroStrategy reports as DataHub charts. Disabled by default because reports can be numerous and are independent from dossier visualization extraction. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">extract_source_warehouses</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to call the MicroStrategy datasource management APIs to discover project source warehouse names, source types, database versions, DBMS names, and connection metadata. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_usage_statistics</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to extract dashboard and report usage statistics (view counts, unique users, per-user counts) by querying the MicroStrategy Platform Analytics telemetry cube. Requires the Platform Analytics project to be enabled on the environment (standard on MicroStrategy Cloud) and readable by the ingestion principal. Disabled by default. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">extract_visualization_details</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to execute dashboards and fetch per-visualization runtime definitions to resolve dataset-to-visualization lineage when the static dashboard definition does not include dataset IDs. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_warehouse_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to execute dashboard/dossier SQL-view APIs and emit upstream coarse table-level lineage from MicroStrategy datasets to source warehouse datasets parsed from SQL. Disabled by default because this is not field-level metric, attribute, or fact lineage. The connector discovers the warehouse platform from MicroStrategy datasource metadata and does not store raw SQL. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_hidden</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to include hidden MicroStrategy objects when APIs support it. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_personal_folders</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to ingest personal folders: each user's profile folder tree under the project's 'Profiles' system folder, holding that user's 'My Reports', 'My Objects' and similar folders. Excluded by default: dashboards, documents and reports filed there are skipped before any definition is fetched (counted in `personal_folder_objects_skipped`) and their folder containers are not emitted. The Profiles root is resolved by id via GET /api/folders/preDefined (EnumDSSXMLFolderNames 19/20) plus the profile folder's own ancestors; when that fails, an ancestor folder named 'Profiles' or 'My Reports' (case-insensitive) identifies personal content instead. Set true to ingest them. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">ingest_owner</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to map API owner fields to DataHub ownership aspects. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">max_consecutive_execution_timeouts</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Stop executing dashboards and reports in a project after this many consecutive instance creations time out (`warehouse_lineage_sql_timeout_seconds`). The remaining content in that project is still ingested, without visualization runtime details or SQL-view lineage. Bounds a run against a project of heavy content that never finishes executing. 0 disables the limit. <div className="default-line default-line-with-docs">Default: <span className="default-value">3</span></div> |
| <div className="path-line"><span className="path-main">max_retries</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum retry attempts for transient API failures. <div className="default-line default-line-with-docs">Default: <span className="default-value">3</span></div> |
| <div className="path-line"><span className="path-main">metric_glossary_term_mapping</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div> |   |
| <div className="path-line"><span className="path-main">page_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of objects requested per paginated metadata search call. <div className="default-line default-line-with-docs">Default: <span className="default-value">100</span></div> |
| <div className="path-line"><span className="path-main">platform_analytics_project_name</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Name of the MicroStrategy project that hosts Platform Analytics telemetry. Only change this if the environment renamed the standard project. <div className="default-line default-line-with-docs">Default: <span className="default-value">Platform Analytics</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">resolve_report_metrics_via_instance</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to read report Modeling definitions through a Modeling-service report instance (`POST /api/model/reports/{id}/instances`, passed in the `X-MSTR-MS-Instance` header), so derived metrics can be resolved; without it the definition is read statically and on some tenants carries no metric expressions. It also enables reading document-level derived metrics (defined in a dossier or document on top of a dataset) from the Modeling document definition, which is otherwise never consulted for them; that endpoint is not in Strategy's published API specification and has no instance route, so it is read statically. Off by default because it adds instance create and delete calls for every report-backed dataset. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">tag_measures_and_dimensions</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Tag metric fields as Measure, attribute fields as Dimension, and date/time attribute forms as Temporal. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">timeout_seconds</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | HTTP request timeout in seconds. <div className="default-line default-line-with-docs">Default: <span className="default-value">30</span></div> |
| <div className="path-line"><span className="path-main">usage_cube_name</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Name of the Platform Analytics cube to query for usage. The default is the aggregate telemetry cube shipped with Platform Analytics; a custom cube works as long as it exposes Date, Project, Object, and User attributes and an executions metric. <div className="default-line default-line-with-docs">Default: <span className="default-value">Platform Analytics (Agg)</span></div> |
| <div className="path-line"><span className="path-main">usage_lookback_days</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | How many days of usage history to request from the Platform Analytics cube. The shipped aggregate cube typically retains a 14-day rolling window, so larger values only help when the environment retains more history. <div className="default-line default-line-with-docs">Default: <span className="default-value">14</span></div> |
| <div className="path-line"><span className="path-main">usage_query_timeout_seconds</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | HTTP timeout in seconds for Platform Analytics cube query calls. Cube execution is server-side work and can be slower than metadata definition APIs. <div className="default-line default-line-with-docs">Default: <span className="default-value">180</span></div> |
| <div className="path-line"><span className="path-main">use_predefined_folder_names</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Resolve MicroStrategy's predefined folders via GET /api/folders/preDefined and shape the folder hierarchy as Strategy Web shows it: the 'Reports' folder (EnumDSSXMLFolderNames type 7) takes its label 'Shared Reports', and the project root folder and 'Public Objects' are omitted, with their children moved up a level. This drives browse-path matching, container identity and display. Adds one cached API call per project. If the lookup fails, the project's dashboards and reports are skipped and the run reports a failure. Disable this to keep the raw hierarchy and prior container URNs, or on servers without this endpoint; see docs/how/breaking-changes.md. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">verify_ssl</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to verify SSL certificates for MicroStrategy API calls. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">warehouse_lineage_sql_timeout_seconds</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | HTTP timeout in seconds for SQL-view APIs. These calls can be slower than metadata definition APIs because MicroStrategy must create and resolve a dashboard instance. <div className="default-line default-line-with-docs">Default: <span className="default-value">180</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">auth</span></div> <div className="type-name-line"><span className="type-name">One of MicroStrategyGuestAuth, MicroStrategyPasswordAuth</span></div> | Authentication mode. Use `type: guest` for public demo-style access or `type: password` with username/password for authenticated tenants.  |
| <div className="path-line"><span className="path-prefix">auth.</span><span className="path-main">password</span>&nbsp;<abbr title="Required if auth is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string(password)</span></div> | MicroStrategy password.  |
| <div className="path-line"><span className="path-prefix">auth.</span><span className="path-main">username</span>&nbsp;<abbr title="Required if auth is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | MicroStrategy username.  |
| <div className="path-line"><span className="path-prefix">auth.</span><span className="path-main">type</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Const value: guest <div className="default-line default-line-with-docs">Default: <span className="default-value">guest</span></div> |
| <div className="path-line"><span className="path-main">dashboard_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes. <br />  <br /> Patterns are matched against the start of the string only, not the entire <br /> string - a pattern does not need to match to the end to be considered a match. <br /> For example, the pattern "prod" matches "prod", "prod_east", and "production". <br /> To require an exact match, anchor your pattern explicitly, e.g. "^prod$".  |
| <div className="path-line"><span className="path-prefix">dashboard_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">datasource_platform_mapping</span></div> <div className="type-name-line"><span className="type-name">map(str,ConnectionPlatformConfig)</span></div> |   |
| <div className="path-line"><span className="path-prefix">datasource_platform_mapping.`key`.</span><span className="path-main">platform</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | DataHub platform name (for example `snowflake`) to override the one auto-detected from the datasource's database type. Set this when the warehouse is custom or its database type is not recognized. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">datasource_platform_mapping.`key`.</span><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Lowercase the upstream warehouse dataset and column URNs. Set to false when this warehouse was ingested with case preserved so lineage URNs match. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">datasource_platform_mapping.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The platform instance the warehouse was ingested under. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">datasource_platform_mapping.`key`.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The environment the warehouse was ingested under. Defaults to this connector's `env` when unset. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">folder_path_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes. <br />  <br /> Patterns are matched against the start of the string only, not the entire <br /> string - a pattern does not need to match to the end to be considered a match. <br /> For example, the pattern "prod" matches "prod", "prod_east", and "production". <br /> To require an exact match, anchor your pattern explicitly, e.g. "^prod$".  |
| <div className="path-line"><span className="path-prefix">folder_path_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">folder_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes. <br />  <br /> Patterns are matched against the start of the string only, not the entire <br /> string - a pattern does not need to match to the end to be considered a match. <br /> For example, the pattern "prod" matches "prod", "prod_east", and "production". <br /> To require an exact match, anchor your pattern explicitly, e.g. "^prod$".  |
| <div className="path-line"><span className="path-prefix">folder_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">project_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes. <br />  <br /> Patterns are matched against the start of the string only, not the entire <br /> string - a pattern does not need to match to the end to be considered a match. <br /> For example, the pattern "prod" matches "prod", "prod_east", and "production". <br /> To require an exact match, anchor your pattern explicitly, e.g. "^prod$".  |
| <div className="path-line"><span className="path-prefix">project_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">report_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes. <br />  <br /> Patterns are matched against the start of the string only, not the entire <br /> string - a pattern does not need to match to the end to be considered a match. <br /> For example, the pattern "prod" matches "prod", "prod_east", and "production". <br /> To require an exact match, anchor your pattern explicitly, e.g. "^prod$".  |
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
      "description": "Regex patterns to filter folder containers by name. When an intermediate folder is denied, its children re-parent to the nearest allowed ancestor rather than being dropped, so this cannot exclude content; personal folders are excluded by `include_personal_folders` instead. To scope by location use `folder_path_pattern`."
    },
    "folder_path_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "description": "Regex patterns to filter dashboards, documents and reports by the folder path they are filed under. The path is the project name, then each folder from the project root down to the object's own folder, joined with `/`, for example `Sales_Project/Shared Reports/Finance/Monthly Close`. Folder names are the ones the browse path shows: with `use_predefined_folder_names` the project root folder and 'Public Objects' are omitted and 'Shared Reports' carries its MicroStrategy-assigned label; `folder_pattern` plays no part. Patterns match from the start of the path, so anchor with `$` or `(/|$)` to stop a folder matching its similarly named siblings. Unlike `folder_pattern`, a denied path excludes the object: it is skipped before any definition is fetched or instance executed, so its visualizations and datasets are not emitted either, and the skip is counted in `dashboards_filtered_by_folder_path` / `reports_filtered_by_folder_path`. Dashboard-linked reports are filtered on their own path. An object whose folder ancestry the search result does not carry cannot be matched (counted in `folder_paths_unresolved`): it is ingested with a warning under the default allow-all pattern and skipped once a pattern is set."
    },
    "include_personal_folders": {
      "default": false,
      "description": "Whether to ingest personal folders: each user's profile folder tree under the project's 'Profiles' system folder, holding that user's 'My Reports', 'My Objects' and similar folders. Excluded by default: dashboards, documents and reports filed there are skipped before any definition is fetched (counted in `personal_folder_objects_skipped`) and their folder containers are not emitted. The Profiles root is resolved by id via GET /api/folders/preDefined (EnumDSSXMLFolderNames 19/20) plus the profile folder's own ancestors; when that fails, an ancestor folder named 'Profiles' or 'My Reports' (case-insensitive) identifies personal content instead. Set true to ingest them.",
      "title": "Include Personal Folders",
      "type": "boolean"
    },
    "use_predefined_folder_names": {
      "default": true,
      "description": "Resolve MicroStrategy's predefined folders via GET /api/folders/preDefined and shape the folder hierarchy as Strategy Web shows it: the 'Reports' folder (EnumDSSXMLFolderNames type 7) takes its label 'Shared Reports', and the project root folder and 'Public Objects' are omitted, with their children moved up a level. This drives browse-path matching, container identity and display. Adds one cached API call per project. If the lookup fails, the project's dashboards and reports are skipped and the run reports a failure. Disable this to keep the raw hierarchy and prior container URNs, or on servers without this endpoint; see docs/how/breaking-changes.md.",
      "title": "Use Predefined Folder Names",
      "type": "boolean"
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
    "extract_derived_metrics": {
      "default": true,
      "description": "Whether to surface derived metrics (grid columns marked `derived: true`) as schema fields on the dataset backing their column group, tagged `Derived`. When the dataset is a report, its definition is fetched (GET /api/model/reports/{id}, falling back to GET /api/v2/reports/{id}) so report-level derived metrics carry the report's object name and formula; every derived metric the report defines is emitted, not only those a grid displays. Derived metrics no definition exposes are visualization-local and carry provenance but no expression. Adds one or two API calls per distinct report-backed dataset per project. Grid input requires `extract_lineage` and `extract_visualization_details`.",
      "title": "Extract Derived Metrics",
      "type": "boolean"
    },
    "extract_metric_formula_lineage": {
      "default": false,
      "description": "Whether to parse `{Metric Name}` references out of catalog metric expressions (fetched via `extract_metric_expressions`) and emit field-to-field lineage from a metric to the sibling fields it references on the same dataset. Best effort: references to objects that are not fields of the dataset are counted in the report and skipped. Disabled by default.",
      "title": "Extract Metric Formula Lineage",
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
    "extract_document_dataset_sql_lineage": {
      "default": false,
      "description": "Whether to emit dataset-to-warehouse lineage for the datasets of Report Services documents. A document's datasets are grid reports, and the dossier SQL-view API answers a document instance with views that carry no SQL, so each report-backed dataset's SQL is read through a report instance instead (`POST /api/v2/reports/{id}/instances`, `GET /api/v2/reports/{id}/instances/{instance}/sqlView`, then `DELETE` of the instance) and parsed like a dossier SQL view. Creating a report instance may execute the report against the warehouse: one execution per distinct report per run (a report shared by several documents is read once), subject to `warehouse_lineage_sql_timeout_seconds` and `max_consecutive_execution_timeouts`. Cube datasets have no report SQL view and are skipped. Requires `extract_warehouse_lineage`. Off by default, so documents keep their previous behaviour: datasets emitted without warehouse lineage.",
      "title": "Extract Document Dataset Sql Lineage",
      "type": "boolean"
    },
    "resolve_report_metrics_via_instance": {
      "default": false,
      "description": "Whether to read report Modeling definitions through a Modeling-service report instance (`POST /api/model/reports/{id}/instances`, passed in the `X-MSTR-MS-Instance` header), so derived metrics can be resolved; without it the definition is read statically and on some tenants carries no metric expressions. It also enables reading document-level derived metrics (defined in a dossier or document on top of a dataset) from the Modeling document definition, which is otherwise never consulted for them; that endpoint is not in Strategy's published API specification and has no instance route, so it is read statically. Off by default because it adds instance create and delete calls for every report-backed dataset.",
      "title": "Resolve Report Metrics Via Instance",
      "type": "boolean"
    },
    "warehouse_lineage_sql_timeout_seconds": {
      "default": 180,
      "description": "HTTP timeout in seconds for SQL-view APIs. These calls can be slower than metadata definition APIs because MicroStrategy must create and resolve a dashboard instance.",
      "exclusiveMinimum": 0,
      "title": "Warehouse Lineage Sql Timeout Seconds",
      "type": "integer"
    },
    "dashboard_instance_resolve_only": {
      "default": false,
      "description": "Whether to create dossier and document instances with `resolveOnly: true`, which Strategy documents as resolving the dashboard without executing it. Instances back visualization runtime details and SQL-view warehouse lineage; executing them runs the dashboard's queries against the warehouse and can take minutes per dashboard. Off by default until verified to return the same runtime grid and SQL view as an executed instance.",
      "title": "Dashboard Instance Resolve Only",
      "type": "boolean"
    },
    "max_consecutive_execution_timeouts": {
      "default": 3,
      "description": "Stop executing dashboards and reports in a project after this many consecutive instance creations time out (`warehouse_lineage_sql_timeout_seconds`). The remaining content in that project is still ingested, without visualization runtime details or SQL-view lineage. Bounds a run against a project of heavy content that never finishes executing. 0 disables the limit.",
      "minimum": 0,
      "title": "Max Consecutive Execution Timeouts",
      "type": "integer"
    },
    "emit_dashboard_dataset_edges": {
      "default": false,
      "description": "Emit DashboardInfo.datasetEdges as a fallback. Disabled by default because BI dashboards with many datasets make lineage views noisy. Governs dossiers only; see `emit_document_dataset_edges` for Report Services documents.",
      "title": "Emit Dashboard Dataset Edges",
      "type": "boolean"
    },
    "emit_document_dataset_edges": {
      "default": true,
      "description": "Whether to record a Report Services document's datasets as DashboardInfo.datasetEdges, so the document lists its datasets and lineage reads dataset -> document. The REST API exposes no visualizations for a document (its definition lists layouts as chapters without pages), so unlike a dossier it has no charts to carry dataset lineage; without these edges the document is an empty page. Independent of `emit_dashboard_dataset_edges`, which keeps governing dossiers.",
      "title": "Emit Document Dataset Edges",
      "type": "boolean"
    },
    "emit_chapter_page_structured_properties": {
      "default": false,
      "description": "Also record each dossier visualization chart's chapter and page as DataHub structured properties, so charts can be filtered by chapter and page in search and the placement is read by AI tools: `io.acryl.microstrategy.chapter` (MicroStrategy Chapter, text), `io.acryl.microstrategy.page` (MicroStrategy Page, text) and `io.acryl.microstrategy.pagePosition` (MicroStrategy Page Position, a number: the page's 1-based position within its chapter, the same value as the `microstrategyPageIndex` custom property). The `microstrategyChapter` and `microstrategyPage` custom properties are emitted regardless. The three property definitions (applicable to charts) are created in DataHub on the first run with this flag on and re-emitted unchanged after that; each run replaces the chart's structured property values with these. A visualization the dossier does not place on a page gets no values. Default is false.",
      "title": "Emit Chapter Page Structured Properties",
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
    "emit_semantic_model_entities": {
      "default": false,
      "description": "If true, additionally emit one `semanticModel` entity per project (logical datasets with subtype `Semantic Model Dataset`, one per MicroStrategy logical table, plus a `metric` entity per project-wide Base/Derived/Consolidated metric with lineage resolved from its expression). This is additive: existing report/dashboard/dataset emission is unaffected. Default is false. Requires a DataHub server that registers semanticModel/metric (Cloud >= 2.1.0, or OSS with `METRICS_ENABLED=true`) and `extract_model_lineage` access to the MicroStrategy modeling APIs.",
      "title": "Emit Semantic Model Entities",
      "type": "boolean"
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
    "dataset_field_order": {
      "default": "report",
      "description": "Order of the schema fields emitted for each dataset. `report` (the default) mirrors Strategy's own Datasets panel: the attributes first, then the metrics, each run sorted by the name the report displays, with report-derived metrics sorted in among the catalog ones. `alphabetical` sorts every field by name in a single run, the behaviour of earlier releases. Either way the DataHub schema tab re-sorts the fields alphabetically when the column header is clicked.",
      "enum": [
        "report",
        "alphabetical"
      ],
      "title": "Dataset Field Order",
      "type": "string"
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

Set `emit_dashboard_dataset_edges: true` if you want every dossier dataset to appear directly upstream of the dossier as fallback lineage.

Report Services documents are the exception. The document definition API returns a document's layouts as chapters without pages and no visualizations anywhere in the response, so a document is emitted as a dashboard with no charts and nothing to carry its dataset lineage. The connector therefore records a document's datasets directly as `DashboardInfo.datasetEdges` (`emit_document_dataset_edges`, enabled by default): the document page lists its datasets and lineage reads dataset -> document, continuing to warehouse tables when `extract_document_dataset_sql_lineage` is on. This is governed by `emit_document_dataset_edges` alone -- `emit_dashboard_dataset_edges` keeps governing dossiers, so a recipe that leaves the dossier fallback off still links its documents. The layout names are recorded on the document as the `microstrategyLayouts` (in definition order, separated by `; `) and `microstrategyLayoutCount` custom properties, and the ingestion report counts the edges in `document_dataset_edges`.

The definition APIs do not expose a visualization's dataset binding directly, so the connector resolves it in tiers. First it reads the modeling document API: derived metrics and attributes are scoped to a single dataset, so a visualization grid referencing them identifies its source dataset with certainty. For compound-grid visualizations, the connector then binds each column group (`columnSets`) to its backing dataset using the page name, group-name tokens, the group's member object IDs, and — when the page's datasets correspond one-to-one with the groups — elimination for the leftover pair; a partially bound grid keeps its resolved groups and counts the rest in the `column_sets_unbound` report counter. When neither applies, the connector falls back to inferring the binding from shared object references and name tokens. If that inference cannot exclude any dataset (for example, dashboards built from one cube per time period where all cubes share one object catalog), the visualization is treated as unresolved and gets no dataset inputs rather than an all-to-all fan-out; use `emit_dashboard_dataset_edges: true` to keep such dashboards connected to their datasets.

When `extract_visualization_details: true`, the connector creates a dashboard instance and calls the v2 visualization definition endpoint to resolve dataset-to-visualization lineage when the static dashboard definition does not include dataset IDs. Use `dashboard_pattern` to scope live validation runs, for example:

```yaml
dashboard_pattern:
  allow:
    - "^Quarterly Business Review$"
```

#### Reports

Set `extract_reports: true` to ingest MicroStrategy reports as DataHub chart entities with the `Report` subtype. Report extraction is disabled by default because report libraries can be much larger than curated dossiers. Use `report_pattern` (by name) or `folder_path_pattern` (by folder path) to scope report extraction.

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

Report Services documents (as opposed to dossiers) get no warehouse lineage from that path: the dossier SQL-view API answers a document instance with dataset views that carry no SQL, so the connector does not call it for documents. A document's datasets are grid reports, so set `extract_document_dataset_sql_lineage: true` (together with `extract_warehouse_lineage`) to read each report-backed dataset's SQL through a report instance instead -- `POST /api/v2/reports/{id}/instances`, `GET /api/v2/reports/{id}/instances/{instance}/sqlView`, then `DELETE` of the instance, the same sequence `extract_report_sql_lineage` uses -- and attach the parsed upstreams to the document-scoped dataset. Creating a report instance may execute the report against the warehouse, one execution per distinct report per run: a grid report shared by several documents is read once and its lineage attached to every document-scoped dataset that embeds it, and a failed read is not retried for the next document. `warehouse_lineage_sql_timeout_seconds` and `max_consecutive_execution_timeouts` apply. Cube datasets have no report SQL view and are skipped. The ingestion report counts `document_dataset_sql_views_scanned`, `document_dataset_sql_views_parsed`, `document_dataset_sql_views_reused`, `document_dataset_sql_view_failures` and `document_datasets_skipped_not_grid_report`. Disabled by default, so documents keep their previous behaviour.

The connector intentionally keeps direct `DashboardInfo.datasetEdges` disabled by default for dossiers so they do not draw edges directly to every dataset in DataHub lineage views; documents, which have no charts, get them by default (see Lineage Behavior).

#### Dependency and Model Lineage Enrichment

When `extract_dashboard_dependencies: true`, the connector uses MicroStrategy metadata search lineage APIs to record direct dashboard component dependency summaries, including dependency counts by MicroStrategy object type.

When `extract_metric_expressions: true`, the connector fetches accessible metric model definitions and stores expression token summaries in metric field `jsonProps`.

When `extract_model_lineage: true`, the connector probes modeling table APIs needed for logical table and physical source warehouse lineage. Missing privileges are reported as warnings and counters; the connector continues with dashboard, dataset, metric, and source warehouse metadata.

#### Semantic Model

Set `emit_semantic_model_entities: true` to additionally emit each project's schema as a first-class `semanticModel` entity, separate from the dashboard/report datasets described above. Attributes and Facts are project-wide MicroStrategy schema objects reusable across every report and cube in a project, so the project — not any one report — is the unit of a semantic model:

- One `semanticModel` entity per project.
- One logical dataset (subtype `Semantic Model Dataset`) per MicroStrategy logical table, with attributes emitted as `Dimension`-annotated fields and facts as `Measure`-annotated fields. Each logical dataset carries coarse upstream lineage to its physical warehouse table when a warehouse context is resolved for the project.
- One `metric` entity per project metric (Base, Derived/Compound, and Consolidated), with lineage resolved from the metric's own expression: a Base metric's `metricUpstreams` point at the logical dataset(s) backing the facts/attributes it reads; a Derived/Compound metric's `metricRelationships.derivedFrom` points at the `metric` entities it is computed from; a Consolidated metric resolves its consolidation's attribute references (one extra API call per distinct consolidation, cached across metrics that share one) and sets `metricUpstreams` from those.
- Relationships between logical datasets, derived from MicroStrategy's attribute hierarchy API: an attribute shared across more than one logical table triggers a lookup of that attribute's parent/child relationships, and a `semanticModelRelationship` is emitted when the two ends resolve to different tables (a hierarchy encoded entirely within one lookup table's columns has nothing to join, so no relationship is emitted for that case).

This is additive: it does not change the dashboard/report/dataset emission described elsewhere on this page, and is disabled by default. It requires `extract_model_lineage` access to the MicroStrategy modeling APIs (the same access `extract_model_lineage` already needs) and a DataHub server that registers `semanticModel`/`metric` (Cloud >= 2.1.0, or OSS with `METRICS_ENABLED=true`).

#### Subtypes

Entity subtypes follow MicroStrategy's own vocabulary, so a business user can filter DataHub by the kind of object they know. Dashboards are `Dossier` for a dossier (what Strategy now calls a dashboard) and `Document` for a Report Services document (object subtype 14081). Datasets are `Cube` for intelligent and super cubes (object subtypes 776 and 779) and `MicroStrategy Dataset` for a report used as a dataset; the distinction comes from the dataset's own object info lookup described under Folder Navigation, so a dataset whose lookup failed keeps `MicroStrategy Dataset`. Visualizations are `Visualization` and reports are `Report`.

#### Folder Navigation

The connector builds its browse hierarchy by walking each object's MicroStrategy folder ancestry, which reflects the metadata layer's internal folder names rather than the curated grouping Strategy Web's Library shows (favorites, "My Reports" vs. "Shared Reports", hidden system folders). With `use_predefined_folder_names: true` (the default), the connector resolves well-known folders via `GET /api/folders/preDefined` and reshapes the hierarchy to match Strategy Web: the folder Strategy Web calls "Shared Reports" (internally named "Reports") gets its MicroStrategy-assigned label everywhere — container identity (URN), `folder_pattern` matching, and display — and two system containers Strategy Web never shows are omitted entirely, with their children re-parented to the nearest kept ancestor: the project root folder (named after the project, which would render as a duplicate project level) and "Public Objects". If the predefined-folder lookup fails, the project's dashboards and reports are skipped and the run reports a failure, rather than filing them under raw-name containers with different URNs. Set `use_predefined_folder_names: false` to keep the raw metadata hierarchy (for example if you already ingested these folders and want to avoid the resulting container URN changes; see `docs/how/breaking-changes.md`).

Datasets embedded in a dossier (and report source datasets) are catalog objects in their own right -- reports or cubes with their own folder, which is frequently not the dossier's folder. The connector looks each dataset object up once per project (`GET /api/objects/{id}?type=3`) and places the dataset under its own folder ancestry, relabelled and pruned exactly like the dossier's path, so the breadcrumb matches where Strategy Web files the object. The dataset's "View in MicroStrategy" link opens the dataset object itself in Library when Library can render it (reports); intelligent and super cubes have no Library viewer, so they keep linking to the dossier or report that embeds them. If the object lookup fails, the dataset falls back to the dossier's folder and URL, keeps the `MicroStrategy Dataset` subtype, and the `dataset_object_lookup_failures` report counter is incremented.

Personal folders -- each user's profile folder tree under the project's "Profiles" system folder, holding that user's "My Reports", "My Objects" and similar folders -- are excluded by default (`include_personal_folders: false`). Dashboards, documents and reports filed there are skipped before any definition is fetched, so they cost no further API calls; the skips are counted in `personal_folder_objects_skipped` (with names sampled in `personal_folder_objects_skipped_samples`) and no folder container under the Profiles tree is emitted. A dataset that lives in a personal folder but is embedded by a public dossier or report is still ingested (it is part of that dossier's lineage) and is parented under the embedding dossier's folder rather than the personal one. With stateful ingestion, personal content ingested by an earlier run is soft-deleted on the next run like any other object that disappeared. Set `include_personal_folders: true` to ingest personal folders.

Personal content is identified by folder id where possible. `GET /api/folders/preDefined` with EnumDSSXMLFolderNames types 19 (the logged-in principal's profile folder) and 20 (its "My Reports") returns the principal's own folders; the profile folder's immediate parent -- read from `GET /api/objects/{id}?type=8` -- is the project's Profiles system folder, and any object whose ancestor chain passes through that folder is personal, whichever user owns it. This is one extra predefined-folder call and one object lookup per project (`personal_folder_roots_resolved` counts the projects where it succeeded). When that resolution fails (an older server or insufficient privilege) the connector reports a warning, and when the Profiles folder cannot be identified without an error (the principal has no profile folder, or its parent is a hidden system folder) it reports an info entry; either way it falls back to ancestor folder names: an ancestor named "Profiles" or "My Reports" (case-insensitive, plus the type-20 folder's label when it was returned) marks an object as personal. The fallback is exact on default-language installations but cannot recognise renamed or localized folders, and would also match a public folder that happens to carry one of those names.

Note that `folder_pattern` cannot exclude content: a denied folder re-parents its children to the nearest allowed ancestor, so a `deny: ["^My Reports$"]` rule removes the folder container but still ingests everything in it under the user's folder. To scope ingestion by location, use `folder_path_pattern` instead.

#### Scoping by Folder Path

Business users navigate Strategy Web by folder, and `folder_path_pattern` scopes ingestion the same way: each dashboard, document and report is matched against its folder path, which is the project name, then each folder from the project root down to the object's own folder, joined with `/`. The folder names are exactly the ones the browse path shows, built from the same ancestry and the same predefined-folder resolution: with `use_predefined_folder_names: true` the project root folder and "Public Objects" are omitted and "Shared Reports" carries its MicroStrategy-assigned label; with it `false`, the raw hierarchy is the path. `folder_pattern` plays no part in the path. For a dossier in project `Sales_Project` filed under `Shared Reports/Finance/Monthly Close`, the path is `Sales_Project/Shared Reports/Finance/Monthly Close`, so

```yaml
folder_path_pattern:
  allow:
    - "^Sales_Project/Shared Reports/Finance/Monthly Close(/|$)"
```

ingests that folder and everything beneath it, and nothing else. Patterns match from the start of the path, so the trailing `(/|$)` is what stops a sibling such as `Monthly Close Archive` from matching too; `deny` wins over `allow`.

Unlike `folder_pattern`, a denied path excludes the object. It is skipped right after the name patterns (`dashboard_pattern`, `report_pattern`), before any definition is fetched or instance executed, so it costs no further API calls, and its visualizations and datasets are not emitted. Reports are filtered on their own path, whether enumerated via `extract_independent_reports` or fetched because an ingested dashboard links to them; the dashboard's chart edge to a linked report is not affected by the report's folder, so a dashboard in an allowed folder can still reference a report chart that was not ingested. Skips are counted in `dashboards_filtered_by_folder_path` and `reports_filtered_by_folder_path`, with the object name and path sampled in `folder_path_filtered_samples`.

An object whose search result carries no folder ancestry has no path to match and is counted in `folder_paths_unresolved`. Under the default allow-all pattern it is ingested and a `Folder path unresolved` warning is recorded, so the catalog never silently shrinks; once a pattern is configured it is skipped and counted with the other skips, so the scope never silently widens.

#### Metric and Attribute Tags

MicroStrategy metrics and attributes are emitted as schema fields on the dashboard dataset/cube. Fields are listed the way Strategy's own Datasets panel lists them -- the attributes first, then the metrics, each run sorted by the name the report displays, with report-derived metrics sorted in among the catalog ones -- so a dataset reads the same way in both products; set `dataset_field_order: alphabetical` to sort every field by name in a single run instead (the DataHub schema tab re-sorts alphabetically on a column-header click either way). An attribute contributes one field per form, and the form name is appended (`District Number.DESC`) only where the attribute contributes more than one; a single-form attribute keeps its bare name. The connector attaches canonical DataHub tags to the fields:

- `urn:li:tag:Measure` for metrics.
- `urn:li:tag:Dimension` for attributes and attribute forms.
- `urn:li:tag:Temporal` for date/time attribute forms.
- `urn:li:tag:Derived` for visualization-local derived metrics (see below).

These tags are written to source-managed `SchemaMetadata` field metadata, not editable schema metadata.

#### Derived Metrics and Column Groups

Compound-grid dossier visualizations show **derived metrics** as grid columns marked `derived: true` (for example an inline percent-to-plan calculation). With `extract_derived_metrics: true` (the default), the connector surfaces each one as a schema field on the dataset backing its column group, tagged `Measure` + `Derived`. Derived metrics whose column group cannot be attributed to a dataset are counted in the `derived_metrics_unattached` report counter rather than silently dropped. The grid input comes from the runtime grids already fetched for lineage, so this requires `extract_lineage` and `extract_visualization_details`.

Where the derived metric is defined decides what the field carries. A derived metric is frequently a **report-level** object: the dossier dataset is a report, and the metric is one of that report's objects with a formula (what Strategy Web's Metric Editor shows on the report). For every report-backed dossier dataset the connector fetches the report definition -- `GET /api/model/reports/{id}` (Modeling service, 2021 Update 7 or later), falling back to `GET /api/v2/reports/{id}` -- and emits every derived metric the report defines, named as the report names it (a dossier grid may alias it differently; the chart field then shows the alias with the report name in `microstrategyObjectName`), with the formula in the field description and in the `microstrategyMetricExpressionText` / `microstrategyMetricExpressionTokens` jsonProps, and `microstrategyDerivedMetricSource: report`. A grid-derived metric the report also defines is upgraded in place (matched by object id, then by normalized name), never duplicated. Cubes cannot define derived metrics and are not consulted. With `extract_metric_formula_lineage: true`, `{Name}` and `[Name]` references in these formulas produce the same field-to-field lineage as catalog metric formulas. Only a derived metric that no report or document definition exposes is treated as visualization-local, with a description naming the column group and source visualization and `microstrategyDerivedMetricSource: visualization`. The `report_derived_metrics_extracted` and `report_definition_failures` counters track this; a report whose definition cannot be fetched keeps grid-level provenance for its derived metrics. The Modeling report definition marks report-level derived metrics with `isEmbedded` but carries no formula for them; with `extract_metric_expressions: true` the connector asks `GET /api/model/metrics/{id}` for each such metric (cached per metric, refusals aggregated under the metric-model warning and `failed_metric_model_ids`) and counts successes in `report_derived_metric_models_resolved`. When the v2 endpoint supplies the names, the derived metrics keep their report names but no formula, and the field description says the v2 definition supplies names only. That happens in two ways, counted separately: the Modeling call failed (`report_model_definition_failures` counts each report with the HTTP status in `report_model_definition_failure_samples`, and one warning per project names the first affected report), or the Modeling call answered but listed no derived metric definition the connector recognises (`report_model_definitions_empty`; with debug logging enabled the payload's key skeleton is logged once per project). When the Modeling endpoint answers but no derived metric carries an expression the connector can read, `report_definitions_without_expressions` is incremented and the field description says "Formula not present in the Modeling API report definition"; with debug logging enabled, lines prefixed `[mstr-derived-debug]` record the payload's key skeleton (keys, list lengths and value types only -- no names or values) so the actual response shape can be read from the execution log. This does not depend on `extract_reports`. It adds one or two API calls per distinct report-backed dataset per project.

Compound grids also organize their columns into named **column groups** (`columnSets`, e.g. one group per business domain), often repeating the same metric names per group with different logic behind each — the grouping is what tells a reader what they are looking at. The connector surfaces this two ways: the chart's `microstrategyColumnGroups` custom property records each group's member metrics and backing dataset, and grouped schema fields carry a `microstrategyColumnGroup` entry in their field `jsonProps`.

#### Chart Field Names and Order

When a runtime grid definition is available (`extract_visualization_details: true`), a visualization's chart fields are named and ordered the way the grid header reads. Row, column and page-by attributes come first, unqualified: a single displayed form is shown as just the attribute name (`Region Number`, not `Region Number.NUMBER`), and only the forms the grid displays become chart fields. Each column group's metrics follow as `GROUP.header`, in group order, where the header is the grid's own text -- when a dossier renames a metric, the alias is the field name and the catalog object name is retained in the field description and in the `microstrategyObjectName` jsonProps entry. Each header cell is attributed to one dataset: the column group's bound dataset or, for ungrouped attributes, the first input dataset in group order that carries the object. The chart field's `schemaFieldUrn` always keeps the dataset's real field path, so dataset schemas and column-level lineage are unaffected by the renaming. Objects a visualization references outside any grid header (and every object when no runtime grid was fetched) keep the previous behaviour: emitted from every input dataset and prefixed with the group or dataset name when more than one dataset feeds the chart.

#### Dossier Chapters and Pages

Every visualization chart records where it sits in its dossier as custom properties: `microstrategyChapter` and `microstrategyPage` (the chapter and page names), `microstrategyChapterKey` and `microstrategyPageKey` (their definition keys), and `microstrategyChapterIndex` and `microstrategyPageIndex` (1-based positions, so charts can be listed in reading order). Custom properties are indexed by DataHub search, so a business user can find every visualization on a given page by typing the page name as free text, or by searching the exact `microstrategyPage=<name>` term (likewise `microstrategyChapter=<name>`) to match only that property. The properties are omitted for a visualization the dossier definition does not place on a page.

Set `emit_chapter_page_structured_properties: true` to also record the placement as structured properties, which search can filter on (custom properties only match as text) and which the chart's Properties tab shows beside its other structured properties: `io.acryl.microstrategy.chapter` (MicroStrategy Chapter) and `io.acryl.microstrategy.page` (MicroStrategy Page), both text, and `io.acryl.microstrategy.pagePosition` (MicroStrategy Page Position), a number holding the page's 1-based position within its chapter -- the same value as `microstrategyPageIndex`. The three definitions apply to charts and are created in DataHub on the first run with the flag on, in that order so the chart lists its chapter above its page; later runs re-emit them unchanged. The ingestion report counts them in `chapter_page_structured_property_definitions_emitted` and the charts given values in `charts_with_chapter_page_structured_properties`. Each run replaces a chart's structured property values with these; a visualization the dossier does not place on a page gets none, and the custom properties above are emitted either way. Disabled by default.

#### Metric Formula Lineage

Set `extract_metric_formula_lineage: true` to parse `{Metric Name}` references out of catalog metric expressions (fetched via `extract_metric_expressions`) and emit field-to-field lineage from each metric to the sibling fields it references on the same dataset. This is best effort: references to objects that are not fields of the dataset (for example, a catalog metric not exposed on the cube) are counted in the `metric_formula_refs_unresolved` report counter and skipped. Disabled by default.

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
- Direct dossier-to-dataset edges are disabled by default; enable `emit_dashboard_dataset_edges` only if you want dashboard-level fallback lineage, which can make lineage views noisy for large dossiers. Report Services documents, which expose no visualizations, are linked to their datasets by default (`emit_document_dataset_edges`).
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

If little or no metadata is ingested, verify that the principal has Library API access and project-scoped metadata search, and check that `project_pattern` (and `dashboard_pattern` / `report_pattern` / `folder_path_pattern`, if set) are not filtering out the content you expect; `folder_path_filtered_samples` in the report shows the paths that were denied. Guest authentication works for public demo exploration but does not expose all modeling APIs.


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
