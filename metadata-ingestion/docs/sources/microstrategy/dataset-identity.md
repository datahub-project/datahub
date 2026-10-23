# MicroStrategy dataset identity

The `dataset_identity` setting decides what one DataHub dataset entity stands for in the MicroStrategy connector. The default, `per_dashboard`, is the historical behaviour and is unchanged; `per_report` is opt-in. Both are described here so a reviewer can see exactly where they diverge.

## The problem

A MicroStrategy report (object type 3: grid report subtype 768, intelligent cube 776, super cube 779) is a catalog object in its own right, filed in its own folder. Dossiers and Report Services documents embed it as a dataset. Under `per_dashboard` the dataset entity is keyed by the dashboard that embeds it, so a report used by N dashboards becomes N dataset entities with the same name and the same schema. On one tenant a single report ("DCS - Dataset") existed 18 times. With `extract_reports: true` the same report is also emitted as a chart (subtype `Report`) plus a report-scoped source dataset, which doubles it again. Users do not navigate by "dataset or dossier"; they look for the report and its lineage.

## Scheme `per_dashboard` (default)

- Dashboard dataset URN name: `<project id>.<dashboard id>.<dataset id>`; `qualifiedName` is the same string. One entity per (dashboard, dataset) pair.
- With `extract_reports`: a chart `<project id>.<report id>` (subtype `Report`) plus a dataset `<project id>.<report id>.<source id or "source">` holding the report's objects.
- Charts (`ChartInfo.inputs`), document `datasetEdges` and dossier `datasetEdges` all point at the dashboard-scoped dataset; a dashboard that depends on a report links to the report chart through `chartEdges`.
- Warehouse lineage is attached per dashboard-scoped dataset, so a shared report's SQL is parsed once per dashboard.

## Scheme `per_report`

- Dataset URN name: `<project id>.<report id>`, where the report id is the dataset's own MicroStrategy object id (the platform instance and env are applied exactly as before). `qualifiedName` is the same string. Exactly one entity per report per project, however many dashboards use it, and whether or not `extract_reports` is on.
- Name: the report's name as the dashboard definition carries it (which is the report object's name), or the report object's name when the report pass sees it first. Subtype as before: `Cube` for subtypes 776/779, `MicroStrategy Dataset` otherwise.
- Browse path and container: the report's own folder ancestry, resolved from the same object lookup (`GET /api/objects/{id}?type=3`) and predefined-folder relabelling the connector already uses for dataset folders. When that ancestry is unavailable the dataset falls back to the first dashboard's folder, or the project container for a report seen only by the report pass.
- Schema: `availableObjects` from the report's own definition when the report pass fetched it (`extract_reports` with `extract_report_definitions`); otherwise the first dashboard-dataset definition seen for the report. Derived metrics, metric expressions and column groups accumulate on the one entity from every dashboard that uses it.
- Warehouse lineage is computed once per report: the first SQL view that parses to at least one upstream table (a dossier SQL view, a document's report instance read through `_report_sql_cache`, or the report pass's `extract_report_sql_lineage` read) is attached; later sightings of the same report neither create a report instance (an execution) nor re-parse. The report instance SQL cache is shared with `per_dashboard` and is unchanged.
- Ownership: the report object's owner is emitted as the dataset's technical owner, since the `Report` chart that used to carry it is not emitted (dashboard-scoped datasets have never carried ownership).
- What links to it: every dossier visualization that reads the report (`ChartInfo.inputs` / `inputEdges` / `inputFields`), every document's `datasetEdges`, and dossier `datasetEdges` when `emit_dashboard_dataset_edges` is on. The lineage therefore reads `warehouse table -> report dataset -> every chart and dashboard that uses it`, and the dataset page lists every downstream dashboard.
- Custom properties record `microstrategyReportId`, `microstrategyDashboardCount` and `microstrategyDashboardIds` (the dashboards that use the report, in the order seen), plus the report definition's prompt count, filter flag and source when the report pass fetched it.
- Emission happens once per project after the dashboard and report passes, so the entity carries everything learned about the report in that run. Edges emitted earlier in the run point at the URN that is emitted later; DataHub does not require the target to exist first.

## `extract_reports` under `per_report`

The chart (subtype `Report`) and the report-scoped source dataset are not emitted. Decision: suppress rather than keep the chart for usage. The chart was the second copy users complained about, and keeping it only for `ChartUsageStatistics` would keep the duplicate for the one aspect business users rarely open. Report usage is emitted instead as `DatasetUsageStatistics` on the report dataset (`totalSqlQueries` carries the Platform Analytics execution count, `userCounts` the per-user counts), keyed by the same object GUID, for every report dataset emitted, not only those the report pass saw.

An independent report (no dashboard uses it, `extract_independent_reports: true`) is the same single dataset entity, with its schema from its definition and its own folder, and no chart. A report both passes see is one entity: the dashboard pass creates it, the report pass upgrades its schema and properties. The dashboard-to-report `chartEdges` that `per_dashboard` emits from dependencies have no target under `per_report`; the dashboard's visualizations and dataset edges carry the link instead.

## Migration

Switching schemes changes every dataset URN, so the first run after switching emits a new set of dataset entities and, with stateful ingestion, soft-deletes the old ones as stale. Steps for a tenant on `per_dashboard` today: (1) set `dataset_identity: per_report`; (2) raise `stateful_ingestion.fail_safe_threshold` for that run, because the number of entities that disappear exceeds the default percentage whenever datasets are a sizeable share of the ingested entities (every dataset URN changes, plus every report chart when `extract_reports` is on); (3) run; (4) restore the threshold. Without stateful ingestion the old entities remain and must be deleted by hand. Editable metadata (descriptions, owners, tags, terms) on the old dataset entities is not carried over.

## Counters

`reports_deduplicated` counts every additional sighting of a report that was already registered in the project (18 dashboards using one report count 17). `report_datasets_emitted` counts the dataset entities emitted under `per_report`.

## Limitations

- A report whose definition cannot be fetched keeps the first dashboard-dataset definition as its schema; a report seen only by the report pass with a failed definition fetch is emitted with an empty schema, as the report-scoped dataset would have been.
- Prompted reports: a report instance may fail to execute for SQL (HTTP 406), so such a report gets no warehouse lineage from the report pass or the document path, exactly as before; a dossier's SQL view can still supply it.
- Cubes have no report SQL view; their warehouse lineage comes only from dossier SQL views, as before.
- A dossier's SQL view reflects the dossier's own view filters, so the lineage attached is that of the first dossier that parsed; a second dossier joining extra tables for a filter is not merged in.
- A dashboard dependency on a report that is not among the dashboard definition's datasets is not linked under `per_report` (under `per_dashboard` it linked to the report chart).
- The report's source cube (`dataSource` in the report definition) is recorded as a property, not emitted as a separate upstream dataset.
