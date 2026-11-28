import threading
from dataclasses import dataclass, field
from typing import Optional

from typing_extensions import LiteralString

from datahub.ingestion.api.source import StructuredLogCategory
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.sql_parsing.schema_resolver import SchemaResolverReport
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.stats_collections import TopKDict

_SQLGLOT_WARNINGS_TOP_K = 10


@dataclass
class MicroStrategyReport(StaleEntityRemovalSourceReport):
    projects_scanned: int = 0
    folders_scanned: int = 0
    dashboards_scanned: int = 0
    reports_scanned: int = 0
    charts_scanned: int = 0
    datasets_scanned: int = 0
    source_warehouses_scanned: int = 0
    source_warehouse_api_failures: int = 0
    dashboard_dependencies_scanned: int = 0
    report_definition_api_failures: int = 0
    report_sql_view_api_failures: int = 0
    metric_expressions_scanned: int = 0
    metric_expression_api_failures: int = 0
    model_tables_scanned: int = 0
    model_lineage_api_failures: int = 0
    # Set only in projects with several source warehouses, where each logical
    # table takes its warehouse from its primary datasource; tables whose
    # datasource is missing or unmapped get no warehouse lineage.
    model_tables_resolved_by_datasource: int = 0
    model_tables_without_warehouse_context: int = 0
    model_lineage_edges: int = 0
    warehouse_lineage_sql_views_scanned: int = 0
    warehouse_lineage_edges: int = 0
    warehouse_lineage_api_failures: int = 0
    metric_fields_scanned: int = 0
    attribute_fields_scanned: int = 0
    temporal_fields_scanned: int = 0
    chart_lineage_edges: int = 0
    dashboard_dataset_edges: int = 0
    # Report Services documents' dataset edges (emit_document_dataset_edges),
    # counted apart from the opt-in dossier fallback above.
    document_dataset_edges: int = 0
    # emit_chapter_page_structured_properties: the chart property definitions
    # emitted this run (3 when the flag is on, 0 when off) and the
    # visualization charts given chapter/page values.
    chapter_page_structured_property_definitions_emitted: int = 0
    charts_with_chapter_page_structured_properties: int = 0
    sessions_reauthenticated: int = 0
    usage_rows_scanned: int = 0
    usage_buckets_emitted: int = 0
    usage_objects_unmatched: int = 0
    usage_dates_unparsed: int = 0
    unresolved_visualizations: int = 0
    visualizations_suppressed_ambiguous: int = 0
    visualizations_bound_by_derived_objects: int = 0
    visualizations_bound_by_column_sets: int = 0
    column_sets_unbound: int = 0
    derived_metric_fields_scanned: int = 0
    derived_metrics_unattached: int = 0
    report_derived_metrics_extracted: int = 0
    report_definition_failures: int = 0
    # Modeling report definition failed but the v2 definition answered: the
    # derived metrics keep their names but lose their formulas.
    report_model_definition_failures: int = 0
    report_model_definition_failure_samples: LossyList[str] = field(
        default_factory=LossyList
    )
    # Modeling report definition answered with derived metrics, none of which
    # carried an expression the connector could read.
    report_definitions_without_expressions: int = 0
    # Modeling report definition answered but the connector found no derived
    # metric definition in it at all, so the v2 definition supplied the names.
    report_model_definitions_empty: int = 0
    # Report-level derived metrics whose formula the report definition did not
    # carry but GET /api/model/metrics/{id} did.
    report_derived_metric_models_resolved: int = 0
    # Modeling-service report instances opened so the Modeling definition
    # could be read against them (resolve_report_metrics_via_instance).
    report_model_instances_created: int = 0
    report_model_instance_failures: int = 0
    report_model_instance_failure_samples: LossyList[str] = field(
        default_factory=LossyList
    )
    # Dashboard instances (executions) shared by more than one consumer, and
    # the execution-timeout limit: timeouts seen, executions skipped once the
    # limit tripped, and the projects it tripped in.
    dashboard_instances_reused: int = 0
    execution_timeouts: int = 0
    executions_skipped_after_timeouts: int = 0
    execution_timeout_limit_projects: LossyList[str] = field(default_factory=LossyList)
    # What the Modeling document definition yielded: document-level derived
    # metrics found, and how many of them carried a formula.
    document_model_definition_failures: int = 0
    document_derived_metric_definitions: int = 0
    document_derived_metric_expressions: int = 0
    # Report Services documents whose definition yielded no visualizations
    # (contents), so the dashboard was emitted with datasets only.
    document_definitions_without_visualizations: int = 0
    # Execution instances that could not be deleted after use.
    instance_cleanup_failures: int = 0
    metric_formula_lineage_edges: int = 0
    metric_formula_refs_unresolved: int = 0
    metric_formula_unresolved_ref_samples: LossyList[str] = field(
        default_factory=LossyList
    )
    warehouse_upstreams_pruned_by_field_evidence: int = 0
    predefined_folder_labels_resolved: int = 0
    # Personal (per-user profile) folder handling: how many projects resolved
    # the Profiles root by id (else name fallback), and the dashboards,
    # documents and reports skipped because they live under it.
    personal_folder_roots_resolved: int = 0
    personal_folder_objects_skipped: int = 0
    personal_folder_objects_skipped_samples: LossyList[str] = field(
        default_factory=LossyList
    )
    # folder_path_pattern: dashboards/documents and reports skipped because
    # their folder path did not match (name and path sampled), and objects
    # whose folder path could not be resolved because their search result
    # carried no folder ancestry.
    dashboards_filtered_by_folder_path: int = 0
    reports_filtered_by_folder_path: int = 0
    folder_path_filtered_samples: LossyList[str] = field(default_factory=LossyList)
    folder_paths_unresolved: int = 0
    dataset_object_lookups: int = 0
    dataset_object_lookup_failures: int = 0
    api_errors: int = 0
    malformed_objects_skipped: LossyList[str] = field(default_factory=LossyList)
    filtered_projects: LossyList[str] = field(default_factory=LossyList)
    filtered_dashboards: LossyList[str] = field(default_factory=LossyList)
    filtered_reports: LossyList[str] = field(default_factory=LossyList)
    sql_parse_failures: LossyList[str] = field(default_factory=LossyList)
    sql_parse_failure_count: int = 0
    sql_views_parsed: int = 0
    sql_view_rows_unmatched: int = 0
    sql_view_rows_without_context: int = 0
    sql_views_without_statement: int = 0
    # Report Services document datasets read through report instances
    # (extract_document_dataset_sql_lineage): datasets given a SQL view, those
    # whose view parsed into at least one warehouse upstream, datasets served
    # from the per-run cache because another document already read the same
    # report (counted in scanned/parsed too), reports whose instance or SQL
    # view failed (once per report, not per document), and datasets skipped
    # because they are not grid reports (cubes, unknown subtypes, failed
    # object lookups).
    document_dataset_sql_views_scanned: int = 0
    document_dataset_sql_views_parsed: int = 0
    document_dataset_sql_views_reused: int = 0
    document_dataset_sql_view_failures: int = 0
    document_datasets_skipped_not_grid_report: int = 0
    reports_skipped_not_dashboard_linked: int = 0
    # per_report: extra sightings of a report already counted, and datasets emitted.
    reports_deduplicated: int = 0
    report_datasets_emitted: int = 0
    # skip_unchanged_objects; an errored change check reads the object instead.
    dashboards_skipped_unchanged: int = 0
    reports_skipped_unchanged: int = 0
    report_datasets_skipped_unchanged: int = 0
    change_check_failures: int = 0
    change_check_failure_samples: LossyList[str] = field(default_factory=LossyList)
    failed_metric_model_ids: LossyList[str] = field(default_factory=LossyList)
    metrics_kept_after_model_failure: int = 0
    metrics_skipped_after_model_failure: int = 0
    metric_presence_check_failures: int = 0
    semantic_models_emitted: int = 0
    semantic_model_datasets_emitted: int = 0
    semantic_model_metrics_emitted: int = 0
    semantic_model_relationships_emitted: int = 0
    semantic_model_attribute_relationship_api_failures: int = 0
    semantic_model_consolidation_api_failures: int = 0
    semantic_model_metric_search_api_failures: int = 0
    sql_parses_executed: int = 0
    sql_parse_cache_hits: int = 0
    # Reuses of a failed parse. The failure itself is counted once per distinct SQL.
    sql_parse_failure_cache_hits: int = 0
    sql_statements_parsed: int = 0
    report_sql_fetched: int = 0
    schema_resolver: SchemaResolverReport = field(default_factory=SchemaResolverReport)
    cache_project_resets: int = 0
    sqlglot_warnings_suppressed: TopKDict[str, int] = field(
        default_factory=lambda: TopKDict(int, top_k=_SQLGLOT_WARNINGS_TOP_K)
    )

    def __post_init__(self) -> None:
        self._lock = threading.RLock()
        super().__post_init__()

    def warning(
        self,
        message: LiteralString,
        context: Optional[str] = None,
        title: Optional[LiteralString] = None,
        exc: Optional[BaseException] = None,
        log: bool = True,
        log_category: Optional[StructuredLogCategory] = None,
    ) -> None:
        with self._lock:
            super().warning(
                message=message,
                context=context,
                title=title,
                exc=exc,
                log=log,
                log_category=log_category,
            )

    def failure(
        self,
        message: LiteralString,
        context: Optional[str] = None,
        title: Optional[LiteralString] = None,
        exc: Optional[BaseException] = None,
        log: bool = True,
        log_category: Optional[StructuredLogCategory] = None,
    ) -> None:
        with self._lock:
            super().failure(
                message=message,
                context=context,
                title=title,
                exc=exc,
                log=log,
                log_category=log_category,
            )

    def info(
        self,
        message: LiteralString,
        context: Optional[str] = None,
        title: Optional[LiteralString] = None,
        exc: Optional[BaseException] = None,
        log: bool = True,
        log_category: Optional[StructuredLogCategory] = None,
    ) -> None:
        with self._lock:
            super().info(
                message=message,
                context=context,
                title=title,
                exc=exc,
                log=log,
                log_category=log_category,
            )

    def report_project_scanned(self) -> None:
        with self._lock:
            self.projects_scanned += 1

    def report_folder_scanned(self) -> None:
        with self._lock:
            self.folders_scanned += 1

    def report_dashboard_scanned(self) -> None:
        with self._lock:
            self.dashboards_scanned += 1

    def report_report_scanned(self) -> None:
        with self._lock:
            self.reports_scanned += 1

    def report_chart_scanned(self) -> None:
        with self._lock:
            self.charts_scanned += 1

    def report_dataset_scanned(self) -> None:
        with self._lock:
            self.datasets_scanned += 1

    def report_source_warehouses_scanned(self, count: int) -> None:
        with self._lock:
            self.source_warehouses_scanned += count

    def report_source_warehouse_api_failure(self) -> None:
        with self._lock:
            self.source_warehouse_api_failures += 1

    def report_dashboard_dependencies_scanned(self, count: int) -> None:
        with self._lock:
            self.dashboard_dependencies_scanned += count

    def report_report_definition_api_failure(self) -> None:
        with self._lock:
            self.report_definition_api_failures += 1

    def report_report_sql_view_api_failure(self) -> None:
        with self._lock:
            self.report_sql_view_api_failures += 1

    def report_metric_expression_scanned(self) -> None:
        with self._lock:
            self.metric_expressions_scanned += 1

    def report_metric_expression_api_failure(self) -> None:
        with self._lock:
            self.metric_expression_api_failures += 1

    def report_model_tables_scanned(self, count: int) -> None:
        with self._lock:
            self.model_tables_scanned += count

    def report_model_table_resolved_by_datasource(self) -> None:
        with self._lock:
            self.model_tables_resolved_by_datasource += 1

    def report_model_table_without_warehouse_context(self) -> None:
        with self._lock:
            self.model_tables_without_warehouse_context += 1

    def report_model_lineage_api_failure(self) -> None:
        with self._lock:
            self.model_lineage_api_failures += 1

    def report_model_lineage_edges(self, count: int) -> None:
        with self._lock:
            self.model_lineage_edges += count

    def report_warehouse_lineage_sql_views_scanned(self, count: int) -> None:
        with self._lock:
            self.warehouse_lineage_sql_views_scanned += count

    def report_warehouse_lineage_edges(self, count: int) -> None:
        with self._lock:
            self.warehouse_lineage_edges += count

    def report_warehouse_lineage_api_failure(self) -> None:
        with self._lock:
            self.warehouse_lineage_api_failures += 1

    def report_metric_field(self) -> None:
        with self._lock:
            self.metric_fields_scanned += 1

    def report_attribute_field(self, temporal: bool = False) -> None:
        with self._lock:
            self.attribute_fields_scanned += 1
            if temporal:
                self.temporal_fields_scanned += 1

    def report_chart_lineage_edges(self, count: int) -> None:
        with self._lock:
            self.chart_lineage_edges += count

    def report_dashboard_dataset_edges(self, count: int) -> None:
        with self._lock:
            self.dashboard_dataset_edges += count

    def report_document_dataset_edges(self, count: int) -> None:
        with self._lock:
            self.document_dataset_edges += count

    def report_chapter_page_structured_property_definition_emitted(self) -> None:
        with self._lock:
            self.chapter_page_structured_property_definitions_emitted += 1

    def report_chart_with_chapter_page_structured_properties(self) -> None:
        with self._lock:
            self.charts_with_chapter_page_structured_properties += 1

    def report_unresolved_visualization(self) -> None:
        with self._lock:
            self.unresolved_visualizations += 1

    def report_visualizations_bound_by_derived_objects(self, count: int) -> None:
        with self._lock:
            self.visualizations_bound_by_derived_objects += count

    def report_visualization_bound_by_column_sets(self) -> None:
        with self._lock:
            self.visualizations_bound_by_column_sets += 1

    def report_column_sets_unbound(self, count: int) -> None:
        with self._lock:
            self.column_sets_unbound += count

    def report_derived_metric_field(self) -> None:
        with self._lock:
            self.derived_metric_fields_scanned += 1

    def report_derived_metric_unattached(self) -> None:
        with self._lock:
            self.derived_metrics_unattached += 1

    def report_report_derived_metrics_extracted(self, count: int) -> None:
        with self._lock:
            self.report_derived_metrics_extracted += count

    def report_report_definition_failure(self) -> None:
        with self._lock:
            self.report_definition_failures += 1

    def report_report_model_definition_failure(self, context: str) -> None:
        with self._lock:
            self.report_model_definition_failures += 1
            self.report_model_definition_failure_samples.append(context)

    def report_report_definition_without_expressions(self) -> None:
        with self._lock:
            self.report_definitions_without_expressions += 1

    def report_report_model_definition_empty(self) -> None:
        with self._lock:
            self.report_model_definitions_empty += 1

    def report_report_derived_metric_model_resolved(self) -> None:
        with self._lock:
            self.report_derived_metric_models_resolved += 1

    def report_report_model_instance_created(self) -> None:
        with self._lock:
            self.report_model_instances_created += 1

    def report_dashboard_instance_reused(self) -> None:
        with self._lock:
            self.dashboard_instances_reused += 1

    def report_execution_timeout(self) -> None:
        with self._lock:
            self.execution_timeouts += 1

    def report_execution_skipped_after_timeouts(self) -> None:
        with self._lock:
            self.executions_skipped_after_timeouts += 1

    def report_execution_timeout_limit_reached(self, project_id: str) -> None:
        with self._lock:
            self.execution_timeout_limit_projects.append(project_id)

    def report_report_model_instance_failure(self, context: str) -> None:
        with self._lock:
            self.report_model_instance_failures += 1
            self.report_model_instance_failure_samples.append(context)

    def report_document_model_definition_failure(self) -> None:
        with self._lock:
            self.document_model_definition_failures += 1

    def report_instance_cleanup_failure(self) -> None:
        with self._lock:
            self.instance_cleanup_failures += 1

    def report_document_definition_without_visualizations(self) -> None:
        with self._lock:
            self.document_definitions_without_visualizations += 1

    def report_document_derived_metric_definitions(
        self, found: int, with_expression: int
    ) -> None:
        with self._lock:
            self.document_derived_metric_definitions += found
            self.document_derived_metric_expressions += with_expression

    def report_metric_formula_lineage_edges(self, count: int) -> None:
        with self._lock:
            self.metric_formula_lineage_edges += count

    def report_metric_formula_refs_unresolved(self, count: int) -> None:
        with self._lock:
            self.metric_formula_refs_unresolved += count

    def report_metric_formula_unresolved_ref(self, context: str) -> None:
        with self._lock:
            self.metric_formula_unresolved_ref_samples.append(context)

    def report_visualization_suppressed_ambiguous(self) -> None:
        with self._lock:
            self.visualizations_suppressed_ambiguous += 1

    def report_session_reauthenticated(self) -> None:
        with self._lock:
            self.sessions_reauthenticated += 1

    def report_usage_rows_scanned(self, count: int) -> None:
        with self._lock:
            self.usage_rows_scanned += count

    def report_usage_bucket_emitted(self) -> None:
        with self._lock:
            self.usage_buckets_emitted += 1

    def report_usage_object_unmatched(self) -> None:
        with self._lock:
            self.usage_objects_unmatched += 1

    def report_usage_date_unparsed(self) -> None:
        with self._lock:
            self.usage_dates_unparsed += 1

    def report_warehouse_upstreams_pruned(self, count: int) -> None:
        with self._lock:
            self.warehouse_upstreams_pruned_by_field_evidence += count

    def report_predefined_folder_labels_resolved(self, count: int) -> None:
        with self._lock:
            self.predefined_folder_labels_resolved += count

    def report_personal_folder_root_resolved(self) -> None:
        with self._lock:
            self.personal_folder_roots_resolved += 1

    def report_personal_folder_object_skipped(self, context: str) -> None:
        with self._lock:
            self.personal_folder_objects_skipped += 1
            self.personal_folder_objects_skipped_samples.append(context)

    def report_dashboard_filtered_by_folder_path(self, context: str) -> None:
        with self._lock:
            self.dashboards_filtered_by_folder_path += 1
            self.folder_path_filtered_samples.append(context)

    def report_report_filtered_by_folder_path(self, context: str) -> None:
        with self._lock:
            self.reports_filtered_by_folder_path += 1
            self.folder_path_filtered_samples.append(context)

    def report_folder_path_unresolved(self) -> None:
        with self._lock:
            self.folder_paths_unresolved += 1

    def report_dataset_object_lookup(self) -> None:
        with self._lock:
            self.dataset_object_lookups += 1

    def report_dataset_object_lookup_failure(self) -> None:
        with self._lock:
            self.dataset_object_lookup_failures += 1

    def report_api_error(self) -> None:
        with self._lock:
            self.api_errors += 1

    def report_malformed_object(self, context: str) -> None:
        with self._lock:
            self.malformed_objects_skipped.append(context)

    def report_sql_parse_failure(self, context: str) -> None:
        with self._lock:
            self.sql_parse_failures.append(context)
            self.sql_parse_failure_count += 1

    def report_sql_view_parsed(self) -> None:
        with self._lock:
            self.sql_views_parsed += 1

    def report_sql_view_row_unmatched(self) -> None:
        with self._lock:
            self.sql_view_rows_unmatched += 1

    def report_sql_view_row_without_context(self) -> None:
        with self._lock:
            self.sql_view_rows_without_context += 1

    def report_sql_view_without_statement(self) -> None:
        with self._lock:
            self.sql_views_without_statement += 1

    def report_document_dataset_sql_view_scanned(self) -> None:
        with self._lock:
            self.document_dataset_sql_views_scanned += 1

    def report_document_dataset_sql_view_parsed(self) -> None:
        with self._lock:
            self.document_dataset_sql_views_parsed += 1

    def report_document_dataset_sql_view_reused(self) -> None:
        with self._lock:
            self.document_dataset_sql_views_reused += 1

    def report_document_dataset_sql_view_failure(self) -> None:
        with self._lock:
            self.document_dataset_sql_view_failures += 1

    def report_document_dataset_skipped_not_grid_report(self) -> None:
        with self._lock:
            self.document_datasets_skipped_not_grid_report += 1

    def report_reports_skipped_not_dashboard_linked(self) -> None:
        with self._lock:
            self.reports_skipped_not_dashboard_linked += 1

    def report_report_deduplicated(self) -> None:
        with self._lock:
            self.reports_deduplicated += 1

    def report_report_dataset_emitted(self) -> None:
        with self._lock:
            self.report_datasets_emitted += 1

    def report_failed_metric_model(self, metric_id: str) -> None:
        with self._lock:
            self.failed_metric_model_ids.append(metric_id)

    def report_metric_kept_after_model_failure(self) -> None:
        with self._lock:
            self.metrics_kept_after_model_failure += 1

    def report_metric_skipped_after_model_failure(self) -> None:
        with self._lock:
            self.metrics_skipped_after_model_failure += 1

    def report_metric_presence_check_failure(self) -> None:
        with self._lock:
            self.metric_presence_check_failures += 1

    def report_semantic_model_emitted(self) -> None:
        with self._lock:
            self.semantic_models_emitted += 1

    def report_semantic_model_datasets_emitted(self, count: int) -> None:
        with self._lock:
            self.semantic_model_datasets_emitted += count

    def report_semantic_model_metrics_emitted(self, count: int) -> None:
        with self._lock:
            self.semantic_model_metrics_emitted += count

    def report_semantic_model_relationships_emitted(self, count: int) -> None:
        with self._lock:
            self.semantic_model_relationships_emitted += count

    def report_semantic_model_attribute_relationship_api_failure(self) -> None:
        with self._lock:
            self.semantic_model_attribute_relationship_api_failures += 1

    def report_semantic_model_consolidation_api_failure(self) -> None:
        with self._lock:
            self.semantic_model_consolidation_api_failures += 1

    def report_semantic_model_metric_search_api_failure(self) -> None:
        with self._lock:
            self.semantic_model_metric_search_api_failures += 1

    def report_sql_parse_executed(self, statements: int) -> None:
        with self._lock:
            self.sql_parses_executed += 1
            self.sql_statements_parsed += statements

    def report_sql_parse_cache_hit(self, failed: bool) -> None:
        with self._lock:
            self.sql_parse_cache_hits += 1
            if failed:
                self.sql_parse_failure_cache_hits += 1

    def report_report_sql_fetched(self) -> None:
        with self._lock:
            self.report_sql_fetched += 1

    def report_filtered_project(self, name: str) -> None:
        with self._lock:
            self.filtered_projects.append(name)

    def report_filtered_dashboard(self, name: str) -> None:
        with self._lock:
            self.filtered_dashboards.append(name)

    def report_filtered_report(self, name: str) -> None:
        with self._lock:
            self.filtered_reports.append(name)

    def report_sqlglot_warning_suppressed(self, message: str) -> None:
        with self._lock:
            self.sqlglot_warnings_suppressed[message] += 1

    def report_cache_project_reset(self) -> None:
        with self._lock:
            self.cache_project_resets += 1

    def report_change_check_failure(self, failure: str) -> None:
        with self._lock:
            self.change_check_failures += 1
            self.change_check_failure_samples.append(failure)

    def report_dashboards_skipped_unchanged(self, delta: int = 1) -> None:
        with self._lock:
            self.dashboards_skipped_unchanged += delta

    def report_report_skipped_unchanged(self) -> None:
        with self._lock:
            self.reports_skipped_unchanged += 1

    def report_report_dataset_skipped_unchanged(self) -> None:
        with self._lock:
            self.report_datasets_skipped_unchanged += 1
