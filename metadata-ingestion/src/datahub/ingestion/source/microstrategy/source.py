import logging
from typing import Any, Dict, Iterable, List, Optional, Sequence

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    SourceReport,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.microstrategy.client import (
    MicroStrategyAPIError,
    MicroStrategyClient,
)
from datahub.ingestion.source.microstrategy.config import MicroStrategyConfig
from datahub.ingestion.source.microstrategy.lineage import (
    ModelLineageIndex,
    WarehouseLineageContext,
    matching_datasource_for_context,
    metric_fact_ids_from_model,
    metric_metric_ids_from_model,
    sql_statement_from_sql_view_entry,
    sql_view_dataset_key,
    sql_view_dataset_name,
    warehouse_context_from_datasource,
    warehouse_context_from_datasources,
    warehouse_context_with_connection,
)
from datahub.ingestion.source.microstrategy.mapper import MicroStrategyMapper
from datahub.ingestion.source.microstrategy.models import (
    DashboardDefinition,
    DatasetObject,
    Datasource,
    MSTRObject,
    ProjectKey,
    ReportDefinition,
    Visualization,
)
from datahub.ingestion.source.microstrategy.report import MicroStrategyReport
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)

logger = logging.getLogger(__name__)


@platform_name("MicroStrategy")
@config_class(MicroStrategyConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.CONTAINERS, "Projects and folders emit as containers")
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(SourceCapability.TAGS, "Metric, attribute, and temporal field tags")
@capability(SourceCapability.OWNERSHIP, "Enabled by default via `ingest_owner`")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Visualization and report inputs when resolvable",
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Modeling API field lineage from MicroStrategy metrics and attributes "
    "to source warehouse fields",
)
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
class MicroStrategySource(StatefulIngestionSourceBase, TestableSource):
    config: MicroStrategyConfig
    report: MicroStrategyReport
    platform: str = "microstrategy"

    def __init__(self, config: MicroStrategyConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = MicroStrategyReport()
        self.client = MicroStrategyClient(config, self.report)
        self.mapper = MicroStrategyMapper(config, self.report)
        self._metric_model_cache: Dict[str, Dict[str, Any]] = {}

    @classmethod
    def create(
        cls,
        config_dict: Dict[str, object],
        ctx: PipelineContext,
    ) -> "MicroStrategySource":
        config = MicroStrategyConfig.model_validate(config_dict)
        return cls(config, ctx)

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        report = MicroStrategyReport()
        client = MicroStrategyClient(
            MicroStrategyConfig.model_validate(config_dict),
            report,
        )
        try:
            client.login()
            client.list_projects()
            test_report.basic_connectivity = CapabilityReport(capable=True)
        except Exception as error:
            test_report.basic_connectivity = CapabilityReport(
                capable=False,
                failure_reason=str(error),
            )
        finally:
            client.close()
        return test_report

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        self.client.login()
        try:
            for project in self.client.list_projects():
                if not self.config.project_pattern.allowed(project.name):
                    continue
                source_warehouses = self._get_project_source_warehouses(project.id)
                warehouse_context = self._get_project_warehouse_context(
                    project.id,
                    source_warehouses,
                )
                model_lineage_index = self._get_project_model_lineage_index(
                    project.id,
                    warehouse_context,
                )
                yield from self.mapper.gen_project_container(
                    project,
                    source_warehouses=source_warehouses,
                )
                if self.config.extract_dashboards:
                    yield from self._process_project_dashboards(
                        project.id,
                        warehouse_context,
                        model_lineage_index,
                    )
                if self.config.extract_reports:
                    yield from self._process_project_reports(
                        project.id,
                        warehouse_context,
                        model_lineage_index,
                    )
        finally:
            self.client.close()

    def _get_project_source_warehouses(self, project_id: str) -> List[Datasource]:
        if not self.config.extract_source_warehouses:
            return []
        try:
            source_warehouses = self.client.list_project_datasources(project_id)
        except MicroStrategyAPIError as error:
            try:
                source_warehouses = self.client.list_datasources(project_id)
            except MicroStrategyAPIError as fallback_error:
                self.report.report_source_warehouse_api_failure()
                self.report.warning(
                    title="Failed to Fetch Source Warehouses",
                    message=(
                        "Skipping project datasource inventory. Dashboard extraction "
                        "will continue without source warehouse summary properties."
                    ),
                    context=f"project_id={project_id}",
                    exc=fallback_error,
                )
                return []
            self.report.warning(
                title="Failed to Fetch Project Source Warehouses",
                message=(
                    "Falling back to the broader datasource inventory endpoint. "
                    "Warehouse lineage context may be less precise."
                ),
                context=f"project_id={project_id}",
                exc=error,
            )

        self.report.report_source_warehouses_scanned(len(source_warehouses))
        return source_warehouses

    def _get_project_warehouse_context(
        self,
        project_id: str,
        source_warehouses: List[Datasource],
    ) -> Optional[WarehouseLineageContext]:
        if not self.config.extract_lineage:
            return None
        if not (
            self.config.extract_warehouse_lineage
            or self.config.extract_model_lineage
            or self.config.extract_report_sql_lineage
        ):
            return None

        context = warehouse_context_from_datasources(source_warehouses, self.config.env)
        if not context:
            return None

        datasource = matching_datasource_for_context(source_warehouses, context)
        if datasource and datasource.connection_id:
            try:
                connection = self.client.get_datasource_connection(
                    datasource.connection_id,
                    project_id=project_id,
                )
            except MicroStrategyAPIError:
                self.report.report_warehouse_lineage_api_failure()
            else:
                context = warehouse_context_with_connection(context, connection)
        return context

    def _process_project_dashboards(
        self,
        project_id: str,
        warehouse_context: Optional[WarehouseLineageContext],
        model_lineage_index: Optional[ModelLineageIndex],
    ) -> Iterable[MetadataWorkUnit]:
        for dashboard_object in self.client.search_dashboards(project_id):
            if not self.config.dashboard_pattern.allowed(dashboard_object.name):
                continue
            yield from self.mapper.gen_folder_containers(project_id, dashboard_object)
            parent_key = self.mapper.folder_container_for_dashboard(
                project_id, dashboard_object
            )
            dashboard = self._get_dashboard_definition(project_id, dashboard_object)
            if dashboard is None:
                continue
            needs_sql_view_context = (
                self.config.extract_warehouse_lineage or model_lineage_index is not None
            )
            has_warehouse_context = warehouse_context or any(
                dataset.source_warehouse for dataset in dashboard.datasets
            )
            if needs_sql_view_context and has_warehouse_context:
                self._enrich_warehouse_lineage(
                    project_id=project_id,
                    dashboard_object=dashboard_object,
                    dashboard=dashboard,
                    context=warehouse_context,
                )
            if model_lineage_index:
                self.mapper.attach_model_lineage(dashboard, model_lineage_index)
            extra_chart_urns = self._dashboard_report_chart_urns(project_id, dashboard)
            yield from self._process_dashboard(
                project_id,
                dashboard_object,
                dashboard,
                parent_key,
                extra_chart_urns=extra_chart_urns,
            )

    def _process_project_reports(
        self,
        project_id: str,
        warehouse_context: Optional[WarehouseLineageContext],
        model_lineage_index: Optional[ModelLineageIndex],
    ) -> Iterable[MetadataWorkUnit]:
        for report_object in self.client.search_reports(project_id):
            if not self.config.report_pattern.allowed(report_object.name):
                continue
            yield from self.mapper.gen_folder_containers(project_id, report_object)
            parent_key = self.mapper.folder_container_for_dashboard(
                project_id,
                report_object,
            )
            report_definition = self._get_report_definition(project_id, report_object)
            source_dataset = self._report_source_dataset(
                report_object,
                report_definition,
            )
            if source_dataset is not None:
                if self.config.extract_metric_expressions:
                    self._enrich_dataset_metric_expressions(
                        project_id,
                        source_dataset,
                    )
                if self.config.extract_report_sql_lineage and warehouse_context:
                    self._enrich_report_sql_lineage(
                        project_id=project_id,
                        report_object=report_object,
                        dataset=source_dataset,
                        context=warehouse_context,
                    )
                if model_lineage_index:
                    self.mapper.attach_dataset_model_lineage(
                        source_dataset,
                        model_lineage_index,
                    )
                yield from self.mapper.gen_report_source_dataset_workunits(
                    project_id,
                    report_object,
                    source_dataset,
                    parent_key,
                )
            yield from self.mapper.gen_report_workunits(
                project_id,
                report_object,
                report_definition,
                source_dataset,
                parent_key,
            )

    def _get_dashboard_definition(
        self,
        project_id: str,
        dashboard_object: MSTRObject,
    ) -> Optional[DashboardDefinition]:
        dashboard_id = dashboard_object.id
        try:
            response = self.client.get_dossier_definition(project_id, dashboard_id)
        except MicroStrategyAPIError:
            try:
                response = self.client.get_document_definition(project_id, dashboard_id)
            except MicroStrategyAPIError as error:
                self.report.warning(
                    title="Failed to Fetch Dashboard Definition",
                    message="Skipping dashboard because definition APIs failed.",
                    context=f"project_id={project_id}, dashboard_id={dashboard_id}",
                    exc=error,
                )
                return None

        dashboard = DashboardDefinition.from_api_response(
            object_id=dashboard_id,
            object_name=dashboard_object.name,
            description=dashboard_object.description,
            response=response,
        )
        if self.config.extract_lineage and self.config.extract_visualization_details:
            self._enrich_visualization_details(project_id, dashboard_object, dashboard)
        if self.config.extract_dashboard_dependencies:
            self._enrich_dashboard_dependencies(
                project_id=project_id,
                dashboard_object=dashboard_object,
                dashboard=dashboard,
            )
        if self.config.extract_metric_expressions:
            self._enrich_metric_expressions(project_id, dashboard)
        return dashboard

    def _get_report_definition(
        self,
        project_id: str,
        report_object: MSTRObject,
    ) -> Optional[ReportDefinition]:
        if not self.config.extract_report_definitions:
            return ReportDefinition.from_search_result(report_object)

        try:
            response = self.client.get_report_definition(project_id, report_object.id)
        except MicroStrategyAPIError as error:
            self.report.report_report_definition_api_failure()
            self.report.warning(
                title="Failed to Fetch Report Definition",
                message=(
                    "Continuing with report search metadata only. Report source "
                    "dataset fields may be unavailable."
                ),
                context=f"project_id={project_id}, report_id={report_object.id}",
                exc=error,
            )
            return ReportDefinition.from_search_result(report_object)

        return ReportDefinition.from_api_response(
            object_id=report_object.id,
            object_name=report_object.name,
            description=report_object.description,
            response=response,
        )

    @staticmethod
    def _report_source_dataset(
        report_object: MSTRObject,
        report_definition: Optional[ReportDefinition],
    ) -> Optional[DatasetObject]:
        if report_definition is None:
            return None
        if not report_definition.source_id and not report_definition.available_objects:
            return None
        return DatasetObject.model_validate(
            {
                "id": report_definition.source_id or "source",
                "name": report_definition.source_name
                or f"{report_object.name} Source",
                "description": report_definition.description,
                "availableObjects": report_definition.available_objects,
            }
        )

    def _get_project_model_lineage_index(
        self,
        project_id: str,
        warehouse_context: Optional[WarehouseLineageContext],
    ) -> Optional[ModelLineageIndex]:
        if not self.config.extract_lineage or not self.config.extract_model_lineage:
            return None
        if warehouse_context is None:
            self.report.warning(
                title="MicroStrategy Model Lineage Unavailable",
                message=(
                    "Skipping logical table lineage because no supported source "
                    "warehouse context was discovered for this project."
                ),
                context=f"project_id={project_id}",
            )
            return None

        model_tables: List[Dict[str, Any]] = []
        offset = 0
        while True:
            try:
                response = self.client.list_model_tables(
                    project_id,
                    limit=self.config.page_size,
                    offset=offset,
                    fields="physicalTable,attributes,facts",
                )
            except MicroStrategyAPIError as error:
                self.report.report_model_lineage_api_failure()
                self.report.warning(
                    title="MicroStrategy Model Lineage Unavailable",
                    message=(
                        "Logical table lineage requires model table access. "
                        "SQL-view physical warehouse lineage may still be emitted "
                        "when dashboard SQL-view APIs are available."
                    ),
                    context=f"project_id={project_id}",
                    exc=error,
                )
                return None

            tables = response.get("tables")
            if not isinstance(tables, list):
                break
            model_tables.extend(table for table in tables if isinstance(table, dict))
            total = response.get("total")
            offset += len(tables)
            if len(tables) < self.config.page_size:
                break
            if isinstance(total, int) and offset >= total:
                break

        self.report.report_model_tables_scanned(len(model_tables))
        if not model_tables:
            return None
        return self.mapper.lineage.model_lineage_index_from_tables(
            model_tables,
            warehouse_context,
        )

    def _enrich_dashboard_dependencies(
        self,
        project_id: str,
        dashboard_object: MSTRObject,
        dashboard: DashboardDefinition,
    ) -> None:
        object_type = dashboard_object.type
        if not object_type:
            return
        try:
            dependencies = self.client.get_object_dependencies(
                project_id=project_id,
                object_id=dashboard.id,
                object_type=object_type,
            )
        except MicroStrategyAPIError as error:
            self.report.warning(
                title="Failed to Fetch Dashboard Dependencies",
                message=(
                    "Skipping direct dashboard component metadata from metadata search."
                ),
                context=f"project_id={project_id}, dashboard_id={dashboard.id}",
                exc=error,
            )
            return
        dashboard.dependencies = dependencies
        self.report.report_dashboard_dependencies_scanned(len(dependencies))

    def _enrich_metric_expressions(
        self,
        project_id: str,
        dashboard: DashboardDefinition,
    ) -> None:
        for dataset in dashboard.datasets:
            self._enrich_dataset_metric_expressions(project_id, dataset)

    def _enrich_dataset_metric_expressions(
        self,
        project_id: str,
        dataset: DatasetObject,
    ) -> None:
        for metric in _metric_items(dataset.available_objects):
            metric_id = str(metric.get("id") or "")
            if not metric_id:
                continue
            model = self._get_metric_model(project_id, metric_id)
            if not model:
                continue
            summary = _metric_expression_summary(model)
            if summary:
                metric["modelExpression"] = summary
            fact_ids = self._metric_model_fact_ids(
                project_id,
                model,
                visited={metric_id.upper()},
            )
            if fact_ids:
                metric["modelFactIds"] = fact_ids

    def _get_metric_model(
        self,
        project_id: str,
        metric_id: str,
    ) -> Dict[str, Any]:
        normalized_metric_id = metric_id.upper()
        model = self._metric_model_cache.get(normalized_metric_id)
        if model is not None:
            return model
        try:
            model = self.client.get_metric_model(project_id, metric_id)
        except MicroStrategyAPIError:
            self.report.report_metric_expression_api_failure()
            self._metric_model_cache[normalized_metric_id] = {}
            return {}
        self._metric_model_cache[normalized_metric_id] = model
        self.report.report_metric_expression_scanned()
        return model

    def _metric_model_fact_ids(
        self,
        project_id: str,
        model: Dict[str, Any],
        visited: set[str],
    ) -> List[str]:
        fact_ids = set(metric_fact_ids_from_model(model))
        for nested_metric_id in metric_metric_ids_from_model(model):
            if nested_metric_id in visited:
                continue
            visited.add(nested_metric_id)
            nested_model = self._get_metric_model(project_id, nested_metric_id)
            if nested_model:
                fact_ids.update(
                    self._metric_model_fact_ids(
                        project_id,
                        nested_model,
                        visited,
                    )
                )
        return sorted(fact_ids)

    def _enrich_visualization_details(
        self,
        project_id: str,
        dashboard_object: MSTRObject,
        dashboard: DashboardDefinition,
    ) -> None:
        if not dashboard.visualizations:
            return
        try:
            instance_id = self._create_dashboard_instance(
                project_id,
                dashboard_object,
                dashboard.id,
            )
        except MicroStrategyAPIError as error:
            self.report.warning(
                title="Failed to Create Dashboard Instance",
                message=(
                    "Skipping runtime visualization detail extraction for dashboard "
                    "lineage."
                ),
                context=f"project_id={project_id}, dashboard_id={dashboard.id}",
                exc=error,
            )
            return

        try:
            enriched = []
            for visualization in dashboard.visualizations:
                if not visualization.chapter_key:
                    enriched.append(visualization)
                    continue
                try:
                    detail = self.client.get_dossier_visualization(
                        project_id=project_id,
                        dossier_id=dashboard.id,
                        instance_id=instance_id,
                        chapter_key=visualization.chapter_key,
                        visualization_key=visualization.key,
                    )
                except MicroStrategyAPIError as error:
                    self.report.warning(
                        title="Failed to Fetch Visualization Definition",
                        message=(
                            "Keeping visualization without runtime lineage enrichment."
                        ),
                        context=(
                            f"project_id={project_id}, dashboard_id={dashboard.id}, "
                            f"visualization_key={visualization.key}"
                        ),
                        exc=error,
                    )
                    enriched.append(visualization)
                    continue

                enriched.append(
                    Visualization.model_validate(
                        {
                            **visualization.raw,
                            "chapterKey": visualization.chapter_key,
                            "pageKey": visualization.page_key,
                            "runtimeDefinition": detail,
                        }
                    )
                )
            dashboard.visualizations = enriched
        finally:
            self._delete_dashboard_instance(
                project_id,
                dashboard_object,
                dashboard.id,
                instance_id,
            )

    def _enrich_warehouse_lineage(
        self,
        project_id: str,
        dashboard_object: MSTRObject,
        dashboard: DashboardDefinition,
        context: Optional[WarehouseLineageContext],
    ) -> None:
        if not dashboard.datasets:
            return
        try:
            instance_id = self._create_dashboard_instance(
                project_id,
                dashboard_object,
                dashboard.id,
            )
        except MicroStrategyAPIError as error:
            self.report.report_warehouse_lineage_api_failure()
            self.report.warning(
                title="Failed to Create Dashboard Instance for Warehouse Lineage",
                message="Skipping SQL-view warehouse lineage for this dashboard.",
                context=f"project_id={project_id}, dashboard_id={dashboard.id}",
                exc=error,
            )
            return

        try:
            sql_view_rows = self.client.get_dossier_datasets_sql(
                project_id=project_id,
                dossier_id=dashboard.id,
                instance_id=instance_id,
            )
        except MicroStrategyAPIError as error:
            self.report.report_warehouse_lineage_api_failure()
            self.report.warning(
                title="Failed to Fetch Dataset SQL View",
                message="Skipping source warehouse lineage for this dashboard.",
                context=f"project_id={project_id}, dashboard_id={dashboard.id}",
                exc=error,
            )
            return
        finally:
            self._delete_dashboard_instance(
                project_id,
                dashboard_object,
                dashboard.id,
                instance_id,
            )

        self.report.report_warehouse_lineage_sql_views_scanned(len(sql_view_rows))
        self._attach_dataset_warehouse_upstreams(sql_view_rows, dashboard, context)

    def _attach_dataset_warehouse_upstreams(
        self,
        sql_view_rows: List[Dict[str, Any]],
        dashboard: DashboardDefinition,
        context: Optional[WarehouseLineageContext],
    ) -> None:
        dataset_by_id = {dataset.id: dataset for dataset in dashboard.datasets}
        dataset_by_name = {
            dataset.name.strip().lower(): dataset for dataset in dashboard.datasets
        }
        for row in sql_view_rows:
            dataset = None
            dataset_key = sql_view_dataset_key(row)
            if dataset_key:
                dataset = dataset_by_id.get(dataset_key)
            if dataset is None:
                dataset_name = sql_view_dataset_name(row)
                if dataset_name:
                    dataset = dataset_by_name.get(dataset_name.strip().lower())
            if dataset is None:
                continue

            dataset_context = (
                warehouse_context_from_datasource(
                    dataset.source_warehouse,
                    self.config.env,
                )
                if dataset.source_warehouse
                else None
            ) or context
            if dataset_context is None:
                continue

            upstream_urns = self.mapper.lineage.warehouse_upstream_urns_from_sql(
                sql_statement_from_sql_view_entry(row),
                dataset_context,
                graph=self.ctx.graph,
            )
            if upstream_urns:
                dataset.warehouse_upstream_urns = upstream_urns

    def _enrich_report_sql_lineage(
        self,
        project_id: str,
        report_object: MSTRObject,
        dataset: DatasetObject,
        context: WarehouseLineageContext,
    ) -> None:
        try:
            instance_id = self.client.create_report_instance(
                project_id,
                report_object.id,
            )
        except MicroStrategyAPIError as error:
            self.report.report_report_sql_view_api_failure()
            self.report.warning(
                title="Failed to Create Report Instance for Warehouse Lineage",
                message="Skipping SQL-view warehouse lineage for this report.",
                context=f"project_id={project_id}, report_id={report_object.id}",
                exc=error,
            )
            return

        try:
            sql_view = self.client.get_report_sql_view(
                project_id=project_id,
                report_id=report_object.id,
                instance_id=instance_id,
            )
        except MicroStrategyAPIError as error:
            self.report.report_report_sql_view_api_failure()
            self.report.warning(
                title="Failed to Fetch Report SQL View",
                message="Skipping source warehouse lineage for this report.",
                context=f"project_id={project_id}, report_id={report_object.id}",
                exc=error,
            )
            return
        finally:
            self._delete_report_instance(project_id, report_object.id, instance_id)

        sql_statement = sql_statement_from_sql_view_entry(sql_view)
        result = sql_view.get("result")
        if not sql_statement and isinstance(result, dict):
            sql_statement = sql_statement_from_sql_view_entry(result)
        if not sql_statement:
            return

        self.report.report_warehouse_lineage_sql_views_scanned(1)
        upstream_urns = self.mapper.lineage.warehouse_upstream_urns_from_sql(
            sql_statement,
            context,
            graph=self.ctx.graph,
        )
        if upstream_urns:
            dataset.warehouse_upstream_urns = upstream_urns

    def _create_dashboard_instance(
        self,
        project_id: str,
        dashboard_object: MSTRObject,
        dashboard_id: str,
    ) -> str:
        if dashboard_object.subtype == "14081":
            return self.client.create_document_instance(project_id, dashboard_id)
        return self.client.create_dossier_instance(project_id, dashboard_id)

    def _delete_dashboard_instance(
        self,
        project_id: str,
        dashboard_object: MSTRObject,
        dashboard_id: str,
        instance_id: str,
    ) -> None:
        try:
            deleted = self.client.delete_dossier_instance(
                project_id,
                dashboard_id,
                instance_id,
            )
            if deleted or dashboard_object.subtype != "14081":
                return
            self.client.delete_document_instance(project_id, dashboard_id, instance_id)
        except MicroStrategyAPIError:
            logger.debug(
                "MicroStrategy dashboard instance cleanup failed",
                exc_info=True,
            )

    def _delete_report_instance(
        self,
        project_id: str,
        report_id: str,
        instance_id: str,
    ) -> None:
        try:
            self.client.delete_report_instance(project_id, report_id, instance_id)
        except MicroStrategyAPIError:
            logger.debug(
                "MicroStrategy report instance cleanup failed",
                exc_info=True,
            )

    def _dashboard_report_chart_urns(
        self,
        project_id: str,
        dashboard: DashboardDefinition,
    ) -> List[str]:
        if not self.config.extract_reports:
            return []
        report_urns: List[str] = []
        seen: set[str] = set()
        for dependency in dashboard.dependencies:
            if not _is_report_dependency(dependency):
                continue
            if not self.config.report_pattern.allowed(dependency.name):
                continue
            report_urn = self.mapper.report_urn(project_id, dependency.id)
            if report_urn in seen:
                continue
            seen.add(report_urn)
            report_urns.append(report_urn)
        return report_urns

    def _process_dashboard(
        self,
        project_id: str,
        dashboard_object: MSTRObject,
        dashboard: DashboardDefinition,
        parent_key: ProjectKey,
        extra_chart_urns: Sequence[str] = (),
    ) -> Iterable[MetadataWorkUnit]:
        if self.config.extract_cubes:
            for dataset in dashboard.datasets:
                yield from self.mapper.gen_dataset_workunits(
                    project_id, dashboard, dataset, parent_key
                )

        if self.config.extract_charts:
            for visualization in dashboard.visualizations:
                yield from self.mapper.gen_chart_workunits(
                    project_id, dashboard, visualization, parent_key
                )

        yield from self.mapper.gen_dashboard_workunits(
            project_id,
            dashboard_object,
            dashboard,
            parent_key,
            extra_chart_urns=extra_chart_urns,
        )

    def get_report(self) -> SourceReport:
        return self.report


def _metric_items(available_objects: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    metrics = available_objects.get("metrics")
    if isinstance(metrics, list):
        for metric in metrics:
            if isinstance(metric, dict):
                yield metric


def _is_report_dependency(dependency: MSTRObject) -> bool:
    object_type = (dependency.type or "").strip()
    if object_type == "3":
        return True
    descriptor = f"{dependency.name} {dependency.subtype}".lower()
    return "report" in descriptor


def _metric_expression_summary(model: Dict[str, Any]) -> Dict[str, str]:
    expression = model.get("expression")
    if not isinstance(expression, dict):
        return {}
    summary: Dict[str, str] = {}
    text = expression.get("text") or expression.get("tree")
    if text:
        summary["text"] = str(text)
    tokens = expression.get("tokens")
    if isinstance(tokens, list):
        object_tokens = []
        for token in tokens:
            if not isinstance(token, dict):
                continue
            value = token.get("value")
            if isinstance(value, dict):
                token_id = value.get("objectId") or value.get("id")
                token_name = value.get("name")
                token_type = value.get("type")
            else:
                token_id = token.get("objectId") or token.get("id")
                token_name = token.get("name")
                token_type = token.get("type")
            if token_id or token_name:
                object_tokens.append(
                    {
                        key: str(value)
                        for key, value in {
                            "id": token_id,
                            "name": token_name,
                            "type": token_type,
                        }.items()
                        if value is not None
                    }
                )
        if object_tokens:
            import json

            summary["tokens"] = json.dumps(object_tokens, sort_keys=True)
    return summary
