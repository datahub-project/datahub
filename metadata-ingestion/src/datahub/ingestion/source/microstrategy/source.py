import logging
from typing import Any, Dict, Iterable, List, Optional

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
    WarehouseLineageContext,
    sql_statement_from_sql_view_entry,
    sql_view_dataset_key,
    sql_view_dataset_name,
    warehouse_context_from_datasources,
    warehouse_context_with_connection,
)
from datahub.ingestion.source.microstrategy.mapper import MicroStrategyMapper
from datahub.ingestion.source.microstrategy.models import (
    DashboardDefinition,
    Datasource,
    MSTRObject,
    ProjectKey,
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
@capability(SourceCapability.LINEAGE_COARSE, "Visualization inputs when resolvable")
@capability(
    SourceCapability.LINEAGE_FINE,
    "Reserved for modeling API and SQL-view lineage enrichment",
    supported=False,
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
        self._model_lineage_probed_projects: set[str] = set()

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
                self._probe_model_lineage_access(project.id)
                yield from self.mapper.gen_project_container(
                    project,
                    source_warehouses=source_warehouses,
                )
                if self.config.extract_dashboards:
                    yield from self._process_project_dashboards(
                        project.id,
                        warehouse_context,
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
        if not self.config.extract_lineage or not self.config.extract_warehouse_lineage:
            return None

        context = warehouse_context_from_datasources(source_warehouses, self.config.env)
        if not context:
            return None

        for datasource in source_warehouses:
            if not datasource.connection_id:
                continue
            try:
                connection = self.client.get_datasource_connection(
                    datasource.connection_id,
                    project_id=project_id,
                )
            except MicroStrategyAPIError:
                self.report.report_warehouse_lineage_api_failure()
                continue
            context = warehouse_context_with_connection(context, connection)
            if context.database and context.schema:
                break
        return context

    def _process_project_dashboards(
        self,
        project_id: str,
        warehouse_context: Optional[WarehouseLineageContext],
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
            if warehouse_context:
                self._enrich_warehouse_lineage(
                    project_id=project_id,
                    dashboard_object=dashboard_object,
                    dashboard=dashboard,
                    context=warehouse_context,
                )
            yield from self._process_dashboard(
                project_id, dashboard_object, dashboard, parent_key
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

    def _probe_model_lineage_access(self, project_id: str) -> None:
        if not self.config.extract_model_lineage:
            return
        if project_id in self._model_lineage_probed_projects:
            return
        self._model_lineage_probed_projects.add(project_id)
        try:
            self.client.list_model_tables(
                project_id,
                limit=1,
                fields="physicalTable,datasource",
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
                    "Skipping direct dashboard component metadata from metadata "
                    "search."
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
            for metric in _metric_items(dataset.available_objects):
                metric_id = str(metric.get("id") or "")
                if not metric_id:
                    continue
                model = self._metric_model_cache.get(metric_id)
                if model is None:
                    try:
                        model = self.client.get_metric_model(project_id, metric_id)
                    except MicroStrategyAPIError:
                        self.report.report_metric_expression_api_failure()
                        self._metric_model_cache[metric_id] = {}
                        continue
                    self._metric_model_cache[metric_id] = model
                    self.report.report_metric_expression_scanned()
                summary = _metric_expression_summary(model)
                if summary:
                    metric["modelExpression"] = summary

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
        context: WarehouseLineageContext,
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
        context: WarehouseLineageContext,
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

            upstream_urns = self.mapper.lineage.warehouse_upstream_urns_from_sql(
                sql_statement_from_sql_view_entry(row),
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

    def _process_dashboard(
        self,
        project_id: str,
        dashboard_object: MSTRObject,
        dashboard: DashboardDefinition,
        parent_key: ProjectKey,
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
            project_id, dashboard_object, dashboard, parent_key
        )

    def get_report(self) -> SourceReport:
        return self.report


def _metric_items(available_objects: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    metrics = available_objects.get("metrics")
    if isinstance(metrics, list):
        for metric in metrics:
            if isinstance(metric, dict):
                yield metric


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
