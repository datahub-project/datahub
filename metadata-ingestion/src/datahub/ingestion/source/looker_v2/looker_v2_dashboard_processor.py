"""Dashboard processor for LookerV2Source."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from json import JSONDecodeError
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Set, Union

from looker_sdk.error import SDKError
from looker_sdk.sdk.api40.models import (
    Dashboard as LookerAPIDashboard,
    DashboardBase as LookerDashboardBase,
    DashboardElement,
    Query,
)

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    BIAssetSubTypes,
)
from datahub.ingestion.source.looker.looker_common import (
    InputFieldElement,
    LookerFolderKey,
    LookerUtil,
    ViewField,
    ViewFieldType,
    get_urn_looker_dashboard_id,
    get_urn_looker_element_id,
)
from datahub.metadata.schema_classes import (
    ChartTypeClass,
    EmbedClass,
    InputFieldClass,
    InputFieldsClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
)
from datahub.metadata.urns import CorpUserUrn
from datahub.sdk.chart import Chart
from datahub.sdk.dashboard import Dashboard
from datahub.sdk.entity import Entity
from datahub.utilities.backpressure_aware_executor import BackpressureAwareExecutor
from datahub.utilities.sentinels import Unset, unset
from datahub.utilities.url_util import remove_port_from_url

if TYPE_CHECKING:
    from looker_sdk.sdk.api40.models import LookmlModelExploreField

    from datahub.ingestion.source.looker.looker_common import LookerExploreRegistry
    from datahub.ingestion.source.looker_v2 import looker_v2_usage as looker_usage
    from datahub.ingestion.source.looker_v2.looker_v2_context import LookerV2Context
    from datahub.ingestion.source.looker_v2.looker_v2_folder_processor import (
        LookerFolderProcessor,
    )

logger = logging.getLogger(__name__)


@dataclass
class DashboardProcessingResult:
    """All output produced while processing a single dashboard."""

    entities: List[Entity]
    extra_mcps: List[MetadataChangeProposalWrapper]
    dashboard_usage: "Optional[looker_usage.LookerDashboardForUsage]"
    dashboard_id: str
    start_time: datetime
    end_time: datetime


class LookerDashboardProcessor:
    """Processes Looker dashboards and their chart elements into workunits."""

    def __init__(
        self,
        ctx: "LookerV2Context",
        folder_proc: "LookerFolderProcessor",
        explore_registry: "LookerExploreRegistry",
        reachable_look_registry: Set[str],
        chart_urns: Set[str],
    ) -> None:
        self._ctx = ctx
        self._folder_proc = folder_proc
        self._explore_registry = explore_registry
        self._reachable_look_registry = reachable_look_registry
        self._chart_urns = chart_urns

    @property
    def _dashboard_chart_platform_instance(self) -> Optional[str]:
        if self._ctx.config.include_platform_instance_in_urns:
            return self._ctx.config.platform_instance
        return None

    def process(self) -> Iterable[MetadataWorkUnit]:
        """Process all dashboards."""
        logger.info("Processing dashboards...")

        try:
            dashboards: List[Union[LookerDashboardBase, LookerAPIDashboard]] = list(
                self._ctx.looker_api.all_dashboards(fields="id,title,folder")
            )
        except SDKError as e:
            self._ctx.reporter.report_failure("fetch_dashboards", str(e))
            return

        if self._ctx.config.include_deleted:
            try:
                deleted_dashboards = self._ctx.looker_api.search_dashboards(
                    fields="id,title,folder", deleted="true"
                )
                existing_ids = {d.id for d in dashboards}
                for d in deleted_dashboards:
                    if d.id not in existing_ids:
                        dashboards.append(d)
            except SDKError as e:
                logger.warning(f"Failed to fetch deleted dashboards: {e}")

        self._ctx.reporter.dashboards_discovered = len(dashboards)

        dashboard_ids = []
        for dashboard in dashboards:
            if dashboard.id and dashboard.title:
                if self._ctx.config.dashboard_pattern.allowed(dashboard.title):
                    dashboard_ids.append(dashboard.id)
                else:
                    self._ctx.reporter.dashboards_filtered.append(dashboard.title)

        def process_dashboard(
            dashboard_id: str,
        ) -> Optional[DashboardProcessingResult]:
            return self._process_single_dashboard(dashboard_id)

        for future in BackpressureAwareExecutor.map(
            fn=process_dashboard,
            args_list=[(did,) for did in dashboard_ids],
            max_workers=self._ctx.config.max_concurrent_requests,
            max_pending=self._ctx.config.max_concurrent_requests * 2,
        ):
            try:
                result = future.result()
                if result:
                    for entity in result.entities:
                        for mcp in entity.as_mcps():
                            yield mcp.as_workunit()
                    for extra_mcp in result.extra_mcps:
                        yield extra_mcp.as_workunit()
                    if result.dashboard_usage:
                        self._ctx.dashboards_for_usage.append(result.dashboard_usage)
                    self._ctx.reporter.dashboards_scanned += 1
            except (SDKError, ValueError, KeyError, TypeError) as e:
                self._ctx.reporter.report_warning(
                    title="Dashboard Processing Failed",
                    message="Error processing dashboard",
                    context=str(e),
                )

    def _process_single_dashboard(
        self, dashboard_id: str
    ) -> Optional[DashboardProcessingResult]:
        """Process a single dashboard."""
        start_time = datetime.now(timezone.utc)
        entities: List[Entity] = []
        extra_mcps: List[MetadataChangeProposalWrapper] = []

        dashboard_fields = [
            "id",
            "title",
            "description",
            "user_id",
            "folder",
            "dashboard_elements",
            "dashboard_elements.look_id",
            "dashboard_elements.look.id",
            "dashboard_elements.look.view_count",
            "dashboard_filters",
            "created_at",
            "updated_at",
            "deleted",
            "deleted_at",
            "deleter_id",
            "hidden",
            "last_updater_id",
        ]

        if self._ctx.config.extract_usage_history:
            dashboard_fields.extend(["favorite_count", "view_count", "last_viewed_at"])

        try:
            api_dashboard = self._ctx.looker_api.dashboard(
                dashboard_id, fields=dashboard_fields
            )
        except SDKError as e:
            logger.warning(f"Failed to fetch dashboard {dashboard_id}: {e}")
            self._ctx.reporter.report_warning(
                title="Dashboard Fetch Failed",
                message="Could not fetch dashboard from Looker API.",
                context=f"dashboard_id={dashboard_id}: {e}",
            )
            return None

        if not api_dashboard:
            return None

        if (
            getattr(api_dashboard, "hidden", False)
            and not self._ctx.config.include_deleted
        ):
            return None

        if self._folder_proc.should_skip_personal_folder(api_dashboard.folder):
            return None

        if api_dashboard.folder:
            folder_path = self._folder_proc.get_folder_path(api_dashboard.folder)
            if not self._ctx.config.folder_path_pattern.allowed(folder_path):
                self._ctx.reporter.dashboards_filtered.append(
                    f"{api_dashboard.title} (folder: {folder_path})"
                )
                return None

        dashboard = self._create_dashboard_entity(api_dashboard)
        if dashboard:
            entities.append(dashboard)
            self._process_dashboard_elements(
                api_dashboard, dashboard, entities, extra_mcps
            )

        if api_dashboard.folder and api_dashboard.folder.id:
            folder_entities = self._folder_proc.get_folder_container(
                api_dashboard.folder
            )
            entities.extend(folder_entities)

        dashboard_usage = self._build_dashboard_usage(api_dashboard)

        end_time = datetime.now(timezone.utc)
        return DashboardProcessingResult(
            entities=entities,
            extra_mcps=extra_mcps,
            dashboard_usage=dashboard_usage,
            dashboard_id=dashboard_id,
            start_time=start_time,
            end_time=end_time,
        )

    def _process_dashboard_elements(
        self,
        api_dashboard: LookerAPIDashboard,
        dashboard: Dashboard,
        entities: List[Entity],
        extra_mcps: List[MetadataChangeProposalWrapper],
    ) -> None:
        """Process dashboard elements (charts) and collect input fields."""
        if not api_dashboard.dashboard_elements:
            return

        for element in api_dashboard.dashboard_elements:
            if element.id and not self._ctx.config.chart_pattern.allowed(
                str(element.id)
            ):
                self._ctx.reporter.charts_dropped += 1
                continue
            chart = self._create_chart_entity(element, api_dashboard)
            if chart:
                self._add_chart_explore_lineage(chart, element)
                entities.append(chart)
                self._ctx.reporter.charts_scanned += 1
                extra_mcps.extend(self._extract_chart_input_fields(element, chart))

        self._ctx.reporter.charts_discovered += len(api_dashboard.dashboard_elements)

        all_input_fields: List[InputFieldClass] = []
        for element in api_dashboard.dashboard_elements:
            all_input_fields.extend(self._get_enriched_input_fields(element))
        if all_input_fields:
            extra_mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=str(dashboard.urn),
                    aspect=InputFieldsClass(fields=all_input_fields),
                )
            )

        if self._ctx.config.extract_embed_urls and self._ctx.config.external_base_url:
            base_url = remove_port_from_url(self._ctx.config.external_base_url)
            embed_url = f"{base_url}/embed/dashboards/{api_dashboard.id}"
            extra_mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=str(dashboard.urn),
                    aspect=EmbedClass(renderUrl=embed_url),
                )
            )

    def _build_dashboard_usage(
        self, api_dashboard: LookerAPIDashboard
    ) -> "Optional[looker_usage.LookerDashboardForUsage]":
        """Build usage tracking data for a dashboard."""
        from datahub.ingestion.source.looker_v2 import looker_v2_usage as looker_usage

        if not self._ctx.config.extract_usage_history:
            return None

        last_viewed_ms = None
        last_viewed_at = getattr(api_dashboard, "last_viewed_at", None)
        if last_viewed_at is not None:
            try:
                last_viewed_ms = round(last_viewed_at.timestamp() * 1000)
            except (AttributeError, OSError):
                pass

        usage_looks: List[looker_usage.LookerChartForUsage] = []
        if api_dashboard.dashboard_elements:
            for element in api_dashboard.dashboard_elements:
                if element.look_id and element.look:
                    usage_looks.append(
                        looker_usage.LookerChartForUsage(
                            id=str(element.look_id),
                            view_count=getattr(element.look, "view_count", None),
                        )
                    )
                    self._reachable_look_registry.add(str(element.look_id))

        return looker_usage.LookerDashboardForUsage(
            id=str(api_dashboard.id),
            view_count=getattr(api_dashboard, "view_count", None),
            favorite_count=getattr(api_dashboard, "favorite_count", None),
            last_viewed_at=last_viewed_ms,
            looks=usage_looks,
        )

    def _create_dashboard_entity(
        self, api_dashboard: LookerAPIDashboard
    ) -> Optional[Dashboard]:
        """Create a Dashboard entity from API response."""
        if not api_dashboard.id or not api_dashboard.title:
            return None

        chart_urns = []
        if api_dashboard.dashboard_elements:
            for element in api_dashboard.dashboard_elements:
                if element.id:
                    chart_urn = builder.make_chart_urn(
                        platform=self._ctx.platform,
                        name=get_urn_looker_element_id(str(element.id)),
                        platform_instance=self._dashboard_chart_platform_instance,
                    )
                    chart_urns.append(chart_urn)

        parent_container: Union[LookerFolderKey, Unset] = unset
        if api_dashboard.folder and api_dashboard.folder.id:
            parent_container = self._folder_proc.gen_folder_key(api_dashboard.folder.id)

        dashboard_url = None
        if self._ctx.config.external_base_url:
            base_url = remove_port_from_url(self._ctx.config.external_base_url)
            dashboard_url = f"{base_url}/dashboards/{api_dashboard.id}"

        dashboard = Dashboard(
            name=get_urn_looker_dashboard_id(str(api_dashboard.id)),
            display_name=api_dashboard.title,
            platform=self._ctx.platform,
            platform_instance=self._dashboard_chart_platform_instance,
            description=api_dashboard.description,
            external_url=api_dashboard.link if hasattr(api_dashboard, "link") else None,
            dashboard_url=dashboard_url,
            subtype=BIAssetSubTypes.DASHBOARD,
            charts=chart_urns,
            parent_container=parent_container,
        )

        if api_dashboard.user_id and self._ctx.config.extract_owners:
            if self._ctx.user_registry is not None:
                user = self._ctx.user_registry.get_by_id(str(api_dashboard.user_id))
                if user and user.email:
                    dashboard.add_owner((CorpUserUrn(user.email), "DATAOWNER"))

        return dashboard

    def _get_chart_query(self, element: DashboardElement) -> Optional[Query]:
        """Get query for a chart element with fallbacks (matching V1)."""
        query = element.query
        if not query and element.look and hasattr(element.look, "query"):
            query = element.look.query
        if not query and element.result_maker and element.result_maker.query:
            query = element.result_maker.query
        return query

    def _compute_upstream_fields(self, element: DashboardElement) -> Set[str]:
        """Compute upstream_fields from query fields, filters, and dynamic fields."""
        upstream_fields: Set[str] = set()
        query = self._get_chart_query(element)
        if query:
            for f in query.fields or []:
                if f:
                    upstream_fields.add(f)
            for f in query.filters or {}:
                if f:
                    upstream_fields.add(f)
            try:
                dynamic_fields = json.loads(query.dynamic_fields or "[]")
            except (JSONDecodeError, TypeError):
                dynamic_fields = []
            for field in dynamic_fields:
                for key in ("table_calculation", "measure", "dimension"):
                    if key in field and field[key]:
                        upstream_fields.add(field[key])
                if "measure" in field:
                    based_on = field.get("based_on")
                    if based_on:
                        upstream_fields.add(based_on)
        return upstream_fields

    def _get_chart_url(self, element: DashboardElement) -> Optional[str]:
        """Generate chart URL with query slug fallbacks (matching V1)."""
        if not self._ctx.config.external_base_url:
            return None
        base_url = remove_port_from_url(self._ctx.config.external_base_url)
        if element.look_id:
            return f"{base_url}/looks/{element.look_id}"
        slug = None
        if element.query and element.query.slug:
            slug = element.query.slug
        elif (
            element.look
            and hasattr(element.look, "query")
            and element.look.query
            and element.look.query.slug
        ):
            slug = element.look.query.slug
        elif (
            element.result_maker
            and element.result_maker.query
            and element.result_maker.query.slug
        ):
            slug = element.result_maker.query.slug
        if slug:
            return f"{base_url}/x/{slug}"
        return None

    def _create_chart_entity(
        self, element: DashboardElement, parent_dashboard: LookerAPIDashboard
    ) -> Optional[Chart]:
        """Create a Chart entity from a dashboard element."""
        if not element.id:
            return None

        chart_id = get_urn_looker_element_id(str(element.id))

        chart_type_value = None
        if element.type:
            chart_type_value = self._map_chart_type(element.type)

        parent_container: Union[LookerFolderKey, Unset] = unset
        if parent_dashboard.folder and parent_dashboard.folder.id:
            parent_container = self._folder_proc.gen_folder_key(
                parent_dashboard.folder.id
            )

        chart_url = self._get_chart_url(element)
        upstream_fields = self._compute_upstream_fields(element)

        chart = Chart(
            name=chart_id,
            display_name=element.title or f"Chart {element.id}",
            platform=self._ctx.platform,
            platform_instance=self._dashboard_chart_platform_instance,
            description=element.subtitle_text,
            subtype=BIAssetSubTypes.LOOKER_LOOK,
            chart_type=chart_type_value,
            chart_url=chart_url,
            custom_properties={
                "upstream_fields": ",".join(sorted(upstream_fields))
                if upstream_fields
                else ""
            },
            parent_container=parent_container,
        )

        if (
            parent_dashboard.user_id
            and hasattr(self._ctx.config, "extract_owners")
            and self._ctx.config.extract_owners
        ):
            if self._ctx.user_registry is not None:
                user = self._ctx.user_registry.get_by_id(str(parent_dashboard.user_id))
                if user and user.email:
                    chart.add_owner((CorpUserUrn(user.email), "DATAOWNER"))

        self._chart_urns.add(str(chart.urn))

        return chart

    def _add_chart_explore_lineage(
        self, chart: Chart, element: DashboardElement
    ) -> None:
        """Add chart-to-explore lineage based on the element's query."""
        query = self._get_chart_query(element)

        added_explores: Set[str] = set()
        if query and query.model and query.view:
            explore_urn = builder.make_dataset_urn_with_platform_instance(
                platform=self._ctx.platform,
                name=f"{query.model}.explore.{query.view}",
                platform_instance=self._ctx.config.platform_instance,
                env=self._ctx.config.env,
            )
            chart.add_input_dataset(explore_urn)
            added_explores.add(explore_urn)

        if element.result_maker and element.result_maker.filterables:
            for filterable in element.result_maker.filterables:
                if filterable.view and filterable.model:
                    explore_urn = builder.make_dataset_urn_with_platform_instance(
                        platform=self._ctx.platform,
                        name=f"{filterable.model}.explore.{filterable.view}",
                        platform_instance=self._ctx.config.platform_instance,
                        env=self._ctx.config.env,
                    )
                    if explore_urn not in added_explores:
                        chart.add_input_dataset(explore_urn)
                        added_explores.add(explore_urn)

    def _map_chart_type(self, looker_type: str) -> Optional[str]:
        """Map Looker chart type to DataHub chart type."""
        type_mapping = {
            "looker_column": ChartTypeClass.BAR,
            "looker_scatter": ChartTypeClass.SCATTER,
            "looker_line": ChartTypeClass.LINE,
            "looker_area": ChartTypeClass.AREA,
            "looker_pie": ChartTypeClass.PIE,
            "looker_donut_multiples": ChartTypeClass.PIE,
            "looker_funnel": ChartTypeClass.BAR,
            "looker_timeline": ChartTypeClass.BAR,
            "looker_waterfall": ChartTypeClass.BAR,
            "looker_single_record": ChartTypeClass.TABLE,
            "looker_grid": ChartTypeClass.TABLE,
            "looker_boxplot": ChartTypeClass.BOX_PLOT,
            "area": ChartTypeClass.AREA,
            "bar": ChartTypeClass.BAR,
            "column": ChartTypeClass.BAR,
            "line": ChartTypeClass.LINE,
            "pie": ChartTypeClass.PIE,
            "scatter": ChartTypeClass.SCATTER,
            "table": ChartTypeClass.TABLE,
            "text": ChartTypeClass.TEXT,
            "single_value": ChartTypeClass.TEXT,
        }
        return type_mapping.get(looker_type.lower() if looker_type else "")

    def _get_enriched_input_fields(
        self, element: DashboardElement
    ) -> List[InputFieldClass]:
        """Get enriched input fields from a dashboard element, using explore metadata."""
        query = self._get_chart_query(element)
        if not query:
            return []

        input_fields = self._get_input_fields_from_query(query)
        if not input_fields:
            return []

        explore = None
        explore_urn = None
        explore_fields_map: Dict[str, Any] = {}
        if query.model and query.view:
            cache_key = (query.model, query.view)
            explore = self._ctx.explore_cache.get(cache_key)
            explore_urn = builder.make_dataset_urn_with_platform_instance(
                platform=self._ctx.platform,
                name=f"{query.model}.explore.{query.view}",
                platform_instance=self._ctx.config.platform_instance,
                env=self._ctx.config.env,
            )
            if explore and explore.fields:
                for dim in explore.fields.dimensions or []:
                    if dim.name:
                        explore_fields_map[dim.name] = dim
                for meas in explore.fields.measures or []:
                    if meas.name:
                        explore_fields_map[meas.name] = meas

        input_field_classes = []
        for field in input_fields:
            if explore_urn and field.name in explore_fields_map:
                api_field = explore_fields_map[field.name]
                view_field = self._api_field_to_view_field(api_field)
                schema_field = LookerUtil.view_field_to_schema_field(
                    view_field,
                    self._ctx.reporter,
                    self._ctx.config.tag_measures_and_dimensions,
                )
                parent_urn = explore_urn
            elif field.view_field:
                schema_field = LookerUtil.view_field_to_schema_field(
                    field.view_field, self._ctx.reporter
                )
                parent_urn = explore_urn or ""
            else:
                schema_field = SchemaFieldClass(
                    fieldPath=field.name,
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="string",
                )
                parent_urn = explore_urn or ""

            if parent_urn:
                input_field_classes.append(
                    InputFieldClass(
                        schemaFieldUrn=builder.make_schema_field_urn(
                            parent_urn=parent_urn,
                            field_path=field.name,
                        ),
                        schemaField=schema_field,
                    )
                )

        return input_field_classes

    def _api_field_to_view_field(
        self, api_field: "LookmlModelExploreField"
    ) -> ViewField:
        """Convert a LookML API explore field to a ViewField."""
        field_type = ViewFieldType.DIMENSION
        if api_field.category == "measure":
            field_type = ViewFieldType.MEASURE
        elif api_field.dimension_group:
            field_type = ViewFieldType.DIMENSION_GROUP

        return ViewField(
            name=api_field.name or "",
            label=api_field.label_short,
            description=api_field.description or "",
            type=api_field.type or "",
            field_type=field_type,
            is_primary_key=api_field.primary_key or False,
            tags=list(api_field.tags) if api_field.tags else [],
            group_label=api_field.field_group_label,
        )

    def _extract_chart_input_fields(
        self, element: DashboardElement, chart: Chart
    ) -> List[MetadataChangeProposalWrapper]:
        """Extract input fields from a dashboard element's query."""
        query = self._get_chart_query(element)
        if query and query.model and query.view:
            key = (query.model, query.view)
            if key not in self._ctx.reachable_explores:
                self._ctx.reachable_explores[key] = []
            self._ctx.reachable_explores[key].append(f"chart:{element.id}")
        if element.result_maker and element.result_maker.filterables:
            for filterable in element.result_maker.filterables:
                if filterable.view and filterable.model:
                    key = (filterable.model, filterable.view)
                    if key not in self._ctx.reachable_explores:
                        self._ctx.reachable_explores[key] = []
                    self._ctx.reachable_explores[key].append(f"chart:{element.id}")

        input_field_classes = self._get_enriched_input_fields(element)
        if not input_field_classes:
            return []

        return [
            MetadataChangeProposalWrapper(
                entityUrn=str(chart.urn),
                aspect=InputFieldsClass(fields=input_field_classes),
            )
        ]

    def _get_input_fields_from_query(self, query: Query) -> List[InputFieldElement]:
        """Extract input fields from a Looker query."""
        result: List[InputFieldElement] = []

        try:
            dynamic_fields = json.loads(query.dynamic_fields or "[]")
        except (JSONDecodeError, TypeError):
            dynamic_fields = []

        for field in dynamic_fields:
            if "table_calculation" in field:
                result.append(
                    InputFieldElement(
                        name=field["table_calculation"],
                        view_field=ViewField(
                            name=field["table_calculation"],
                            label=field.get("label"),
                            field_type=ViewFieldType.UNKNOWN,
                            type="string",
                            description="",
                        ),
                    )
                )
            if "measure" in field:
                based_on = field.get("based_on")
                if based_on is not None:
                    result.append(
                        InputFieldElement(
                            name=based_on,
                            view_field=None,
                            model=query.model or "",
                            explore=query.view or "",
                        )
                    )
                result.append(
                    InputFieldElement(
                        name=field["measure"],
                        view_field=ViewField(
                            name=field["measure"],
                            label=field.get("label"),
                            field_type=ViewFieldType.MEASURE,
                            type="string",
                            description="",
                        ),
                    )
                )
            if "dimension" in field:
                result.append(
                    InputFieldElement(
                        name=field["dimension"],
                        view_field=ViewField(
                            name=field["dimension"],
                            label=field.get("label"),
                            field_type=ViewFieldType.DIMENSION,
                            type="string",
                            description="",
                        ),
                    )
                )

        for f in query.fields or []:
            if f:
                result.append(
                    InputFieldElement(
                        name=f,
                        view_field=None,
                        model=query.model or "",
                        explore=query.view or "",
                    )
                )

        for f in query.filters or {}:
            if f:
                result.append(
                    InputFieldElement(
                        name=f,
                        view_field=None,
                        model=query.model or "",
                        explore=query.view or "",
                    )
                )

        return result

    def _make_dashboard_urn(self, dashboard_id: str) -> str:
        """Build dashboard URN, respecting include_platform_instance_in_urns."""
        platform_instance: Optional[str] = None
        if self._ctx.config.include_platform_instance_in_urns:
            platform_instance = self._ctx.config.platform_instance
        return builder.make_dashboard_urn(
            name=dashboard_id,
            platform=self._ctx.platform,
            platform_instance=platform_instance,
        )

    def _make_chart_urn(self, element_id: str) -> str:
        """Build chart URN, respecting include_platform_instance_in_urns."""
        platform_instance: Optional[str] = None
        if self._ctx.config.include_platform_instance_in_urns:
            platform_instance = self._ctx.config.platform_instance
        return builder.make_chart_urn(
            name=element_id,
            platform=self._ctx.platform,
            platform_instance=platform_instance,
        )
