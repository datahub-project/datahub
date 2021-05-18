import datetime
import json
import logging
import os
import time
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any, Iterable, List, MutableMapping, Optional, Sequence

import looker_sdk
from looker_sdk.error import SDKError
from looker_sdk.sdk.api31.models import (
    Dashboard,
    DashboardElement,
    LookWithQuery,
    Query,
)

from datahub.configuration import ConfigModel
from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.source.metadata_common import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    AuditStamp,
    ChangeAuditStamps,
    Status,
)
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
    ChartSnapshot,
    DashboardSnapshot,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChartInfoClass,
    ChartTypeClass,
    DashboardInfoClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)

logger = logging.getLogger(__name__)


class LookerDashboardSourceConfig(ConfigModel):
    client_id: str
    client_secret: str
    base_url: str
    platform_name: str = "looker"
    # The datahub platform where looker views are stored, must be the same as `platform_name` in lookml source
    view_platform_name: str = "looker_views"
    actor: str = "urn:li:corpuser:etl"
    dashboard_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    chart_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    env: str = "PROD"


@dataclass
class LookerDashboardSourceReport(SourceReport):
    dashboards_scanned: int = 0
    charts_scanned: int = 0
    filtered_dashboards: List[str] = dataclass_field(default_factory=list)
    filtered_charts: List[str] = dataclass_field(default_factory=list)

    def report_dashboards_scanned(self) -> None:
        self.dashboards_scanned += 1

    def report_charts_scanned(self) -> None:
        self.charts_scanned += 1

    def report_dashboards_dropped(self, model: str) -> None:
        self.filtered_dashboards.append(model)

    def report_charts_dropped(self, view: str) -> None:
        self.filtered_charts.append(view)


@dataclass
class LookerDashboardElement:
    id: str
    title: str
    query_slug: str
    looker_views: List[str]
    look_id: Optional[str]
    type: Optional[str] = None
    description: Optional[str] = None

    def url(self, base_url: str) -> str:
        # A dashboard element can use a look or just a raw query against an explore
        if self.look_id is not None:
            return base_url + "/looks/" + self.look_id
        else:
            return base_url + "/x/" + self.query_slug

    def get_urn_element_id(self):
        # A dashboard element can use a look or just a raw query against an explore
        return f"dashboard_elements.{self.id}"

    def get_view_urns(self, platform_name: str) -> List[str]:
        return [
            f"urn:li:dataset:(urn:li:dataPlatform:{platform_name},{v},PROD)"
            for v in self.looker_views
        ]


@dataclass
class LookerDashboard:
    id: str
    title: str
    dashboard_elements: List[LookerDashboardElement]
    created_at: Optional[datetime.datetime]
    description: Optional[str] = None
    is_deleted: bool = False
    is_hidden: bool = False

    def url(self, base_url):
        return base_url + "/dashboards/" + self.id

    def get_urn_dashboard_id(self):
        return f"dashboards.{self.id}"


class LookerDashboardSource(Source):
    source_config: LookerDashboardSourceConfig
    report = LookerDashboardSourceReport()

    def __init__(self, config: LookerDashboardSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_config = config
        self.reporter = LookerDashboardSourceReport()

    @staticmethod
    def _extract_view_from_field(field: str) -> str:
        assert (
            field.count(".") == 1
        ), f"Error: A field must be prefixed by a view name, field is: {field}"
        view_name = field.split(".")[0]
        return view_name

    def _get_views_from_query(self, query: Optional[Query]) -> List[str]:
        if query is None:
            return []

        all_views = set()

        # query.dynamic_fields can contain:
        # - looker table calculations: https://docs.looker.com/exploring-data/using-table-calculations
        # - looker custom measures: https://docs.looker.com/de/exploring-data/adding-fields/custom-measure
        # - looker custom dimensions: https://docs.looker.com/exploring-data/adding-fields/custom-measure#creating_a_custom_dimension_using_a_looker_expression
        dynamic_fields = json.loads(
            query.dynamic_fields if query.dynamic_fields is not None else "[]"
        )
        custom_field_to_underlying_field = {}
        for field in dynamic_fields:
            # Table calculations can only reference fields used in the fields section, so this will always be a subset of of the query.fields
            if "table_calculation" in field:
                continue
            # Looker custom measures can reference fields in arbitrary views, so this needs to be parsed to find the underlying view field the custom measure is based on
            if "measure" in field:
                measure = field["measure"]
                based_on = field["based_on"]
                custom_field_to_underlying_field[measure] = based_on

            # Looker custom dimensions can reference fields in arbitrary views, so this needs to be parsed to find the underlying view field the custom measure is based on
            # However, unlike custom measures custom dimensions can be defined using an arbitrary expression
            # We are not going to support parsing arbitrary Looker expressions here, so going to ignore these fields for now
            if "dimension" in field:
                dimension = field["dimension"]
                expression = field["expression"]  # noqa: F841
                custom_field_to_underlying_field[dimension] = None

        # A query uses fields defined in views, find the views those fields use
        fields: Sequence[str] = query.fields if query.fields is not None else []
        for field in fields:
            # If the field is a custom field, look up the field it is based on
            field_name = (
                custom_field_to_underlying_field[field]
                if field in custom_field_to_underlying_field
                else field
            )
            if field_name is None:
                continue

            try:
                view_name = self._extract_view_from_field(field_name)
            except AssertionError:
                self.reporter.report_warning(
                    key=f"chart-field-{field_name}",
                    reason="The field was not prefixed by a view name. This can happen when the field references another dynamic field.",
                )
                continue
            all_views.add(view_name)

        # A query uses fields for filtering and those fields are defined in views, find the views those fields use
        filters: MutableMapping[str, Any] = (
            query.filters if query.filters is not None else {}
        )
        for field in filters.keys():
            # If the field is a custom field, look up the field it is based on
            field_name = (
                custom_field_to_underlying_field[field]
                if field in custom_field_to_underlying_field
                else field
            )
            if field_name is None:
                continue
            try:
                view_name = self._extract_view_from_field(field_name)
            except AssertionError:
                self.reporter.report_warning(
                    f"chart-field-{field_name}",
                    "The field was not prefixed by a view name. This can happen when the field references another dynamic field.",
                )
                continue
            all_views.add(view_name)

        return list(all_views)

    def _get_views_from_look(self, look: LookWithQuery) -> List[str]:
        return self._get_views_from_query(look.query)

    def _get_looker_dashboard_element(
        self,
        element: DashboardElement,
    ) -> Optional[LookerDashboardElement]:
        # Dashboard elements can use raw queries against explores
        if element.id is None:
            raise ValueError("Element ID can't be None")

        if element.query is not None:
            views = self._get_views_from_query(element.query)
            return LookerDashboardElement(
                id=element.id,
                title=element.title if element.title is not None else "",
                type=element.type,
                description=element.subtitle_text,
                look_id=None,
                query_slug=element.query.slug if element.query.slug is not None else "",
                looker_views=views,
            )

        # Dashboard elements can *alternatively* link to an existing look
        if element.look is not None:
            views = self._get_views_from_look(element.look)
            if element.look.query and element.look.query.slug:
                slug = element.look.query.slug
            else:
                slug = ""
            return LookerDashboardElement(
                id=element.id,
                title=element.title if element.title is not None else "",
                type=element.type,
                description=element.subtitle_text,
                look_id=element.look_id,
                query_slug=slug,
                looker_views=views,
            )

        return None

    def _get_chart_type(
        self, dashboard_element: LookerDashboardElement
    ) -> Optional[str]:
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
            "text": ChartTypeClass.TEXT,
            "single_value": ChartTypeClass.TEXT,
            "looker_single_record": ChartTypeClass.TABLE,
            "table": ChartTypeClass.TABLE,
            "looker_grid": ChartTypeClass.TABLE,
            "looker_map": None,
            "looker_geo_coordinates": None,
            "looker_geo_choropleth": None,
            "looker_boxplot": ChartTypeClass.BOX_PLOT,
            "vis": None,
        }
        type_str = dashboard_element.type
        if not type_str:
            self.reporter.report_warning(
                key=f"looker-chart-{dashboard_element.id}",
                reason=f"Chart type {type_str} is missing. Setting to None",
            )
            return None
        try:
            chart_type = type_mapping[type_str]
        except KeyError:
            self.reporter.report_warning(
                key=f"looker-chart-{dashboard_element.id}",
                reason=f"Chart type {type_str} not supported. Setting to None",
            )
            chart_type = None

        return chart_type

    def _make_chart_mce(
        self, dashboard_element: LookerDashboardElement
    ) -> MetadataChangeEvent:
        actor = self.source_config.actor
        sys_time = int(time.time()) * 1000
        chart_urn = f"urn:li:chart:({self.source_config.platform_name},{dashboard_element.get_urn_element_id()})"
        chart_snapshot = ChartSnapshot(
            urn=chart_urn,
            aspects=[],
        )

        last_modified = ChangeAuditStamps(
            created=AuditStamp(time=sys_time, actor=actor),
            lastModified=AuditStamp(time=sys_time, actor=actor),
        )

        chart_type = self._get_chart_type(dashboard_element)

        chart_info = ChartInfoClass(
            type=chart_type,
            description=dashboard_element.description
            if dashboard_element.description is not None
            else "",
            title=dashboard_element.title
            if dashboard_element.title is not None
            else "",
            lastModified=last_modified,
            chartUrl=dashboard_element.url(self.source_config.base_url),
            inputs=dashboard_element.get_view_urns(self.source_config.platform_name),
        )
        chart_snapshot.aspects.append(chart_info)

        return MetadataChangeEvent(proposedSnapshot=chart_snapshot)

    def _make_dashboard_and_chart_mces(
        self, looker_dashboard: LookerDashboard
    ) -> List[MetadataChangeEvent]:
        actor = self.source_config.actor
        sys_time = int(time.time()) * 1000

        chart_mces = [
            self._make_chart_mce(element)
            for element in looker_dashboard.dashboard_elements
        ]

        dashboard_urn = f"urn:li:dashboard:({self.source_config.platform_name},{looker_dashboard.get_urn_dashboard_id()})"
        dashboard_snapshot = DashboardSnapshot(
            urn=dashboard_urn,
            aspects=[],
        )

        last_modified = ChangeAuditStamps(
            created=AuditStamp(time=sys_time, actor=actor),
            lastModified=AuditStamp(time=sys_time, actor=actor),
        )

        dashboard_info = DashboardInfoClass(
            description=looker_dashboard.description
            if looker_dashboard.description is not None
            else "",
            title=looker_dashboard.title,
            charts=[mce.proposedSnapshot.urn for mce in chart_mces],
            lastModified=last_modified,
            dashboardUrl=looker_dashboard.url(self.source_config.base_url),
        )

        dashboard_snapshot.aspects.append(dashboard_info)
        owners = [OwnerClass(owner=actor, type=OwnershipTypeClass.DATAOWNER)]
        dashboard_snapshot.aspects.append(
            OwnershipClass(
                owners=owners,
                lastModified=AuditStampClass(
                    time=sys_time, actor=self.source_config.actor
                ),
            )
        )
        dashboard_snapshot.aspects.append(Status(removed=looker_dashboard.is_deleted))

        dashboard_mce = MetadataChangeEvent(proposedSnapshot=dashboard_snapshot)

        return chart_mces + [dashboard_mce]

    def _get_looker_dashboard(self, dashboard: Dashboard) -> LookerDashboard:
        dashboard_elements: List[LookerDashboardElement] = []
        elements = (
            dashboard.dashboard_elements
            if dashboard.dashboard_elements is not None
            else []
        )
        for element in elements:
            self.reporter.report_charts_scanned()
            if element.id is not None and self.source_config.chart_pattern.allowed(
                element.id
            ):
                self.reporter.report_charts_dropped(element.id)
                continue
            looker_dashboard_element = self._get_looker_dashboard_element(element)
            if looker_dashboard_element is not None:
                dashboard_elements.append(looker_dashboard_element)

        if dashboard.id is None or dashboard.title is None:
            raise ValueError("Both dashboard ID and title are None")

        looker_dashboard = LookerDashboard(
            id=dashboard.id,
            title=dashboard.title,
            description=dashboard.description,
            dashboard_elements=dashboard_elements,
            created_at=dashboard.created_at,
            is_deleted=dashboard.deleted if dashboard.deleted is not None else False,
            is_hidden=dashboard.deleted if dashboard.deleted is not None else False,
        )
        return looker_dashboard

    def _get_looker_client(self):
        # The Looker SDK looks wants these as environment variables
        os.environ["LOOKERSDK_CLIENT_ID"] = self.source_config.client_id
        os.environ["LOOKERSDK_CLIENT_SECRET"] = self.source_config.client_secret
        os.environ["LOOKERSDK_BASE_URL"] = self.source_config.base_url

        return looker_sdk.init31()

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        client = self._get_looker_client()
        dashboard_ids = [
            dashboard_base.id
            for dashboard_base in client.all_dashboards(fields="id")
            if dashboard_base.id is not None
        ]

        for dashboard_id in dashboard_ids:
            self.reporter.report_dashboards_scanned()
            if not self.source_config.dashboard_pattern.allowed(dashboard_id):
                self.reporter.report_dashboards_dropped(dashboard_id)
                continue
            try:
                fields = ["id", "title", "dashboard_elements", "dashboard_filters"]
                dashboard_object = client.dashboard(
                    dashboard_id=dashboard_id, fields=",".join(fields)
                )
            except SDKError:
                # A looker dashboard could be deleted in between the list and the get
                logger.warning(
                    f"Error occuried while loading dashboard {dashboard_id}. Skipping."
                )
                continue

            looker_dashboard = self._get_looker_dashboard(dashboard_object)
            mces = self._make_dashboard_and_chart_mces(looker_dashboard)
            for mce in mces:
                workunit = MetadataWorkUnit(
                    id=f"looker-{mce.proposedSnapshot.urn}", mce=mce
                )
                self.reporter.report_workunit(workunit)
                yield workunit

    @classmethod
    def create(cls, config_dict, ctx):
        config = LookerDashboardSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_report(self) -> SourceReport:
        return self.reporter
