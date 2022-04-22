import concurrent.futures
import datetime
import json
import logging
import os
import re
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    MutableMapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

import looker_sdk
from looker_sdk.error import SDKError
from looker_sdk.rtl.transport import TransportOptions
from looker_sdk.sdk.api31.methods import Looker31SDK
from looker_sdk.sdk.api31.models import (
    Dashboard,
    DashboardElement,
    FolderBase,
    Query,
    User,
)
from pydantic import validator

import datahub.emitter.mce_builder as builder
from datahub.configuration import ConfigModel
from datahub.configuration.common import AllowDenyPattern, ConfigurationError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.looker_common import (
    LookerCommonConfig,
    LookerExplore,
    LookerUtil,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import ChangeAuditStamps, Status
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
    ChartSnapshot,
    DashboardSnapshot,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    ChartInfoClass,
    ChartTypeClass,
    DashboardInfoClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)

logger = logging.getLogger(__name__)


class TransportOptionsConfig(ConfigModel):
    timeout: int
    headers: MutableMapping[str, str]

    def get_transport_options(self) -> TransportOptions:
        return TransportOptions(timeout=self.timeout, headers=self.headers)


class LookerAPIConfig(ConfigModel):
    client_id: str
    client_secret: str
    base_url: str
    transport_options: Optional[TransportOptionsConfig]


class LookerAPI:
    """A holder class for a Looker client"""

    def __init__(self, config: LookerAPIConfig) -> None:
        # The Looker SDK looks wants these as environment variables
        os.environ["LOOKERSDK_CLIENT_ID"] = config.client_id
        os.environ["LOOKERSDK_CLIENT_SECRET"] = config.client_secret
        os.environ["LOOKERSDK_BASE_URL"] = config.base_url

        self.client = looker_sdk.init31()

        # try authenticating current user to check connectivity
        # (since it's possible to initialize an invalid client without any complaints)
        try:
            self.client.me(
                transport_options=config.transport_options.get_transport_options()
                if config.transport_options is not None
                else None
            )
        except SDKError as e:
            raise ConfigurationError(
                "Failed to initialize Looker client. Please check your configuration."
            ) from e

    def get_client(self) -> Looker31SDK:
        return self.client


class LookerDashboardSourceConfig(LookerAPIConfig, LookerCommonConfig):
    actor: Optional[str]
    dashboard_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    chart_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    include_deleted: bool = False
    extract_owners: bool = True
    strip_user_ids_from_email: bool = False
    skip_personal_folders: bool = False
    max_threads: int = os.cpu_count() or 40
    external_base_url: Optional[str]

    @validator("external_base_url", pre=True, always=True)
    def external_url_defaults_to_api_config_base_url(
        cls, v: Optional[str], *, values: Dict[str, Any], **kwargs: Dict[str, Any]
    ) -> str:
        return v or values["base_url"]

    @validator("platform_instance")
    def platform_instance_not_supported(cls, v: str) -> str:
        raise ConfigurationError("Looker Source doesn't support platform instances")


@dataclass
class LookerDashboardSourceReport(SourceReport):
    dashboards_scanned: int = 0
    charts_scanned: int = 0
    filtered_dashboards: List[str] = dataclass_field(default_factory=list)
    filtered_charts: List[str] = dataclass_field(default_factory=list)
    upstream_start_time: Optional[datetime.datetime] = None
    upstream_end_time: Optional[datetime.datetime] = None
    upstream_total_latency_in_seconds: Optional[float] = None

    def report_dashboards_scanned(self) -> None:
        self.dashboards_scanned += 1

    def report_charts_scanned(self) -> None:
        self.charts_scanned += 1

    def report_dashboards_dropped(self, model: str) -> None:
        self.filtered_dashboards.append(model)

    def report_charts_dropped(self, view: str) -> None:
        self.filtered_charts.append(view)

    def report_upstream_latency(
        self, start_time: datetime.datetime, end_time: datetime.datetime
    ) -> None:
        if self.upstream_start_time is None or self.upstream_start_time > start_time:
            self.upstream_start_time = start_time
        if self.upstream_end_time is None or self.upstream_end_time < end_time:
            self.upstream_end_time = end_time
        self.upstream_total_latency_in_seconds = (
            self.upstream_end_time - self.upstream_start_time
        ).total_seconds()


@dataclass
class LookerDashboardElement:
    id: str
    title: str
    query_slug: str
    upstream_explores: List[LookerExplore]
    look_id: Optional[str]
    type: Optional[str] = None
    description: Optional[str] = None
    upstream_fields: Optional[List[str]] = None

    def url(self, base_url: str) -> str:
        # A dashboard element can use a look or just a raw query against an explore
        # If the base_url contains a port number (like https://company.looker.com:19999) remove the port number
        m = re.match("^(.*):([0-9]+)$", base_url)
        if m is not None:
            base_url = m.group(1)
        if self.look_id is not None:
            return base_url + "/looks/" + self.look_id
        else:
            return base_url + "/x/" + self.query_slug

    def get_urn_element_id(self):
        # A dashboard element can use a look or just a raw query against an explore
        return f"dashboard_elements.{self.id}"

    def get_view_urns(self, config: LookerCommonConfig) -> List[str]:
        return [v.get_explore_urn(config) for v in self.upstream_explores]


@dataclass
class LookerUser:
    id: int
    email: Optional[str]
    display_name: Optional[str]
    first_name: Optional[str]
    last_name: Optional[str]

    @classmethod
    def _from_user(cls, raw_user: User) -> "LookerUser":
        assert raw_user.id is not None
        return LookerUser(
            raw_user.id,
            raw_user.email,
            raw_user.display_name,
            raw_user.first_name,
            raw_user.last_name,
        )

    def _get_urn(self, strip_user_ids_from_email: bool) -> Optional[str]:
        if self.email is not None:
            if strip_user_ids_from_email:
                return builder.make_user_urn(self.email.split("@")[0])
            else:
                return builder.make_user_urn(self.email)
        return None


class LookerUserRegistry:
    user_map: Dict[int, LookerUser]
    client: Looker31SDK
    fields: str = ",".join(["id", "email", "display_name", "first_name", "last_name"])

    def __init__(self, client: Looker31SDK):
        self.client = client
        self.user_map = {}

    def get_by_id(
        self, id: int, transport_options: Optional[TransportOptions]
    ) -> Optional[LookerUser]:
        logger.debug("Will get user {}".format(id))
        if id in self.user_map:
            return self.user_map[id]
        else:
            try:
                raw_user: User = self.client.user(
                    id,
                    fields=self.fields,
                    transport_options=transport_options,
                )
                looker_user = LookerUser._from_user(raw_user)
                self.user_map[id] = looker_user
                return looker_user
            except SDKError as e:
                logger.warn("Could not find user with id {}".format(id))
                logger.warn("Failure was {}".format(e))
                return None


@dataclass
class LookerDashboard:
    id: str
    title: str
    dashboard_elements: List[LookerDashboardElement]
    created_at: Optional[datetime.datetime]
    description: Optional[str] = None
    folder_path: Optional[str] = None
    is_deleted: bool = False
    is_hidden: bool = False
    owner: Optional[LookerUser] = None
    strip_user_ids_from_email: Optional[bool] = True

    def url(self, base_url):
        # If the base_url contains a port number (like https://company.looker.com:19999) remove the port number
        m = re.match("^(.*):([0-9]+)$", base_url)
        if m is not None:
            base_url = m.group(1)
        return base_url + "/dashboards/" + self.id

    def get_urn_dashboard_id(self):
        return f"dashboards.{self.id}"


class LookerDashboardSource(Source):
    source_config: LookerDashboardSourceConfig
    reporter: LookerDashboardSourceReport
    client: Looker31SDK
    user_registry: LookerUserRegistry
    explore_set: Set[Tuple[str, str]] = set()
    accessed_dashboards: int = 0
    resolved_user_ids: int = 0
    email_ids_missing: int = 0  # resolved users with missing email addresses

    def __init__(self, config: LookerDashboardSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_config = config
        self.reporter = LookerDashboardSourceReport()
        self.client = LookerAPI(self.source_config).get_client()
        self.user_registry = LookerUserRegistry(self.client)

    @staticmethod
    def _extract_view_from_field(field: str) -> str:
        assert (
            field.count(".") == 1
        ), f"Error: A field must be prefixed by a view name, field is: {field}"
        view_name = field.split(".")[0]
        return view_name

    def _get_views_from_fields(self, fields: List[str]) -> List[str]:
        field_set = set(fields)
        views = set()
        for field_name in field_set:
            try:
                view_name = self._extract_view_from_field(field_name)
                views.add(view_name)
            except AssertionError:
                self.reporter.report_warning(
                    key=f"chart-field-{field_name}",
                    reason="The field was not prefixed by a view name. This can happen when the field references another dynamic field.",
                )
                continue

        return list(views)

    def _get_fields_from_query(self, query: Optional[Query]) -> List[str]:
        if query is None:
            return []

        all_fields = set()

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

            all_fields.add(field_name)

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
            all_fields.add(field_name)

        return list(all_fields)

    def _get_looker_dashboard_element(  # noqa: C901
        self, element: DashboardElement
    ) -> Optional[LookerDashboardElement]:
        # Dashboard elements can use raw queries against explores
        explores: List[str]
        fields: List[str]

        if element.id is None:
            raise ValueError("Element ID can't be None")

        if element.query is not None:
            explores = []
            fields = self._get_fields_from_query(element.query)
            if element.query.view is not None:
                # Get the explore from the view directly
                explores = [element.query.view]

            logger.debug(
                "Element {}: Explores added: {}".format(element.title, explores)
            )
            for exp in explores:
                self.explore_set.add((element.query.model, exp))
            return LookerDashboardElement(
                id=element.id,
                title=element.title if element.title is not None else "",
                type=element.type,
                description=element.subtitle_text,
                look_id=None,
                query_slug=element.query.slug if element.query.slug is not None else "",
                upstream_explores=[
                    LookerExplore(model_name=element.query.model, name=exp)
                    for exp in explores
                ],
                upstream_fields=fields,
            )

        # Dashboard elements can *alternatively* link to an existing look
        elif element.look is not None:
            # we pick from element title by default, falling back to look title.
            title: str = (
                element.title
                if element.title is not None and element.title != ""
                else element.look.title
                if element.look.title is not None
                else ""
            )
            if element.look.query is not None:
                fields = self._get_fields_from_query(element.look.query)
                if element.look.query.view is not None:
                    explores = [element.look.query.view]
                logger.debug(
                    "Element {}: Explores added: {}".format(element.title, explores)
                )
                for exp in explores:
                    self.explore_set.add((element.look.query.model, exp))

                if element.look.query and element.look.query.slug:
                    slug = element.look.query.slug
                else:
                    slug = ""
                return LookerDashboardElement(
                    id=element.id,
                    title=title,
                    type=element.type,
                    description=element.subtitle_text,
                    look_id=element.look_id,
                    query_slug=slug,
                    upstream_explores=[
                        LookerExplore(model_name=element.look.query.model, name=exp)
                        for exp in explores
                    ],
                    upstream_fields=fields,
                )

        # Failing the above two approaches, pick out details from result_maker
        elif element.result_maker is not None:
            model: str = ""
            fields = []
            explores = []
            if element.result_maker.query is not None:
                model = element.result_maker.query.model
                if element.result_maker.query.view is not None:
                    explores.append(element.result_maker.query.view)
                fields = self._get_fields_from_query(element.result_maker.query)
                logger.debug(
                    "Element {}: Explores added: {}".format(element.title, explores)
                )

                for exp in explores:
                    self.explore_set.add((element.result_maker.query.model, exp))

            # In addition to the query, filters can point to fields as well
            assert element.result_maker.filterables is not None
            for filterable in element.result_maker.filterables:
                if filterable.view is not None and filterable.model is not None:
                    model = filterable.model
                    explores.append(filterable.view)
                    self.explore_set.add((filterable.model, filterable.view))
                listen = filterable.listen
                if listen is not None:
                    for listener in listen:
                        if listener.field is not None:
                            fields.append(listener.field)

            explores = list(set(explores))  # dedup the list of views

            return LookerDashboardElement(
                id=element.id,
                title=element.title if element.title is not None else "",
                type=element.type,
                description=element.subtitle_text,
                look_id=element.look_id,
                query_slug=element.result_maker.query.slug
                if element.result_maker.query is not None
                and element.result_maker.query.slug is not None
                else "",
                upstream_explores=[
                    LookerExplore(model_name=model, name=exp) for exp in explores
                ],
                upstream_fields=fields,
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
        self, dashboard_element: LookerDashboardElement, dashboard: LookerDashboard
    ) -> MetadataChangeEvent:
        chart_urn = builder.make_chart_urn(
            self.source_config.platform_name, dashboard_element.get_urn_element_id()
        )
        chart_snapshot = ChartSnapshot(
            urn=chart_urn,
            aspects=[],
        )

        chart_type = self._get_chart_type(dashboard_element)
        chart_info = ChartInfoClass(
            type=chart_type,
            description=dashboard_element.description or "",
            title=dashboard_element.title or "",
            lastModified=ChangeAuditStamps(),
            chartUrl=dashboard_element.url(self.source_config.external_base_url or ""),
            inputs=dashboard_element.get_view_urns(self.source_config),
            customProperties={
                "upstream_fields": ",".join(
                    sorted(set(dashboard_element.upstream_fields))
                )
                if dashboard_element.upstream_fields
                else ""
            },
        )
        chart_snapshot.aspects.append(chart_info)

        ownership = self.get_ownership(dashboard)
        if ownership is not None:
            chart_snapshot.aspects.append(ownership)

        return MetadataChangeEvent(proposedSnapshot=chart_snapshot)

    def _make_explore_metadata_events(
        self,
    ) -> List[Union[MetadataChangeEvent, MetadataChangeProposalWrapper]]:
        explore_events: List[
            Union[MetadataChangeEvent, MetadataChangeProposalWrapper]
        ] = []
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.source_config.max_threads
        ) as async_executor:
            explore_futures = [
                async_executor.submit(self.fetch_one_explore, model, explore)
                for (model, explore) in self.explore_set
            ]
            for future in concurrent.futures.as_completed(explore_futures):
                events, explore_id, start_time, end_time = future.result()
                explore_events.extend(events)
                self.reporter.report_upstream_latency(start_time, end_time)
                logger.debug(
                    f"Running time of fetch_one_explore for {explore_id}: {(end_time - start_time).total_seconds()}"
                )

        return explore_events

    def fetch_one_explore(
        self, model: str, explore: str
    ) -> Tuple[
        List[Union[MetadataChangeEvent, MetadataChangeProposalWrapper]],
        str,
        datetime.datetime,
        datetime.datetime,
    ]:
        start_time = datetime.datetime.now()
        events: List[Union[MetadataChangeEvent, MetadataChangeProposalWrapper]] = []
        looker_explore = LookerExplore.from_api(
            model,
            explore,
            self.client,
            self.reporter,
            transport_options=self.source_config.transport_options.get_transport_options()
            if self.source_config.transport_options is not None
            else None,
        )
        if looker_explore is not None:
            events = (
                looker_explore._to_metadata_events(
                    self.source_config, self.reporter, self.source_config.base_url
                )
                or events
            )

        return events, f"{model}:{explore}", start_time, datetime.datetime.now()

    def _make_dashboard_and_chart_mces(
        self, looker_dashboard: LookerDashboard
    ) -> List[MetadataChangeEvent]:
        chart_mces = [
            self._make_chart_mce(element, looker_dashboard)
            for element in looker_dashboard.dashboard_elements
            if element.type == "vis"
        ]

        dashboard_urn = builder.make_dashboard_urn(
            self.source_config.platform_name, looker_dashboard.get_urn_dashboard_id()
        )
        dashboard_snapshot = DashboardSnapshot(
            urn=dashboard_urn,
            aspects=[],
        )

        dashboard_info = DashboardInfoClass(
            description=looker_dashboard.description or "",
            title=looker_dashboard.title,
            charts=[mce.proposedSnapshot.urn for mce in chart_mces],
            lastModified=ChangeAuditStamps(),
            dashboardUrl=looker_dashboard.url(self.source_config.external_base_url),
        )

        dashboard_snapshot.aspects.append(dashboard_info)
        if looker_dashboard.folder_path is not None:
            browse_path = BrowsePathsClass(
                paths=[f"/looker/{looker_dashboard.folder_path}/{looker_dashboard.id}"]
            )
            dashboard_snapshot.aspects.append(browse_path)

        ownership = self.get_ownership(looker_dashboard)
        if ownership is not None:
            dashboard_snapshot.aspects.append(ownership)

        dashboard_snapshot.aspects.append(Status(removed=looker_dashboard.is_deleted))

        dashboard_mce = MetadataChangeEvent(proposedSnapshot=dashboard_snapshot)

        return chart_mces + [dashboard_mce]

    def get_ownership(
        self, looker_dashboard: LookerDashboard
    ) -> Optional[OwnershipClass]:
        if looker_dashboard.owner is not None:
            owner_urn = looker_dashboard.owner._get_urn(
                self.source_config.strip_user_ids_from_email
            )
            if owner_urn is not None:
                ownership: OwnershipClass = OwnershipClass(
                    owners=[
                        OwnerClass(
                            owner=owner_urn,
                            type=OwnershipTypeClass.DATAOWNER,
                        )
                    ]
                )
                return ownership
        return None

    folder_path_cache: Dict[str, str] = {}

    def _get_folder_path(self, folder: FolderBase, client: Looker31SDK) -> str:
        assert folder.id
        if not self.folder_path_cache.get(folder.id):
            ancestors = [
                ancestor.name
                for ancestor in client.folder_ancestors(
                    folder.id,
                    fields="name",
                    transport_options=self.source_config.transport_options.get_transport_options()
                    if self.source_config.transport_options is not None
                    else None,
                )
            ]
            self.folder_path_cache[folder.id] = "/".join(ancestors + [folder.name])
        return self.folder_path_cache[folder.id]

    def _get_looker_dashboard(
        self, dashboard: Dashboard, client: Looker31SDK
    ) -> LookerDashboard:

        self.accessed_dashboards += 1
        if dashboard.folder is None:
            logger.debug(f"{dashboard.id} has no folder")
        dashboard_folder_path = None
        if dashboard.folder is not None:
            dashboard_folder_path = self._get_folder_path(dashboard.folder, client)
        dashboard_elements: List[LookerDashboardElement] = []
        elements = (
            dashboard.dashboard_elements
            if dashboard.dashboard_elements is not None
            else []
        )
        for element in elements:
            self.reporter.report_charts_scanned()
            if element.id is not None and not self.source_config.chart_pattern.allowed(
                element.id
            ):
                self.reporter.report_charts_dropped(element.id)
                continue
            looker_dashboard_element = self._get_looker_dashboard_element(element)
            if looker_dashboard_element is not None:
                dashboard_elements.append(looker_dashboard_element)

        if dashboard.id is None or dashboard.title is None:
            raise ValueError("Both dashboard ID and title are None")

        dashboard_owner = (
            self.user_registry.get_by_id(
                dashboard.user_id,
                self.source_config.transport_options.get_transport_options()
                if self.source_config.transport_options is not None
                else None,
            )
            if self.source_config.extract_owners and dashboard.user_id is not None
            else None
        )

        if dashboard_owner is not None and self.source_config.extract_owners:
            # Keep track of how many user ids we were able to resolve
            self.resolved_user_ids += 1
            if dashboard_owner.email is None:
                self.email_ids_missing += 1

        looker_dashboard = LookerDashboard(
            id=dashboard.id,
            title=dashboard.title,
            description=dashboard.description,
            dashboard_elements=dashboard_elements,
            created_at=dashboard.created_at,
            is_deleted=dashboard.deleted if dashboard.deleted is not None else False,
            is_hidden=dashboard.deleted if dashboard.deleted is not None else False,
            folder_path=dashboard_folder_path,
            owner=dashboard_owner,
            strip_user_ids_from_email=self.source_config.strip_user_ids_from_email,
        )
        return looker_dashboard

    def process_dashboard(
        self, dashboard_id: str
    ) -> Tuple[List[MetadataWorkUnit], str, datetime.datetime, datetime.datetime]:
        start_time = datetime.datetime.now()
        assert dashboard_id is not None
        self.reporter.report_dashboards_scanned()
        if not self.source_config.dashboard_pattern.allowed(dashboard_id):
            self.reporter.report_dashboards_dropped(dashboard_id)
            return [], dashboard_id, start_time, datetime.datetime.now()
        try:
            fields = [
                "id",
                "title",
                "dashboard_elements",
                "dashboard_filters",
                "deleted",
                "description",
                "folder",
                "user_id",
            ]
            dashboard_object = self.client.dashboard(
                dashboard_id=dashboard_id,
                fields=",".join(fields),
                transport_options=self.source_config.transport_options.get_transport_options()
                if self.source_config.transport_options is not None
                else None,
            )
        except SDKError:
            # A looker dashboard could be deleted in between the list and the get
            self.reporter.report_warning(
                dashboard_id,
                f"Error occurred while loading dashboard {dashboard_id}. Skipping.",
            )
            return [], dashboard_id, start_time, datetime.datetime.now()

        if self.source_config.skip_personal_folders:
            if dashboard_object.folder is not None and (
                dashboard_object.folder.is_personal
                or dashboard_object.folder.is_personal_descendant
            ):
                self.reporter.report_warning(
                    dashboard_id, "Dropped due to being a personal folder"
                )
                self.reporter.report_dashboards_dropped(dashboard_id)
                return [], dashboard_id, start_time, datetime.datetime.now()

        looker_dashboard = self._get_looker_dashboard(dashboard_object, self.client)
        mces = self._make_dashboard_and_chart_mces(looker_dashboard)
        # for mce in mces:
        workunits = [
            MetadataWorkUnit(id=f"looker-{mce.proposedSnapshot.urn}", mce=mce)
            for mce in mces
        ]
        return workunits, dashboard_id, start_time, datetime.datetime.now()

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        dashboards = self.client.all_dashboards(
            fields="id",
            transport_options=self.source_config.transport_options.get_transport_options()
            if self.source_config.transport_options is not None
            else None,
        )
        deleted_dashboards = (
            self.client.search_dashboards(
                fields="id",
                deleted="true",
                transport_options=self.source_config.transport_options.get_transport_options()
                if self.source_config.transport_options is not None
                else None,
            )
            if self.source_config.include_deleted
            else []
        )
        if deleted_dashboards != []:
            logger.debug(f"Deleted Dashboards = {deleted_dashboards}")

        dashboard_ids = [dashboard_base.id for dashboard_base in dashboards]
        dashboard_ids.extend(
            [deleted_dashboard.id for deleted_dashboard in deleted_dashboards]
        )

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.source_config.max_threads
        ) as async_executor:
            async_workunits = [
                async_executor.submit(self.process_dashboard, dashboard_id)
                for dashboard_id in dashboard_ids
            ]
            for async_workunit in concurrent.futures.as_completed(async_workunits):
                work_units, dashboard_id, start_time, end_time = async_workunit.result()
                logger.debug(
                    f"Running time of process_dashboard for {dashboard_id} = {(end_time - start_time).total_seconds()}"
                )
                self.reporter.report_upstream_latency(start_time, end_time)
                for mwu in work_units:
                    yield mwu
                    self.reporter.report_workunit(mwu)

        if (
            self.source_config.extract_owners
            and self.resolved_user_ids > 0
            and self.email_ids_missing == self.resolved_user_ids
        ):
            # Looks like we tried to extract owners and could not find their email addresses. This is likely a permissions issue
            self.reporter.report_warning(
                "api",
                "Failed to extract owners emails for any dashboards. Please enable the see_users permission for your Looker API key",
            )

        explore_events = self._make_explore_metadata_events()
        for event in explore_events:
            if isinstance(event, MetadataChangeEvent):
                workunit = MetadataWorkUnit(
                    id=f"looker-{event.proposedSnapshot.urn}", mce=event
                )
            elif isinstance(event, MetadataChangeProposalWrapper):
                # We want to treat subtype aspects as optional, so allowing failures in this aspect to be treated as warnings rather than failures
                workunit = MetadataWorkUnit(
                    id=f"looker-{event.entityUrn}-{event.aspectName}",
                    mcp=event,
                    treat_errors_as_warnings=True
                    if event.aspectName in ["subTypes"]
                    else False,
                )
            else:
                raise Exception("Unexpected type of event {}".format(event))

            self.reporter.report_workunit(workunit)
            yield workunit

        if self.source_config.tag_measures_and_dimensions and explore_events != []:
            # Emit tag MCEs for measures and dimensions if we produced any explores:
            for tag_mce in LookerUtil.get_tag_mces():
                workunit = MetadataWorkUnit(
                    id=f"tag-{tag_mce.proposedSnapshot.urn}",
                    mce=tag_mce,
                )
                self.reporter.report_workunit(workunit)
                yield workunit

    @classmethod
    def create(cls, config_dict, ctx):
        config = LookerDashboardSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_report(self) -> SourceReport:
        return self.reporter
