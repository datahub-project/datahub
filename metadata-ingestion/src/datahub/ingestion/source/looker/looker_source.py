import concurrent.futures
import datetime
import json
import logging
import os
from json import JSONDecodeError
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
    cast,
)

from looker_sdk.error import SDKError
from looker_sdk.sdk.api31.models import Dashboard, DashboardElement, FolderBase, Query
from pydantic import Field, validator

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import AllowDenyPattern, ConfigurationError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    SourceCapability,
    SourceReport,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.looker import looker_usage
from datahub.ingestion.source.looker.looker_common import (
    InputFieldElement,
    LookerCommonConfig,
    LookerDashboard,
    LookerDashboardElement,
    LookerDashboardSourceReport,
    LookerExplore,
    LookerExploreRegistry,
    LookerUser,
    LookerUserRegistry,
    LookerUtil,
    ViewField,
    ViewFieldType,
)
from datahub.ingestion.source.looker.looker_lib_wrapper import (
    LookerAPI,
    LookerAPIConfig,
)
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
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
    BrowsePathsClass,
    ChangeTypeClass,
    ChartInfoClass,
    ChartTypeClass,
    DashboardInfoClass,
    InputFieldClass,
    InputFieldsClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)
from datahub.utilities.source_helpers import (
    auto_stale_entity_removal,
    auto_status_aspect,
)

logger = logging.getLogger(__name__)


class LookerDashboardSourceConfig(
    LookerAPIConfig, LookerCommonConfig, StatefulIngestionConfigBase
):
    dashboard_pattern: AllowDenyPattern = Field(
        AllowDenyPattern.allow_all(),
        description="Patterns for selecting dashboard ids that are to be included",
    )
    chart_pattern: AllowDenyPattern = Field(
        AllowDenyPattern.allow_all(),
        description="Patterns for selecting chart ids that are to be included",
    )
    include_deleted: bool = Field(
        False, description="Whether to include deleted dashboards."
    )
    extract_owners: bool = Field(
        True,
        description="When enabled, extracts ownership from Looker directly. When disabled, ownership is left empty for dashboards and charts.",
    )
    actor: Optional[str] = Field(
        None,
        description="This config is deprecated in favor of `extract_owners`. Previously, was the actor to use in ownership properties of ingested metadata.",
    )
    strip_user_ids_from_email: bool = Field(
        False,
        description="When enabled, converts Looker user emails of the form name@domain.com to urn:li:corpuser:name when assigning ownership",
    )
    skip_personal_folders: bool = Field(
        False,
        description="Whether to skip ingestion of dashboards in personal folders. Setting this to True will only ingest dashboards in the Shared folder space.",
    )
    max_threads: int = Field(
        os.cpu_count() or 40,
        description="Max parallelism for Looker API calls. Defaults to cpuCount or 40",
    )
    external_base_url: Optional[str] = Field(
        None,
        description="Optional URL to use when constructing external URLs to Looker if the `base_url` is not the correct one to use. For example, `https://looker-public.company.com`. If not provided, the external base URL will default to `base_url`.",
    )
    extract_usage_history: bool = Field(
        False,
        description="Whether to ingest usage statistics for dashboards. Setting this to True will query looker system activity explores to fetch historical dashboard usage.",
    )
    # TODO - stateful ingestion to autodetect usage history interval
    extract_usage_history_for_interval: str = Field(
        "30 days",
        description="Used only if extract_usage_history is set to True. Interval to extract looker dashboard usage history for. See https://docs.looker.com/reference/filter-expressions#date_and_time.",
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None, description=""
    )

    @validator("external_base_url", pre=True, always=True)
    def external_url_defaults_to_api_config_base_url(
        cls, v: Optional[str], *, values: Dict[str, Any], **kwargs: Dict[str, Any]
    ) -> Optional[str]:
        return v or values.get("base_url")

    @validator("platform_instance")
    def platform_instance_not_supported(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            raise ConfigurationError("Looker Source doesn't support platform instances")
        return v


@platform_name("Looker")
@support_status(SupportStatus.CERTIFIED)
@config_class(LookerDashboardSourceConfig)
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(
    SourceCapability.OWNERSHIP, "Enabled by default, configured using `extract_owners`"
)
@capability(
    SourceCapability.USAGE_STATS, "Can be enabled using `extract_usage_history`"
)
class LookerDashboardSource(TestableSource, StatefulIngestionSourceBase):
    """
    This plugin extracts the following:
    - Looker dashboards, dashboard elements (charts) and explores
    - Names, descriptions, URLs, chart types, input explores for the charts
    - Schemas and input views for explores
    - Owners of dashboards

    :::note
    To get complete Looker metadata integration (including Looker views and lineage to the underlying warehouse tables), you must ALSO use the `lookml` module.
    :::
    """

    platform = "looker"
    source_config: LookerDashboardSourceConfig
    reporter: LookerDashboardSourceReport
    user_registry: LookerUserRegistry
    accessed_dashboards: int = 0
    resolved_user_ids: int = 0
    email_ids_missing: int = 0  # resolved users with missing email addresses

    def __init__(self, config: LookerDashboardSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.source_config = config
        self.reporter = LookerDashboardSourceReport()
        self.looker_api: LookerAPI = LookerAPI(self.source_config)
        self.user_registry = LookerUserRegistry(self.looker_api)
        self.explore_registry = LookerExploreRegistry(self.looker_api, self.reporter)
        self.reporter._looker_explore_registry = self.explore_registry
        self.reporter._looker_api = self.looker_api

        self.explores_to_fetch_set: Dict[Tuple[str, str], List[str]] = {}

        # Keep stat generators to generate entity stat aspect later
        stat_generator_config: looker_usage.StatGeneratorConfig = (
            looker_usage.StatGeneratorConfig(
                looker_api_wrapper=self.looker_api,
                looker_user_registry=self.user_registry,
                interval=self.source_config.extract_usage_history_for_interval,
                strip_user_ids_from_email=self.source_config.strip_user_ids_from_email,
                platform_name=self.source_config.platform_name,
                max_threads=self.source_config.max_threads,
            )
        )

        self.dashboard_stat_generator = looker_usage.create_stat_entity_generator(
            looker_usage.SupportedStatEntity.DASHBOARD,
            config=stat_generator_config,
        )

        self.chart_stat_generator = looker_usage.create_stat_entity_generator(
            looker_usage.SupportedStatEntity.CHART,
            config=stat_generator_config,
        )

        # Create and register the stateful ingestion use-case handlers.
        self.stale_entity_removal_handler = StaleEntityRemovalHandler(
            source=self,
            config=self.source_config,
            state_type_class=GenericCheckpointState,
            pipeline_name=self.ctx.pipeline_name,
            run_id=self.ctx.run_id,
        )

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        try:
            self = cast(
                LookerDashboardSource,
                LookerDashboardSource.create(
                    config_dict, PipelineContext("looker-test-connection")
                ),
            )
            test_report.basic_connectivity = CapabilityReport(capable=True)
            test_report.capability_report = {}

            permissions = self.looker_api.get_available_permissions()

            BASIC_INGEST_REQUIRED_PERMISSIONS = {
                # TODO: Make this a bit more granular.
                "access_data",
                "explore",
                "manage_models",
                "see_datagroups",
                "see_lookml",
                "see_lookml_dashboards",
                "see_looks",
                "see_pdts",
                "see_queries",
                "see_schedules",
                "see_sql",
                "see_user_dashboards",
                "see_users",
            }

            USAGE_INGEST_REQUIRED_PERMISSIONS = {
                "see_system_activity",
            }

            LookerDashboardSource._set_test_connection_capability(
                test_report,
                permissions,
                SourceCapability.DESCRIPTIONS,
                BASIC_INGEST_REQUIRED_PERMISSIONS,
            )
            LookerDashboardSource._set_test_connection_capability(
                test_report,
                permissions,
                SourceCapability.OWNERSHIP,
                BASIC_INGEST_REQUIRED_PERMISSIONS,
            )
            LookerDashboardSource._set_test_connection_capability(
                test_report,
                permissions,
                SourceCapability.USAGE_STATS,
                USAGE_INGEST_REQUIRED_PERMISSIONS,
            )
        except Exception as e:
            logger.exception(f"Failed to test connection due to {e}")
            test_report.internal_failure = True
            test_report.internal_failure_reason = f"{e}"

            if test_report.basic_connectivity is None:
                test_report.basic_connectivity = CapabilityReport(
                    capable=False, failure_reason=f"{e}"
                )

        return test_report

    @staticmethod
    def _set_test_connection_capability(
        test_report: TestConnectionReport,
        permissions: Set[str],
        perm: SourceCapability,
        required: Set[str],
    ) -> None:
        assert test_report.capability_report is not None

        if required.issubset(permissions):
            test_report.capability_report[perm] = CapabilityReport(capable=True)
        else:
            missing = required - permissions
            test_report.capability_report[perm] = CapabilityReport(
                capable=False,
                error_message=f"Missing required permissions: {', '.join(missing)}",
            )

    @staticmethod
    def _extract_view_from_field(field: str) -> str:
        assert (
            field.count(".") == 1
        ), f"Error: A field must be prefixed by a view name, field is: {field}"
        return field.split(".")[0]

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

    def _get_input_fields_from_query(
        self, query: Optional[Query]
    ) -> List[InputFieldElement]:
        if query is None:
            return []
        result = []

        # query.dynamic_fields can contain:
        # - looker table calculations: https://docs.looker.com/exploring-data/using-table-calculations
        # - looker custom measures: https://docs.looker.com/de/exploring-data/adding-fields/custom-measure
        # - looker custom dimensions: https://docs.looker.com/exploring-data/adding-fields/custom-measure#creating_a_custom_dimension_using_a_looker_expression
        try:
            dynamic_fields = json.loads(
                query.dynamic_fields if query.dynamic_fields is not None else "[]"
            )
        except JSONDecodeError as e:
            logger.warning(
                f"Json load failed on loading dynamic field with error: {e}. The field value was: {query.dynamic_fields}"
            )
            dynamic_fields = []

        for field in dynamic_fields:
            if "table_calculation" in field:
                result.append(
                    InputFieldElement(
                        name=field["table_calculation"],
                        view_field=ViewField(
                            name=field["table_calculation"],
                            label=field["label"],
                            field_type=ViewFieldType.UNKNOWN,
                            type="string",
                            description="",
                        ),
                    )
                )
            if "measure" in field:
                # for measure, we can also make sure to index the underlying field that the measure uses
                based_on = field["based_on"]
                if based_on is not None:
                    result.append(
                        InputFieldElement(
                            based_on,
                            view_field=None,
                            model=query.model,
                            explore=query.view,
                        )
                    )
                result.append(
                    InputFieldElement(
                        name=field["measure"],
                        view_field=ViewField(
                            name=field["measure"],
                            label=field["label"],
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
                            label=field["label"],
                            field_type=ViewFieldType.DIMENSION,
                            type="string",
                            description="",
                        ),
                    )
                )

        # A query uses fields defined in explores, find the metadata about that field
        fields: Sequence[str] = query.fields if query.fields is not None else []
        for field in fields:
            if field is None:
                continue

            # we haven't loaded in metadata about the explore yet, so we need to wait until explores are populated later to fetch this
            result.append(
                InputFieldElement(
                    name=field, view_field=None, model=query.model, explore=query.view
                )
            )

        # A query uses fields for filtering and those fields are defined in views, find the views those fields use
        filters: MutableMapping[str, Any] = (
            query.filters if query.filters is not None else {}
        )
        for field in filters.keys():
            if field is None:
                continue

            # we haven't loaded in metadata about the explore yet, so we need to wait until explores are populated later to fetch this
            result.append(
                InputFieldElement(
                    name=field, view_field=None, model=query.model, explore=query.view
                )
            )

        return result

    def add_explore_to_fetch(self, model: str, explore: str, via: str) -> None:
        if (model, explore) not in self.explores_to_fetch_set:
            self.explores_to_fetch_set[(model, explore)] = []

        self.explores_to_fetch_set[(model, explore)].append(via)

    def _get_looker_dashboard_element(  # noqa: C901
        self, element: DashboardElement
    ) -> Optional[LookerDashboardElement]:
        # Dashboard elements can use raw usage_queries against explores
        explores: List[str]
        input_fields: List[InputFieldElement]

        if element.id is None:
            raise ValueError("Element ID can't be None")

        if element.query is not None:
            input_fields = self._get_input_fields_from_query(element.query)

            # Get the explore from the view directly
            explores = [element.query.view] if element.query.view is not None else []
            logger.debug(
                "Element {}: Explores added: {}".format(element.title, explores)
            )
            for exp in explores:
                self.add_explore_to_fetch(
                    model=element.query.model,
                    explore=exp,
                    via=f"look:{element.look_id}:query:{element.dashboard_id}",
                )

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
                input_fields=input_fields,
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
                input_fields = self._get_input_fields_from_query(element.look.query)
                if element.look.query.view is not None:
                    explores = [element.look.query.view]
                logger.debug(f"Element {title}: Explores added: {explores}")
                for exp in explores:
                    self.add_explore_to_fetch(
                        model=element.look.query.model,
                        explore=exp,
                        via=f"Look:{element.look_id}:query:{element.dashboard_id}",
                    )

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
                    input_fields=input_fields,
                )

        # Failing the above two approaches, pick out details from result_maker
        elif element.result_maker is not None:
            model: str = ""
            input_fields = []

            explores = []
            if element.result_maker.query is not None:
                model = element.result_maker.query.model
                if element.result_maker.query.view is not None:
                    explores.append(element.result_maker.query.view)
                input_fields = self._get_input_fields_from_query(
                    element.result_maker.query
                )

                logger.debug(
                    "Element {}: Explores added: {}".format(element.title, explores)
                )

                for exp in explores:
                    self.add_explore_to_fetch(
                        model=element.result_maker.query.model,
                        explore=exp,
                        via=f"Look:{element.look_id}:resultmaker:query",
                    )

            # In addition to the query, filters can point to fields as well
            assert element.result_maker.filterables is not None
            for filterable in element.result_maker.filterables:
                if filterable.view is not None and filterable.model is not None:
                    model = filterable.model
                    explores.append(filterable.view)
                    self.add_explore_to_fetch(
                        model=filterable.model,
                        explore=filterable.view,
                        via=f"Look:{element.look_id}:resultmaker:filterable",
                    )
                listen = filterable.listen
                query = element.result_maker.query
                if listen is not None:
                    for listener in listen:
                        if listener.field is not None:
                            input_fields.append(
                                InputFieldElement(
                                    listener.field,
                                    view_field=None,
                                    model=query.model if query is not None else "",
                                    explore=query.view if query is not None else "",
                                )
                            )

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
                input_fields=input_fields,
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
            aspects=[Status(removed=False)],
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
                    sorted(set(field.name for field in dashboard_element.input_fields))
                )
                if dashboard_element.input_fields
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
    ) -> Iterable[Union[MetadataChangeEvent, MetadataChangeProposalWrapper]]:
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.source_config.max_threads
        ) as async_executor:
            self.reporter.total_explores = len(self.explores_to_fetch_set)

            explore_futures = {
                async_executor.submit(self.fetch_one_explore, model, explore): (
                    model,
                    explore,
                )
                for (model, explore) in self.explores_to_fetch_set
            }

            for future in concurrent.futures.as_completed(explore_futures):
                events, explore_id, start_time, end_time = future.result()
                del explore_futures[future]
                self.reporter.explores_scanned += 1
                yield from events
                self.reporter.report_upstream_latency(start_time, end_time)
                logger.debug(
                    f"Running time of fetch_one_explore for {explore_id}: {(end_time - start_time).total_seconds()}"
                )

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
        looker_explore = self.explore_registry.get_explore(model, explore)
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
    ) -> Iterable[Union[MetadataChangeEvent, MetadataChangeProposalWrapper]]:
        chart_mces = [
            self._make_chart_mce(element, looker_dashboard)
            for element in looker_dashboard.dashboard_elements
            if element.type == "vis"
        ]
        for chart_mce in chart_mces:
            yield chart_mce

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
            lastModified=self._get_change_audit_stamps(looker_dashboard),
            dashboardUrl=looker_dashboard.url(self.source_config.external_base_url),
        )

        dashboard_snapshot.aspects.append(dashboard_info)
        if looker_dashboard.folder_path is not None:
            browse_path = BrowsePathsClass(
                paths=[f"/looker/{looker_dashboard.folder_path}"]
            )
            dashboard_snapshot.aspects.append(browse_path)

        ownership = self.get_ownership(looker_dashboard)
        if ownership is not None:
            dashboard_snapshot.aspects.append(ownership)

        dashboard_snapshot.aspects.append(Status(removed=looker_dashboard.is_deleted))

        dashboard_mce = MetadataChangeEvent(proposedSnapshot=dashboard_snapshot)
        yield dashboard_mce

    def get_ownership(
        self, looker_dashboard: LookerDashboard
    ) -> Optional[OwnershipClass]:
        if looker_dashboard.owner is not None:
            owner_urn = looker_dashboard.owner.get_urn(
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

    def _get_change_audit_stamps(
        self, looker_dashboard: LookerDashboard
    ) -> ChangeAuditStamps:
        change_audit_stamp: ChangeAuditStamps = ChangeAuditStamps()
        if looker_dashboard.created_at is not None:
            change_audit_stamp.created.time = round(
                looker_dashboard.created_at.timestamp() * 1000
            )
        if looker_dashboard.owner is not None:
            owner_urn = looker_dashboard.owner.get_urn(
                self.source_config.strip_user_ids_from_email
            )
            if owner_urn:
                change_audit_stamp.created.actor = owner_urn
        if looker_dashboard.last_updated_at is not None:
            change_audit_stamp.lastModified.time = round(
                looker_dashboard.last_updated_at.timestamp() * 1000
            )
        if looker_dashboard.last_updated_by is not None:
            updated_by_urn = looker_dashboard.last_updated_by.get_urn(
                self.source_config.strip_user_ids_from_email
            )
            if updated_by_urn:
                change_audit_stamp.lastModified.actor = updated_by_urn
        if (
            looker_dashboard.is_deleted
            and looker_dashboard.deleted_by is not None
            and looker_dashboard.deleted_at is not None
        ):
            deleter_urn = looker_dashboard.deleted_by.get_urn(
                self.source_config.strip_user_ids_from_email
            )
            if deleter_urn:
                change_audit_stamp.deleted = AuditStamp(
                    actor=deleter_urn,
                    time=round(looker_dashboard.deleted_at.timestamp() * 1000),
                )

        return change_audit_stamp

    def _get_folder_path(self, folder: FolderBase, client: LookerAPI) -> str:
        assert folder.id
        ancestors = [
            ancestor.name for ancestor in client.folder_ancestors(folder_id=folder.id)
        ]
        return "/".join(ancestors + [folder.name])

    def _get_looker_dashboard(
        self, dashboard: Dashboard, client: LookerAPI
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

        looker_dashboard = LookerDashboard(
            id=dashboard.id,
            title=dashboard.title,
            description=dashboard.description,
            dashboard_elements=dashboard_elements,
            created_at=dashboard.created_at,
            is_deleted=dashboard.deleted if dashboard.deleted is not None else False,
            is_hidden=dashboard.hidden if dashboard.hidden is not None else False,
            folder_path=dashboard_folder_path,
            owner=self._get_looker_user(dashboard.user_id),
            strip_user_ids_from_email=self.source_config.strip_user_ids_from_email,
            last_updated_at=dashboard.updated_at,
            last_updated_by=self._get_looker_user(dashboard.last_updater_id),
            deleted_at=dashboard.deleted_at,
            deleted_by=self._get_looker_user(dashboard.deleter_id),
            favorite_count=dashboard.favorite_count,
            view_count=dashboard.view_count,
            last_viewed_at=dashboard.last_viewed_at,
        )
        return looker_dashboard

    def _get_looker_user(self, user_id: Optional[int]) -> Optional[LookerUser]:
        user = (
            self.user_registry.get_by_id(
                user_id,
            )
            if self.source_config.extract_owners and user_id is not None
            else None
        )

        if user is not None and self.source_config.extract_owners:
            # Keep track of how many user ids we were able to resolve
            self.resolved_user_ids += 1
            if user.email is None:
                self.email_ids_missing += 1

        return user

    def process_metrics_dimensions_and_fields_for_dashboard(
        self, dashboard: LookerDashboard
    ) -> List[MetadataWorkUnit]:
        chart_mcps = [
            self._make_metrics_dimensions_chart_mcp(element, dashboard)
            for element in dashboard.dashboard_elements
        ]
        dashboard_mcp = self._make_metrics_dimensions_dashboard_mcp(dashboard)

        mcps = chart_mcps
        mcps.append(dashboard_mcp)

        workunits = [
            MetadataWorkUnit(
                id=f"looker-{mcp.aspectName}-{mcp.entityUrn}",
                mcp=mcp,
                treat_errors_as_warnings=True,
            )
            for mcp in mcps
        ]

        return workunits

    def _input_fields_from_dashboard_element(
        self, dashboard_element: LookerDashboardElement
    ) -> List[InputFieldClass]:
        input_fields = (
            dashboard_element.input_fields
            if dashboard_element.input_fields is not None
            else []
        )

        fields_for_mcp = []

        # enrich the input_fields with the fully hydrated ViewField from the now fetched explores
        for input_field in input_fields:
            entity_urn = builder.make_chart_urn(
                self.source_config.platform_name, dashboard_element.get_urn_element_id()
            )
            view_field_for_reference = input_field.view_field

            if input_field.view_field is None:
                explore = self.explore_registry.get_explore(
                    input_field.model, input_field.explore
                )
                if explore is not None:
                    # add this to the list of explores to finally generate metadata for
                    self.add_explore_to_fetch(
                        input_field.model, input_field.explore, entity_urn
                    )
                    entity_urn = explore.get_explore_urn(self.source_config)
                    explore_fields = (
                        explore.fields if explore.fields is not None else []
                    )

                    relevant_field = next(
                        (
                            field
                            for field in explore_fields
                            if field.name == input_field.name
                        ),
                        None,
                    )
                    if relevant_field is not None:
                        view_field_for_reference = relevant_field

            if view_field_for_reference is not None:
                fields_for_mcp.append(
                    InputFieldClass(
                        schemaFieldUrn=builder.make_schema_field_urn(
                            entity_urn, view_field_for_reference.name
                        ),
                        schemaField=LookerUtil.view_field_to_schema_field(
                            view_field_for_reference,
                            self.reporter,
                            self.source_config.tag_measures_and_dimensions,
                        ),
                    )
                )

        return fields_for_mcp

    def _make_metrics_dimensions_dashboard_mcp(
        self, dashboard: LookerDashboard
    ) -> MetadataChangeProposalWrapper:
        dashboard_urn = builder.make_dashboard_urn(
            self.source_config.platform_name, dashboard.get_urn_dashboard_id()
        )
        all_fields = []
        for dashboard_element in dashboard.dashboard_elements:
            all_fields.extend(
                self._input_fields_from_dashboard_element(dashboard_element)
            )

        input_fields_aspect = InputFieldsClass(fields=all_fields)

        return MetadataChangeProposalWrapper(
            entityType="dashboard",
            entityUrn=dashboard_urn,
            changeType=ChangeTypeClass.UPSERT,
            aspectName="inputFields",
            aspect=input_fields_aspect,
        )

    def _make_metrics_dimensions_chart_mcp(
        self, dashboard_element: LookerDashboardElement, dashboard: LookerDashboard
    ) -> MetadataChangeProposalWrapper:
        chart_urn = builder.make_chart_urn(
            self.source_config.platform_name, dashboard_element.get_urn_element_id()
        )
        input_fields_aspect = InputFieldsClass(
            fields=self._input_fields_from_dashboard_element(dashboard_element)
        )

        return MetadataChangeProposalWrapper(
            entityType="chart",
            entityUrn=chart_urn,
            changeType=ChangeTypeClass.UPSERT,
            aspectName="inputFields",
            aspect=input_fields_aspect,
        )

    def process_dashboard(
        self, dashboard_id: str, fields: List[str]
    ) -> Tuple[
        List[MetadataWorkUnit],
        Optional[Dashboard],
        str,
        datetime.datetime,
        datetime.datetime,
    ]:
        start_time = datetime.datetime.now()
        assert dashboard_id is not None
        if not self.source_config.dashboard_pattern.allowed(dashboard_id):
            self.reporter.report_dashboards_dropped(dashboard_id)
            return [], None, dashboard_id, start_time, datetime.datetime.now()
        try:
            dashboard_object: Dashboard = self.looker_api.dashboard(
                dashboard_id=dashboard_id,
                fields=fields,
            )
        except SDKError:
            # A looker dashboard could be deleted in between the list and the get
            self.reporter.report_warning(
                dashboard_id,
                f"Error occurred while loading dashboard {dashboard_id}. Skipping.",
            )
            return [], None, dashboard_id, start_time, datetime.datetime.now()

        if self.source_config.skip_personal_folders:
            if dashboard_object.folder is not None and (
                dashboard_object.folder.is_personal
                or dashboard_object.folder.is_personal_descendant
            ):
                self.reporter.report_warning(
                    dashboard_id, "Dropped due to being a personal folder"
                )
                self.reporter.report_dashboards_dropped(dashboard_id)
                return [], None, dashboard_id, start_time, datetime.datetime.now()

        looker_dashboard = self._get_looker_dashboard(dashboard_object, self.looker_api)
        mces = self._make_dashboard_and_chart_mces(looker_dashboard)
        workunits = [
            MetadataWorkUnit(id=f"looker-{mce.proposedSnapshot.urn}", mce=mce)
            if isinstance(mce, MetadataChangeEvent)
            else MetadataWorkUnit(
                id=f"looker-{mce.aspectName}-{mce.entityUrn}", mcp=mce
            )
            for mce in mces
        ]

        # add on metrics, dimensions, fields events
        metric_dim_workunits = self.process_metrics_dimensions_and_fields_for_dashboard(
            looker_dashboard
        )
        workunits.extend(metric_dim_workunits)
        self.reporter.report_dashboards_scanned()
        return (
            workunits,
            dashboard_object,
            dashboard_id,
            start_time,
            datetime.datetime.now(),
        )

    def extract_usage_stat(
        self, looker_dashboards: List[looker_usage.LookerDashboardForUsage]
    ) -> List[MetadataChangeProposalWrapper]:
        mcps: List[MetadataChangeProposalWrapper] = []
        looks: List[looker_usage.LookerChartForUsage] = []
        # filter out look from all dashboard
        for dashboard in looker_dashboards:
            if dashboard.looks is None:
                continue
            looks.extend(dashboard.looks)

        # dedup looks
        looks = list({str(look.id): look for look in looks}.values())

        usage_stat_generators = [
            self.dashboard_stat_generator(
                cast(List[looker_usage.ModelForUsage], looker_dashboards), self.reporter
            ),
            self.chart_stat_generator(
                cast(List[looker_usage.ModelForUsage], looks), self.reporter
            ),
        ]

        for usage_stat_generator in usage_stat_generators:
            for mcp in usage_stat_generator.generate_usage_stat_mcps():
                mcps.append(mcp)

        return mcps

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        return auto_stale_entity_removal(
            self.stale_entity_removal_handler,
            auto_status_aspect(self.get_workunits_internal()),
        )

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        self.reporter.report_stage_start("list_dashboards")
        dashboards = self.looker_api.all_dashboards(fields="id")
        deleted_dashboards = (
            self.looker_api.search_dashboards(fields="id", deleted="true")
            if self.source_config.include_deleted
            else []
        )
        if deleted_dashboards != []:
            logger.debug(f"Deleted Dashboards = {deleted_dashboards}")

        dashboard_ids = [dashboard_base.id for dashboard_base in dashboards]
        dashboard_ids.extend(
            [deleted_dashboard.id for deleted_dashboard in deleted_dashboards]
        )
        selected_dashboard_ids: List[Optional[str]] = []
        for id in dashboard_ids:
            if id is None:
                continue
            if not self.source_config.dashboard_pattern.allowed(id):
                self.reporter.report_dashboards_dropped(id)
            else:
                selected_dashboard_ids.append(id)
        dashboard_ids = selected_dashboard_ids
        self.reporter.report_stage_end("list_dashboards")
        self.reporter.report_total_dashboards(len(dashboard_ids))

        # List dashboard fields to extract for processing
        fields = [
            "id",
            "title",
            "dashboard_elements",
            "dashboard_filters",
            "deleted",
            "hidden",
            "description",
            "folder",
            "user_id",
            "created_at",
            "updated_at",
            "last_updater_id",
            "deleted_at",
            "deleter_id",
        ]
        if self.source_config.extract_usage_history:
            fields += [
                "favorite_count",
                "view_count",
                "last_viewed_at",
            ]

        looker_dashboards_for_usage: List[looker_usage.LookerDashboardForUsage] = []
        self.reporter.report_stage_start("dashboard_chart_metadata")

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.source_config.max_threads
        ) as async_executor:
            async_workunits = {}
            for dashboard_id in dashboard_ids:
                if dashboard_id is not None:
                    job = async_executor.submit(
                        self.process_dashboard, dashboard_id, fields
                    )
                    async_workunits[job] = dashboard_id

            for job in concurrent.futures.as_completed(async_workunits):
                (
                    work_units,
                    dashboard_object,
                    dashboard_id,
                    start_time,
                    end_time,
                ) = job.result()
                del async_workunits[job]
                logger.debug(
                    f"Running time of process_dashboard for {dashboard_id} = {(end_time - start_time).total_seconds()}"
                )
                self.reporter.report_upstream_latency(start_time, end_time)

                for mwu in work_units:
                    yield mwu
                    self.reporter.report_workunit(mwu)
                if dashboard_object is not None:
                    looker_dashboards_for_usage.append(
                        looker_usage.LookerDashboardForUsage.from_dashboard(
                            dashboard_object
                        )
                    )

        self.reporter.report_stage_end("dashboard_chart_metadata")

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

        self.reporter.report_stage_start("explore_metadata")
        for event in self._make_explore_metadata_events():
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
                raise Exception(f"Unexpected type of event {event}")
            self.reporter.report_workunit(workunit)
            yield workunit
        self.reporter.report_stage_end("explore_metadata")

        if (
            self.source_config.tag_measures_and_dimensions
            and self.reporter.explores_scanned > 0
        ):
            # Emit tag MCEs for measures and dimensions if we produced any explores:
            for tag_mce in LookerUtil.get_tag_mces():
                workunit = MetadataWorkUnit(
                    id=f"tag-{tag_mce.proposedSnapshot.urn}",
                    mce=tag_mce,
                )
                self.reporter.report_workunit(workunit)
                yield workunit

        # Extract usage history is enabled
        if self.source_config.extract_usage_history:
            self.reporter.report_stage_start("usage_extraction")
            usage_mcps: List[MetadataChangeProposalWrapper] = self.extract_usage_stat(
                looker_dashboards_for_usage
            )
            for usage_mcp in usage_mcps:
                workunit = MetadataWorkUnit(
                    id=f"looker-{usage_mcp.aspectName}-{usage_mcp.entityUrn}-{usage_mcp.aspect.timestampMillis}",  # type:ignore
                    mcp=usage_mcp,
                )
                self.reporter.report_workunit(workunit)
                yield workunit
            self.reporter.report_stage_end("usage_extraction")

    def get_report(self) -> SourceReport:
        return self.reporter

    def get_platform_instance_id(self) -> str:
        return self.source_config.platform_instance or self.platform

    def close(self):
        self.prepare_for_commit()
