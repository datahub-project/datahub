import datetime
import json
import logging
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
)

from looker_sdk.error import SDKError
from looker_sdk.rtl.serialize import DeserializeError
from looker_sdk.sdk.api40.models import (
    Dashboard,
    DashboardElement,
    Folder,
    FolderBase,
    Look,
    LookWithQuery,
    Query,
)

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import create_embed_mcp, gen_containers
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
    MetadataWorkUnitProcessor,
    SourceCapability,
    SourceReport,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    BIAssetSubTypes,
    BIContainerSubTypes,
)
from datahub.ingestion.source.looker import looker_usage
from datahub.ingestion.source.looker.looker_common import (
    InputFieldElement,
    LookerDashboard,
    LookerDashboardElement,
    LookerDashboardSourceReport,
    LookerExplore,
    LookerExploreRegistry,
    LookerFolder,
    LookerFolderKey,
    LookerUser,
    LookerUserRegistry,
    LookerUtil,
    ViewField,
    ViewFieldType,
    gen_model_key,
    get_urn_looker_element_id,
)
from datahub.ingestion.source.looker.looker_config import LookerDashboardSourceConfig
from datahub.ingestion.source.looker.looker_lib_wrapper import LookerAPI
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    AuditStamp,
    ChangeAuditStamps,
    DataPlatformInstance,
    Status,
)
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
    ChartSnapshot,
    DashboardSnapshot,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    BrowsePathEntryClass,
    BrowsePathsClass,
    BrowsePathsV2Class,
    ChartInfoClass,
    ChartTypeClass,
    ContainerClass,
    DashboardInfoClass,
    InputFieldClass,
    InputFieldsClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SubTypesClass,
)
from datahub.utilities.backpressure_aware_executor import BackpressureAwareExecutor

logger = logging.getLogger(__name__)


@platform_name("Looker")
@support_status(SupportStatus.CERTIFIED)
@config_class(LookerDashboardSourceConfig)
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Use the `platform_instance` field")
@capability(
    SourceCapability.OWNERSHIP, "Enabled by default, configured using `extract_owners`"
)
@capability(SourceCapability.LINEAGE_COARSE, "Supported by default")
@capability(
    SourceCapability.LINEAGE_FINE,
    "Enabled by default, configured using `extract_column_level_lineage`",
)
@capability(
    SourceCapability.USAGE_STATS,
    "Enabled by default, configured using `extract_usage_history`",
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

    def __init__(self, config: LookerDashboardSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.source_config: LookerDashboardSourceConfig = config
        self.reporter: LookerDashboardSourceReport = LookerDashboardSourceReport()
        self.looker_api: LookerAPI = LookerAPI(self.source_config)
        self.user_registry: LookerUserRegistry = LookerUserRegistry(
            self.looker_api, self.reporter
        )
        self.explore_registry: LookerExploreRegistry = LookerExploreRegistry(
            self.looker_api, self.reporter, self.source_config
        )
        self.reporter._looker_explore_registry = self.explore_registry
        self.reporter._looker_api = self.looker_api

        # Keep track of look-id which are reachable from Dashboard
        self.reachable_look_registry: Set[str] = set()

        # (model, explore) -> list of charts/looks/dashboards that reference this explore
        # The list values are used purely for debugging purposes.
        self.reachable_explores: Dict[Tuple[str, str], List[str]] = {}

        # To keep track of folders (containers) which have already been ingested
        # Required, as we do not ingest all folders but only those that have dashboards/looks
        self.processed_folders: List[str] = []

        # Keep track of ingested chart urns, to omit usage for non-ingested entities
        self.chart_urns: Set[str] = set()

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        try:
            config = LookerDashboardSourceConfig.parse_obj_allow_extras(config_dict)
            permissions = LookerAPI(config).get_available_permissions()

            test_report.basic_connectivity = CapabilityReport(capable=True)
            test_report.capability_report = {}

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
        assert field.count(".") == 1, (
            f"Error: A field must be prefixed by a view name, field is: {field}"
        )
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
                    title="Failed to Extract View Name from Field",
                    message="The field was not prefixed by a view name. This can happen when the field references another dynamic field.",
                    context=f"Field Name: {field_name}",
                )
                continue

        return list(views)

    def _get_input_fields_from_query(
        self, query: Optional[Query]
    ) -> List[InputFieldElement]:
        if query is None:
            return []
        result = []

        if query is not None:
            logger.debug(
                f"Processing query: model={query.model}, view={query.view}, input_fields_count={len(query.fields) if query.fields else 0}"
            )

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
                            label=field.get("label"),
                            field_type=ViewFieldType.UNKNOWN,
                            type="string",
                            description="",
                        ),
                    )
                )
            if "measure" in field:
                # for measure, we can also make sure to index the underlying field that the measure uses
                based_on = field.get("based_on")
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

        # A query uses fields defined in explores, find the metadata about that field
        fields: Sequence[str] = query.fields if query.fields is not None else []
        for field in fields:
            if field is None:
                continue

            # we haven't loaded in metadata about the explore yet, so we need to wait until explores are populated
            # later to fetch this
            result.append(
                InputFieldElement(
                    name=field, view_field=None, model=query.model, explore=query.view
                )
            )

        # A query uses fields for filtering, and those fields are defined in views, find the views those fields use
        filters: MutableMapping[str, Any] = (
            query.filters if query.filters is not None else {}
        )
        for field in filters:
            if field is None:
                continue

            # we haven't loaded in metadata about the explore yet, so we need to wait until explores are populated
            # later to fetch this
            result.append(
                InputFieldElement(
                    name=field, view_field=None, model=query.model, explore=query.view
                )
            )

        return result

    def add_reachable_explore(self, model: str, explore: str, via: str) -> None:
        if (model, explore) not in self.reachable_explores:
            self.reachable_explores[(model, explore)] = []

        self.reachable_explores[(model, explore)].append(via)

    def _get_looker_dashboard_element(
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
                f"Dashboard element {element.title} (ID: {element.id}): Upstream explores added via query={explores} with model={element.query.model}, explore={element.query.view}"
            )
            for exp in explores:
                logger.debug(
                    f"Adding reachable explore: model={element.query.model}, explore={exp}, element_id={element.id}, title={element.title}"
                )
                self.add_reachable_explore(
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
                owner=None,
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
                logger.debug(f"Element {title}: Explores added via look: {explores}")
                for exp in explores:
                    self.add_reachable_explore(
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
                    folder=(
                        self._get_looker_folder(element.look.folder)
                        if element.look.folder
                        else None
                    ),
                    folder_path=(
                        self._get_folder_path(element.look.folder, self.looker_api)
                        if element.look.folder
                        else None
                    ),
                    owner=self._get_looker_user(element.look.user_id),
                )

        # Failing the above two approaches, pick out details from result_maker
        elif element.result_maker is not None:
            input_fields = []

            explores = []
            if element.result_maker.query is not None:
                if element.result_maker.query.view is not None:
                    explores.append(element.result_maker.query.view)
                input_fields = self._get_input_fields_from_query(
                    element.result_maker.query
                )

                logger.debug(
                    f"Element {element.title}: Explores added via result_maker: {explores}"
                )

                for exp in explores:
                    self.add_reachable_explore(
                        model=element.result_maker.query.model,
                        explore=exp,
                        via=f"Look:{element.look_id}:resultmaker:query",
                    )

            # In addition to the query, filters can point to fields as well
            assert element.result_maker.filterables is not None

            # Different dashboard elements my reference explores from different models
            # so we need to create a mapping of explore names to their models to maintain correct associations
            explore_to_model_map = {}

            for filterable in element.result_maker.filterables:
                if filterable.view is not None and filterable.model is not None:
                    # Store the model for this view/explore in our mapping
                    explore_to_model_map[filterable.view] = filterable.model
                    explores.append(filterable.view)
                    self.add_reachable_explore(
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

            explores = sorted(list(set(explores)))  # dedup the list of views

            logger.debug(
                f"Dashboard element {element.id} and their explores with the corresponding model: {explore_to_model_map}"
            )

            # If we have a query, use its model as the default for any explores that don't have a model in our mapping
            default_model = ""
            if (
                element.result_maker.query is not None
                and element.result_maker.query.model is not None
            ):
                default_model = element.result_maker.query.model

            return LookerDashboardElement(
                id=element.id,
                title=element.title if element.title is not None else "",
                type=element.type,
                description=element.subtitle_text,
                look_id=element.look_id,
                query_slug=(
                    element.result_maker.query.slug
                    if element.result_maker.query is not None
                    and element.result_maker.query.slug is not None
                    else ""
                ),
                upstream_explores=[
                    LookerExplore(
                        model_name=explore_to_model_map.get(exp, default_model),
                        name=exp,
                    )
                    for exp in explores
                ],
                input_fields=input_fields,
                owner=None,
            )

        logger.debug(f"Element {element.title}: Unable to parse LookerDashboardElement")
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
            self.reporter.info(
                title="Unrecognized Chart Type",
                message="Chart is missing a chart type.",
                context=f"Chart Id: {dashboard_element.id}",
                log=False,
            )
            return None
        try:
            chart_type = type_mapping[type_str]
        except KeyError:
            self.reporter.info(
                title="Unrecognized Chart Type",
                message=f"Chart type {type_str} is not recognized. Setting to None",
                context=f"Chart Id: {dashboard_element.id}",
                log=False,
            )
            chart_type = None

        return chart_type

    def _get_folder_browse_path_v2_entries(
        self, folder: LookerFolder, include_current_folder: bool = True
    ) -> Iterable[BrowsePathEntryClass]:
        for ancestor in self.looker_api.folder_ancestors(folder_id=folder.id):
            assert ancestor.id
            urn = self._gen_folder_key(ancestor.id).as_urn()
            yield BrowsePathEntryClass(id=urn, urn=urn)

        urn = self._gen_folder_key(folder.id).as_urn()
        if include_current_folder:
            yield BrowsePathEntryClass(id=urn, urn=urn)

    def _create_platform_instance_aspect(
        self,
    ) -> DataPlatformInstance:
        assert self.source_config.platform_name, (
            "Platform name is not set in the configuration."
        )
        assert self.source_config.platform_instance, (
            "Platform instance is not set in the configuration."
        )

        return DataPlatformInstance(
            platform=builder.make_data_platform_urn(self.source_config.platform_name),
            instance=builder.make_dataplatform_instance_urn(
                platform=self.source_config.platform_name,
                instance=self.source_config.platform_instance,
            ),
        )

    def _make_chart_urn(self, element_id: str) -> str:
        platform_instance: Optional[str] = None
        if self.source_config.include_platform_instance_in_urns:
            platform_instance = self.source_config.platform_instance

        return builder.make_chart_urn(
            name=element_id,
            platform=self.source_config.platform_name,
            platform_instance=platform_instance,
        )

    def _make_chart_metadata_events(
        self,
        dashboard_element: LookerDashboardElement,
        dashboard: Optional[
            LookerDashboard
        ],  # dashboard will be None if this is a standalone look
    ) -> List[Union[MetadataChangeEvent, MetadataChangeProposalWrapper]]:
        chart_urn = self._make_chart_urn(
            element_id=dashboard_element.get_urn_element_id()
        )
        self.chart_urns.add(chart_urn)
        chart_snapshot = ChartSnapshot(
            urn=chart_urn,
            aspects=[Status(removed=False)],
        )
        browse_path_v2: Optional[BrowsePathsV2Class] = None

        chart_type = self._get_chart_type(dashboard_element)
        chart_info = ChartInfoClass(
            type=chart_type,
            description=dashboard_element.description or "",
            title=dashboard_element.title or "",
            lastModified=ChangeAuditStamps(),
            chartUrl=dashboard_element.url(self.source_config.external_base_url or ""),
            inputs=dashboard_element.get_view_urns(self.source_config),
            customProperties={
                "upstream_fields": (
                    ",".join(
                        sorted({field.name for field in dashboard_element.input_fields})
                    )
                    if dashboard_element.input_fields
                    else ""
                )
            },
        )
        chart_snapshot.aspects.append(chart_info)

        if (
            dashboard
            and dashboard.folder_path is not None
            and dashboard.folder is not None
        ):
            browse_path = BrowsePathsClass(
                paths=[f"/Folders/{dashboard.folder_path}/{dashboard.title}"]
            )
            chart_snapshot.aspects.append(browse_path)

            dashboard_urn = self.make_dashboard_urn(dashboard)
            browse_path_v2 = BrowsePathsV2Class(
                path=[
                    BrowsePathEntryClass("Folders"),
                    *self._get_folder_browse_path_v2_entries(dashboard.folder),
                    BrowsePathEntryClass(id=dashboard_urn, urn=dashboard_urn),
                ],
            )
        elif (
            dashboard is None
            and dashboard_element.folder_path is not None
            and dashboard_element.folder is not None
        ):  # independent look
            browse_path = BrowsePathsClass(
                paths=[f"/Folders/{dashboard_element.folder_path}"]
            )
            chart_snapshot.aspects.append(browse_path)
            browse_path_v2 = BrowsePathsV2Class(
                path=[
                    BrowsePathEntryClass("Folders"),
                    *self._get_folder_browse_path_v2_entries(dashboard_element.folder),
                ],
            )

        if dashboard is not None:
            ownership = self.get_ownership(dashboard)
            if ownership is not None:
                chart_snapshot.aspects.append(ownership)
        elif dashboard is None and dashboard_element is not None:
            ownership = self.get_ownership(dashboard_element)
            if ownership is not None:
                chart_snapshot.aspects.append(ownership)

        chart_mce = MetadataChangeEvent(proposedSnapshot=chart_snapshot)

        proposals: List[Union[MetadataChangeEvent, MetadataChangeProposalWrapper]] = [
            chart_mce,
            MetadataChangeProposalWrapper(
                entityUrn=chart_urn,
                aspect=SubTypesClass(typeNames=[BIAssetSubTypes.LOOKER_LOOK]),
            ),
        ]

        if self.source_config.include_platform_instance_in_urns:
            proposals.append(
                MetadataChangeProposalWrapper(
                    entityUrn=chart_urn,
                    aspect=self._create_platform_instance_aspect(),
                ),
            )

        # If extracting embeds is enabled, produce an MCP for embed URL.
        if (
            self.source_config.extract_embed_urls
            and self.source_config.external_base_url
        ):
            maybe_embed_url = dashboard_element.embed_url(
                self.source_config.external_base_url
            )
            if maybe_embed_url:
                proposals.append(
                    create_embed_mcp(
                        chart_snapshot.urn,
                        maybe_embed_url,
                    )
                )

        if dashboard is None and dashboard_element.folder:
            container = ContainerClass(
                container=self._gen_folder_key(dashboard_element.folder.id).as_urn(),
            )
            proposals.append(
                MetadataChangeProposalWrapper(entityUrn=chart_urn, aspect=container)
            )

        if browse_path_v2:
            proposals.append(
                MetadataChangeProposalWrapper(
                    entityUrn=chart_urn, aspect=browse_path_v2
                )
            )

        return proposals

    def _make_dashboard_metadata_events(
        self, looker_dashboard: LookerDashboard, chart_urns: List[str]
    ) -> List[Union[MetadataChangeEvent, MetadataChangeProposalWrapper]]:
        dashboard_urn = self.make_dashboard_urn(looker_dashboard)
        dashboard_snapshot = DashboardSnapshot(
            urn=dashboard_urn,
            aspects=[],
        )
        browse_path_v2: Optional[BrowsePathsV2Class] = None
        dashboard_info = DashboardInfoClass(
            description=looker_dashboard.description or "",
            title=looker_dashboard.title,
            charts=chart_urns,
            lastModified=self._get_change_audit_stamps(looker_dashboard),
            dashboardUrl=looker_dashboard.url(self.source_config.external_base_url),
        )

        dashboard_snapshot.aspects.append(dashboard_info)
        if (
            looker_dashboard.folder_path is not None
            and looker_dashboard.folder is not None
        ):
            browse_path = BrowsePathsClass(
                paths=[f"/Folders/{looker_dashboard.folder_path}"]
            )
            browse_path_v2 = BrowsePathsV2Class(
                path=[
                    BrowsePathEntryClass("Folders"),
                    *self._get_folder_browse_path_v2_entries(looker_dashboard.folder),
                ],
            )
            dashboard_snapshot.aspects.append(browse_path)

        ownership = self.get_ownership(looker_dashboard)
        if ownership is not None:
            dashboard_snapshot.aspects.append(ownership)

        dashboard_snapshot.aspects.append(Status(removed=looker_dashboard.is_deleted))

        dashboard_mce = MetadataChangeEvent(proposedSnapshot=dashboard_snapshot)

        proposals: List[Union[MetadataChangeEvent, MetadataChangeProposalWrapper]] = [
            dashboard_mce
        ]

        if looker_dashboard.folder is not None:
            container = ContainerClass(
                container=self._gen_folder_key(looker_dashboard.folder.id).as_urn(),
            )
            proposals.append(
                MetadataChangeProposalWrapper(entityUrn=dashboard_urn, aspect=container)
            )

        if browse_path_v2:
            proposals.append(
                MetadataChangeProposalWrapper(
                    entityUrn=dashboard_urn, aspect=browse_path_v2
                )
            )

        # If extracting embeds is enabled, produce an MCP for embed URL.
        if (
            self.source_config.extract_embed_urls
            and self.source_config.external_base_url
        ):
            proposals.append(
                create_embed_mcp(
                    dashboard_snapshot.urn,
                    looker_dashboard.embed_url(self.source_config.external_base_url),
                )
            )

        if self.source_config.include_platform_instance_in_urns:
            proposals.append(
                MetadataChangeProposalWrapper(
                    entityUrn=dashboard_urn,
                    aspect=self._create_platform_instance_aspect(),
                )
            )

        return proposals

    def _make_dashboard_urn(self, looker_dashboard_name_part: str) -> str:
        # Note that `looker_dashboard_name_part` will like be `dashboard.1234`.
        platform_instance: Optional[str] = None
        if self.source_config.include_platform_instance_in_urns:
            platform_instance = self.source_config.platform_instance

        return builder.make_dashboard_urn(
            name=looker_dashboard_name_part,
            platform=self.source_config.platform_name,
            platform_instance=platform_instance,
        )

    def make_dashboard_urn(self, looker_dashboard: LookerDashboard) -> str:
        return self._make_dashboard_urn(looker_dashboard.get_urn_dashboard_id())

    def _make_explore_metadata_events(
        self,
    ) -> Iterable[
        Union[MetadataChangeEvent, MetadataChangeProposalWrapper, MetadataWorkUnit]
    ]:
        if not self.source_config.emit_used_explores_only:
            explores_to_fetch = list(self.list_all_explores())
        else:
            # We don't keep track of project names for each explore right now.
            # Because project names are just used for a custom property, it's
            # fine to set them to None.
            # TODO: Track project names for each explore.
            explores_to_fetch = [
                (None, model, explore) for (model, explore) in self.reachable_explores
            ]
        explores_to_fetch.sort()

        processed_models: List[str] = []

        for project_name, model, _ in explores_to_fetch:
            if model not in processed_models:
                model_key = gen_model_key(self.source_config, model)
                yield from gen_containers(
                    container_key=model_key,
                    name=model,
                    sub_types=[BIContainerSubTypes.LOOKML_MODEL],
                    extra_properties=(
                        {"project": project_name} if project_name is not None else None
                    ),
                )
                yield MetadataChangeProposalWrapper(
                    entityUrn=model_key.as_urn(),
                    aspect=BrowsePathsV2Class(
                        path=[BrowsePathEntryClass("Explore")],
                    ),
                )

                processed_models.append(model)

        self.reporter.total_explores = len(explores_to_fetch)
        for future in BackpressureAwareExecutor.map(
            self.fetch_one_explore,
            ((model, explore) for (_project, model, explore) in explores_to_fetch),
            max_workers=self.source_config.max_threads,
        ):
            events, explore_id, start_time, end_time = future.result()
            self.reporter.explores_scanned += 1
            yield from events
            self.reporter.report_upstream_latency(start_time, end_time)
            logger.debug(
                f"Running time of fetch_one_explore for {explore_id}: {(end_time - start_time).total_seconds()}"
            )

    def list_all_explores(self) -> Iterable[Tuple[Optional[str], str, str]]:
        # returns a list of (model, explore) tuples

        for model in self.looker_api.all_lookml_models():
            if model.name is None or model.explores is None:
                continue
            for explore in model.explores:
                if explore.name is None:
                    continue
                yield (model.project_name, model.name, explore.name)

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
                    self.source_config,
                    self.reporter,
                    self.source_config.external_base_url or self.source_config.base_url,
                    self.source_config.extract_embed_urls,
                )
                or events
            )

        return events, f"{model}:{explore}", start_time, datetime.datetime.now()

    def _extract_event_urn(
        self, event: Union[MetadataChangeEvent, MetadataChangeProposalWrapper]
    ) -> Optional[str]:
        if isinstance(event, MetadataChangeEvent):
            return event.proposedSnapshot.urn
        else:
            return event.entityUrn

    def _emit_folder_as_container(
        self, folder: LookerFolder
    ) -> Iterable[MetadataWorkUnit]:
        if folder.id not in self.processed_folders:
            yield from gen_containers(
                container_key=self._gen_folder_key(folder.id),
                name=folder.name,
                sub_types=[BIContainerSubTypes.LOOKER_FOLDER],
                parent_container_key=(
                    self._gen_folder_key(folder.parent_id) if folder.parent_id else None
                ),
            )
            if folder.parent_id is None:
                yield MetadataChangeProposalWrapper(
                    entityUrn=self._gen_folder_key(folder.id).as_urn(),
                    aspect=BrowsePathsV2Class(
                        path=[BrowsePathEntryClass("Folders")],
                    ),
                ).as_workunit()
            else:
                yield MetadataChangeProposalWrapper(
                    entityUrn=self._gen_folder_key(folder.id).as_urn(),
                    aspect=BrowsePathsV2Class(
                        path=[
                            BrowsePathEntryClass("Folders"),
                            *self._get_folder_browse_path_v2_entries(
                                folder, include_current_folder=False
                            ),
                        ],
                    ),
                ).as_workunit()
            self.processed_folders.append(folder.id)

    def _gen_folder_key(self, folder_id: str) -> LookerFolderKey:
        return LookerFolderKey(
            folder_id=folder_id,
            env=self.source_config.env,
            platform=self.source_config.platform_name,
            instance=self.source_config.platform_instance,
        )

    def _make_dashboard_and_chart_mces(
        self, looker_dashboard: LookerDashboard
    ) -> Iterable[Union[MetadataChangeEvent, MetadataChangeProposalWrapper]]:
        # Step 1: Emit metadata for each Chart inside the Dashboard.
        chart_events = []
        for element in looker_dashboard.dashboard_elements:
            if element.type == "vis":
                chart_events.extend(
                    self._make_chart_metadata_events(element, looker_dashboard)
                )

        yield from chart_events

        # Step 2: Emit metadata events for the Dashboard itself.
        chart_urns: Set[str] = (
            set()
        )  # Collect the unique child chart urns for dashboard input lineage.
        for chart_event in chart_events:
            chart_event_urn = self._extract_event_urn(chart_event)
            if chart_event_urn:
                chart_urns.add(chart_event_urn)

        dashboard_events = self._make_dashboard_metadata_events(
            looker_dashboard, list(chart_urns)
        )
        yield from dashboard_events

    def get_ownership(
        self, looker_dashboard_look: Union[LookerDashboard, LookerDashboardElement]
    ) -> Optional[OwnershipClass]:
        if looker_dashboard_look.owner is not None:
            owner_urn = looker_dashboard_look.owner.get_urn(
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

    def _get_looker_folder(self, folder: Union[Folder, FolderBase]) -> LookerFolder:
        assert folder.id
        return LookerFolder(id=folder.id, name=folder.name, parent_id=folder.parent_id)

    def _get_folder_path(self, folder: FolderBase, client: LookerAPI) -> str:
        assert folder.id
        ancestors = [
            ancestor.name for ancestor in client.folder_ancestors(folder_id=folder.id)
        ]
        return "/".join(ancestors + [folder.name])

    def _get_looker_dashboard(self, dashboard: Dashboard) -> LookerDashboard:
        self.reporter.accessed_dashboards += 1
        if dashboard.folder is None:
            logger.debug(f"{dashboard.id} has no folder")
        dashboard_folder = None
        if dashboard.folder is not None:
            dashboard_folder = self._get_looker_folder(dashboard.folder)
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
            if element.look_id is not None:
                # Keeping track of reachable element from Dashboard
                # Later we need to ingest looks which are not reachable from any dashboard
                self.reachable_look_registry.add(element.look_id)
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
            folder=dashboard_folder,
            folder_path=(
                self._get_folder_path(dashboard.folder, self.looker_api)
                if dashboard.folder
                else None
            ),
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

    def _get_looker_user(self, user_id: Optional[str]) -> Optional[LookerUser]:
        user = (
            self.user_registry.get_by_id(
                user_id,
            )
            if self.source_config.extract_owners and user_id is not None
            else None
        )

        if user is not None and self.source_config.extract_owners:
            # Keep track of how many user ids we were able to resolve
            self.reporter.resolved_user_ids += 1
            if user.email is None:
                self.reporter.email_ids_missing += 1

        return user

    def process_metrics_dimensions_and_fields_for_dashboard(
        self, dashboard: LookerDashboard
    ) -> List[MetadataWorkUnit]:
        chart_mcps = [
            self._make_metrics_dimensions_chart_mcp(element)
            for element in dashboard.dashboard_elements
        ]
        dashboard_mcp = self._make_metrics_dimensions_dashboard_mcp(dashboard)

        mcps = chart_mcps
        mcps.append(dashboard_mcp)

        workunits = [mcp.as_workunit() for mcp in mcps]

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
            entity_urn = self._make_chart_urn(
                element_id=dashboard_element.get_urn_element_id()
            )
            view_field_for_reference = input_field.view_field

            if input_field.view_field is None:
                explore = self.explore_registry.get_explore(
                    input_field.model, input_field.explore
                )
                if explore is not None:
                    # add this to the list of explores to finally generate metadata for
                    self.add_reachable_explore(
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

            if view_field_for_reference and view_field_for_reference.name:
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
        dashboard_urn = self.make_dashboard_urn(dashboard)
        all_fields = []
        for dashboard_element in dashboard.dashboard_elements:
            all_fields.extend(
                self._input_fields_from_dashboard_element(dashboard_element)
            )

        input_fields_aspect = InputFieldsClass(fields=all_fields)

        return MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn,
            aspect=input_fields_aspect,
        )

    def _make_metrics_dimensions_chart_mcp(
        self, dashboard_element: LookerDashboardElement
    ) -> MetadataChangeProposalWrapper:
        chart_urn = self._make_chart_urn(
            element_id=dashboard_element.get_urn_element_id()
        )

        input_fields_aspect = InputFieldsClass(
            fields=self._input_fields_from_dashboard_element(dashboard_element)
        )

        return MetadataChangeProposalWrapper(
            entityUrn=chart_urn,
            aspect=input_fields_aspect,
        )

    def process_dashboard(
        self, dashboard_id: str, fields: List[str]
    ) -> Tuple[
        List[MetadataWorkUnit],
        Optional[looker_usage.LookerDashboardForUsage],
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
        except (SDKError, DeserializeError) as e:
            # A looker dashboard could be deleted in between the list and the get
            self.reporter.report_warning(
                title="Failed to fetch dashboard from the Looker API",
                message="Error occurred while attempting to loading dashboard from Looker API. Skipping.",
                context=f"Dashboard ID: {dashboard_id}",
                exc=e,
            )
            return [], None, dashboard_id, start_time, datetime.datetime.now()

        if self.source_config.skip_personal_folders:
            if dashboard_object.folder is not None and (
                dashboard_object.folder.is_personal
                or dashboard_object.folder.is_personal_descendant
            ):
                self.reporter.info(
                    title="Dropped Dashboard",
                    message="Dropped due to being a personal folder",
                    context=f"Dashboard ID: {dashboard_id}",
                )
                self.reporter.report_dashboards_dropped(dashboard_id)
                return [], None, dashboard_id, start_time, datetime.datetime.now()

        looker_dashboard = self._get_looker_dashboard(dashboard_object)

        workunits = []
        if (
            looker_dashboard.folder_path is not None
            and not self.source_config.folder_path_pattern.allowed(
                looker_dashboard.folder_path
            )
        ):
            logger.debug(
                f"Folder path {looker_dashboard.folder_path} is denied in folder_path_pattern"
            )
            return [], None, dashboard_id, start_time, datetime.datetime.now()

        if looker_dashboard.folder:
            workunits += list(
                self._get_folder_and_ancestors_workunits(looker_dashboard.folder)
            )

        mces = self._make_dashboard_and_chart_mces(looker_dashboard)
        workunits += [
            (
                MetadataWorkUnit(id=f"looker-{mce.proposedSnapshot.urn}", mce=mce)
                if isinstance(mce, MetadataChangeEvent)
                else MetadataWorkUnit(
                    id=f"looker-{mce.aspectName}-{mce.entityUrn}", mcp=mce
                )
            )
            for mce in mces
        ]

        # add on metrics, dimensions, fields events
        metric_dim_workunits = self.process_metrics_dimensions_and_fields_for_dashboard(
            looker_dashboard
        )

        workunits.extend(metric_dim_workunits)

        self.reporter.report_dashboards_scanned()

        # generate usage tracking object
        dashboard_usage = looker_usage.LookerDashboardForUsage.from_dashboard(
            dashboard_object
        )

        return (
            workunits,
            dashboard_usage,
            dashboard_id,
            start_time,
            datetime.datetime.now(),
        )

    def _get_folder_and_ancestors_workunits(
        self, folder: LookerFolder
    ) -> Iterable[MetadataWorkUnit]:
        for ancestor_folder in self.looker_api.folder_ancestors(folder.id):
            yield from self._emit_folder_as_container(
                self._get_looker_folder(ancestor_folder)
            )
        yield from self._emit_folder_as_container(folder)

    def extract_usage_stat(
        self,
        looker_dashboards: List[looker_usage.LookerDashboardForUsage],
        ingested_chart_urns: Set[str],
    ) -> List[MetadataChangeProposalWrapper]:
        looks: List[looker_usage.LookerChartForUsage] = []
        # filter out look from all dashboard
        for dashboard in looker_dashboards:
            if dashboard.looks is None:
                continue
            looks.extend(dashboard.looks)

        # dedup looks
        looks = list({str(look.id): look for look in looks}.values())
        filtered_looks = []
        for look in looks:
            if not look.id:
                continue
            chart_urn = self._make_chart_urn(get_urn_looker_element_id(look.id))
            if chart_urn in ingested_chart_urns:
                filtered_looks.append(look)
            else:
                self.reporter.charts_skipped_for_usage.add(look.id)

        # Keep stat generators to generate entity stat aspect later
        stat_generator_config: looker_usage.StatGeneratorConfig = (
            looker_usage.StatGeneratorConfig(
                looker_api_wrapper=self.looker_api,
                looker_user_registry=self.user_registry,
                interval=self.source_config.extract_usage_history_for_interval,
                strip_user_ids_from_email=self.source_config.strip_user_ids_from_email,
                max_threads=self.source_config.max_threads,
            )
        )

        dashboard_usage_generator = looker_usage.create_dashboard_stat_generator(
            stat_generator_config,
            self.reporter,
            self._make_dashboard_urn,
            looker_dashboards,
        )

        chart_usage_generator = looker_usage.create_chart_stat_generator(
            stat_generator_config,
            self.reporter,
            self._make_chart_urn,
            filtered_looks,
        )

        mcps: List[MetadataChangeProposalWrapper] = []
        for usage_stat_generator in [dashboard_usage_generator, chart_usage_generator]:
            for mcp in usage_stat_generator.generate_usage_stat_mcps():
                mcps.append(mcp)

        return mcps

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.source_config, self.ctx
            ).workunit_processor,
        ]

    def emit_independent_looks_mcp(
        self, dashboard_element: LookerDashboardElement
    ) -> Iterable[MetadataWorkUnit]:
        if dashboard_element.folder:  # independent look
            yield from self._get_folder_and_ancestors_workunits(
                dashboard_element.folder
            )

        yield from auto_workunit(
            stream=self._make_chart_metadata_events(
                dashboard_element=dashboard_element,
                dashboard=None,
            )
        )

        yield from auto_workunit(
            [
                self._make_metrics_dimensions_chart_mcp(
                    dashboard_element,
                )
            ]
        )

    def extract_independent_looks(self) -> Iterable[MetadataWorkUnit]:
        """
        Emit MetadataWorkUnit for looks which are not part of any Dashboard
        """
        if self.source_config.extract_independent_looks is False:
            return

        self.reporter.report_stage_start("extract_independent_looks")

        logger.debug("Extracting looks not part of Dashboard")
        look_fields: List[str] = [
            "id",
            "title",
            "description",
            "query_id",
            "folder",
            "user_id",
        ]
        query_fields: List[str] = [
            "id",
            "view",
            "model",
            "dynamic_fields",
            "filters",
            "fields",
            "slug",
        ]

        all_looks: List[Look] = self.looker_api.all_looks(
            fields=look_fields, soft_deleted=self.source_config.include_deleted
        )
        for look in all_looks:
            if look.id in self.reachable_look_registry:
                # This look is reachable from the Dashboard
                continue

            if look.query_id is None:
                logger.info(f"query_id is None for look {look.title}({look.id})")
                continue

            if self.source_config.skip_personal_folders:
                if look.folder is not None and (
                    look.folder.is_personal or look.folder.is_personal_descendant
                ):
                    self.reporter.info(
                        title="Dropped Look",
                        message="Dropped due to being a personal folder",
                        context=f"Look ID: {look.id}",
                    )

                    assert look.id, "Looker id is null"
                    self.reporter.report_charts_dropped(look.id)
                    continue

            if look.id is not None:
                query: Optional[Query] = self.looker_api.get_look(
                    look.id, fields=["query"]
                ).query
                # Only include fields that are in the query_fields list
                query = Query(
                    **{
                        key: getattr(query, key)
                        for key in query_fields
                        if hasattr(query, key)
                    }
                )

            dashboard_element: Optional[LookerDashboardElement] = (
                self._get_looker_dashboard_element(
                    DashboardElement(
                        id=f"looks_{look.id}",  # to avoid conflict with non-standalone looks (element.id prefixes),
                        # we add the "looks_" prefix to look.id.
                        title=look.title,
                        subtitle_text=look.description,
                        look_id=look.id,
                        dashboard_id=None,  # As this is an independent look
                        look=LookWithQuery(
                            query=query, folder=look.folder, user_id=look.user_id
                        ),
                    ),
                )
            )

            if dashboard_element is not None:
                logger.debug(f"Emitting MCPS for look {look.title}({look.id})")
                yield from self.emit_independent_looks_mcp(
                    dashboard_element=dashboard_element
                )

        self.reporter.report_stage_end("extract_independent_looks")

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

        with self.reporter.report_stage("dashboard_chart_metadata"):
            for job in BackpressureAwareExecutor.map(
                self.process_dashboard,
                (
                    (dashboard_id, fields)
                    for dashboard_id in dashboard_ids
                    if dashboard_id is not None
                ),
                max_workers=self.source_config.max_threads,
            ):
                (
                    work_units,
                    dashboard_usage,
                    dashboard_id,
                    start_time,
                    end_time,
                ) = job.result()
                logger.debug(
                    f"Running time of process_dashboard for {dashboard_id} = {(end_time - start_time).total_seconds()}"
                )
                self.reporter.report_upstream_latency(start_time, end_time)

                yield from work_units
                if dashboard_usage is not None:
                    looker_dashboards_for_usage.append(dashboard_usage)

        if (
            self.source_config.extract_owners
            and self.reporter.resolved_user_ids > 0
            and self.reporter.email_ids_missing == self.reporter.resolved_user_ids
        ):
            # Looks like we tried to extract owners and could not find their email addresses. This is likely a permissions issue
            self.reporter.report_warning(
                "Failed to extract owner emails",
                "Failed to extract owners emails for any dashboards. Please enable the see_users permission for your Looker API key",
            )

        # Extract independent look here, so that explore of this look would get consider in _make_explore_metadata_events
        yield from self.extract_independent_looks()

        self.reporter.report_stage_start("explore_metadata")

        for event in self._make_explore_metadata_events():
            if isinstance(event, MetadataChangeEvent):
                yield MetadataWorkUnit(
                    id=f"looker-{event.proposedSnapshot.urn}", mce=event
                )
            elif isinstance(event, MetadataChangeProposalWrapper):
                yield event.as_workunit()
            elif isinstance(event, MetadataWorkUnit):
                yield event
            else:
                raise Exception(f"Unexpected type of event {event}")
        self.reporter.report_stage_end("explore_metadata")

        if (
            self.source_config.tag_measures_and_dimensions
            and self.reporter.explores_scanned > 0
        ):
            # Emit tag MCEs for measures and dimensions if we produced any explores:
            for tag_mce in LookerUtil.get_tag_mces():
                yield MetadataWorkUnit(
                    id=f"tag-{tag_mce.proposedSnapshot.urn}",
                    mce=tag_mce,
                )

        # Extract usage history is enabled
        if self.source_config.extract_usage_history:
            self.reporter.report_stage_start("usage_extraction")
            usage_mcps: List[MetadataChangeProposalWrapper] = self.extract_usage_stat(
                looker_dashboards_for_usage, self.chart_urns
            )
            for usage_mcp in usage_mcps:
                yield usage_mcp.as_workunit()
            self.reporter.report_stage_end("usage_extraction")

        # Dump looker user resource mappings.
        logger.info("Ingesting looker user resource mapping workunits")
        self.reporter.report_stage_start("user_resource_extraction")
        yield from auto_workunit(
            self.user_registry.to_platform_resource(
                self.source_config.platform_instance
            )
        )

    def get_report(self) -> SourceReport:
        return self.reporter
