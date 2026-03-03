import datetime
import json
import logging
from dataclasses import dataclass
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
    Dashboard as LookerAPIDashboard,
    DashboardElement,
    Folder,
    FolderBase,
    Look,
    LookWithQuery,
    Query,
)

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import mcps_from_mce
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
    SourceCapabilityModifier,
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
    Status,
)
from datahub.metadata.schema_classes import (
    ChartTypeClass,
    EmbedClass,
    InputFieldClass,
    InputFieldsClass,
    OwnerClass,
    OwnershipTypeClass,
)
from datahub.sdk.chart import Chart
from datahub.sdk.container import Container
from datahub.sdk.dashboard import Dashboard
from datahub.sdk.dataset import Dataset
from datahub.sdk.entity import Entity
from datahub.utilities.backpressure_aware_executor import BackpressureAwareExecutor
from datahub.utilities.sentinels import Unset, unset

logger = logging.getLogger(__name__)


@dataclass
class DashboardProcessingResult:
    """Result of processing a single dashboard."""

    entities: List[Entity]
    dashboard_usage: Optional[looker_usage.LookerDashboardForUsage]
    dashboard_id: str
    start_time: datetime.datetime
    end_time: datetime.datetime


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
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
@capability(
    SourceCapability.CONTAINERS,
    "Enabled by default",
    subtype_modifier=[
        SourceCapabilityModifier.LOOKML_MODEL,
        SourceCapabilityModifier.LOOKER_FOLDER,
    ],
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

    def _get_folder_ancestors_urn_entries(
        self, folder: LookerFolder, include_current_folder: bool = True
    ) -> Iterable[str]:
        for ancestor in self.looker_api.folder_ancestors(folder_id=folder.id):
            assert ancestor.id  # to make the linter happy as `Folder` has id field marked optional - which is always returned by the API
            urn = self._gen_folder_key(ancestor.id).as_urn()
            yield urn

        urn = self._gen_folder_key(folder.id).as_urn()
        if include_current_folder:
            yield urn

    def _make_chart_urn(self, element_id: str) -> str:
        platform_instance: Optional[str] = None
        if self.source_config.include_platform_instance_in_urns:
            platform_instance = self.source_config.platform_instance

        return builder.make_chart_urn(
            name=element_id,
            platform=self.source_config.platform_name,
            platform_instance=platform_instance,
        )

    def _make_chart_entities(
        self,
        dashboard_element: LookerDashboardElement,
        dashboard: Optional[
            LookerDashboard
        ],  # dashboard will be None if this is a standalone look
    ) -> List[Chart]:
        chart_parent_container: Union[List[str], Unset] = unset
        if (
            dashboard
            and dashboard.folder_path is not None
            and dashboard.folder is not None
        ):
            chart_parent_container = [
                "Folders",
                *self._get_folder_ancestors_urn_entries(dashboard.folder),
                self.make_dashboard_urn(dashboard),
            ]
        elif (
            dashboard is None
            and dashboard_element.folder_path is not None
            and dashboard_element.folder is not None
        ):  # Independent look
            chart_parent_container = [
                "Folders",
                *self._get_folder_ancestors_urn_entries(dashboard_element.folder),
            ]

        # Determine chart ownership
        chart_ownership: Optional[List[OwnerClass]] = None
        if dashboard is not None:
            ownership = self.get_ownership(dashboard)
            if ownership is not None:
                chart_ownership = [ownership]
        elif dashboard is None and dashboard_element is not None:
            ownership = self.get_ownership(dashboard_element)
            if ownership is not None:
                chart_ownership = [ownership]

        chart_extra_aspects: List[Union[InputFieldsClass, EmbedClass]] = []
        # If extracting embeds is enabled, produce an MCP for embed URL.
        if (
            self.source_config.extract_embed_urls
            and self.source_config.external_base_url
        ):
            maybe_embed_url = dashboard_element.embed_url(
                self.source_config.external_base_url
            )
            if maybe_embed_url:
                chart_extra_aspects.append(EmbedClass(renderUrl=maybe_embed_url))

        chart_extra_aspects.append(
            InputFieldsClass(
                fields=self._input_fields_from_dashboard_element(dashboard_element)
            )
        )
        return [
            Chart(
                chart_type=self._get_chart_type(dashboard_element),
                chart_url=dashboard_element.url(
                    self.source_config.external_base_url or ""
                ),
                custom_properties={
                    "upstream_fields": (
                        ",".join(
                            sorted(
                                {field.name for field in dashboard_element.input_fields}
                            )
                        )
                        if dashboard_element.input_fields
                        else ""
                    )
                },
                description=dashboard_element.description or "",
                display_name=dashboard_element.title,  # title is (deprecated) using display_name
                extra_aspects=chart_extra_aspects,
                input_datasets=dashboard_element.get_view_urns(self.source_config),
                last_modified=self._get_last_modified_time(
                    dashboard
                ),  # Inherited from Dashboard
                last_modified_by=self._get_last_modified_by(
                    dashboard
                ),  # Inherited from Dashboard
                created_at=self._get_created_at(dashboard),  # Inherited from Dashboard
                created_by=self._get_created_by(dashboard),  # Inherited from Dashboard
                deleted_on=self._get_deleted_on(dashboard),  # Inherited from Dashboard
                deleted_by=self._get_deleted_by(dashboard),  # Inherited from Dashboard
                name=dashboard_element.get_urn_element_id(),
                owners=chart_ownership,
                parent_container=chart_parent_container,
                platform=self.source_config.platform_name,
                platform_instance=self.source_config.platform_instance
                if self.source_config.include_platform_instance_in_urns
                else None,
                subtype=BIAssetSubTypes.LOOKER_LOOK,
            )
        ]

    def _make_dashboard_entities(
        self, looker_dashboard: LookerDashboard, charts: List[Chart]
    ) -> List[Dashboard]:
        dashboard_ownership: Optional[List[OwnerClass]] = None
        ownership: Optional[OwnerClass] = self.get_ownership(looker_dashboard)
        if ownership is not None:
            dashboard_ownership = [ownership]

        # Extra Aspects not yet supported in the Dashboard entity class SDKv2
        dashboard_extra_aspects: List[Union[EmbedClass, InputFieldsClass, Status]] = []

        # Embed URL aspect
        if (
            self.source_config.extract_embed_urls
            and self.source_config.external_base_url
        ):
            dashboard_extra_aspects.append(
                EmbedClass(
                    renderUrl=looker_dashboard.embed_url(
                        self.source_config.external_base_url
                    )
                )
            )

        # Input fields aspect
        # Populate input fields from all the dashboard elements
        all_fields: List[InputFieldClass] = []
        for dashboard_element in looker_dashboard.dashboard_elements:
            all_fields.extend(
                self._input_fields_from_dashboard_element(dashboard_element)
            )
        dashboard_extra_aspects.append(InputFieldsClass(fields=all_fields))
        # Status aspect
        dashboard_extra_aspects.append(Status(removed=looker_dashboard.is_deleted))

        dashboard_parent_container: Union[List[str], Unset] = unset
        if (
            looker_dashboard.folder_path is not None
            and looker_dashboard.folder is not None
        ):
            dashboard_parent_container = [
                "Folders",
                *self._get_folder_ancestors_urn_entries(looker_dashboard.folder),
            ]

        return [
            Dashboard(
                charts=charts,
                dashboard_url=looker_dashboard.url(
                    self.source_config.external_base_url
                ),
                description=looker_dashboard.description or "",
                display_name=looker_dashboard.title,  # title is (deprecated) using display_name
                extra_aspects=dashboard_extra_aspects,
                last_modified=self._get_last_modified_time(looker_dashboard),
                last_modified_by=self._get_last_modified_by(looker_dashboard),
                created_at=self._get_created_at(looker_dashboard),
                created_by=self._get_created_by(looker_dashboard),
                deleted_on=self._get_deleted_on(looker_dashboard),
                deleted_by=self._get_deleted_by(looker_dashboard),
                name=looker_dashboard.get_urn_dashboard_id(),
                owners=dashboard_ownership,
                parent_container=dashboard_parent_container,
                platform=self.source_config.platform_name,
                platform_instance=self.source_config.platform_instance
                if self.source_config.include_platform_instance_in_urns
                else None,
            )
        ]

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

    def _make_explore_containers(
        self,
    ) -> Iterable[Union[Container, Dataset]]:
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
                yield Container(
                    container_key=model_key,
                    display_name=model,
                    subtype=BIContainerSubTypes.LOOKML_MODEL,
                    extra_properties=(
                        {"project": project_name} if project_name is not None else None
                    ),
                    parent_container=["Explore"],
                )

                processed_models.append(model)

        self.reporter.total_explores = len(explores_to_fetch)
        for future in BackpressureAwareExecutor.map(
            self.fetch_one_explore,
            ((model, explore) for (_project, model, explore) in explores_to_fetch),
            max_workers=self.source_config.max_threads,
        ):
            explore_dataset_entity, explore_id, start_time, end_time = future.result()
            self.reporter.explores_scanned += 1
            if explore_dataset_entity:
                yield explore_dataset_entity
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
        Optional[Dataset],
        str,
        datetime.datetime,
        datetime.datetime,
    ]:
        start_time = datetime.datetime.now()
        looker_explore = self.explore_registry.get_explore(model, explore)
        explore_dataset_entity: Optional[Dataset] = None
        if looker_explore is not None:
            explore_dataset_entity = looker_explore._to_metadata_events(
                self.source_config,
                self.reporter,
                self.source_config.external_base_url or self.source_config.base_url,
                self.source_config.extract_embed_urls,
            )

        return (
            explore_dataset_entity,
            f"{model}:{explore}",
            start_time,
            datetime.datetime.now(),
        )

    def _emit_folder_as_container(self, folder: LookerFolder) -> Iterable[Container]:
        if folder.id not in self.processed_folders:
            if folder.parent_id is None:
                yield Container(
                    container_key=self._gen_folder_key(folder.id),
                    display_name=folder.name,
                    subtype=BIContainerSubTypes.LOOKER_FOLDER,
                    parent_container=["Folders"],
                )
            else:
                yield Container(
                    container_key=self._gen_folder_key(folder.id),
                    display_name=folder.name,
                    subtype=BIContainerSubTypes.LOOKER_FOLDER,
                    parent_container=[
                        "Folders",
                        *self._get_folder_ancestors_urn_entries(
                            folder, include_current_folder=False
                        ),
                    ],
                )
            self.processed_folders.append(folder.id)

    def _gen_folder_key(self, folder_id: str) -> LookerFolderKey:
        return LookerFolderKey(
            folder_id=folder_id,
            env=self.source_config.env,
            platform=self.source_config.platform_name,
            instance=self.source_config.platform_instance,
        )

    def _make_dashboard_and_chart_entities(
        self, looker_dashboard: LookerDashboard
    ) -> Iterable[Union[Chart, Dashboard]]:
        # Step 1: Emit metadata for each Chart inside the Dashboard.
        chart_events: List[Chart] = []
        for element in looker_dashboard.dashboard_elements:
            if element.type == "vis":
                chart_events.extend(
                    self._make_chart_entities(element, looker_dashboard)
                )

        yield from chart_events

        # # Step 2: Emit metadata events for the Dashboard itself.
        # Create a set of unique chart entities for dashboard input lineage based in chart.urn
        unique_chart_entities: List[Chart] = []
        for chart_event in chart_events:
            # Use chart.urn to ensure uniqueness based on the chart's URN property
            # Also, update the set of processed chart urns
            if str(chart_event.urn) not in self.chart_urns:
                self.chart_urns.add(str(chart_event.urn))
                unique_chart_entities.append(chart_event)

        dashboard_events = self._make_dashboard_entities(
            looker_dashboard, unique_chart_entities
        )
        yield from dashboard_events

    def get_ownership(
        self, looker_dashboard_look: Union[LookerDashboard, LookerDashboardElement]
    ) -> Optional[OwnerClass]:
        if looker_dashboard_look.owner is not None:
            owner_urn = looker_dashboard_look.owner.get_urn(
                self.source_config.strip_user_ids_from_email
            )
            if owner_urn is not None:
                return OwnerClass(
                    owner=owner_urn,
                    type=OwnershipTypeClass.DATAOWNER,
                )
        return None

    def _get_last_modified_time(
        self, looker_dashboard: Optional[LookerDashboard]
    ) -> Optional[datetime.datetime]:
        return looker_dashboard.last_updated_at if looker_dashboard else None

    def _get_last_modified_by(
        self, looker_dashboard: Optional[LookerDashboard]
    ) -> Optional[str]:
        if not looker_dashboard or not looker_dashboard.last_updated_by:
            return None
        return looker_dashboard.last_updated_by.get_urn(
            self.source_config.strip_user_ids_from_email
        )

    def _get_created_at(
        self, looker_dashboard: Optional[LookerDashboard]
    ) -> Optional[datetime.datetime]:
        return looker_dashboard.created_at if looker_dashboard else None

    def _get_created_by(
        self, looker_dashboard: Optional[LookerDashboard]
    ) -> Optional[str]:
        if not looker_dashboard or not looker_dashboard.owner:
            return None
        return looker_dashboard.owner.get_urn(
            self.source_config.strip_user_ids_from_email
        )

    def _get_deleted_on(
        self, looker_dashboard: Optional[LookerDashboard]
    ) -> Optional[datetime.datetime]:
        return looker_dashboard.deleted_at if looker_dashboard else None

    def _get_deleted_by(
        self, looker_dashboard: Optional[LookerDashboard]
    ) -> Optional[str]:
        if not looker_dashboard or not looker_dashboard.deleted_by:
            return None
        return looker_dashboard.deleted_by.get_urn(
            self.source_config.strip_user_ids_from_email
        )

    def _get_looker_folder(self, folder: Union[Folder, FolderBase]) -> LookerFolder:
        assert folder.id
        return LookerFolder(id=folder.id, name=folder.name, parent_id=folder.parent_id)

    def _get_folder_path(self, folder: FolderBase, client: LookerAPI) -> str:
        assert folder.id
        ancestors = [
            ancestor.name for ancestor in client.folder_ancestors(folder_id=folder.id)
        ]
        return "/".join(ancestors + [folder.name])

    def _get_looker_dashboard(self, dashboard: LookerAPIDashboard) -> LookerDashboard:
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

    def _should_skip_personal_folder_dashboard(
        self, dashboard_object: LookerAPIDashboard
    ) -> bool:
        """Check if dashboard should be skipped due to being in personal folder."""
        if not self.source_config.skip_personal_folders:
            return False

        if dashboard_object.folder is not None and (
            dashboard_object.folder.is_personal
            or dashboard_object.folder.is_personal_descendant
        ):
            self.reporter.info(
                title="Dropped Dashboard",
                message="Dropped due to being a personal folder",
                context=f"Dashboard ID: {dashboard_object.id}",
            )
            assert dashboard_object.id is not None
            self.reporter.report_dashboards_dropped(dashboard_object.id)
            return True
        return False

    def _should_skip_dashboard_by_folder_path(
        self, looker_dashboard: LookerDashboard
    ) -> bool:
        """Check if dashboard should be skipped based on folder path pattern."""
        if (
            looker_dashboard.folder_path is not None
            and not self.source_config.folder_path_pattern.allowed(
                looker_dashboard.folder_path
            )
        ):
            logger.debug(
                f"Folder path {looker_dashboard.folder_path} is denied in folder_path_pattern"
            )
            self.reporter.report_dashboards_dropped(looker_dashboard.id)
            return True
        return False

    def _fetch_dashboard_from_api(
        self, dashboard_id: str, fields: List[str]
    ) -> Optional[LookerAPIDashboard]:
        """Fetch dashboard object from Looker API with error handling."""
        try:
            return self.looker_api.dashboard(
                dashboard_id=dashboard_id,
                fields=fields,
            )
        except (SDKError, DeserializeError) as e:
            self.reporter.report_warning(
                title="Failed to fetch dashboard from the Looker API",
                message="Error occurred while attempting to loading dashboard from Looker API. Skipping.",
                context=f"Dashboard ID: {dashboard_id}",
                exc=e,
            )
            return None

    def _create_empty_result(
        self, dashboard_id: str, start_time: datetime.datetime
    ) -> DashboardProcessingResult:
        """Create an empty result for skipped or failed dashboard processing."""
        return DashboardProcessingResult(
            entities=[],
            dashboard_usage=None,
            dashboard_id=dashboard_id,
            start_time=start_time,
            end_time=datetime.datetime.now(),
        )

    def process_dashboard(
        self, dashboard_id: str, fields: List[str]
    ) -> DashboardProcessingResult:
        """
        Process a single dashboard and return the metadata workunits.

        Args:
            dashboard_id: The ID of the dashboard to process
            fields: List of fields to fetch from the Looker API

        Returns:
            DashboardProcessingResult containing entities, usage data, and timing information
        """
        start_time = datetime.datetime.now()

        if dashboard_id is None:
            raise ValueError("Dashboard ID cannot be None")

        # Fetch dashboard from API
        dashboard_object: Optional[LookerAPIDashboard] = self._fetch_dashboard_from_api(
            dashboard_id, fields
        )
        if dashboard_object is None:
            return self._create_empty_result(dashboard_id, start_time)

        # Check if dashboard should be skipped due to personal folder
        if self._should_skip_personal_folder_dashboard(dashboard_object):
            return self._create_empty_result(dashboard_id, start_time)

        # Convert to internal representation
        looker_dashboard: LookerDashboard = self._get_looker_dashboard(dashboard_object)

        # Check folder path pattern
        if self._should_skip_dashboard_by_folder_path(looker_dashboard):
            return self._create_empty_result(dashboard_id, start_time)

        # Build entities list
        entities: List[Entity] = []

        # Add folder containers if dashboard has a folder
        if looker_dashboard.folder:
            entities.extend(
                list(self._get_folder_and_ancestors_containers(looker_dashboard.folder))
            )

        # Add dashboard and chart entities
        entities.extend(list(self._make_dashboard_and_chart_entities(looker_dashboard)))

        # Report successful processing
        self.reporter.report_dashboards_scanned()

        # Generate usage tracking object
        dashboard_usage = looker_usage.LookerDashboardForUsage.from_dashboard(
            dashboard_object
        )

        return DashboardProcessingResult(
            entities=entities,
            dashboard_usage=dashboard_usage,
            dashboard_id=dashboard_id,
            start_time=start_time,
            end_time=datetime.datetime.now(),
        )

    def _get_folder_and_ancestors_containers(
        self, folder: LookerFolder
    ) -> Iterable[Container]:
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

    def emit_independent_looks_entities(
        self, dashboard_element: LookerDashboardElement
    ) -> Iterable[Union[Container, Chart]]:
        if dashboard_element.folder:  # independent look
            yield from self._get_folder_and_ancestors_containers(
                dashboard_element.folder
            )

        yield from self._make_chart_entities(
            dashboard_element=dashboard_element,
            dashboard=None,
        )

    def extract_independent_looks(self) -> Iterable[Union[Container, Chart]]:
        """
        Emit entities for Looks which are not part of any Dashboard.

        Returns: Containers for the folders and ancestors folders and Charts for the looks
        """
        logger.debug("Extracting Looks not part of any Dashboard")

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
            # Skip looks that are already referenced from a dashboard
            if look.id is None:
                logger.warning("Encountered Look with no ID, skipping.")
                continue

            if look.id in self.reachable_look_registry:
                continue

            if look.query_id is None:
                logger.info(f"query_id is None for look {look.title}({look.id})")
                continue

            # Skip looks in personal folders if configured
            if self.source_config.skip_personal_folders:
                if look.folder is not None and (
                    look.folder.is_personal or look.folder.is_personal_descendant
                ):
                    self.reporter.info(
                        title="Dropped Look",
                        message="Dropped due to being a personal folder",
                        context=f"Look ID: {look.id}",
                    )

                    self.reporter.report_charts_dropped(look.id)
                    continue

            # Fetch the Look's query and filter to allowed fields
            query: Optional[Query] = None
            try:
                look_with_query = self.looker_api.get_look(look.id, fields=["query"])
                query_obj = look_with_query.query
                if query_obj:
                    query = Query(
                        **{
                            key: getattr(query_obj, key)
                            for key in query_fields
                            if hasattr(query_obj, key)
                        }
                    )
            except Exception as exc:
                logger.warning(f"Failed to fetch query for Look {look.id}: {exc}")
                continue

            dashboard_element = self._get_looker_dashboard_element(
                DashboardElement(
                    id=f"looks_{look.id}",  # to avoid conflict with non-standalone looks (element.id prefixes),
                    # we add the "looks_" prefix to look.id.
                    title=look.title,
                    subtitle_text=look.description,
                    look_id=look.id,
                    dashboard_id=None,  # As this is an independent look
                    look=LookWithQuery(
                        query=query,
                        folder=getattr(look, "folder", None),
                        user_id=getattr(look, "user_id", None),
                    ),
                )
            )

            if dashboard_element is not None:
                logger.debug(f"Emitting MCPs for look {look.title}({look.id})")
                yield from self.emit_independent_looks_entities(
                    dashboard_element=dashboard_element
                )

    def get_workunits_internal(self) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        """
        Note: Returns Entities from SDKv2 where possible else MCPs only.

        Using SDKv2: Containers, Datasets, Dashboards and Charts
        Using MCPW: Tags, DashboardUsageStats and UserResourceMapping

        TODO: Convert MCPWs to use SDKv2 entities
        """
        with self.reporter.report_stage("list_dashboards"):
            # Fetch all dashboards (not deleted)
            dashboards = self.looker_api.all_dashboards(fields="id")

            # Optionally fetch deleted dashboards if configured
            if self.source_config.include_deleted:
                deleted_dashboards = self.looker_api.search_dashboards(
                    fields="id", deleted="true"
                )
            else:
                deleted_dashboards = []

            if deleted_dashboards:
                logger.debug(f"Deleted Dashboards = {deleted_dashboards}")

            # Collect all dashboard IDs (including deleted if applicable)
            all_dashboard_ids: List[Optional[str]] = [
                dashboard.id for dashboard in dashboards
            ]
            all_dashboard_ids.extend([dashboard.id for dashboard in deleted_dashboards])

            # Filter dashboard IDs based on the allowed pattern
            filtered_dashboard_ids: List[str] = []
            for dashboard_id in all_dashboard_ids:
                if dashboard_id is None:
                    continue
                if not self.source_config.dashboard_pattern.allowed(dashboard_id):
                    self.reporter.report_dashboards_dropped(dashboard_id)
                else:
                    filtered_dashboard_ids.append(dashboard_id)

            # Use the filtered list for further processing
            dashboard_ids: List[str] = filtered_dashboard_ids

            # Report the total number of dashboards to be processed
            self.reporter.report_total_dashboards(len(dashboard_ids))

        # Define the fields to extract for each dashboard
        dashboard_fields = [
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

        # Add usage-related fields if usage history extraction is enabled
        if self.source_config.extract_usage_history:
            dashboard_fields.extend(
                [
                    "favorite_count",
                    "view_count",
                    "last_viewed_at",
                ]
            )

        # Store dashboards for which usage stats will be extracted
        looker_dashboards_for_usage: List[looker_usage.LookerDashboardForUsage] = []

        # Process dashboard and chart metadata
        with self.reporter.report_stage("dashboard_chart_metadata"):
            dashboard_jobs = (
                (dashboard_id, dashboard_fields)
                for dashboard_id in dashboard_ids
                if dashboard_id is not None
            )
            for job in BackpressureAwareExecutor.map(
                self.process_dashboard,
                dashboard_jobs,
                max_workers=self.source_config.max_threads,
            ):
                result: DashboardProcessingResult = job.result()

                logger.debug(
                    f"Running time of process_dashboard for {result.dashboard_id} = {(result.end_time - result.start_time).total_seconds()}"
                )
                self.reporter.report_upstream_latency(
                    result.start_time, result.end_time
                )

                yield from result.entities

                if result.dashboard_usage is not None:
                    looker_dashboards_for_usage.append(result.dashboard_usage)

        # Warn if owner extraction was enabled but no emails could be found
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

        # Extract independent looks first, so their explores are considered in _make_explore_containers.
        if self.source_config.extract_independent_looks:
            with self.reporter.report_stage("extract_independent_looks"):
                yield from self.extract_independent_looks()

        # Process explore containers and yield them.
        with self.reporter.report_stage("explore_metadata"):
            yield from self._make_explore_containers()

        if (
            self.source_config.tag_measures_and_dimensions
            and self.reporter.explores_scanned > 0
        ):
            # Emit tag MCPs for measures and dimensions if we produced any explores:
            # Tags MCEs are converted to MCPs
            for tag_mce in LookerUtil.get_tag_mces():
                yield from auto_workunit(mcps_from_mce(tag_mce))

        # Extract usage history is enabled
        if self.source_config.extract_usage_history:
            with self.reporter.report_stage("usage_extraction"):
                usage_mcps: List[MetadataChangeProposalWrapper] = (
                    self.extract_usage_stat(
                        looker_dashboards_for_usage, self.chart_urns
                    )
                )
                yield from auto_workunit(usage_mcps)

        # Ingest looker user resource mapping workunits.
        logger.info("Ingesting looker user resource mapping workunits")
        with self.reporter.report_stage("user_resource_extraction"):
            yield from auto_workunit(
                self.user_registry.to_platform_resource(
                    self.source_config.platform_instance
                )
            )

    def get_report(self) -> SourceReport:
        return self.reporter
