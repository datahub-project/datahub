import copy
import hashlib
import json
import logging
from datetime import datetime
from functools import partial
from typing import Dict, Iterable, List, Optional, Union

import datahub.emitter.mce_builder as builder
from datahub._version import __version__
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import add_entity_to_container, gen_containers
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
    MetadataWorkUnitProcessor,
    SourceReport,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.common.subtypes import (
    BIContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.qlik_sense.app_state import (
    AppCheckpoint,
    QlikAppStateHandler,
)
from datahub.ingestion.source.qlik_sense.config import (
    Constant,
    PlatformDetail,
    QlikSourceConfig,
    QlikSourceReport,
)
from datahub.ingestion.source.qlik_sense.data_classes import (
    FIELD_TYPE_MAPPING,
    KNOWN_DATA_PLATFORM_MAPPING,
    App,
    AppKey,
    BoxType,
    Chart,
    Item,
    QlikDataset,
    QlikTable,
    SchemaField as QlikDatasetSchemaField,
    Sheet,
    Space,
    SpaceKey,
    UnchangedApp,
)
from datahub.ingestion.source.qlik_sense.qlik_api import QlikAPI
from datahub.ingestion.source.qlik_sense.qlik_base_api import QlikBaseAPI
from datahub.ingestion.source.qlik_sense.qlik_onprem_api import QlikOnPremAPI
from datahub.ingestion.source.qlik_sense.script_lineage import (
    BACKTICK_QUOTED_PLATFORMS,
    SPACE_SEPARATOR,
    DataConnectionInfo,
    ResolvedConnection,
    ScriptTable,
    ScriptTableLineage,
    brackets_to_backticks,
    connection_lookup_names,
    drop_condition_variables,
    has_qualified_name,
    infer_data_connection,
    is_connector_sql,
    is_inline_connection,
    parse_load_script,
    redact_connection,
)
from datahub.ingestion.source.state.entity_removal_state import (
    STATEFUL_INGESTION_IGNORED_ENTITY_TYPES,
    GenericCheckpointState,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    auto_stale_entity_removal,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.ingestion.workunit_processors.auto_stale_entity_removal import (
    AutoStaleEntityRemovalProcessor,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    Status,
    SubTypes,
    TimeStamp,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    DatasetProperties,
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    Upstream,
    UpstreamLineage,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    MySqlDDL,
    NullType,
    SchemaField,
    SchemaMetadata,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeAuditStampsClass,
    ChartInfoClass,
    ContainerPropertiesClass,
    DashboardInfoClass,
    DataPlatformInstanceClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaFieldDataTypeClass,
)
from datahub.specific.chart import ChartPatchBuilder
from datahub.sql_parsing.sqlglot_lineage import (
    SqlParsingResult,
    column_refs_to_schema_field_urns,
    create_lineage_sql_parsed_result,
)
from datahub.utilities.urns.urn import guess_entity_type

# Logger instance
logger = logging.getLogger(__name__)

# Entities per DataHub request when checking that a skipped app's entities exist.
ENTITY_CHECK_BATCH_SIZE = 100


def _error_type(error: Optional[Exception]) -> str:
    # SQL parser errors quote the statement, which can hold credentials when a
    # script statement was split wrongly, so only the type goes in the report.
    return type(error).__name__ if error else "unknown"


@platform_name("Qlik Sense")
@config_class(QlikSourceConfig)
@support_status(SupportStatus.ALPHA)
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Enabled by default.",
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Disabled by default.",
)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(
    SourceCapability.OWNERSHIP,
    "Enabled by default, configured using `ingest_owner`",
)
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
class QlikSenseSource(StatefulIngestionSourceBase, TestableSource):
    """
    This plugin extracts the following:
    - Qlik Sense Spaces (Cloud) or Streams (Enterprise on Windows) and Apps as Container.
    - Qlik Datasets (Cloud)
    - Sheets as dashboard and its charts
    """

    config: QlikSourceConfig
    reporter: QlikSourceReport
    platform: str = "qlik-sense"
    qlik_api: QlikBaseAPI

    def __init__(self, config: QlikSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.reporter = QlikSourceReport()
        self.ui_host = config.ui_hostname()
        self._data_connections: Optional[Dict[str, DataConnectionInfo]] = None
        # Owned here rather than by the automatic processor so that entities of
        # apps skipped as unchanged or only partly read can be kept in the state.
        self.stale_entity_removal_handler: Optional[StaleEntityRemovalHandler] = (
            StaleEntityRemovalHandler(
                state_provider=self.state_provider,
                report=self.reporter,
                config=self.config,
                state_type_class=GenericCheckpointState,
                pipeline_name=ctx.pipeline_name,
                run_id=ctx.run_id,
                platform=self.platform,
            )
        )
        self.app_state = QlikAppStateHandler(
            state_provider=self.state_provider,
            config=self.config,
            pipeline_name=ctx.pipeline_name,
            run_id=ctx.run_id,
            stale_job_id=self.stale_entity_removal_handler.job_id
            if self.stale_entity_removal_handler
            and self.stale_entity_removal_handler.is_checkpointing_enabled()
            else None,
        )
        self._fingerprint_salt: Optional[str] = None
        # Filled from worker threads, so only copied to the report at the end.
        self._change_check_failures: List[str] = []
        if config.skips_unchanged_apps() and not self._skips_unchanged_apps():
            self.reporter.warning(
                title="skip_unchanged_apps is ignored",
                message="Skipping unchanged apps needs stateful ingestion, so every "
                "app is read.",
            )
        try:
            self.qlik_api = self._create_api(self.config)
        except Exception as e:
            logger.warning(e)
            exit(
                1
            )  # Exit pipeline as we are not able to connect to Qlik Client Service.

    @staticmethod
    def _create_api(config: QlikSourceConfig) -> QlikBaseAPI:
        return QlikOnPremAPI(config) if config.on_prem else QlikAPI(config)

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        try:
            config = QlikSourceConfig.parse_obj_allow_extras(config_dict)
            api = QlikSenseSource._create_api(config)
            if isinstance(api, QlikOnPremAPI):
                api.check_engine_port()
            test_report.basic_connectivity = CapabilityReport(capable=True)
        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=str(e)
            )
        return test_report

    @classmethod
    def create(cls, config_dict, ctx):
        config = QlikSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_excluded_workunit_processors(self):
        return (
            [AutoStaleEntityRemovalProcessor]
            if self.stale_entity_removal_handler
            else []
        )

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        processors = super().get_workunit_processors()
        if self.stale_entity_removal_handler:
            processors.append(
                partial(auto_stale_entity_removal, self.stale_entity_removal_handler)
            )
        return processors

    def _skips_unchanged_apps(self) -> bool:
        return (
            self.config.skips_unchanged_apps()
            and self.app_state.is_checkpointing_enabled()
        )

    def _init_fingerprint_salt(self) -> None:
        # The recipe and data connections shape every app's output, and a new
        # connector version may emit differently, so any change re-reads all apps.
        self._fingerprint_salt = json.dumps(
            {
                "version": __version__,
                "config": self.config.model_dump(
                    mode="json",
                    exclude={
                        "stateful_ingestion": True,
                        "max_workers": True,
                        "on_prem": {"skip_unchanged_apps"},
                    },
                ),
                "connections": {
                    name: info.model_dump(mode="json")
                    for name, info in sorted(self._get_data_connections().items())
                },
            },
            sort_keys=True,
        )

    def _salted_fingerprint(self, fingerprint: str) -> str:
        assert self._fingerprint_salt is not None
        return hashlib.sha256(
            f"{self._fingerprint_salt}|{fingerprint}".encode("utf-8")
        ).hexdigest()

    def _usable_checkpoint(
        self, app_id: str, failures: List[str]
    ) -> Optional[AppCheckpoint]:
        """The app's last read, when it can stand in for this run's read: it
        is recorded by the run the stale entity removal state comes from, and
        it holds the entities to keep. A failed lookup goes to `failures`."""
        try:
            previous = self.app_state.previous(app_id)
        except Exception as e:
            logger.debug(f"Unable to load the last checkpoint of app {app_id}: {e}")
            failures.append(f"{app_id}: unable to load the last checkpoint: {e}")
            return None
        return previous if previous is not None and previous.urns else None

    def _is_app_unchanged(self, app_id: str, fingerprint: str) -> bool:
        previous = self._usable_checkpoint(app_id, self._change_check_failures)
        if previous is None or previous.fingerprint != self._salted_fingerprint(
            fingerprint
        ):
            return False
        # Checkpoints are committed even when the sink failed, so only trust one
        # whose metadata actually reached DataHub and was not deleted since.
        graph = self.ctx.graph
        if graph is None:
            return False
        container_urn = self._gen_app_key(app_id).as_urn()
        try:
            properties = graph.get_aspect(container_urn, ContainerPropertiesClass)
            if (
                properties is None
                or (properties.customProperties or {}).get(Constant.FINGERPRINT)
                != previous.fingerprint
            ):
                return False
            return self._entities_present(graph, previous.urns)
        except Exception as e:
            logger.debug(f"Unable to read app container {container_urn}: {e}")
            self._change_check_failures.append(
                f"{app_id}: unable to read the app back from DataHub: {e}"
            )
            return False

    @staticmethod
    def _entities_present(graph: DataHubGraph, urns: List[str]) -> bool:
        """Whether every entity the app's last read produced is in DataHub and
        not soft-deleted, which also catches entities the sink failed to write
        and hard-deleted ones. Entities stale removal deleted after they left
        Qlik are not in the checkpoint."""
        by_type: Dict[str, List[str]] = {}
        for urn in urns:
            by_type.setdefault(guess_entity_type(urn), []).append(urn)
        for entity_type, typed_urns in by_type.items():
            for start in range(0, len(typed_urns), ENTITY_CHECK_BATCH_SIZE):
                batch = typed_urns[start : start + ENTITY_CHECK_BATCH_SIZE]
                entities = graph.get_entities(
                    entity_type, batch, aspects=[Status.ASPECT_NAME]
                )
                for urn in batch:
                    status = entities.get(urn, {}).get(Status.ASPECT_NAME)
                    if status is None or getattr(status[0], "removed", True):
                        return False
        return True

    def _keep_previous_urns(self, app_id: str) -> List[str]:
        try:
            previous = self.app_state.previous(app_id)
        except Exception as e:
            # Reporting a failure stops stale entity removal from soft-deleting
            # what the app's last read produced.
            self.reporter.failure(
                title="Unable to load the last checkpoint of a Qlik app",
                message="Entities from the app's last read could not be kept, so "
                "stale metadata removal is skipped for this run.",
                context=app_id,
                exc=e,
            )
            return []
        if previous is None:
            return []
        assert self.stale_entity_removal_handler is not None
        for urn in previous.urns:
            self.stale_entity_removal_handler.add_entity_to_state(
                guess_entity_type(urn), urn
            )
        return previous.urns

    def _keep_unchanged_app(self, app: UnchangedApp) -> None:
        urns = self._keep_previous_urns(app.id)
        # Without the kept entities, which _keep_previous_urns reports as a
        # failure, nothing is recorded, so the next run reads the app again.
        if urns:
            self.app_state.record(
                app.id,
                AppCheckpoint(
                    fingerprint=self._salted_fingerprint(app.fingerprint), urns=urns
                ),
            )
        self.reporter.apps_skipped_unchanged += 1

    def _keep_partly_read_app(self, app: App) -> bool:
        """Keeps what the app's last read produced instead of emitting a partial
        read, which would overwrite its relationships. False when there is no
        usable checkpoint to keep."""
        failures: List[str] = []
        if self._usable_checkpoint(app.id, failures) is None:
            for failure in failures:
                self.reporter.warning(
                    title="Unable to load the last checkpoint of a Qlik app",
                    message="The partly read app could not keep its last read.",
                    context=failure,
                )
            return False
        urns = self._keep_previous_urns(app.id)
        if not urns:
            return False
        # No fingerprint, so the next run reads the app again.
        self.app_state.record(app.id, AppCheckpoint(fingerprint=None, urns=urns))
        return True

    def _gen_recorded_app_workunit(self, app: App) -> Iterable[MetadataWorkUnit]:
        fingerprint = (
            self._salted_fingerprint(app.fingerprint)
            if app.fingerprint and app.read_complete
            else None
        )
        urns: Dict[str, None] = {}
        for wu in self._gen_app_workunit(app, fingerprint):
            urn = wu.get_urn()
            if guess_entity_type(urn) not in STATEFUL_INGESTION_IGNORED_ENTITY_TYPES:
                urns[urn] = None
            yield wu
        if not app.read_complete:
            # Only reached without a usable last read, and reported as a failure.
            # Recording the partial entity list as the app's last read would let
            # a later partial read keep only those entities.
            return
        # A complete read records only what it produced, so entities removed in
        # Qlik are soft-deleted even when the change check keeps failing. No
        # fingerprint never matches, so such an app is read again next run.
        self.app_state.record(
            app.id, AppCheckpoint(fingerprint=fingerprint, urns=list(urns))
        )

    def _gen_space_key(self, space_id: str) -> SpaceKey:
        return SpaceKey(
            space=space_id,
            platform=self.platform,
            instance=self.config.platform_instance,
        )

    def _gen_app_key(self, app_id: str) -> AppKey:
        return AppKey(
            app=app_id,
            platform=self.platform,
            instance=self.config.platform_instance,
        )

    def _get_audit_stamp(
        self, date: datetime, username: Optional[str]
    ) -> AuditStampClass:
        return AuditStampClass(
            time=int(date.timestamp() * 1000),
            actor=builder.make_user_urn(username) if username else builder.UNKNOWN_USER,
        )

    def _get_owner_name(self, owner_id: Optional[str]) -> Optional[str]:
        return self.qlik_api.get_user_name(owner_id) if owner_id else None

    def _get_allowed_spaces(self) -> List[Space]:
        all_spaces = self.qlik_api.get_spaces()
        allowed_spaces = [
            space
            for space in all_spaces
            if self.config.space_pattern.allowed(space.name)
        ]
        logger.info(f"Number of spaces = {len(all_spaces)}")
        self.reporter.report_number_of_spaces(len(all_spaces))
        logger.info(f"Number of allowed spaces = {len(allowed_spaces)}")
        return allowed_spaces

    def _gen_space_workunit(self, space: Space) -> Iterable[MetadataWorkUnit]:
        """
        Map Qlik space to Datahub container
        """
        owner_username = self._get_owner_name(space.ownerId)
        yield from gen_containers(
            container_key=self._gen_space_key(space.id),
            name=space.name,
            description=space.description,
            sub_types=[BIContainerSubTypes.QLIK_SPACE],
            extra_properties={Constant.TYPE: str(space.type)},
            owner_urn=(
                builder.make_user_urn(owner_username)
                if self.config.ingest_owner and owner_username
                else None
            ),
            external_url=f"https://{self.ui_host}/hub/stream/{space.id}"
            if self.config.on_prem
            else f"https://{self.ui_host}/catalog?space_filter={space.id}",
            created=int(space.createdAt.timestamp() * 1000),
            last_modified=int(space.updatedAt.timestamp() * 1000),
        )

    def _gen_entity_status_aspect(self, entity_urn: str) -> MetadataWorkUnit:
        return MetadataChangeProposalWrapper(
            entityUrn=entity_urn, aspect=Status(removed=False)
        ).as_workunit()

    def _gen_entity_owner_aspect(
        self, entity_urn: str, user_name: str
    ) -> MetadataWorkUnit:
        aspect = OwnershipClass(
            owners=[
                OwnerClass(
                    owner=builder.make_user_urn(user_name),
                    type=OwnershipTypeClass.DATAOWNER,
                )
            ]
        )
        return MetadataChangeProposalWrapper(
            entityUrn=entity_urn,
            aspect=aspect,
        ).as_workunit()

    def _gen_dashboard_urn(self, dashboard_identifier: str) -> str:
        return builder.make_dashboard_urn(
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            name=dashboard_identifier,
        )

    def _gen_dashboard_info_workunit(
        self, sheet: Sheet, app_id: str
    ) -> MetadataWorkUnit:
        dashboard_urn = self._gen_dashboard_urn(sheet.id)
        custom_properties: Dict[str, str] = {"chartCount": str(len(sheet.charts))}
        dashboard_info_cls = DashboardInfoClass(
            title=sheet.title,
            description=sheet.description,
            charts=[
                builder.make_chart_urn(
                    platform=self.platform,
                    platform_instance=self.config.platform_instance,
                    name=chart.qId,
                )
                for chart in sheet.charts
            ],
            lastModified=ChangeAuditStampsClass(
                created=self._get_audit_stamp(sheet.createdAt, sheet.ownerId),
                lastModified=self._get_audit_stamp(sheet.updatedAt, sheet.ownerId),
            ),
            customProperties=custom_properties,
            dashboardUrl=f"https://{self.ui_host}/sense/app/{app_id}/sheet/{sheet.id}/state/analysis",
        )
        return MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn, aspect=dashboard_info_cls
        ).as_workunit()

    def _gen_charts_workunit(
        self,
        charts: List[Chart],
        input_tables_urns: Optional[List[str]],
        app_id: str,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Map Qlik Chart to Datahub Chart
        """
        for chart in charts:
            chart_urn = builder.make_chart_urn(
                platform=self.platform,
                platform_instance=self.config.platform_instance,
                name=chart.qId,
            )

            yield self._gen_entity_status_aspect(chart_urn)

            custom_properties = {
                "Dimension": str(chart.qDimension),
                "Measure": str(chart.qMeasure),
            }

            if input_tables_urns is None:
                # Unknown inputs would overwrite the ones the last full read
                # emitted, so only the other chart fields are patched.
                yield from self._gen_chart_info_patch(
                    chart_urn, chart, custom_properties
                )
            else:
                yield MetadataChangeProposalWrapper(
                    entityUrn=chart_urn,
                    aspect=ChartInfoClass(
                        title=chart.title,
                        description=chart.visualization,
                        lastModified=ChangeAuditStampsClass(),
                        customProperties=custom_properties,
                        inputs=input_tables_urns,
                    ),
                ).as_workunit()

            yield from add_entity_to_container(
                container_key=self._gen_app_key(app_id),
                entity_type="chart",
                entity_urn=chart_urn,
            )

    @staticmethod
    def _gen_chart_info_patch(
        chart_urn: str, chart: Chart, custom_properties: Dict[str, str]
    ) -> Iterable[MetadataWorkUnit]:
        # GMS creates missing chart info from its patch template, which also
        # stamps lastModified. An empty stamp here would erase the last one.
        patch = ChartPatchBuilder(chart_urn)
        if chart.title:
            patch.set_title(chart.title)
        if chart.visualization:
            patch.set_description(chart.visualization)
        for key, value in custom_properties.items():
            patch.add_custom_property(key, value)
        for mcp in patch.build():
            yield MetadataWorkUnit(
                id=MetadataWorkUnit.generate_workunit_id(mcp), mcp_raw=mcp
            )

    def _gen_sheets_workunit(
        self, app: App, input_tables_urns: Optional[List[str]]
    ) -> Iterable[MetadataWorkUnit]:
        """
        Map Qlik Sheet to Datahub dashboard
        """
        for sheet in app.sheets:
            dashboard_urn = self._gen_dashboard_urn(sheet.id)

            yield self._gen_entity_status_aspect(dashboard_urn)

            # A partial chart list would overwrite the one the last full read
            # emitted.
            if sheet.charts_complete:
                yield self._gen_dashboard_info_workunit(sheet, app.id)

            yield from add_entity_to_container(
                container_key=self._gen_app_key(app.id),
                entity_type="dashboard",
                entity_urn=dashboard_urn,
            )

            dpi_aspect = self._gen_dataplatform_instance_aspect(dashboard_urn)
            if dpi_aspect:
                yield dpi_aspect

            owner_username = self._get_owner_name(sheet.ownerId)
            if self.config.ingest_owner and owner_username:
                yield self._gen_entity_owner_aspect(dashboard_urn, owner_username)

            yield from self._gen_charts_workunit(
                sheet.charts, input_tables_urns, app.id
            )

    def _gen_app_table_upstream_lineage(
        self, dataset_urn: str, table: QlikTable
    ) -> Optional[MetadataWorkUnit]:
        upstream_dataset_urn: Optional[str] = None
        if table.type == BoxType.BLACKBOX:
            upstream_dataset_platform_detail = (
                self.config.data_connection_to_platform_instance.get(
                    table.dataconnectorName,
                    PlatformDetail(
                        env=self.config.env,
                        platform_instance=self.config.platform_instance,
                    ),
                )
            )
            if not table.selectStatement:
                return None
            platform = upstream_dataset_platform_detail.platform or (
                KNOWN_DATA_PLATFORM_MAPPING.get(
                    table.dataconnectorPlatform, table.dataconnectorPlatform
                )
            )
            # The database and schema Qlik reports for the table win over the
            # connection's configured defaults.
            result = create_lineage_sql_parsed_result(
                query=table.selectStatement.strip(),
                default_db=table.databaseName
                or upstream_dataset_platform_detail.database,
                default_schema=table.schemaName
                or upstream_dataset_platform_detail.default_schema,
                platform=platform,
                # Like script tables, an unset env follows the recipe's env.
                env=upstream_dataset_platform_detail.env
                if "env" in upstream_dataset_platform_detail.model_fields_set
                else self.config.env,
                platform_instance=upstream_dataset_platform_detail.platform_instance,
            )
            if not result.in_tables:
                self.reporter.warning(
                    title="Unable to parse Qlik table SQL",
                    message="Upstream lineage for this Qlik table is skipped.",
                    context=f"table={table.tableName}, connection={table.dataconnectorName}, "
                    f"error={_error_type(result.debug_info.table_error)}",
                )
                return None
            qualified = [
                urn
                for urn in result.in_tables
                if has_qualified_name(
                    urn, platform, upstream_dataset_platform_detail.platform_instance
                )
            ]
            if not qualified:
                self.reporter.warning(
                    title="Qlik table is missing its database or schema",
                    message="Upstream lineage for this Qlik table is skipped. Set "
                    "`database` and `default_schema` for its connection in "
                    "`data_connection_to_platform_instance`.",
                    context=f"table={table.tableName}, upstreams={result.in_tables}",
                )
                return None
            upstream_dataset_urn = qualified[0]
        elif table.type == BoxType.LOADFILE:
            upstream_dataset_urn = self._gen_qlik_dataset_urn(
                f"{table.spaceId}.{table.databaseName}".lower()
            )

        if upstream_dataset_urn:
            # Generate finegrained lineage
            fine_grained_lineages = [
                FineGrainedLineage(
                    upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                    upstreams=[
                        builder.make_schema_field_urn(upstream_dataset_urn, field.name)
                    ],
                    downstreamType=FineGrainedLineageDownstreamType.FIELD,
                    downstreams=[
                        builder.make_schema_field_urn(dataset_urn, field.name)
                    ],
                )
                for field in table.datasetSchema
            ]
            return MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=UpstreamLineage(
                    upstreams=[
                        Upstream(
                            dataset=upstream_dataset_urn, type=DatasetLineageType.COPY
                        )
                    ],
                    fineGrainedLineages=fine_grained_lineages,
                ),
            ).as_workunit()
        else:
            return None

    def _gen_app_table_properties(
        self, dataset_urn: str, table: QlikTable
    ) -> MetadataWorkUnit:
        dataset_properties = DatasetProperties(
            name=table.tableName,
            qualifiedName=table.tableAlias,
        )
        dataset_properties.customProperties.update(
            {
                Constant.TYPE: "Qlik Table",
                Constant.DATACONNECTORID: table.dataconnectorid,
                Constant.DATACONNECTORNAME: table.dataconnectorName,
            }
        )
        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=dataset_properties
        ).as_workunit()

    def _get_app_table_identifier(self, table: QlikTable) -> Optional[str]:
        if table.tableQri:
            return table.tableQri.lower()
        return None

    def _gen_app_table_entity(
        self,
        table_identifier: str,
        schema: List[QlikDatasetSchemaField],
        app_id: str,
    ) -> Iterable[MetadataWorkUnit]:
        dataset_urn = self._gen_qlik_dataset_urn(table_identifier)

        yield self._gen_entity_status_aspect(dataset_urn)

        yield self._gen_schema_metadata(table_identifier, schema)

        yield from add_entity_to_container(
            container_key=self._gen_app_key(app_id),
            entity_type="dataset",
            entity_urn=dataset_urn,
        )

        dpi_aspect = self._gen_dataplatform_instance_aspect(dataset_urn)
        if dpi_aspect:
            yield dpi_aspect

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypes(typeNames=[DatasetSubTypes.QLIK_DATASET]),
        ).as_workunit()

    def _get_data_connections(self) -> Dict[str, DataConnectionInfo]:
        if self._data_connections is None:
            self._data_connections = self.qlik_api.get_data_connections()
        return self._data_connections

    def _find_data_connection(
        self, connection: str, space_id: str
    ) -> Optional[DataConnectionInfo]:
        connections = self._get_data_connections()
        space_name = self.qlik_api.spaces.get(space_id)
        if (
            self.config.on_prem is None
            and space_name
            and SPACE_SEPARATOR not in connection
        ):
            # Qlik Cloud looks a bare name up in the app's own space only. A
            # same-named connection elsewhere may point at another database
            # when the app's own one is not visible to the API key.
            return connections.get(f"{space_name}{SPACE_SEPARATOR}{connection}")
        return connections.get(connection)

    def _resolve_script_connection(
        self, connection: Optional[str], space_id: str
    ) -> Optional[ResolvedConnection]:
        if not connection:
            return None
        names = connection_lookup_names(connection)
        configured = next(
            (
                self.config.data_connection_to_platform_instance[name]
                for name in names
                if name in self.config.data_connection_to_platform_instance
            ),
            None,
        )
        inferred = self._find_data_connection(connection, space_id)
        # Inline ODBC/OLEDB connection strings carry the driver and database
        # themselves. A bare name says nothing about the platform behind it.
        if inferred is None and is_inline_connection(connection):
            inferred = infer_data_connection(connection, connection)
        platform = (configured.platform if configured else None) or (
            inferred.platform if inferred else None
        )
        if not platform:
            return None
        return ResolvedConnection(
            platform=platform,
            database=(configured.database if configured else None)
            or (inferred.database if inferred else None),
            default_schema=configured.default_schema if configured else None,
            env=configured.env
            if configured and "env" in configured.model_fields_set
            else self.config.env,
            platform_instance=configured.platform_instance if configured else None,
        )

    def _parse_script_sql(
        self, sql: str, connection: ResolvedConnection
    ) -> SqlParsingResult:
        return create_lineage_sql_parsed_result(
            query=sql,
            default_db=connection.database,
            default_schema=connection.default_schema,
            platform=connection.platform,
            platform_instance=connection.platform_instance,
            env=connection.env,
            graph=self.ctx.graph,
        )

    def _parse_script_table(
        self, app_id: str, script_table: ScriptTable, connection: ResolvedConnection
    ) -> Optional[SqlParsingResult]:
        sql = drop_condition_variables(script_table.sql)
        if "$(" in sql:
            # orders_$(vYear) would parse as a table named orders_$.
            self.reporter.warning(
                title="Qlik load script SQL uses a variable that is not set",
                message="Upstream lineage for this script table is skipped.",
                context=f"app={app_id}, table={script_table.label}",
            )
            return None
        result = self._parse_script_sql(sql, connection)
        if (
            result.debug_info.table_error
            and connection.platform in BACKTICK_QUOTED_PLATFORMS
            and "[" in sql
        ):
            result = self._parse_script_sql(brackets_to_backticks(sql), connection)
        if result.in_tables:
            return result
        if result.debug_info.table_error:
            self.reporter.warning(
                title="Unable to parse Qlik load script SQL",
                message="Upstream lineage for this script table is skipped.",
                context=f"app={app_id}, table={script_table.label}, "
                f"error={_error_type(result.debug_info.table_error)}",
            )
        else:
            logger.debug(
                f"App {app_id} script table {script_table.label!r} reads no tables"
            )
        return None

    @staticmethod
    def _merge_script_table(
        tables: Dict[str, ScriptTableLineage],
        app_id: str,
        script_table: ScriptTable,
        connection_name: Optional[str],
        upstream_urns: List[str],
        result: SqlParsingResult,
    ) -> None:
        # A label loaded by several statements is one Qlik table.
        lineage = tables.setdefault(
            script_table.label,
            ScriptTableLineage(
                label=script_table.label,
                identifier=f"{app_id}.{script_table.label}".lower(),
                connection=connection_name,
            ),
        )
        lineage.upstream_urns.extend(
            urn for urn in upstream_urns if urn not in lineage.upstream_urns
        )
        for column in result.column_lineage or []:
            lineage.column_upstreams.setdefault(column.downstream.column, []).extend(
                column_refs_to_schema_field_urns(
                    ref for ref in column.upstreams if ref.table in upstream_urns
                )
            )

    def _build_script_tables(self, app: App) -> List[ScriptTableLineage]:
        if not app.script:
            return []
        tables: Dict[str, ScriptTableLineage] = {}
        # Tables the data model already traces are ingested from it instead.
        traced = {
            name.lower()
            for table in app.tables
            if table.tableQri
            for name in (table.tableName, table.tableAlias)
        }
        unqualified: List[str] = []
        for script_table in parse_load_script(app.script):
            if script_table.label.lower() in traced:
                continue
            connection_name = (
                redact_connection(script_table.connection)
                if script_table.connection
                else None
            )
            self.reporter.script_tables_found += 1
            if is_connector_sql(script_table.sql):
                logger.debug(
                    f"App {app.id} script table {script_table.label!r} reads from "
                    "a Qlik connector, not a database table"
                )
                continue
            connection = self._resolve_script_connection(
                script_table.connection, app.spaceId
            )
            logger.debug(
                f"App {app.id} script table {script_table.label!r} via connection "
                f"{connection_name!r} resolved to {connection}"
            )
            if connection is None:
                self.reporter.unresolved_data_connections.append(
                    f"{app.id}: {connection_name}"
                )
                continue
            result = self._parse_script_table(app.id, script_table, connection)
            if result is None:
                continue
            upstream_urns = [
                urn
                for urn in result.in_tables
                if has_qualified_name(
                    urn, connection.platform, connection.platform_instance
                )
            ]
            unqualified.extend(
                urn for urn in result.in_tables if urn not in upstream_urns
            )
            if upstream_urns:
                self._merge_script_table(
                    tables, app.id, script_table, connection_name, upstream_urns, result
                )
        if unqualified:
            self.reporter.warning(
                title="Qlik load script table is missing its database or schema",
                message="Lineage to these tables is skipped. Set `database` and "
                "`default_schema` for their connection in "
                "`data_connection_to_platform_instance`.",
                context=f"app={app.id}, tables={sorted(set(unqualified))}",
            )
        self.reporter.script_tables_with_lineage += len(tables)
        return list(tables.values())

    @staticmethod
    def _script_table_fields(
        table: ScriptTableLineage,
    ) -> List[QlikDatasetSchemaField]:
        return [
            QlikDatasetSchemaField(name=column) for column in table.column_upstreams
        ]

    def _gen_script_tables_workunit(
        self, tables: List[ScriptTableLineage], app_id: str
    ) -> Iterable[MetadataWorkUnit]:
        for table in tables:
            dataset_urn = self._gen_qlik_dataset_urn(table.identifier)
            yield from self._gen_app_table_entity(
                table.identifier, self._script_table_fields(table), app_id
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=DatasetProperties(
                    name=table.label,
                    qualifiedName=table.label,
                    customProperties={
                        Constant.TYPE: "Qlik Script Table",
                        Constant.DATACONNECTORNAME: table.connection or "",
                    },
                ),
            ).as_workunit()
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=UpstreamLineage(
                    upstreams=[
                        Upstream(dataset=urn, type=DatasetLineageType.TRANSFORMED)
                        for urn in table.upstream_urns
                    ],
                    fineGrainedLineages=[
                        FineGrainedLineage(
                            upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                            upstreams=upstreams,
                            downstreamType=FineGrainedLineageDownstreamType.FIELD,
                            downstreams=[
                                builder.make_schema_field_urn(dataset_urn, column)
                            ],
                        )
                        for column, upstreams in table.column_upstreams.items()
                        if upstreams
                    ]
                    or None,
                ),
            ).as_workunit()

    def _gen_app_tables_workunit(
        self, tables: List[QlikTable], app_id: str
    ) -> Iterable[MetadataWorkUnit]:
        for table in tables:
            table_identifier = self._get_app_table_identifier(table)
            if not table_identifier:
                continue
            dataset_urn = self._gen_qlik_dataset_urn(table_identifier)

            yield from self._gen_app_table_entity(
                table_identifier, table.datasetSchema, app_id
            )

            yield self._gen_app_table_properties(dataset_urn, table)

            upstream_lineage_workunit = self._gen_app_table_upstream_lineage(
                dataset_urn, table
            )
            if upstream_lineage_workunit:
                yield upstream_lineage_workunit

    def _gen_app_workunit(
        self, app: App, fingerprint: Optional[str] = None
    ) -> Iterable[MetadataWorkUnit]:
        """
        Map Qlik App to Datahub container
        """
        owner_username = self._get_owner_name(app.ownerId)
        if self.config.on_prem is None and not app.tables:
            self.reporter.apps_without_load_model.append(app.id)
        script_tables = self._build_script_tables(app)
        input_tables_urns = [
            self._gen_qlik_dataset_urn(identifier)
            for identifier in map(self._get_app_table_identifier, app.tables)
            if identifier
        ] + [self._gen_qlik_dataset_urn(table.identifier) for table in script_tables]
        container_workunits = list(
            gen_containers(
                container_key=self._gen_app_key(app.id),
                name=app.qTitle,
                description=app.description,
                sub_types=[BIContainerSubTypes.QLIK_APP],
                parent_container_key=self._gen_space_key(app.spaceId),
                extra_properties={
                    Constant.QRI: app.qri,
                    **({Constant.USAGE: app.qUsage} if app.qUsage else {}),
                },
                owner_urn=(
                    builder.make_user_urn(owner_username)
                    if self.config.ingest_owner and owner_username
                    else None
                ),
                external_url=f"https://{self.ui_host}/sense/app/{app.id}/overview",
                created=int(app.createdAt.timestamp() * 1000),
                last_modified=int(app.updatedAt.timestamp() * 1000),
            )
        )
        yield from container_workunits
        yield from self._gen_sheets_workunit(
            app, None if app.tables_read_failed else input_tables_urns
        )
        yield from self._gen_app_tables_workunit(app.tables, app.id)
        yield from self._gen_script_tables_workunit(script_tables, app.id)
        if fingerprint:
            # Skipping an unchanged app trusts this fingerprint, so it is written
            # only after everything else the app produced.
            yield self._gen_fingerprint_workunit(container_workunits, fingerprint)

    def _gen_fingerprint_workunit(
        self, container_workunits: List[MetadataWorkUnit], fingerprint: str
    ) -> MetadataWorkUnit:
        properties = next(
            aspect
            for aspect in (
                wu.get_aspect_of_type(ContainerPropertiesClass)
                for wu in container_workunits
            )
            if aspect
        )
        stamped = copy.deepcopy(properties)
        stamped.customProperties = {
            **(stamped.customProperties or {}),
            Constant.FINGERPRINT: fingerprint,
        }
        urn = container_workunits[0].get_urn()
        return MetadataWorkUnit(
            id=f"{urn}-fingerprint",
            mcp=MetadataChangeProposalWrapper(entityUrn=urn, aspect=stamped),
        )

    def _gen_qlik_dataset_urn(self, dataset_identifier: str) -> str:
        return builder.make_dataset_urn_with_platform_instance(
            name=dataset_identifier,
            env=self.config.env,
            platform=self.platform,
            platform_instance=self.config.platform_instance,
        )

    def _gen_dataplatform_instance_aspect(
        self, entity_urn: str
    ) -> Optional[MetadataWorkUnit]:
        if self.config.platform_instance:
            aspect = DataPlatformInstanceClass(
                platform=builder.make_data_platform_urn(self.platform),
                instance=builder.make_dataplatform_instance_urn(
                    self.platform, self.config.platform_instance
                ),
            )
            return MetadataChangeProposalWrapper(
                entityUrn=entity_urn, aspect=aspect
            ).as_workunit()
        else:
            return None

    def _gen_schema_fields(
        self, schema: List[QlikDatasetSchemaField]
    ) -> List[SchemaField]:
        schema_fields: List[SchemaField] = []
        for field in schema:
            schema_field = SchemaField(
                fieldPath=field.name,
                type=SchemaFieldDataTypeClass(
                    type=(
                        FIELD_TYPE_MAPPING.get(field.dataType, NullType)()
                        if field.dataType
                        else NullType()
                    )
                ),
                nativeDataType=field.dataType if field.dataType else "",
                nullable=field.nullable,
                isPartOfKey=field.primaryKey,
            )
            schema_fields.append(schema_field)
        return schema_fields

    def _gen_schema_metadata(
        self, dataset_identifier: str, dataset_schema: List[QlikDatasetSchemaField]
    ) -> MetadataWorkUnit:
        dataset_urn = self._gen_qlik_dataset_urn(dataset_identifier)

        schema_metadata = SchemaMetadata(
            schemaName=dataset_identifier,
            platform=builder.make_data_platform_urn(self.platform),
            version=0,
            hash="",
            platformSchema=MySqlDDL(tableSchema=""),
            fields=self._gen_schema_fields(dataset_schema),
        )

        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=schema_metadata
        ).as_workunit()

    def _gen_dataset_properties(
        self, dataset_urn: str, dataset: QlikDataset
    ) -> MetadataWorkUnit:
        dataset_properties = DatasetProperties(
            name=dataset.name,
            description=dataset.description,
            qualifiedName=dataset.name,
            externalUrl=f"https://{self.ui_host}/dataset/{dataset.itemId}",
            created=TimeStamp(time=int(dataset.createdAt.timestamp() * 1000)),
            lastModified=TimeStamp(time=int(dataset.updatedAt.timestamp() * 1000)),
        )
        dataset_properties.customProperties.update(
            {
                Constant.QRI: dataset.secureQri,
                Constant.SPACEID: dataset.spaceId,
                Constant.TYPE: "Qlik Dataset",
                Constant.DATASETTYPE: dataset.type,
                Constant.SIZE: str(dataset.size),
                Constant.ROWCOUNT: str(dataset.rowCount),
            }
        )

        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=dataset_properties
        ).as_workunit()

    def _get_qlik_dataset_identifier(self, dataset: QlikDataset) -> str:
        return f"{dataset.spaceId}.{dataset.name}".lower()

    def _gen_dataset_workunit(self, dataset: QlikDataset) -> Iterable[MetadataWorkUnit]:
        dataset_identifier = self._get_qlik_dataset_identifier(dataset)
        dataset_urn = self._gen_qlik_dataset_urn(dataset_identifier)

        yield self._gen_entity_status_aspect(dataset_urn)

        yield self._gen_schema_metadata(dataset_identifier, dataset.datasetSchema)

        yield self._gen_dataset_properties(dataset_urn, dataset)

        yield from add_entity_to_container(
            container_key=self._gen_space_key(dataset.spaceId),
            entity_type="dataset",
            entity_urn=dataset_urn,
        )

        dpi_aspect = self._gen_dataplatform_instance_aspect(dataset_urn)
        if dpi_aspect:
            yield dpi_aspect

        owner_username = self._get_owner_name(dataset.ownerId)
        if self.config.ingest_owner and owner_username:
            yield self._gen_entity_owner_aspect(dataset_urn, owner_username)

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypes(typeNames=[DatasetSubTypes.QLIK_DATASET]),
        ).as_workunit()

    def _gen_item_workunits(
        self,
        item: Union[Item, UnchangedApp],
        space_name: str,
        record_state: bool,
    ) -> Iterable[MetadataWorkUnit]:
        if isinstance(item, UnchangedApp):
            self._keep_unchanged_app(item)
        elif isinstance(item, App):
            self.reporter.apps_read += 1
            logger.info(
                f"Processed app {item.qTitle} in space {space_name} "
                f"({self.reporter.apps_read} apps read from the Engine)"
            )
            if (
                not item.read_complete
                and record_state
                and self._keep_partly_read_app(item)
            ):
                self.reporter.warning(
                    title="Unable to fully read Qlik app",
                    message="Nothing from this read of the app is emitted. Its "
                    "metadata stays as of its last complete read until a read of "
                    "it completes.",
                    context=self._read_errors_context(item),
                )
                return
            if not item.read_complete:
                # Reporting a failure also stops stale entity removal from
                # soft-deleting what this app's last full read produced.
                self.reporter.failure(
                    title="Unable to fully read Qlik app",
                    message="Some sheets, charts or load script lineage of this "
                    "app could not be read from the Engine.",
                    context=self._read_errors_context(item),
                )
            if item.script_read_failed:
                self.reporter.warning(
                    title="Unable to read Qlik app load script",
                    message="Load script lineage of this app is skipped. Reading "
                    "load scripts needs the Owner, Can manage or Can operate role "
                    "in managed spaces.",
                    context=item.id,
                )
            yield from (
                self._gen_recorded_app_workunit(item)
                if record_state
                else self._gen_app_workunit(item)
            )
        elif isinstance(item, QlikDataset):
            yield from self._gen_dataset_workunit(item)

    @staticmethod
    def _read_errors_context(app: App) -> str:
        first, *rest = app.read_errors
        more = f" (and {len(rest)} more)" if rest else ""
        return f"{app.id}: {first}{more}"

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """
        Datahub Ingestion framework invoke this method
        """
        logger.info("Qlik Sense plugin execution is started")
        allowed_spaces = self._get_allowed_spaces()
        skip_unchanged = self._skips_unchanged_apps()
        record_state = self.app_state.is_checkpointing_enabled()
        if skip_unchanged:
            self._init_fingerprint_salt()
        space_names = {space.id: space.name for space in allowed_spaces}
        for space in allowed_spaces:
            yield from self._gen_space_workunit(space)
        logger.info(f"Reading the items of {len(allowed_spaces)} spaces")
        # Items of all spaces share one pool of workers, so max_workers bounds
        # the whole run.
        items = self.qlik_api.get_items(
            [
                "personal" if space.id == Constant.PERSONAL_SPACE_ID else space.id
                for space in allowed_spaces
            ],
            is_unchanged=self._is_app_unchanged if skip_unchanged else None,
        )
        for item in items:
            space_id = getattr(item, "spaceId", "")
            yield from self._gen_item_workunits(
                item, space_names.get(space_id, space_id), record_state
            )
        change_check_failures = [
            *self.qlik_api.change_check_failures,
            *self._change_check_failures,
        ]
        for failure in change_check_failures:
            self.reporter.apps_change_check_failed.append(failure)
        if change_check_failures:
            # Reported here rather than from the worker threads that check apps.
            self.reporter.warning(
                title="Unable to check whether Qlik apps changed",
                message="These apps were read in full instead of skipped. "
                "See apps_change_check_failed.",
                context=f"{len(change_check_failures)} apps, for example "
                f"{change_check_failures[0]}",
            )
        failed_users = self.qlik_api.failed_user_lookups
        if failed_users:
            self.reporter.warning(
                title="Unable to look up Qlik owners",
                message="Ownership is left out for entities owned by these users.",
                context=", ".join(
                    f"{user_id}: {cause}"
                    for user_id, cause in sorted(failed_users.items())
                ),
            )
        for unread in self.qlik_api.unread_items:
            # Also stops stale entity removal from soft-deleting what was not read.
            self.reporter.failure(
                title="Unable to read from Qlik",
                message="Left out of this run after a request failed.",
                context=f"{unread.name}: {unread.cause}",
            )
        if self.reporter.failures:
            # Like the stale entity removal state, a failed run keeps the last
            # read of the apps it did not reach.
            self.app_state.keep_unrecorded_apps()
        if self.app_state.is_too_large():
            self.reporter.warning(
                title="Qlik app checkpoint is too large to save",
                message="Unchanged apps are not skipped and partly read apps are "
                "reported as failures until it fits. Raise "
                "stateful_ingestion.max_checkpoint_state_size.",
            )

    def get_report(self) -> SourceReport:
        return self.reporter
