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
from datahub.ingestion.graph.filters import RemovedStatusFilter
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
    QlikDeployment,
    QlikSourceConfig,
    QlikSourceReport,
)
from datahub.ingestion.source.qlik_sense.data_classes import (
    FIELD_TYPE_MAPPING,
    KNOWN_DATA_PLATFORM_MAPPING,
    App,
    AppField,
    AppKey,
    BoxType,
    Chart,
    QlikDataset,
    QlikTable,
    SchemaField as QlikDatasetSchemaField,
    Sheet,
    Space,
    SpaceKey,
    UnchangedApp,
)
from datahub.ingestion.source.qlik_sense.qlik_api import QlikAPI
from datahub.ingestion.source.qlik_sense.qlik_onprem_api import QlikOnPremAPI
from datahub.ingestion.source.qlik_sense.script_lineage import (
    DataConnectionInfo,
    ScriptTableLineage,
    apply_preceding_loads,
    connection_lookup_names,
    expression_names,
    infer_data_connection,
    parse_load_script,
    strip_set_values,
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
    InputFieldClass,
    InputFieldsClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaFieldDataTypeClass,
)
from datahub.sql_parsing.sqlglot_lineage import create_lineage_sql_parsed_result
from datahub.utilities.urns.urn import guess_entity_type

# Logger instance
logger = logging.getLogger(__name__)


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
    qlik_api: Union[QlikAPI, QlikOnPremAPI]

    def __init__(self, config: QlikSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.reporter = QlikSourceReport()
        self.ui_host = config.ui_hostname()
        self._data_connections: Optional[Dict[str, DataConnectionInfo]] = None
        # Owned here rather than by the automatic processor so that entities of
        # apps skipped as unchanged can be kept in the state.
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
            if config.skip_unchanged_apps
            else None
        )
        self.app_state = QlikAppStateHandler(
            state_provider=self.state_provider,
            config=self.config,
            pipeline_name=ctx.pipeline_name,
            run_id=ctx.run_id,
        )
        self._fingerprint_salt: Optional[str] = None
        if config.skip_unchanged_apps and not self._skips_unchanged_apps():
            self.reporter.warning(
                title="skip_unchanged_apps is ignored",
                message="Skipping unchanged apps needs deployment: on_prem and "
                "stateful ingestion, so every app is read.",
            )
        try:
            if config.deployment == QlikDeployment.ON_PREM:
                self.qlik_api = QlikOnPremAPI(self.config)
            else:
                self.qlik_api = QlikAPI(self.config)
        except Exception as e:
            logger.warning(e)
            exit(
                1
            )  # Exit pipeline as we are not able to connect to Qlik Client Service.

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        try:
            config = QlikSourceConfig.parse_obj_allow_extras(config_dict)
            if config.deployment == QlikDeployment.ON_PREM:
                QlikOnPremAPI(config)
            else:
                QlikAPI(config)
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
            self.config.skip_unchanged_apps
            and self.config.deployment == QlikDeployment.ON_PREM
            and self.app_state.is_checkpointing_enabled()
        )

    def _init_fingerprint_salt(self) -> None:
        # The recipe and data connections shape every app's output, and a new
        # connector version may emit differently, so any change re-reads all apps.
        if self._data_connections is None:
            self._data_connections = self.qlik_api.get_data_connections()
        self._fingerprint_salt = json.dumps(
            {
                "version": __version__,
                "config": self.config.model_dump(
                    mode="json",
                    exclude={
                        "stateful_ingestion",
                        "max_workers",
                        "skip_unchanged_apps",
                    },
                ),
                "connections": {
                    name: info.model_dump(mode="json")
                    for name, info in sorted(self._data_connections.items())
                },
            },
            sort_keys=True,
        )

    def _salted_fingerprint(self, fingerprint: str) -> str:
        assert self._fingerprint_salt is not None
        return hashlib.sha256(
            f"{self._fingerprint_salt}|{fingerprint}".encode("utf-8")
        ).hexdigest()

    def _is_app_unchanged(self, app_id: str, fingerprint: str) -> bool:
        previous = self.app_state.previous(app_id)
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
            status = graph.get_aspect(container_urn, Status)
            soft_deleted_child = next(
                iter(
                    graph.get_urns_by_filter(
                        container=container_urn,
                        status=RemovedStatusFilter.ONLY_SOFT_DELETED,
                        batch_size=1,
                    )
                ),
                None,
            )
        except Exception as e:
            logger.debug(f"Unable to read app container {container_urn}: {e}")
            return False
        return (
            properties is not None
            and (properties.customProperties or {}).get(Constant.FINGERPRINT)
            == previous.fingerprint
            and not (status is not None and status.removed)
            and soft_deleted_child is None
        )

    def _keep_previous_urns(self, app_id: str) -> List[str]:
        previous = self.app_state.previous(app_id)
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
        self.app_state.record(
            app.id,
            AppCheckpoint(
                fingerprint=self._salted_fingerprint(app.fingerprint), urns=urns
            ),
        )
        self.reporter.apps_skipped_unchanged += 1

    def _gen_recorded_app_workunit(self, app: App) -> Iterable[MetadataWorkUnit]:
        self.reporter.apps_read += 1
        urns: Dict[str, None] = {}
        for wu in self._gen_app_workunit(app):
            urn = wu.get_urn()
            if guess_entity_type(urn) not in STATEFUL_INGESTION_IGNORED_ENTITY_TYPES:
                urns[urn] = None
            yield wu
        if app.fingerprint and app.read_complete:
            self.app_state.record(
                app.id, AppCheckpoint(fingerprint=app.fingerprint, urns=list(urns))
            )
            return
        # A failed read emits only part of the app, so keep what the last read
        # produced. The empty fingerprint never matches, so the next run reads it.
        urns.update(dict.fromkeys(self._keep_previous_urns(app.id)))
        self.app_state.record(app.id, AppCheckpoint(fingerprint="", urns=list(urns)))

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
        owner_username: Optional[str] = None
        if space.ownerId:
            owner_username = self.qlik_api.get_user_name(space.ownerId)
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
            if self.config.deployment == QlikDeployment.ON_PREM
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
        input_tables_urns: List[str],
        app_fields: Dict[str, List[AppField]],
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
            chart_fields = self._get_chart_input_fields(chart, app_fields)
            # The fields behind a $(variable) expansion are unknown, so the chart
            # may read any of the app's tables, as may a measure that could not
            # be read. Set modifier values only filter.
            reads_unknown_fields = not chart.fields_complete or any(
                "$(" in strip_set_values(expression)
                for expression in chart.field_expressions
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=chart_urn,
                aspect=ChartInfoClass(
                    title=chart.title,
                    description=chart.visualization,
                    lastModified=ChangeAuditStampsClass(),
                    customProperties=custom_properties,
                    inputs=(
                        input_tables_urns
                        if reads_unknown_fields
                        else list(
                            dict.fromkeys(field.dataset_urn for field in chart_fields)
                        )
                        or input_tables_urns
                    ),
                ),
            ).as_workunit()

            # Emitted even when empty so fields a chart stopped using are cleared.
            yield MetadataChangeProposalWrapper(
                entityUrn=chart_urn,
                aspect=InputFieldsClass(
                    fields=[
                        InputFieldClass(
                            schemaFieldUrn=builder.make_schema_field_urn(
                                field.dataset_urn, field.field.name
                            ),
                            schemaField=self._gen_schema_fields([field.field])[0],
                        )
                        for field in chart_fields
                    ]
                ),
            ).as_workunit()

            yield from add_entity_to_container(
                container_key=self._gen_app_key(app_id),
                entity_type="chart",
                entity_urn=chart_urn,
            )

    @staticmethod
    def _get_chart_input_fields(
        chart: Chart, app_fields: Dict[str, List[AppField]]
    ) -> List[AppField]:
        matched: Dict[str, AppField] = {}
        for expression in chart.field_expressions:
            # A dimension is usually a bare field name, which may contain spaces.
            names = (
                [expression]
                if expression.lower() in app_fields
                else expression_names(expression)
            )
            for name in names:
                for app_field in app_fields.get(name.lower(), []):
                    matched.setdefault(
                        f"{app_field.dataset_urn}.{app_field.field.name}", app_field
                    )
        return list(matched.values())

    @staticmethod
    def _index_app_fields(
        tables: Dict[str, List[QlikDatasetSchemaField]],
    ) -> Dict[str, List[AppField]]:
        # Qlik field names are case-sensitive, but the SQL parser may change the
        # case of script table columns.
        app_fields: Dict[str, List[AppField]] = {}
        for dataset_urn, fields in tables.items():
            for field in fields:
                app_fields.setdefault(field.name.lower(), []).append(
                    AppField(dataset_urn=dataset_urn, field=field)
                )
        return app_fields

    def _gen_sheets_workunit(
        self,
        app: App,
        input_tables_urns: List[str],
        app_fields: Dict[str, List[AppField]],
    ) -> Iterable[MetadataWorkUnit]:
        """
        Map Qlik Sheet to Datahub dashboard
        """
        for sheet in app.sheets:
            dashboard_urn = self._gen_dashboard_urn(sheet.id)

            yield self._gen_entity_status_aspect(dashboard_urn)

            yield self._gen_dashboard_info_workunit(sheet, app.id)

            yield from add_entity_to_container(
                container_key=self._gen_app_key(app.id),
                entity_type="dashboard",
                entity_urn=dashboard_urn,
            )

            dpi_aspect = self._gen_dataplatform_instance_aspect(dashboard_urn)
            if dpi_aspect:
                yield dpi_aspect

            owner_username = (
                self.qlik_api.get_user_name(sheet.ownerId) if sheet.ownerId else None
            )
            if self.config.ingest_owner and owner_username:
                yield self._gen_entity_owner_aspect(dashboard_urn, owner_username)

            yield from self._gen_charts_workunit(
                sheet.charts, input_tables_urns, app_fields, app.id
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
            result = create_lineage_sql_parsed_result(
                query=table.selectStatement.strip(),
                default_db=upstream_dataset_platform_detail.database
                or table.databaseName,
                default_schema=upstream_dataset_platform_detail.default_schema
                or table.schemaName,
                platform=upstream_dataset_platform_detail.platform
                or KNOWN_DATA_PLATFORM_MAPPING.get(
                    table.dataconnectorPlatform, table.dataconnectorPlatform
                ),
                env=upstream_dataset_platform_detail.env,
                platform_instance=upstream_dataset_platform_detail.platform_instance,
            )
            if not result.in_tables:
                self.reporter.warning(
                    title="Unable to parse Qlik table SQL",
                    message="Upstream lineage for this Qlik table is skipped.",
                    context=f"table={table.tableName}, connection={table.dataconnectorName}",
                    exc=result.debug_info.table_error,
                )
                return None
            upstream_dataset_urn = result.in_tables[0]
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

    def _resolve_script_connection(
        self, connection: Optional[str]
    ) -> Optional[PlatformDetail]:
        if not connection:
            return None
        if self._data_connections is None:
            self._data_connections = self.qlik_api.get_data_connections()
        names = connection_lookup_names(connection)
        configured = next(
            (
                self.config.data_connection_to_platform_instance[name]
                for name in names
                if name in self.config.data_connection_to_platform_instance
            ),
            None,
        )
        # Inline ODBC/OLEDB connection strings carry the driver and database themselves.
        inferred = next(
            (
                self._data_connections[name]
                for name in names
                if name in self._data_connections
            ),
            None,
        ) or infer_data_connection(connection, connection)
        platform = (configured.platform if configured else None) or inferred.platform
        if not platform:
            return None
        return PlatformDetail(
            platform=platform,
            database=(configured.database if configured else None) or inferred.database,
            default_schema=configured.default_schema if configured else None,
            env=configured.env
            if configured and "env" in configured.model_fields_set
            else self.config.env,
            platform_instance=configured.platform_instance if configured else None,
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
        for script_table in parse_load_script(app.script):
            if script_table.label.lower() in traced:
                continue
            self.reporter.script_tables_found += 1
            detail = self._resolve_script_connection(script_table.connection)
            logger.debug(
                f"App {app.id} script table {script_table.label!r} via connection "
                f"{script_table.connection!r} resolved to {detail}"
            )
            if not detail or not detail.platform:
                self.reporter.unresolved_data_connections.append(
                    f"{app.id}: {script_table.connection}"
                )
                continue
            result = create_lineage_sql_parsed_result(
                query=script_table.sql,
                default_db=detail.database,
                default_schema=detail.default_schema,
                platform=detail.platform,
                platform_instance=detail.platform_instance,
                env=detail.env,
                graph=self.ctx.graph,
            )
            if not result.in_tables:
                self.reporter.warning(
                    title="Unable to parse Qlik load script SQL",
                    message="Upstream lineage for this script table is skipped.",
                    context=f"app={app.id}, table={script_table.label}",
                    exc=result.debug_info.table_error,
                )
                continue
            lineage = tables.setdefault(
                script_table.label,
                ScriptTableLineage(
                    label=script_table.label,
                    identifier=f"{app.id}.{script_table.label}".lower(),
                    connection=script_table.connection,
                ),
            )
            lineage.upstream_urns.extend(
                urn for urn in result.in_tables if urn not in lineage.upstream_urns
            )
            sql_columns: Dict[str, List[str]] = {}
            for column in result.column_lineage or []:
                sql_columns.setdefault(column.downstream.column, []).extend(
                    column.upstream_schema_field_urns()
                )
            for field, upstreams in apply_preceding_loads(
                script_table.preceding_loads, sql_columns
            ).items():
                lineage.column_upstreams.setdefault(field, []).extend(upstreams)
        self.reporter.script_tables_with_lineage += len(tables)
        return list(tables.values())

    def _gen_script_tables_workunit(
        self, tables: List[ScriptTableLineage], app_id: str
    ) -> Iterable[MetadataWorkUnit]:
        for table in tables:
            dataset_urn = self._gen_qlik_dataset_urn(table.identifier)
            yield from self._gen_app_table_entity(
                table.identifier,
                [
                    QlikDatasetSchemaField(name=column)
                    for column in table.column_upstreams
                ],
                app_id,
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

    def _gen_app_workunit(self, app: App) -> Iterable[MetadataWorkUnit]:
        """
        Map Qlik App to Datahub container
        """
        owner_username = self.qlik_api.get_user_name(app.ownerId)
        if self.config.deployment == QlikDeployment.CLOUD and not app.tables:
            self.reporter.apps_without_load_model.append(app.id)
        script_tables = self._build_script_tables(app)
        table_fields: Dict[str, List[QlikDatasetSchemaField]] = {}
        for table in app.tables:
            identifier = self._get_app_table_identifier(table)
            if identifier:
                table_fields[self._gen_qlik_dataset_urn(identifier)] = (
                    table.datasetSchema
                )
        for script_table in script_tables:
            table_fields[self._gen_qlik_dataset_urn(script_table.identifier)] = [
                QlikDatasetSchemaField(name=column)
                for column in script_table.column_upstreams
            ]
        input_tables_urns = list(table_fields)
        container_workunits = list(
            gen_containers(
                container_key=self._gen_app_key(app.id),
                name=app.qTitle,
                description=app.description,
                sub_types=[BIContainerSubTypes.QLIK_APP],
                parent_container_key=self._gen_space_key(app.spaceId),
                extra_properties={
                    Constant.QRI: app.qri,
                    Constant.USAGE: app.qUsage,
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
            app, input_tables_urns, self._index_app_fields(table_fields)
        )
        yield from self._gen_app_tables_workunit(app.tables, app.id)
        yield from self._gen_script_tables_workunit(script_tables, app.id)
        if app.fingerprint:
            # Skipping an unchanged app trusts this fingerprint, so it is written
            # only after everything else the app produced.
            yield self._gen_fingerprint_workunit(container_workunits, app.fingerprint)

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

        owner_username = self.qlik_api.get_user_name(dataset.ownerId)
        if self.config.ingest_owner and owner_username:
            yield self._gen_entity_owner_aspect(dataset_urn, owner_username)

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypes(typeNames=[DatasetSubTypes.QLIK_DATASET]),
        ).as_workunit()

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """
        Datahub Ingestion framework invoke this method
        """
        logger.info("Qlik Sense plugin execution is started")
        allowed_spaces = self._get_allowed_spaces()
        skip_unchanged = self._skips_unchanged_apps()
        if skip_unchanged:
            self._init_fingerprint_salt()
        for space_number, allowed_space in enumerate(allowed_spaces, start=1):
            logger.info(
                f"Reading space {space_number} of {len(allowed_spaces)}: {allowed_space.name}"
            )
            yield from self._gen_space_workunit(allowed_space)
            space_id = (
                "personal"
                if allowed_space.id == Constant.PERSONAL_SPACE_ID
                else allowed_space.id
            )
            items = (
                self.qlik_api.get_items(space_id, is_unchanged=self._is_app_unchanged)
                if skip_unchanged and isinstance(self.qlik_api, QlikOnPremAPI)
                else self.qlik_api.get_items(space_id)
            )
            for item in items:
                if isinstance(item, UnchangedApp):
                    self._keep_unchanged_app(item)
                elif isinstance(item, App) and skip_unchanged:
                    if not item.read_complete:
                        self.reporter.warning(
                            title="Unable to fully read Qlik app",
                            message="Entities from the last full read of this app "
                            "are kept, and it is read again on the next run.",
                            context=item.id,
                        )
                    if item.fingerprint:
                        item.fingerprint = self._salted_fingerprint(item.fingerprint)
                    yield from self._gen_recorded_app_workunit(item)
                elif isinstance(item, App):
                    if not item.read_complete:
                        # Reporting a failure also stops stale entity removal from
                        # soft-deleting what this app's last full read produced.
                        self.reporter.failure(
                            title="Unable to fully read Qlik app",
                            message="Some sheets, charts or load script lineage of this "
                            "app could not be read from the Engine.",
                            context=item.id,
                        )
                    self.reporter.apps_read += 1
                    logger.info(
                        f"Read app {item.qTitle} in space {allowed_space.name} ({self.reporter.apps_read} apps read)"
                    )
                    yield from self._gen_app_workunit(item)
                elif isinstance(item, QlikDataset):
                    yield from self._gen_dataset_workunit(item)
        for context in self.qlik_api.unread_items:
            # Also stops stale entity removal from soft-deleting what was not read.
            self.reporter.failure(
                title="Unable to read from Qlik",
                message="Left out of this run after a request failed.",
                context=context,
            )

    def get_report(self) -> SourceReport:
        return self.reporter
