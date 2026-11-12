import base64
import copy
import functools
import hashlib
import json
import logging
import re
from datetime import datetime
from functools import partial
from typing import Collection, Dict, Iterable, List, Optional, Set, Tuple, Union

from pydantic import BaseModel

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
from datahub.ingestion.api.incremental_lineage_helper import (
    convert_upstream_lineage_to_patch,
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
    AppField,
    AppFieldIndex,
    AppKey,
    BoxType,
    Chart,
    Item,
    LineageTable,
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
    app_table_identifier,
    apply_crosstable,
    apply_preceding_loads,
    brackets_to_backticks,
    connection_lookup_names,
    drop_condition_variables,
    expand_variables,
    expression_names,
    has_qualified_name,
    infer_data_connection,
    is_connector_sql,
    is_inline_connection,
    parse_load_script,
    reads_unknown_fields,
    redact_connection,
    select_list_size,
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
from datahub.metadata.urns import SchemaFieldUrn
from datahub.specific.chart import ChartPatchBuilder
from datahub.specific.dataset import DatasetPatchBuilder
from datahub.sql_parsing.sqlglot_lineage import (
    SqlParsingResult,
    column_refs_to_schema_field_urns,
    create_lineage_sql_parsed_result,
)
from datahub.utilities.str_enum import StrEnum
from datahub.utilities.urns.urn import guess_entity_type

# Logger instance
logger = logging.getLogger(__name__)


class _PartialRead(StrEnum):
    KEPT = "kept"
    LIMIT_REACHED = "limit_reached"
    NOT_KEPT = "not_kept"


# Entities per DataHub request when checking that a skipped app's entities exist.
ENTITY_CHECK_BATCH_SIZE = 100

# Lineage graph QRIs of Qlik dataset tables, `<dataset secureQri>#<table>`.
# An analytics app is a sense resource in the lineage graph, a script app a
# dataprep resource.
QLIK_APP_QRI_RE = re.compile(r"^qri:app:(?:sense|dataprep)://([^#]+)")
QVD_EXTENSION = ".qvd"


def _is_qvd(dataset: QlikDataset) -> bool:
    return dataset.type.upper() == "QVD" or dataset.name.lower().endswith(QVD_EXTENSION)


def _dataset_subtype(dataset: QlikDataset) -> str:
    return (
        DatasetSubTypes.QLIK_QVD if _is_qvd(dataset) else DatasetSubTypes.QLIK_DATASET
    )


def _dataset_display_name(dataset: QlikDataset) -> str:
    # The URN keeps the file name, so `sales.qvd` and `sales.csv` in one space
    # stay apart and QVDs ingested before keep their URNs.
    if _is_qvd(dataset) and dataset.name.lower().endswith(QVD_EXTENSION):
        return dataset.name[: -len(QVD_EXTENSION)]
    return dataset.name


def _qlik_app_id(qri: str) -> Optional[str]:
    match = QLIK_APP_QRI_RE.match(qri)
    return match.group(1) if match else None


def _schema_field_parent(urn: str) -> Optional[str]:
    # Lineage another tool wrote may hold any kind of URN.
    try:
        return SchemaFieldUrn.from_string(urn).parent
    except Exception:
        return None


def _qlik_field_id(name: str) -> str:
    """The last part of a field node's QRI in Qlik's lineage graph, which is the
    unpadded base64url SHA-256 of the field name. Not documented by Qlik, so it
    is only used to confirm a name, never to make one up."""
    digest = hashlib.sha256(name.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")


class _ResolvedSources(BaseModel):
    urns: List[str] = []
    # False when a source could not be resolved this run.
    complete: bool = True


def _bounded_list(items: List[str], limit: int = 10) -> str:
    more = f" (and {len(items) - limit} more)" if len(items) > limit else ""
    return ", ".join(items[:limit]) + more


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
    "Enabled by default, can be disabled via configuration `include_column_lineage`",
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
    - Tables of each app's data model
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
        self._allowed_space_ids: Set[str] = set()
        self._reported_no_graph = False
        # Datasets with a column edge whose source field could not be named.
        self._unnamed_field_lineage: Set[str] = set()
        # Table QRI -> dataset URN for the tables of apps read or kept this run.
        # Lineage only links to tables in here, as other URNs may never exist.
        self._app_table_urns_by_qri: Dict[str, str] = {}
        # Apps read or kept this run, by whether all their tables are known.
        self._apps_tables_known: Set[str] = set()
        self._apps_tables_unknown: Set[str] = set()
        # The data model fields of the app being emitted, by table identifier.
        self._data_model_fields: Dict[str, List[QlikDatasetSchemaField]] = {}
        # Datasets and the app table QRIs stored into them, linked once every
        # app has been read.
        self._dataset_store_sources: Dict[str, List[str]] = {}
        self._dataset_store_field_sources: Dict[str, Dict[str, List[str]]] = {}
        self._dataset_store_fields_unknown: Set[str] = set()
        # Graph tables whose fields have no type, such as a table the script
        # drops after storing it, by URN.
        self._untyped_tables: Dict[str, Tuple[str, List[QlikDatasetSchemaField]]] = {}
        self._dataset_field_types: Dict[str, Dict[str, Optional[str]]] = {}
        # Field names seen this run, by their field id in Qlik's lineage graph.
        self._field_names_by_id: Dict[str, str] = {}
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
                        "max_partial_reads_kept": True,
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
            self._apps_tables_unknown.add(app_id)
            return []
        if previous is None:
            self._apps_tables_unknown.add(app_id)
            return []
        if previous.table_urns_by_qri is None:
            # Recorded before table URNs were kept in the state.
            self._apps_tables_unknown.add(app_id)
        else:
            self._apps_tables_known.add(app_id)
            self._app_table_urns_by_qri.update(previous.table_urns_by_qri)
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
                    fingerprint=self._salted_fingerprint(app.fingerprint),
                    urns=urns,
                    table_urns_by_qri=self._kept_table_urns(app.id),
                ),
            )
        self.reporter.apps_skipped_unchanged += 1

    def _keep_partly_read_app(self, app: App) -> _PartialRead:
        """Keeps what the app's last read produced instead of emitting a partial
        read, which would overwrite its relationships, for up to
        max_partial_reads_kept consecutive runs."""
        failures: List[str] = []
        previous = self._usable_checkpoint(app.id, failures)
        for failure in failures:
            self.reporter.warning(
                title="Unable to load the last checkpoint of a Qlik app",
                message="The partly read app could not keep its last read.",
                context=failure,
            )
        if previous is None:
            return _PartialRead.NOT_KEPT
        if previous.partial_reads >= self.config.max_partial_reads_kept:
            return _PartialRead.LIMIT_REACHED
        urns = self._keep_previous_urns(app.id)
        if not urns:
            return _PartialRead.NOT_KEPT
        # No fingerprint, so the next run reads the app again.
        self.app_state.record(
            app.id,
            AppCheckpoint(
                fingerprint=None,
                urns=urns,
                partial_reads=previous.partial_reads + 1,
                table_urns_by_qri=previous.table_urns_by_qri,
            ),
        )
        return _PartialRead.KEPT

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
            app.id,
            AppCheckpoint(
                fingerprint=fingerprint,
                urns=list(urns),
                # Tables that could not all be read are recorded as unknown, so
                # a later run can't take them for the app having no tables.
                table_urns_by_qri=None
                if app.tables_read_failed
                else self._app_table_urns(app),
            ),
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
        app_fields: AppFieldIndex,
        has_unknown_schema: bool,
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
                        inputs=self._get_chart_inputs(
                            chart,
                            chart_fields,
                            app_fields,
                            input_tables_urns,
                            has_unknown_schema,
                        ),
                    ),
                ).as_workunit()

            # Emitted even when empty so fields a chart stopped using are cleared,
            # but not from a partial list of a chart whose measures were unread.
            if input_tables_urns is not None and not chart.fields_unread:
                yield MetadataChangeProposalWrapper(
                    entityUrn=chart_urn,
                    aspect=InputFieldsClass(
                        fields=[
                            InputFieldClass(
                                schemaFieldUrn=builder.make_schema_field_urn(
                                    field.dataset_urn, field.field.name
                                ),
                                schemaField=self._gen_schema_field(field.field),
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

    @staticmethod
    def _chart_field_names(expression: str, app_fields: AppFieldIndex) -> List[str]:
        # A dimension is usually a bare field name, which may contain spaces.
        return (
            [expression] if app_fields.get(expression) else expression_names(expression)
        )

    @staticmethod
    def _app_field_key(name: str, app_fields: AppFieldIndex) -> Optional[str]:
        """The app field a chart name reads. A derived field such as
        Date.autoCalendar.Year reads its base field, the longest leading part
        of the name that is a field (field names can contain dots), unless a
        field of that exact name exists."""
        key = name
        while not app_fields.get(key) and "." in key:
            key = key.rsplit(".", 1)[0]
        return key if app_fields.get(key) else None

    @staticmethod
    def _get_chart_input_fields(
        chart: Chart, app_fields: AppFieldIndex
    ) -> List[AppField]:
        matched: Dict[str, AppField] = {}
        for expression in chart.field_expressions:
            for name in QlikSenseSource._chart_field_names(expression, app_fields):
                key = QlikSenseSource._app_field_key(name, app_fields)
                for app_field in app_fields.get(key) if key else []:
                    matched.setdefault(
                        f"{app_field.dataset_urn}.{app_field.field.name}", app_field
                    )
        return list(matched.values())

    def _get_chart_inputs(
        self,
        chart: Chart,
        chart_fields: List[AppField],
        app_fields: AppFieldIndex,
        input_tables_urns: List[str],
        has_unknown_schema: bool,
    ) -> List[str]:
        # The fields behind a $(variable) expansion that could not be expanded
        # are unknown, so the chart may read any of the app's tables, as may a
        # measure that could not be read, a derived field such as
        # Date.autoCalendar.Year whose base field is not known, or a name that
        # is not a known field while some table's columns are unknown (e.g.
        # SELECT *). Set modifier values only filter.
        unmatched = [
            name
            for expression in chart.field_expressions
            for name in self._chart_field_names(expression, app_fields)
            if self._app_field_key(name, app_fields) is None
        ]
        if (
            not chart.fields_complete
            or any(map(reads_unknown_fields, chart.field_expressions))
            or any("." in name for name in unmatched)
            or (has_unknown_schema and unmatched)
        ):
            return input_tables_urns
        # Otherwise every name is known: a chart that reads no field of the
        # app, such as a text object, has no input table.
        return list(dict.fromkeys(field.dataset_urn for field in chart_fields))

    @staticmethod
    def _index_app_fields(
        tables: Dict[str, List[QlikDatasetSchemaField]],
        script_table_urns: Collection[str] = (),
    ) -> AppFieldIndex:
        index = AppFieldIndex()
        for dataset_urn, fields in tables.items():
            for field in fields:
                app_field = AppField(dataset_urn=dataset_urn, field=field)
                index.exact.setdefault(field.name, []).append(app_field)
                if dataset_urn in script_table_urns:
                    index.folded.setdefault(field.name.lower(), []).append(app_field)
        return index

    def _gen_sheets_workunit(
        self,
        app: App,
        input_tables_urns: Optional[List[str]],
        app_fields: AppFieldIndex,
        has_unknown_schema: bool,
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

            for chart in sheet.charts:
                chart.field_expressions = [
                    expand_variables(expression, app.variables)
                    for expression in chart.field_expressions
                ]
            yield from self._gen_charts_workunit(
                sheet.charts,
                input_tables_urns,
                app_fields,
                has_unknown_schema,
                app.id,
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
                    fineGrainedLineages=self._column_lineage(fine_grained_lineages),
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
                Constant.TYPE: DatasetSubTypes.QLIK_TABLE,
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
            aspect=SubTypes(typeNames=[DatasetSubTypes.QLIK_TABLE]),
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
        tables: Dict[Tuple[str, bool], ScriptTableLineage],
        app_id: str,
        script_table: ScriptTable,
        connection_name: Optional[str],
        upstream_urns: List[str],
        result: SqlParsingResult,
    ) -> None:
        # Loads of one dataset URN are one table, so labels that differ only
        # in case are merged. Mapping tables are a separate namespace, so a
        # mapping load is kept apart.
        identifier = app_table_identifier(app_id, script_table.label)
        lineage = tables.setdefault(
            (identifier, script_table.mapping),
            ScriptTableLineage(
                label=script_table.label,
                identifier=identifier,
                label_unknown=script_table.label_unknown,
                connection=connection_name,
                mapping=script_table.mapping,
            ),
        )
        lineage.upstream_urns.extend(
            urn for urn in upstream_urns if urn not in lineage.upstream_urns
        )
        sql_columns: Dict[str, List[str]] = {}
        for column in result.column_lineage or []:
            sql_columns.setdefault(column.downstream.column, []).extend(
                column_refs_to_schema_field_urns(
                    ref for ref in column.upstreams if ref.table in upstream_urns
                )
            )
        fields: Optional[Dict[str, List[str]]] = apply_preceding_loads(
            script_table.preceding_loads, sql_columns
        )
        if script_table.reshaped:
            # Only a CrossTable whose fields are known keeps column lineage;
            # otherwise the fields are not the columns and only table lineage
            # is kept.
            selected = [
                column.downstream.column for column in result.column_lineage or []
            ]
            fields = apply_crosstable(
                script_table.crosstable,
                script_table.preceding_loads,
                sql_columns,
                # The parser lists columns in SELECT order, but leaves out a *
                # and repeats a name selected twice, which would shift slots.
                sql_columns_in_order=len(set(selected))
                == len(selected)
                == select_list_size(script_table.sql),
            )
        for field, upstreams in (fields or {}).items():
            lineage.column_upstreams.setdefault(field, []).extend(upstreams)

    def _build_script_tables(self, app: App) -> List[ScriptTableLineage]:
        if not app.script:
            return []
        tables: Dict[Tuple[str, bool], ScriptTableLineage] = {}
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
        # A data table and a mapping table with the same identifier share one
        # dataset URN, so only the data table, which charts read, is kept.
        script_tables = [
            lineage
            for (identifier, mapping), lineage in tables.items()
            if not (mapping and (identifier, False) in tables)
        ]
        self.reporter.script_tables_with_lineage += len(script_tables)
        return script_tables

    @staticmethod
    def _script_table_fields(
        table: ScriptTableLineage,
    ) -> List[QlikDatasetSchemaField]:
        return [
            QlikDatasetSchemaField(name=column) for column in table.column_upstreams
        ]

    def _gen_script_tables_workunit(
        self,
        tables: List[ScriptTableLineage],
        app_id: str,
        replace_lineage: bool = True,
    ) -> Iterable[MetadataWorkUnit]:
        for table in tables:
            dataset_urn = self._gen_qlik_dataset_urn(table.identifier)
            yield from self._gen_app_table_entity(
                table.identifier,
                self._table_schema(table.identifier, self._script_table_fields(table)),
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
            if not replace_lineage:
                yield from self._patch_upstreams(
                    dataset_urn,
                    table.upstream_urns,
                    DatasetLineageType.TRANSFORMED,
                    self._column_lineage(
                        self._script_column_lineage(dataset_urn, table)
                    ),
                )
                continue
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=UpstreamLineage(
                    upstreams=[
                        Upstream(dataset=urn, type=DatasetLineageType.TRANSFORMED)
                        for urn in table.upstream_urns
                    ],
                    fineGrainedLineages=self._column_lineage(
                        self._script_column_lineage(dataset_urn, table)
                    ),
                ),
            ).as_workunit()

    @staticmethod
    def _script_column_lineage(
        dataset_urn: str, table: ScriptTableLineage
    ) -> Optional[List[FineGrainedLineage]]:
        return [
            FineGrainedLineage(
                upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                upstreams=upstreams,
                downstreamType=FineGrainedLineageDownstreamType.FIELD,
                downstreams=[builder.make_schema_field_urn(dataset_urn, column)],
            )
            for column, upstreams in table.column_upstreams.items()
            if upstreams
        ] or None

    @functools.cached_property
    def _dataset_urns_by_secure_qri(self) -> Optional[Dict[str, str]]:
        """Dataset URNs by secure QRI, or None when the datasets could not be
        listed. First read after the allowed spaces are known."""
        datasets = self.qlik_api.list_datasets()
        if datasets is None:
            self.reporter.warning(
                title="Unable to list Qlik datasets",
                message="Lineage to and from Qlik datasets, such as QVDs, is only "
                "added to this run.",
                context=str(self.qlik_api.dataset_listing_error),
            )
            return None
        # Only datasets of allowed spaces are ingested.
        return {
            dataset.secure_qri: self._gen_qlik_dataset_urn(
                self._qlik_dataset_identifier(dataset.space_id, dataset.name)
            )
            for dataset in datasets
            if dataset.space_id in self._allowed_space_ids
        }

    @staticmethod
    def _lineage_table_identifier(app_id: str, table: LineageTable) -> str:
        # The identifier load script tables use, so a table keeps its URN
        # whether or not its load script SQL resolves to a database table.
        label = table.label or table.qri.rpartition("#")[2]
        return app_table_identifier(app_id, label)

    def _app_table_urns(self, app: App) -> Dict[str, str]:
        urns = {
            table.tableQri: self._gen_qlik_dataset_urn(table.tableQri.lower())
            for table in app.tables
            if table.tableQri
        }
        urns.update(
            (
                table.qri,
                self._gen_qlik_dataset_urn(
                    self._lineage_table_identifier(app.id, table)
                ),
            )
            for table in app.lineage_tables
        )
        return urns

    def _register_app_tables(self, app: App) -> None:
        self._app_table_urns_by_qri.update(self._app_table_urns(app))
        if app.tables_read_failed:
            self._apps_tables_unknown.add(app.id)
        else:
            self._apps_tables_known.add(app.id)

    def _kept_table_urns(self, app_id: str) -> Optional[Dict[str, str]]:
        if app_id not in self._apps_tables_known:
            return None
        return {
            qri: urn
            for qri, urn in self._app_table_urns_by_qri.items()
            if _qlik_app_id(qri) == app_id
        }

    def _app_tables_known(self, app_id: str) -> bool:
        if app_id in self._apps_tables_unknown:
            return False
        if app_id in self._apps_tables_known:
            return True
        # Not read or kept this run, so deleted or outside the recipe, unless
        # part of the run could not be read.
        return not self.qlik_api.unread_items

    def _resolve_lineage_sources(
        self, source_qris: List[str], dataset_urn: str, own_app_id: Optional[str]
    ) -> "_ResolvedSources":
        """URNs of the sources this run ingests. A source that can't be resolved
        this run, because the datasets or its app could not be read, makes the
        result incomplete. With `own_app_id`, only that app's tables are linked."""
        resolved = _ResolvedSources()
        for source_qri in source_qris:
            urn, known = self._resolve_source(source_qri, dataset_urn, own_app_id)
            if not known:
                resolved.complete = False
            elif urn and urn not in resolved.urns:
                resolved.urns.append(urn)
        return resolved

    def _resolve_source(
        self, table_qri: str, dataset_urn: str, own_app_id: Optional[str]
    ) -> Tuple[Optional[str], bool]:
        """The URN of a source table node, if this run ingests it, and whether
        that could be told this run."""
        app_id = _qlik_app_id(table_qri)
        if app_id is not None:
            if own_app_id is not None and app_id != own_app_id:
                return None, True
            if not self._app_tables_known(app_id):
                return None, False
            urn = self._app_table_urns_by_qri.get(table_qri)
        else:
            # Data files, and files Qlik catalogued on a data connection such
            # as ADLS, are datasets. Other sources, such as database tables,
            # have opaque QRIs; the load script parser traces SQL loads instead.
            datasets = self._dataset_urns_by_secure_qri
            if datasets is None:
                return None, False
            urn = datasets.get(table_qri.rpartition("#")[0])
        return (urn if urn != dataset_urn else None), True

    def _learn_field_names(self, names: Iterable[str]) -> None:
        for name in names:
            self._field_names_by_id.setdefault(_qlik_field_id(name), name)

    def _graph_column_lineage(
        self,
        dataset_urn: str,
        field_source_qris: Dict[str, List[str]],
        own_app_id: Optional[str],
    ) -> List[FineGrainedLineage]:
        """Column lineage from the field level of Qlik's lineage graph. A source
        field is named only when its id is the hash of a field name the run
        has seen, such as the field it is loaded into."""
        lineages = []
        for field, source_qris in field_source_qris.items():
            upstreams: List[str] = []
            for field_qri in source_qris:
                table_qri, _, field_id = field_qri.rpartition("#")
                urn, _ = self._resolve_source(table_qri, dataset_urn, own_app_id)
                name = (
                    field
                    if _qlik_field_id(field) == field_id
                    else self._field_names_by_id.get(field_id)
                )
                if urn and not name:
                    # The source field is real but no field of that name has
                    # been read yet, such as a renamed load from a QVD read
                    # later in the run, so the table's column lineage is not
                    # all known.
                    self._unnamed_field_lineage.add(dataset_urn)
                if urn and name:
                    upstream = builder.make_schema_field_urn(urn, name)
                    if upstream not in upstreams:
                        upstreams.append(upstream)
            if upstreams:
                lineages.append(
                    FineGrainedLineage(
                        upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                        upstreams=upstreams,
                        downstreamType=FineGrainedLineageDownstreamType.FIELD,
                        downstreams=[builder.make_schema_field_urn(dataset_urn, field)],
                    )
                )
        return lineages

    def _column_lineage(
        self, lineages: Optional[List[FineGrainedLineage]]
    ) -> Optional[List[FineGrainedLineage]]:
        return (lineages or None) if self.config.include_column_lineage else None

    @staticmethod
    def _patch_upstreams(
        dataset_urn: str,
        upstream_urns: List[str],
        lineage_type: str,
        fine_grained: Optional[List[FineGrainedLineage]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        """Adds lineage without replacing the edges an earlier run wrote."""
        if not upstream_urns and not fine_grained:
            return
        yield convert_upstream_lineage_to_patch(
            dataset_urn,
            UpstreamLineage(
                upstreams=[
                    Upstream(dataset=urn, type=lineage_type) for urn in upstream_urns
                ],
                fineGrainedLineages=fine_grained or None,
            ),
            None,
        )

    def _gen_lineage_tables_workunit(
        self, app: App, script_tables: Dict[str, ScriptTableLineage]
    ) -> Iterable[MetadataWorkUnit]:
        for table in app.lineage_tables:
            script_table = script_tables.get(table.qri)
            identifier = self._lineage_table_identifier(app.id, table)
            dataset_urn = self._gen_qlik_dataset_urn(identifier)
            schema = self._table_schema(
                identifier, self._lineage_table_fields(table, script_table)
            )
            if schema and all(field.dataType is None for field in schema):
                self._untyped_tables[dataset_urn] = (identifier, schema)
            yield from self._gen_app_table_entity(identifier, schema, app.id)
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=DatasetProperties(
                    name=table.label,
                    qualifiedName=table.label,
                    customProperties={
                        Constant.TYPE: DatasetSubTypes.QLIK_TABLE,
                        Constant.QRI: table.qri,
                        **(
                            {Constant.DATACONNECTORNAME: script_table.connection}
                            if script_table and script_table.connection
                            else {}
                        ),
                    },
                ),
            ).as_workunit()
            resolved = self._resolve_lineage_sources(
                table.source_qris, dataset_urn, own_app_id=app.id
            )
            upstream_urns = list(resolved.urns)
            if script_table:
                upstream_urns.extend(
                    urn
                    for urn in script_table.upstream_urns
                    if urn not in upstream_urns
                )
            fine_grained = self._column_lineage(
                [
                    *(
                        self._script_column_lineage(dataset_urn, script_table) or []
                        if script_table
                        else []
                    ),
                    *self._graph_column_lineage(
                        dataset_urn, table.field_source_qris, own_app_id=app.id
                    ),
                ]
            )
            if (
                resolved.complete
                and not app.script_read_failed
                and not app.tables_read_failed
                and not table.field_lineage_failed
                and dataset_urn not in self._unnamed_field_lineage
            ):
                # The app owns its tables, so the lineage is written whole, which
                # also drops upstreams the app no longer reads.
                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=UpstreamLineage(
                        upstreams=[
                            Upstream(dataset=urn, type=DatasetLineageType.TRANSFORMED)
                            for urn in upstream_urns
                        ],
                        fineGrainedLineages=fine_grained,
                    ),
                ).as_workunit()
                continue
            # A dataset, or the load script and the SQL lineage in it, could not
            # be read this run: replacing the lineage would drop the edges an
            # earlier run wrote, so what was read is added to them instead.
            self.reporter.warning(
                title="Lineage of Qlik app table only added to",
                message="A source of the table could not be read this run, so the "
                "lineage read this run is added to the table's existing lineage "
                "instead of replacing it.",
                context=f"app={app.id}, table={table.label}",
            )
            yield from self._patch_upstreams(
                dataset_urn,
                upstream_urns,
                DatasetLineageType.TRANSFORMED,
                fine_grained,
            )

    @staticmethod
    def _match_script_tables(
        lineage_tables: List[LineageTable], script_tables: List[ScriptTableLineage]
    ) -> Dict[str, ScriptTableLineage]:
        """Script tables that are also lineage graph tables, by graph table QRI,
        so each table is ingested once."""
        by_label = {
            table.label.lower(): table for table in script_tables if not table.mapping
        }
        return {
            table.qri: by_label[table.label.lower()]
            for table in lineage_tables
            if table.label.lower() in by_label
        }

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

    @staticmethod
    def _name_unknown_tables(app: App, script_tables: List[ScriptTableLineage]) -> None:
        """Gives a script table the parser could not name its name in the data
        model, when exactly one data model table no other table claims has
        exactly its columns. Otherwise it keeps its made-up name."""
        # A lineage graph table stays claimable: an unlabelled load the graph
        # also traces must take its name, so both are ingested as one table.
        claimed = {
            table.label.lower() for table in script_tables if not table.label_unknown
        }
        unclaimed = [
            table
            for table in app.data_model_tables
            if table.name.lower() not in claimed
        ]
        by_fields: Dict[frozenset, List[str]] = {}
        for model_table in unclaimed:
            fields = frozenset(field.name.lower() for field in model_table.fields)
            by_fields.setdefault(fields, []).append(model_table.name)
        unknown = [
            table
            for table in script_tables
            if table.label_unknown and not table.mapping and table.column_upstreams
        ]
        columns = [frozenset(c.lower() for c in t.column_upstreams) for t in unknown]
        for table, table_columns in zip(unknown, columns, strict=True):
            names = by_fields.get(table_columns, [])
            # Two unnamed tables with the same columns can't be told apart.
            if len(names) == 1 and columns.count(table_columns) == 1:
                table.label = names[0]
                table.identifier = app_table_identifier(app.id, names[0])
                table.label_unknown = False

    def _use_data_model_field_names(
        self, script_tables: List[ScriptTableLineage]
    ) -> None:
        """Spells each column as the data model does, so column lineage lands on
        the schema's fields. The SQL parser may change a column's case."""
        for table in script_tables:
            fields = self._data_model_fields.get(table.identifier)
            if not fields:
                continue
            names = {field.name for field in fields}
            by_lower: Dict[str, List[str]] = {}
            for name in names:
                by_lower.setdefault(name.lower(), []).append(name)
            column_upstreams: Dict[str, List[str]] = {}
            for column, upstreams in table.column_upstreams.items():
                matches = by_lower.get(column.lower(), [])
                # Qlik field names are case-sensitive, so a column matching two
                # fields keeps its own spelling.
                name = column if column in names or len(matches) != 1 else matches[0]
                merged = column_upstreams.setdefault(name, [])
                merged.extend(u for u in upstreams if u not in merged)
            table.column_upstreams = column_upstreams

    def _lineage_table_fields(
        self, table: LineageTable, script_table: Optional[ScriptTableLineage]
    ) -> List[QlikDatasetSchemaField]:
        """The fields the load script or, failing that, the field level of the
        lineage graph gives a table, for a table the data model no longer
        holds, such as one the script drops after storing it."""
        parsed = self._script_table_fields(script_table) if script_table else []
        return parsed or [QlikDatasetSchemaField(name=name) for name in table.fields]

    def _table_schema(
        self, identifier: str, parsed: List[QlikDatasetSchemaField]
    ) -> List[QlikDatasetSchemaField]:
        """A table's fields as the app's data model holds them, which the load
        script only shows when its SELECT lists them."""
        return self._data_model_fields.get(identifier) or parsed

    def _app_table_fields(
        self, tables: List[QlikTable], script_tables: List[ScriptTableLineage]
    ) -> Dict[str, List[QlikDatasetSchemaField]]:
        table_fields: Dict[str, List[QlikDatasetSchemaField]] = {}
        for table in tables:
            identifier = self._get_app_table_identifier(table)
            if identifier:
                table_fields[self._gen_qlik_dataset_urn(identifier)] = (
                    table.datasetSchema
                )
        for script_table in script_tables:
            # A mapping table is only used by ApplyMap and is not in the data
            # model, so charts cannot read it.
            if script_table.mapping:
                continue
            table_fields[self._gen_qlik_dataset_urn(script_table.identifier)] = (
                self._table_schema(
                    script_table.identifier, self._script_table_fields(script_table)
                )
            )
        return table_fields

    def _gen_app_workunit(
        self, app: App, fingerprint: Optional[str] = None
    ) -> Iterable[MetadataWorkUnit]:
        """
        Map Qlik App to Datahub container
        """
        owner_username = self._get_owner_name(app.ownerId)
        self._data_model_fields = {
            app_table_identifier(app.id, table.name): table.fields
            for table in app.data_model_tables
        }
        self._learn_field_names(
            field.name for table in app.data_model_tables for field in table.fields
        )
        self._learn_field_names(
            field for table in app.lineage_tables for field in table.fields
        )
        if self.config.on_prem is None and not app.tables:
            self.reporter.apps_without_load_model.append(app.id)
        script_tables = self._build_script_tables(app)
        self._name_unknown_tables(app, script_tables)
        self._use_data_model_field_names(script_tables)
        matched_script_tables = self._match_script_tables(
            app.lineage_tables, script_tables
        )
        matched_ids = {id(table) for table in matched_script_tables.values()}
        script_tables = [
            table for table in script_tables if id(table) not in matched_ids
        ]
        table_fields = self._app_table_fields(app.tables, script_tables)
        script_table_urns = {
            self._gen_qlik_dataset_urn(table.identifier) for table in script_tables
        }
        self._register_app_tables(app)
        for table in app.lineage_tables:
            matched = matched_script_tables.get(table.qri)
            urn = self._gen_qlik_dataset_urn(
                self._lineage_table_identifier(app.id, table)
            )
            table_fields[urn] = self._table_schema(
                self._lineage_table_identifier(app.id, table),
                self._lineage_table_fields(table, matched),
            )
            if matched:
                script_table_urns.add(urn)
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
            app,
            None if app.tables_read_failed else input_tables_urns,
            self._index_app_fields(table_fields, script_table_urns),
            any(not fields for fields in table_fields.values()),
        )
        yield from self._gen_app_tables_workunit(app.tables, app.id)
        yield from self._gen_lineage_tables_workunit(app, matched_script_tables)
        yield from self._gen_script_tables_workunit(
            script_tables,
            app.id,
            # A table the lineage graph could not be read for may be one an
            # earlier run merged with a graph table, whose other edges stay.
            replace_lineage=not app.tables_read_failed,
        )
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

    @staticmethod
    def _gen_schema_field(field: QlikDatasetSchemaField) -> SchemaField:
        return SchemaField(
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

    def _gen_schema_fields(
        self, schema: List[QlikDatasetSchemaField]
    ) -> List[SchemaField]:
        return [self._gen_schema_field(field) for field in schema]

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
            name=_dataset_display_name(dataset),
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
                Constant.TYPE: _dataset_subtype(dataset),
                Constant.DATASETTYPE: dataset.type,
                Constant.SIZE: str(dataset.size),
                Constant.ROWCOUNT: str(dataset.rowCount),
            }
        )

        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=dataset_properties
        ).as_workunit()

    @staticmethod
    def _qlik_dataset_identifier(space_id: str, name: str) -> str:
        return f"{space_id}.{name}".lower()

    def _gen_dataset_workunit(self, dataset: QlikDataset) -> Iterable[MetadataWorkUnit]:
        dataset_identifier = self._qlik_dataset_identifier(
            dataset.spaceId, dataset.name
        )
        dataset_urn = self._gen_qlik_dataset_urn(dataset_identifier)
        if dataset.stored_from_qris is not None:
            self._dataset_store_sources[dataset_urn] = dataset.stored_from_qris
            self._dataset_store_field_sources[dataset_urn] = (
                dataset.stored_field_source_qris
            )
            if dataset.stored_field_lineage_failed:
                self._dataset_store_fields_unknown.add(dataset_urn)
        self._learn_field_names(field.name for field in dataset.datasetSchema)
        self._dataset_field_types[dataset_urn] = {
            field.name: field.dataType for field in dataset.datasetSchema
        }

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
            aspect=SubTypes(typeNames=[_dataset_subtype(dataset)]),
        ).as_workunit()

    def _check_missing_lineage_graph(self, app: App) -> None:
        # Qlik also answers 404 for lineage it has not computed yet, so a 404
        # only means the app has no tables when its last read had none.
        failures: List[str] = []
        previous = self._usable_checkpoint(app.id, failures)
        if failures:
            # Without the last read, the app's tables may still exist.
            app.read_errors.append(
                "Qlik Cloud has no lineage graph for the app, and its last read "
                "could not be loaded"
            )
            app.tables_read_failed = True
        elif previous is not None and previous.table_urns_by_qri:
            app.read_errors.append(
                "Qlik Cloud has no lineage graph for the app, whose last read "
                "had tables"
            )
            app.tables_read_failed = True
        else:
            self.reporter.apps_without_lineage_graph.append(app.id)
            # A last read whose tables are unknown can't tell either.
            unknown = previous is None or previous.table_urns_by_qri is None
            if unknown and self.app_state.is_checkpointing_enabled():
                # With no last read to compare with, the graph may only be
                # missing because Qlik has not computed it yet, such as right
                # after a reload, so the app's lineage is only added to. Without
                # stateful ingestion there is never a last read, so the app is
                # taken as having no tables, as before.
                app.tables_read_failed = True
                self.reporter.warning(
                    title="Qlik Cloud has no lineage graph for app",
                    message="Qlik may not have computed it yet, so the lineage "
                    "of the app's tables is only added to this run.",
                    context=app.id,
                )

    def _gen_item_workunits(
        self,
        item: Union[Item, UnchangedApp],
        space_name: str,
        record_state: bool,
    ) -> Iterable[MetadataWorkUnit]:
        if isinstance(item, UnchangedApp):
            self._keep_unchanged_app(item)
        elif isinstance(item, App):
            for fallback in item.read_fallbacks:
                self.reporter.info(
                    title="Qlik Engine batch read unavailable",
                    message="Fell back to one Engine request per chart or master "
                    "measure, which is slower on large apps.",
                    context=f"{item.id}: {fallback}",
                )
            self.reporter.apps_read += 1
            if item.lineage_graph_missing:
                self._check_missing_lineage_graph(item)
            logger.info(
                f"Processed app {item.qTitle} in space {space_name} "
                f"({self.reporter.apps_read} apps read from the Engine)"
            )
            partial_read = (
                self._keep_partly_read_app(item)
                if not item.read_complete and record_state
                else _PartialRead.NOT_KEPT
            )
            if partial_read == _PartialRead.KEPT:
                self.reporter.warning(
                    title="Unable to fully read Qlik app",
                    message="Nothing from this read of the app is emitted. Its "
                    "metadata stays as of its last complete read until a read of "
                    "it completes.",
                    context=self._read_errors_context(item),
                )
                return
            if partial_read == _PartialRead.LIMIT_REACHED:
                self.reporter.failure(
                    title="Qlik app has been only partly read for too many runs",
                    message="Its last complete read is no longer kept, so this "
                    "partial read is ingested. Stale metadata removal is skipped "
                    "for this run.",
                    context=self._read_errors_context(item),
                )
            elif not item.read_complete:
                # Reporting a failure also stops stale entity removal from
                # soft-deleting what this app's last full read produced.
                self.reporter.failure(
                    title="Unable to fully read Qlik app",
                    message="Some sheets, charts, tables or load script lineage "
                    "of this app could not be read from Qlik.",
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

    def _current_upstreams(self, dataset_urn: str) -> Optional[UpstreamLineage]:
        graph = self.ctx.graph
        if graph is None:
            if not self._reported_no_graph:
                self._reported_no_graph = True
                self.reporter.info(
                    title="Qlik dataset lineage only added to",
                    message="Without a DataHub connection, such as with a file "
                    "sink, edges from app tables that no longer store into a "
                    "dataset can't be found, so none are removed.",
                )
            return None
        try:
            return graph.get_aspect(dataset_urn, UpstreamLineage) or UpstreamLineage(
                upstreams=[]
            )
        except Exception as e:
            self.reporter.warning(
                title="Unable to read Qlik dataset lineage from DataHub",
                message="The dataset's lineage from apps that store into it is "
                "only added to.",
                context=dataset_urn,
                exc=e,
            )
            return None

    def _gen_stored_table_types(self) -> Iterable[MetadataWorkUnit]:
        """Types for the fields of untyped graph tables, from the datasets they
        are stored into: a STORE writes a field's values unchanged, so a field
        stored under its own name has the stored field's type. A field stored
        into datasets that disagree on its type keeps none."""
        types: Dict[str, Dict[str, Set[Optional[str]]]] = {}
        for dataset_urn, field_sources in self._dataset_store_field_sources.items():
            dataset_types = self._dataset_field_types.get(dataset_urn, {})
            for field, source_qris in field_sources.items():
                for field_qri in source_qris:
                    table_qri, _, field_id = field_qri.rpartition("#")
                    table_urn = self._app_table_urns_by_qri.get(table_qri)
                    if (
                        table_urn in self._untyped_tables
                        and field_id == _qlik_field_id(field)
                        and dataset_types.get(field)
                    ):
                        types.setdefault(table_urn, {}).setdefault(field, set()).add(
                            dataset_types[field]
                        )
        for table_urn, field_types in types.items():
            identifier, schema = self._untyped_tables[table_urn]
            # Columns traced from a load script may have their case changed by
            # the SQL parser, so a field matches a stored one of another case
            # when no other stored field folds to the same name.
            folded: Dict[str, List[str]] = {}
            for name in field_types:
                folded.setdefault(name.lower(), []).append(name)
            typed = []
            for schema_field in schema:
                matches = folded.get(schema_field.name.lower(), [])
                stored_name: Optional[str] = (
                    schema_field.name
                    if schema_field.name in field_types
                    else (matches[0] if len(matches) == 1 else None)
                )
                stored_types = (
                    field_types.get(stored_name, set()) if stored_name else set()
                )
                typed.append(
                    schema_field.model_copy(
                        update={"dataType": next(iter(stored_types))}
                    )
                    if len(stored_types) == 1
                    else schema_field
                )
            yield self._gen_schema_metadata(identifier, typed)

    def _gen_dataset_store_lineage(self) -> Iterable[MetadataWorkUnit]:
        datasets = self._dataset_urns_by_secure_qri
        qlik_datasets = set(datasets.values()) if datasets is not None else None
        owned = self._known_app_table_urns()
        for dataset_urn, source_qris in self._dataset_store_sources.items():
            resolved = self._resolve_lineage_sources(
                source_qris, dataset_urn, own_app_id=None
            )
            stored_fields = self._graph_column_lineage(
                dataset_urn,
                self._dataset_store_field_sources.get(dataset_urn, {}),
                own_app_id=None,
            )
            if not resolved.complete:
                self.reporter.warning(
                    title="Lineage of Qlik dataset only added to",
                    message="An app that stores into the dataset could not be read "
                    "this run, so its storing tables are added to the dataset's "
                    "lineage and none are removed.",
                    context=dataset_urn,
                )
            # Added as a patch, so lineage from elsewhere, such as added in
            # DataHub, stays.
            patch = DatasetPatchBuilder(dataset_urn)
            for urn in resolved.urns:
                patch.add_upstream_lineage(
                    Upstream(dataset=urn, type=DatasetLineageType.COPY)
                )
            for lineage in stored_fields:
                patch.add_fine_grained_lineage(lineage)
            # Edges this connector owns are those from the tables of apps read
            # in full or kept this run. Those that no longer store into the
            # dataset are removed one by one; while a storing app is unread
            # this run, nothing is.
            current = (
                self._current_upstreams(dataset_urn)
                if resolved.complete and qlik_datasets is not None
                else None
            )
            if current is not None:
                for upstream in current.upstreams:
                    if upstream.dataset in owned and upstream.dataset not in (
                        resolved.urns
                    ):
                        patch.remove_upstream_lineage(upstream.dataset)
                if (
                    self.config.include_column_lineage
                    and dataset_urn not in self._dataset_store_fields_unknown
                    and dataset_urn not in self._unnamed_field_lineage
                ):
                    for lineage in self._stale_field_lineage(
                        current.fineGrainedLineages or [], stored_fields, owned
                    ):
                        patch.remove_fine_grained_lineage(lineage)
            for mcp in patch.build():
                yield MetadataWorkUnit(
                    id=MetadataWorkUnit.generate_workunit_id(mcp), mcp_raw=mcp
                )

    def _known_app_table_urns(self) -> Set[str]:
        return {
            urn
            for qri, urn in self._app_table_urns_by_qri.items()
            if _qlik_app_id(qri) in self._apps_tables_known
        }

    @staticmethod
    def _stale_field_lineage(
        current: List[FineGrainedLineage],
        stored: List[FineGrainedLineage],
        owned: Set[str],
    ) -> List[FineGrainedLineage]:
        """The column edges from owned tables that this run no longer stores,
        each as a lineage holding only those upstream fields."""
        stored_edges = {
            (downstream, upstream)
            for lineage in stored
            for downstream in lineage.downstreams or []
            for upstream in lineage.upstreams or []
        }
        stale = []
        for lineage in current:
            # A column edge can only be removed by patch through its single
            # downstream field; one another tool wrote with none or several
            # is not this connector's.
            if len(lineage.downstreams or []) != 1:
                continue
            upstreams = [
                upstream
                for upstream in lineage.upstreams or []
                if _schema_field_parent(upstream) in owned
                and not any(
                    (downstream, upstream) in stored_edges
                    for downstream in lineage.downstreams or []
                )
            ]
            if upstreams:
                stale_lineage = FineGrainedLineage.from_obj(lineage.to_obj())
                stale_lineage.upstreams = upstreams
                stale.append(stale_lineage)
        return stale

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
        self._allowed_space_ids = set(space_names)
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
        yield from self._gen_dataset_store_lineage()
        yield from self._gen_stored_table_types()
        if self.qlik_api.field_lineage_failures:
            self.reporter.warning(
                title="Unable to read Qlik column lineage",
                message="Column lineage of these tables is only added to this run, "
                "and their table lineage is still read.",
                context=_bounded_list(self.qlik_api.field_lineage_failures),
            )
        if self.qlik_api.lineage_read_failures:
            self.reporter.warning(
                title="Unable to read Qlik dataset lineage",
                message="Lineage from the apps that store these datasets is skipped.",
                context=_bounded_list(self.qlik_api.lineage_read_failures),
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
