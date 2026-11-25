import logging
from collections import Counter, defaultdict
from functools import partial
from typing import (
    DefaultDict,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp_builder import gen_containers
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
from datahub.ingestion.source.common.subtypes import (
    BIContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.qlik_sense.app_state import (
    AppCheckpoint,
    QlikAppStateHandler,
    UnmappedFileStore,
)
from datahub.ingestion.source.qlik_sense.config import (
    Constant,
    QlikSourceConfig,
    QlikSourceReport,
)
from datahub.ingestion.source.qlik_sense.constants import (
    APP_PARTLY_READ_TITLE,
    CHECKPOINT_LOAD_FAILED_TITLE,
    UNRESOLVED_STORE,
)
from datahub.ingestion.source.qlik_sense.data_classes import (
    App,
    Item,
    LineageUpstreams,
    QlikDataset,
    SchemaField as QlikDatasetSchemaField,
    UnchangedApp,
)
from datahub.ingestion.source.qlik_sense.file_lineage import FileSourceResolver
from datahub.ingestion.source.qlik_sense.lib_path import _path_space
from datahub.ingestion.source.qlik_sense.qlik_api import QlikAPI
from datahub.ingestion.source.qlik_sense.qlik_base_api import QlikBaseAPI
from datahub.ingestion.source.qlik_sense.qlik_onprem_api import QlikOnPremAPI
from datahub.ingestion.source.qlik_sense.qvd_lineage import AppQvds
from datahub.ingestion.source.qlik_sense.reload_sql import _ReloadFile
from datahub.ingestion.source.qlik_sense.script_lineage import (
    LIB_PATH_RE,
    DataConnectionInfo,
    app_table_identifier,
)
from datahub.ingestion.source.qlik_sense.source.onprem_binary import OnPremBinaryMixin
from datahub.ingestion.source.qlik_sense.source_models import (
    PendingStoredFile,
    PendingTableLineage,
    _BinaryLink,
    _ParsedApp,
    _PartialRead,
    _StorageFile,
)
from datahub.ingestion.source.qlik_sense.source_utils import (
    _app_field_names,
    _bounded_list,
    _own_table_urns,
    _refs_of_field_sources as _refs_of_field_sources,
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
    "Enabled by default, can be disabled via configuration `include_column_lineage`",
)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(
    SourceCapability.OWNERSHIP,
    "Enabled by default, configured using `ingest_owner`",
)
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
class QlikSenseSource(OnPremBinaryMixin, StatefulIngestionSourceBase, TestableSource):
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
        # The files each app's load script reads, to resolve the files in its
        # lineage graph with.
        self._file_sources_by_app: Dict[str, FileSourceResolver] = {}
        self._app_space_ids: Dict[str, str] = {}
        self._apps_variables_unread: Set[str] = set()
        # The tables whose lineage is only added to, by app and reason, reported
        # once per app at the end of the run.
        self._only_added_tables: Dict[Tuple[str, str], List[str]] = {}
        # Apps that load files by a wildcard or a path the connector can't
        # expand, so the files in their lineage graph can't all be named.
        self._apps_file_paths_unknown: Set[str] = set()
        self._apps_with_includes: Set[str] = set()
        self._reported_include_apps: Set[str] = set()
        self._resolved_file_qris: Set[str] = set()
        # Files on connections of unknown storage, by URN, as `lib://` paths,
        # and those that lineage links to, which get a dataset.
        self._unmapped_files: Dict[str, str] = {}
        # How load scripts spell each `lib://` path prefix, by its lowercase
        # key, and the paths it was built from.
        self._script_spellings: Dict[str, Counter[str]] = {}
        self._script_spellings_built_from: Optional[List[str]] = None
        self._linked_unmapped_files: Set[str] = set()
        # Tables kept apps store into files on storage that was unknown when
        # their last read recorded them.
        self._kept_storage_file_stores: Dict[str, List[str]] = {}
        # The tables each app stores into such a file, linked at the end of
        # the run only if some app's lineage graph reads the file, as a STORE
        # in the script may never run.
        self._unmapped_file_stores: DefaultDict[str, Dict[str, List[str]]] = (
            defaultdict(dict)
        )
        # Datasets emitted at the end of the run for the apps whose lineage
        # names them, added to those apps' checkpoints so a later run that
        # keeps an app keeps them too.
        self._app_end_of_run_urns: DefaultDict[str, Set[str]] = defaultdict(set)
        # Unmapped files each app's lineage read this run.
        self._app_read_unmapped_files: DefaultDict[str, Set[str]] = defaultdict(set)
        self._app_read_graph_named: DefaultDict[str, Set[str]] = defaultdict(set)
        # Upstream graphs of the apps read this run, fetched when a source only
        # they can name is met, None when the read failed, and the resources
        # only they name, by URN, with their labels and the app tables storing
        # into them.
        self._app_qris: Dict[str, str] = {}
        self._lineage_upstreams: Dict[str, Optional[LineageUpstreams]] = {}
        self._upstream_graph_read_failures: List[str] = []
        # Apps whose last graph-named datasets are kept, as this run could not
        # tell whether they are still sources.
        self._apps_graph_named_kept: Set[str] = set()
        self._graph_named_resources: Dict[str, str] = {}
        self._graph_resource_writers: Dict[str, Set[str]] = {}
        # The files apps stored or loaded in their last reload, by app id.
        self._reload_files: Dict[str, List[_ReloadFile]] = {}
        # Apps whose last reload's statements could not be read.
        self._reload_files_unread: Set[str] = set()
        self._reload_path_failures: List[str] = []
        # Spaces the API key can't see that lineage goes through, by lowercased
        # name, with the name as written and what lineage needs from them.
        self._unseen_spaces: Dict[str, Tuple[str, Set[str]]] = {}
        self._listed_space_names: Optional[Set[str]] = None
        # The apps whose upstream graph names each graph-named dataset.
        self._graph_resource_readers: Dict[str, Set[str]] = {}
        self._allowed_space_ids: Set[str] = set()
        self._reported_no_graph = False
        # Datasets with a column edge whose source field could not be named.
        self._unnamed_field_lineage: Set[str] = set()
        # A listed QVD of an allowed space had no secure QRI to match.
        self._datasets_without_qri = False
        # Every listed dataset, so one in a space the recipe leaves out is not
        # named from the lineage graph instead.
        self._listed_dataset_qris: Set[str] = set()
        # A listed QVD of a left-out space had no secure QRI to match.
        self._excluded_datasets_without_qri = False
        # App tables whose lineage waits for field names read later in the run.
        self._pending_table_lineage: List[PendingTableLineage] = []
        self._pending_stored_files: List[PendingStoredFile] = []
        # Table QRI -> dataset URN for the tables of apps read or kept this run.
        # Lineage only links to tables in here, as other URNs may never exist.
        self._app_table_urns_by_qri: Dict[str, str] = {}
        # The table URNs of each app read or kept this run.
        self._table_urns_by_app: Dict[str, Set[str]] = {}
        # Tables the last run recorded for apps whose tables are known this
        # run, so edges from a table since renamed or dropped are still owned.
        self._previous_app_table_urns: Set[str] = set()
        # Apps read or kept this run, by whether all their tables are known.
        self._apps_tables_known: Set[str] = set()
        self._apps_tables_unknown: Set[str] = set()
        # Apps whose tables are known, but whose lineage graph names tables
        # with no URN, so only references to those tables are unknown.
        self._apps_graph_tables_unresolved: Set[str] = set()
        # Fingerprints of apps with table lineage still pending, written after
        # it so a failed run can't skip the app as unchanged without it.
        self._deferred_fingerprints: Dict[str, MetadataWorkUnit] = {}
        # On-prem, the QVDs each app read or kept this run writes and reads, and
        # the apps whose QVDs are unknown.
        self._app_qvds: Dict[str, AppQvds] = {}
        # Apps whose QVD writes are not known this run, with why.
        self._apps_qvds_unknown: Dict[str, str] = {}
        # On Qlik Cloud, the files on storage each app read this run stores into.
        self._app_stored_files: Dict[str, List[str]] = {}
        self._unresolved_qvd_loads = 0
        self._unlabelled_qvd_loads = 0
        # On-prem, the URNs each app BINARY loads come from produced, read or
        # kept this run, the data model fields of those apps, the tables linked
        # to them once every app has been read, and the apps whose read
        # ingested tables a BINARY statement brought in.
        self._binary_source_apps: Set[str] = (
            set(self.config.on_prem.binary_file_to_app.values())
            if self.config.on_prem is not None
            else set()
        )
        self._app_urns: Dict[str, Set[str]] = {}
        # Apps whose last read is kept, whose checkpoints may hold unmapped
        # files they linked then.
        self._kept_app_ids: Set[str] = set()
        self._app_model_fields: Dict[str, Dict[str, List[str]]] = {}
        self._binary_links: List[_BinaryLink] = []
        self._apps_with_binary_tables: Set[str] = set()
        self._last_parsed: Optional[_ParsedApp] = None
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

    def _keep_previous_read(self, app_id: str) -> Optional[AppCheckpoint]:
        try:
            previous = self.app_state.previous(app_id)
        except Exception as e:
            # Reporting a failure stops stale entity removal from soft-deleting
            # what the app's last read produced.
            self.reporter.failure(
                title=CHECKPOINT_LOAD_FAILED_TITLE,
                message="Entities from the app's last read could not be kept, so "
                "stale metadata removal is skipped for this run.",
                context=app_id,
                exc=e,
            )
            self._apps_tables_unknown.add(app_id)
            self._apps_qvds_unknown[app_id] = "its last read could not be loaded"
            return None
        if previous is None:
            self._apps_tables_unknown.add(app_id)
            self._apps_qvds_unknown[app_id] = "it has no last read to keep"
            return None
        self._kept_app_ids.add(app_id)
        if app_id in self._binary_source_apps:
            self._app_urns.setdefault(app_id, set()).update(previous.urns)
        if previous.binary_tables:
            self._apps_with_binary_tables.add(app_id)
        if previous.data_model_fields is not None:
            self._app_model_fields[app_id] = previous.data_model_fields
        if previous.qvds is None:
            self._apps_qvds_unknown[app_id] = (
                "its last read was recorded before QVD lineage"
            )
        else:
            self._app_qvds[app_id] = previous.qvds
            if not previous.qvds.writes_complete:
                self._apps_qvds_unknown[app_id] = UNRESOLVED_STORE
        if previous.field_names:
            self._learn_field_names(previous.field_names)
        unmapped_stores: Dict[str, UnmappedFileStore] = {}
        stored_file_urns = list(previous.stored_file_urns)
        for file_urn, store in previous.unmapped_file_stores.items():
            storage_file = self._storage_of_unmapped_store(store)
            if storage_file is None:
                unmapped_stores[file_urn] = store
                self._add_unmapped_file(file_urn, store.path)
                urns = self._unmapped_file_stores[file_urn].setdefault(app_id, [])
                urns.extend(urn for urn in store.table_urns if urn not in urns)
                continue
            # Its storage is known now, so the file is the storage file a full
            # read links, not the Qlik dataset named by its connection.
            if storage_file.urn is not None:
                tables = self._kept_storage_file_stores.setdefault(storage_file.urn, [])
                tables.extend(urn for urn in store.table_urns if urn not in tables)
                if storage_file.urn not in stored_file_urns:
                    stored_file_urns.append(storage_file.urn)
        dropped = (
            set(previous.unmapped_file_stores)
            - set(unmapped_stores)
            - set(previous.read_unmapped_file_urns)
        )
        previous = previous.model_copy(
            update={
                "urns": [urn for urn in previous.urns if urn not in dropped],
                "unmapped_file_stores": unmapped_stores,
                "stored_file_urns": sorted(stored_file_urns),
            }
        )
        if previous.table_urns_by_qri is not None:
            kept = _own_table_urns(app_id, previous.table_urns_by_qri)
            self._app_table_urns_by_qri.update(kept)
            self._table_urns_by_app[app_id] = set(kept.values())
        # None was recorded before table URNs were kept in the state.
        if previous.table_urns_by_qri is None or previous.tables_unknown:
            self._apps_tables_unknown.add(app_id)
        else:
            self._apps_tables_known.add(app_id)
            if previous.graph_tables_unresolved:
                self._apps_graph_tables_unresolved.add(app_id)
        assert self.stale_entity_removal_handler is not None
        for urn in previous.urns:
            self.stale_entity_removal_handler.add_entity_to_state(
                guess_entity_type(urn), urn
            )
        return previous

    def _storage_of_unmapped_store(
        self, store: UnmappedFileStore
    ) -> Optional[_StorageFile]:
        """The storage file a kept app's store into a file on a connection of
        unknown storage now resolves to, or None while that storage is still
        unknown, as when the data connections or Qlik datasets could not be
        listed."""
        lib_path = LIB_PATH_RE.match(store.path)
        if lib_path is None:
            return None
        # The recorded connection is space-qualified, so no app space applies.
        storage, detail = self._file_storage(lib_path.group("connection"), "")
        if storage is None:
            return None
        path = lib_path.group("path")
        catalogued = self._catalogued_as_qlik_dataset(storage, path)
        # A full read keeps last run's edges when the datasets can't be listed.
        if catalogued is None:
            return None
        # A file Qlik catalogues is not linked on storage.
        if catalogued:
            return _StorageFile(urn=None)
        return _StorageFile(urn=self._file_urn(storage, detail, path))

    def _keep_unchanged_app(self, app: UnchangedApp) -> None:
        previous = self._keep_previous_read(app.id)
        # Without the kept entities, which _keep_previous_read reports as a
        # failure, nothing is recorded, so the next run reads the app again.
        if previous is not None and previous.urns:
            self.app_state.record(
                app.id,
                AppCheckpoint(
                    fingerprint=self._salted_fingerprint(app.fingerprint),
                    urns=previous.urns,
                    table_urns_by_qri=previous.table_urns_by_qri,
                    tables_unknown=previous.tables_unknown,
                    graph_tables_unresolved=previous.graph_tables_unresolved,
                    qvds=self._app_qvds.get(app.id),
                    data_model_fields=self._app_model_fields.get(app.id),
                    binary_tables=previous.binary_tables,
                    stored_file_urns=previous.stored_file_urns,
                    unmapped_file_stores=previous.unmapped_file_stores,
                    read_unmapped_file_urns=previous.read_unmapped_file_urns,
                    read_graph_named_urns=previous.read_graph_named_urns,
                    field_names=previous.field_names,
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
                title=CHECKPOINT_LOAD_FAILED_TITLE,
                message="The partly read app could not keep its last read.",
                context=failure,
            )
        if previous is None:
            return _PartialRead.NOT_KEPT
        if previous.partial_reads >= self.config.max_partial_reads_kept:
            return _PartialRead.LIMIT_REACHED
        kept = self._keep_previous_read(app.id)
        if kept is None or not kept.urns:
            return _PartialRead.NOT_KEPT
        # Its QVD writes are from its last full read and may have changed, so
        # they protect its QVD edges without replacing anyone else's.
        self._apps_qvds_unknown.setdefault(
            app.id, "it was partly read, so its QVDs are from its last full read"
        )
        # No fingerprint, so the next run reads the app again.
        self.app_state.record(
            app.id,
            AppCheckpoint(
                fingerprint=None,
                urns=kept.urns,
                partial_reads=previous.partial_reads + 1,
                table_urns_by_qri=previous.table_urns_by_qri,
                tables_unknown=previous.tables_unknown,
                graph_tables_unresolved=previous.graph_tables_unresolved,
                qvds=previous.qvds,
                data_model_fields=previous.data_model_fields,
                binary_tables=previous.binary_tables,
                stored_file_urns=kept.stored_file_urns,
                unmapped_file_stores=kept.unmapped_file_stores,
                read_unmapped_file_urns=previous.read_unmapped_file_urns,
                read_graph_named_urns=previous.read_graph_named_urns,
                field_names=previous.field_names,
            ),
        )
        return _PartialRead.KEPT

    def _gen_recorded_app_workunit(self, app: App) -> Iterable[MetadataWorkUnit]:
        fingerprint = (
            self._salted_fingerprint(app.fingerprint)
            if app.fingerprint
            and app.read_complete
            and not app.data_load_failed
            # Its connection paths were unresolved, so it is read again next run.
            and not self._data_connections_unread()
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
                table_urns_by_qri=self._app_table_urns(app),
                # So a later run that keeps the app can't take its tables as
                # complete, nor a 404 as the app having no tables.
                tables_unknown=self._tables_unknown(app),
                graph_tables_unresolved=self._graph_tables_unresolved(app),
                qvds=self._app_qvds.get(app.id),
                data_model_fields=self._app_model_fields.get(app.id),
                binary_tables=app.id in self._apps_with_binary_tables,
                stored_file_urns=self._app_stored_files.get(app.id, []),
                unmapped_file_stores={
                    file_urn: UnmappedFileStore(
                        path=self._unmapped_files[file_urn], table_urns=stores[app.id]
                    )
                    for file_urn, stores in self._unmapped_file_stores.items()
                    if app.id in stores
                },
                field_names=_app_field_names(app),
            ),
        )

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
        self._learn_field_names(_app_field_names(app))
        if self._on_cloud and not app.tables:
            self.reporter.apps_without_load_model.append(app.id)
        parsed = self._parsed_script(app)
        self._file_sources_by_app[app.id] = self._file_source_resolver(app, parsed)
        if parsed.has_includes:
            self._apps_with_includes.add(app.id)
        if app.variables_read_failed:
            self._apps_variables_unread.add(app.id)
            self.reporter.warning(
                title="Unable to read Qlik app variables",
                message="Load script paths and chart expressions that use these "
                "apps' variables are only expanded from `script_variables`. "
                "Lineage to the files they load is only added to this run, and "
                "chart fields read through other variables are left out.",
                context=app.id,
            )
        self._app_space_ids[app.id] = app.spaceId
        script_tables = self._build_script_tables(app, parsed=parsed)
        self._name_unknown_tables(app, script_tables)
        qvd_lineage = (
            self._app_qvd_lineage(app, parsed, script_tables)
            if self.config.on_prem is not None
            else None
        )
        binary_load = None
        if qvd_lineage is not None:
            script_tables = self._merge_tables(script_tables, qvd_lineage.tables)
            # Read, even with no tables, so a BINARY load from it isn't
            # reported as from an app the run didn't read.
            if app.id in self._binary_source_apps:
                self._app_urns.setdefault(app.id, set())
            self._record_model_fields(app)
            binary_load = self._binary_load(app, parsed)
            if binary_load.tables:
                self._apps_with_binary_tables.add(app.id)
            script_tables = self._merge_tables(script_tables, binary_load.tables)
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
                # "Qlik App" first, as the UI labels a container by its first
                # subtype; "Workbook" makes the UI treat the app as its charts'
                # workbook, as it does for Tableau.
                sub_types=[
                    BIContainerSubTypes.QLIK_APP,
                    BIContainerSubTypes.WORKBOOK,
                ],
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
        app_fields = self._index_app_fields(table_fields, script_table_urns)
        has_unknown_schema = any(not fields for fields in table_fields.values())
        chart_inputs = (
            None
            if app.tables_read_failed
            else self._chart_inputs(
                (chart for sheet in app.sheets for chart in sheet.charts),
                self._app_variables(app),
                app_fields,
                input_tables_urns,
                has_unknown_schema,
            )
        )
        yield from self._gen_app_dashboard_workunit(
            app,
            owner_username,
            None
            if chart_inputs is None
            else self._tables_without_chart_path(app, chart_inputs, input_tables_urns),
        )
        yield from self._gen_sheets_workunit(app, chart_inputs)
        yield from self._gen_app_tables_workunit(app.tables, app.id)
        yield from self._gen_lineage_tables_workunit(app, matched_script_tables)
        yield from self._gen_script_tables_workunit(
            script_tables,
            app.id,
            # A table the lineage graph could not be read for may be one an
            # earlier run merged with a graph table, whose other edges stay.
            replace_lineage=not app.tables_read_failed,
            binary_links=binary_load.links if binary_load is not None else {},
        )
        if qvd_lineage is not None:
            yield from self._gen_file_entities(
                qvd_lineage.paths, DatasetSubTypes.QLIK_QVD
            )
        if binary_load is not None:
            yield from self._gen_file_entities(
                binary_load.paths, DatasetSubTypes.QLIK_APP_FILE
            )
        if qvd_lineage is None:
            self._queue_file_store_lineage(app, parsed, table_fields)
        if fingerprint:
            # Skipping an unchanged app trusts this fingerprint, so it is written
            # only after everything else the app produced.
            fingerprint_wu = self._gen_fingerprint_workunit(
                container_workunits, fingerprint
            )
            if any(p.app_id == app.id for p in self._pending_table_lineage) or any(
                p.app_id == app.id for p in self._pending_stored_files
            ):
                self._deferred_fingerprints[app.id] = fingerprint_wu
            else:
                yield fingerprint_wu

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
        elif previous is not None and (
            previous.table_urns_by_qri or previous.graph_tables_unresolved
        ):
            # Unresolved graph tables are tables the last read saw too.
            app.read_errors.append(
                "Qlik Cloud has no lineage graph for the app, whose last read "
                "had tables"
            )
            app.tables_read_failed = True
        else:
            self.reporter.apps_without_lineage_graph.append(app.id)
            # A last read whose tables are unknown can't tell either.
            unknown = (
                previous is None
                or previous.table_urns_by_qri is None
                or previous.tables_unknown
            )
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
            if self.config.on_prem is not None and record_state:
                self._check_binary_read(item)
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
                    title=APP_PARTLY_READ_TITLE,
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
                    title=APP_PARTLY_READ_TITLE,
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
        yield from self._gen_pending_table_lineage()
        yield from self._gen_dataset_store_lineage()
        yield from self._gen_stored_table_types()
        yield from self._gen_qvd_lineage()
        yield from self._gen_binary_lineage()
        # Before any file is placed, so the spellings hold every STORE.
        graph_named_apps = self._read_graph_named_reloads()
        yield from self._gen_unmapped_file_entities()
        yield from self._gen_graph_named_resources(graph_named_apps)
        self._record_end_of_run_urns()
        if self.qlik_api.opened_without_data:
            self.reporter.warning(
                title="Qlik apps read without their data",
                message="Opening these apps with their data, or reading their "
                "data model, was denied, as with section access, or timed out, so "
                "they were read without it: their tables have no field types, and "
                "the next run reads them again.",
                context=_bounded_list(self.qlik_api.opened_without_data),
            )
        refusals = self.qlik_api.engine_refusals
        if refusals is not None and refusals.longest_streak_seconds > 0:
            self.reporter.info(
                title="Qlik Engine refused connections",
                message="App reads waited for the Engine to accept connections, "
                "as it does while its service restarts, which slows the run.",
                context=f"Longest run of refusals: "
                f"{refusals.longest_streak_seconds:.0f}s",
            )
        if self.qlik_api.field_lineage_failures:
            self.reporter.warning(
                title="Unable to read Qlik column lineage",
                message="Column lineage of these tables is only added to this run, "
                "and their table lineage is still read.",
                context=_bounded_list(self.qlik_api.field_lineage_failures),
            )
        for failure in self.qlik_api.include_read_failures:
            # Entries are `<path> (app <id> in space <name>): <reason>`, and a
            # bracketed space name in the path may hold `: ` itself.
            path = failure.partition(" (app ")[0]
            self._note_unseen_space(_path_space(path), "included scripts")
        if self._unseen_spaces:
            self.reporter.warning(
                title="Qlik spaces the API key can't see",
                message="Lineage goes through these spaces, which the API key's "
                "user can't see, so it is incomplete: included scripts in them "
                "are not read, their data connections don't resolve, and their "
                "datasets are named only from Qlik's lineage graph or left out. "
                "Give the user Can view in each space for its scripts and "
                "datasets, and Can consume data for its data connections, or map "
                "the connections in `data_connection_to_platform_instance` and "
                "`file_connection_to_storage`.",
                context=_bounded_list(
                    sorted(
                        f"{name}: {', '.join(sorted(needs))}"
                        for name, needs in self._unseen_spaces.values()
                    )
                ),
            )
        if self.qlik_api.include_read_failures:
            self.reporter.warning(
                title="Unable to read included Qlik scripts",
                message="Apps include these scripts with $(Include) or "
                "$(Must_Include), but they could not be read, so what they load "
                "is missing from lineage, and the apps' lineage is only added to. "
                "Each entry gives the path, the app and space including it, and "
                "the reason. A path without a space resolves in that app's space. "
                "An access error needs Can view for the API key's user in the "
                "space holding the file.",
                context=_bounded_list(sorted(set(self.qlik_api.include_read_failures))),
            )
        if self.reporter.unresolved_file_connections:
            self.reporter.warning(
                title="Qlik file connections have no known storage",
                message="Files on these data connections are named by their "
                "space, connection and path, so they only join the Qlik apps "
                "that write and read them. A personal-space app's files on a "
                "connection named without its space are named from the loading "
                "app's lineage graph, when its tables come from the graph, and "
                "stores into them are not linked. Add each Azure Storage, Amazon "
                "S3 or Google Cloud Storage connection to "
                "`file_connection_to_storage` with the URI of its storage; other "
                "connectors, such as SFTP, can't be mapped.",
                context=_bounded_list(
                    sorted(self.reporter.unresolved_file_connections)
                ),
            )
        if self.qlik_api.lineage_read_failures:
            self.reporter.warning(
                title="Unable to read Qlik dataset lineage",
                message="Lineage from the apps that store these datasets is skipped.",
                context=_bounded_list(self.qlik_api.lineage_read_failures),
            )
        self._report_reload_sql()
        if self._upstream_graph_read_failures:
            self.reporter.warning(
                title="Unable to read Qlik upstream lineage graph",
                message="Sources of these apps that only Qlik's lineage graph "
                "names, such as datasets in spaces the API key can't see, are "
                "left out, and the apps' lineage is only added to this run. "
                "Datasets the last run named from the graph are kept.",
                context=_bounded_list(self._upstream_graph_read_failures),
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
