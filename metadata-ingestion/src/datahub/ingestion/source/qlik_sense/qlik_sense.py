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
)

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
from datahub.ingestion.source.qlik_sense.app_state import QlikAppStateHandler
from datahub.ingestion.source.qlik_sense.config import (
    Constant,
    QlikSourceConfig,
    QlikSourceReport,
)
from datahub.ingestion.source.qlik_sense.constants import CHECKPOINT_TOO_LARGE_TITLE
from datahub.ingestion.source.qlik_sense.data_classes import (
    LineageUpstreams,
    SchemaField as QlikDatasetSchemaField,
)
from datahub.ingestion.source.qlik_sense.file_lineage import FileSourceResolver
from datahub.ingestion.source.qlik_sense.qlik_api import QlikAPI
from datahub.ingestion.source.qlik_sense.qlik_base_api import QlikBaseAPI
from datahub.ingestion.source.qlik_sense.qlik_onprem_api import QlikOnPremAPI
from datahub.ingestion.source.qlik_sense.qvd_lineage import AppQvds
from datahub.ingestion.source.qlik_sense.reload_sql import _ReloadFile
from datahub.ingestion.source.qlik_sense.script_lineage import DataConnectionInfo
from datahub.ingestion.source.qlik_sense.source.end_of_run import EndOfRunMixin
from datahub.ingestion.source.qlik_sense.source_models import (
    PendingStoredFile,
    PendingTableLineage,
    _BinaryLink,
    _OnlyAddedTablesKey,
    _ParsedApp,
    _QvdsUnknownReason,
    _UnseenSpace,
)
from datahub.ingestion.source.qlik_sense.source_utils import (
    _refs_of_field_sources as _refs_of_field_sources,
)
from datahub.ingestion.source.state.entity_removal_state import (
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
class QlikSenseSource(EndOfRunMixin, StatefulIngestionSourceBase, TestableSource):
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
        self._only_added_tables: Dict[_OnlyAddedTablesKey, List[str]] = {}
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
        self._unseen_spaces: Dict[str, _UnseenSpace] = {}
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
        self._apps_qvds_unknown: Dict[str, _QvdsUnknownReason] = {}
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
        self._report_end_of_run()
        if self.reporter.failures:
            # Like the stale entity removal state, a failed run keeps the last
            # read of the apps it did not reach.
            self.app_state.keep_unrecorded_apps()
        if self.app_state.is_too_large():
            self.reporter.warning(
                title=CHECKPOINT_TOO_LARGE_TITLE,
                message="Unchanged apps are not skipped and partly read apps are "
                "reported as failures until it fits. Raise "
                "stateful_ingestion.max_checkpoint_state_size.",
            )

    def get_report(self) -> SourceReport:
        return self.reporter
