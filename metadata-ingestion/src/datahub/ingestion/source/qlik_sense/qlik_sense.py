import logging
from collections import defaultdict
from functools import partial
from typing import (
    Iterable,
    List,
    Optional,
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
from datahub.ingestion.source.qlik_sense.qlik_api import QlikAPI
from datahub.ingestion.source.qlik_sense.qlik_base_api import QlikBaseAPI
from datahub.ingestion.source.qlik_sense.qlik_onprem_api import QlikOnPremAPI
from datahub.ingestion.source.qlik_sense.source.end_of_run import EndOfRunMixin
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

    platform = "qlik-sense"

    def __init__(self, config: QlikSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.reporter = QlikSourceReport()
        self.ui_host = config.ui_hostname()
        self._data_connections = None
        self._file_sources_by_app = {}
        self._app_space_ids = {}
        self._apps_variables_unread = set()
        self._only_added_tables = {}
        self._apps_file_paths_unknown = set()
        self._apps_with_includes = set()
        self._reported_include_apps = set()
        self._resolved_file_qris = set()
        self._unmapped_files = {}
        self._script_spellings = {}
        self._script_spellings_built_from = None
        self._linked_unmapped_files = set()
        self._linked_storage_qvds = set()
        self._share_file_urns = set()
        self._kept_storage_file_stores = {}
        self._unmapped_file_stores = defaultdict(dict)
        self._app_end_of_run_urns = defaultdict(set)
        self._app_read_unmapped_files = defaultdict(set)
        self._app_read_graph_named = defaultdict(set)
        self._app_qris = {}
        self._lineage_upstreams = {}
        self._upstream_graph_read_failures = []
        self._apps_graph_named_kept = set()
        self._graph_named_resources = {}
        self._graph_resource_writers = {}
        self._reload_files = {}
        self._reload_files_unread = set()
        self._reload_path_failures = []
        self._unseen_spaces = {}
        self._listed_space_names = None
        self._graph_resource_readers = {}
        self._allowed_space_ids = set()
        self._reported_no_graph = False
        self._unnamed_field_lineage = set()
        self._datasets_without_qri = False
        self._listed_dataset_qris = set()
        self._excluded_datasets_without_qri = False
        self._pending_table_lineage = []
        self._pending_stored_files = []
        self._app_table_urns_by_qri = {}
        self._table_urns_by_app = {}
        self._previous_app_table_urns = set()
        self._apps_tables_known = set()
        self._apps_tables_unknown = set()
        self._apps_graph_tables_unresolved = set()
        self._deferred_fingerprints = {}
        self._app_qvds = {}
        self._apps_qvds_unknown = {}
        self._app_stored_files = {}
        self._unresolved_qvd_loads = 0
        self._unlabelled_qvd_loads = 0
        self._binary_source_apps = (
            set(self.config.on_prem.binary_file_to_app.values())
            if self.config.on_prem is not None
            else set()
        )
        self._app_urns = {}
        self._kept_app_ids = set()
        self._app_model_fields = {}
        self._binary_links = []
        self._apps_with_binary_tables = set()
        self._last_parsed = None
        self._data_model_fields = {}
        self._dataset_store_sources = {}
        self._dataset_store_field_sources = {}
        self._dataset_store_fields_unknown = set()
        self._untyped_tables = {}
        self._dataset_field_types = {}
        self._field_names_by_id = {}
        # Owned here rather than by the automatic processor so that entities of
        # apps skipped as unchanged or only partly read can be kept in the state.
        self.stale_entity_removal_handler = StaleEntityRemovalHandler(
            state_provider=self.state_provider,
            report=self.reporter,
            config=self.config,
            state_type_class=GenericCheckpointState,
            pipeline_name=ctx.pipeline_name,
            run_id=ctx.run_id,
            platform=self.platform,
        )
        self.app_state = QlikAppStateHandler(
            state_provider=self.state_provider,
            config=self.config,
            pipeline_name=ctx.pipeline_name,
            run_id=ctx.run_id,
            stale_job_id=self.stale_entity_removal_handler.job_id
            if self.stale_entity_removal_handler.is_checkpointing_enabled()
            else None,
        )
        self._fingerprint_salt = None
        self._change_check_failures = []
        if config.skips_unchanged_apps() and not self._skips_unchanged_apps():
            self.reporter.warning(
                title="skip_unchanged_apps is ignored",
                message="Skipping unchanged apps needs stateful ingestion, so every "
                "app is read.",
            )
        # A failure to connect is raised, so the pipeline fails with its cause.
        self.qlik_api = self._create_api(self.config)

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
        return [AutoStaleEntityRemovalProcessor]

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        processors = super().get_workunit_processors()
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
        yield from self._gen_storage_qvd_types()
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
