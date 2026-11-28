import logging
import threading
import time
from collections import Counter
from functools import partial
from typing import (
    Callable,
    Dict,
    Generic,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import requests
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from datahub.configuration.common import AllowDenyPattern, ConfigurationError
from datahub.emitter.emit_mode import EmitMode
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
from datahub.ingestion.api.workunit_processor import WorkunitProcessor
from datahub.ingestion.source.microstrategy.checkpoint_state import (
    MicroStrategyStateHandler,
    ObjectCheckpoint,
    RecordedUsageTarget,
    ReportDatasetCheckpoint,
)
from datahub.ingestion.source.microstrategy.client import (
    MicroStrategyAPIError,
    MicroStrategyAuthError,
    MicroStrategyClient,
    ensure_session_pool_size,
)
from datahub.ingestion.source.microstrategy.config import MicroStrategyConfig
from datahub.ingestion.source.microstrategy.constants import (
    MICROSTRATEGY_PLATFORM,
    MSTR_CUBE_SUBTYPES,
    MSTR_DATASOURCE_MAPPING_HINT,
    MSTR_DEFINITION_ENDPOINT_METRIC_MODEL,
    MSTR_DEFINITION_ENDPOINT_MODEL,
    MSTR_DEFINITION_ENDPOINT_V2,
    MSTR_FOLDER_TYPE_PROFILE_OBJECTS,
    MSTR_FOLDER_TYPE_PROFILE_REPORTS,
    MSTR_MODEL_TABLE_DATASOURCE_FIELD,
    MSTR_MODEL_TABLE_FIELDS,
    MSTR_OBJECT_SUBTYPE_DOCUMENT,
    MSTR_OBJECT_SUBTYPE_REPORT_GRID,
    MSTR_OBJECT_TYPE_FOLDER,
    MSTR_OBJECT_TYPE_REPORT,
    MSTR_PERSONAL_FOLDER_NAMES,
    MSTR_PERSONAL_FOLDER_TYPES,
    MSTR_PREDEFINED_FOLDER_LABELS,
    MSTR_PREDEFINED_HIDDEN_FOLDER_TYPES,
    MSTR_SQLGLOT_LOGGER,
    USAGE_TARGET_CHART,
    USAGE_TARGET_DASHBOARD,
    USAGE_TARGET_DATASET,
    MicroStrategyCache,
    MicroStrategyDatasetIdentity,
    MicroStrategyPhase,
)
from datahub.ingestion.source.microstrategy.incremental import (
    SHARED_REFERENCE_ENTITY_TYPES,
    ChangeCheckResult,
    MicroStrategyChangeDetector,
    ReportDatasetPlan,
)
from datahub.ingestion.source.microstrategy.lineage import (
    ModelLineageIndex,
    SqlViewLineage,
    TableContextResolver,
    WarehouseLineageContext,
    bind_visualizations_by_derived_objects,
    datasource_for_model_table,
    distinct_warehouse_contexts,
    ensure_sql_lineage_dependencies,
    matching_datasource_for_context,
    metric_fact_ids_from_model,
    metric_metric_ids_from_model,
    sql_statement_from_sql_view_entry,
    sql_view_dataset_key,
    sql_view_dataset_name,
    unique_derived_object_owners,
    warehouse_context_from_datasource,
    warehouse_context_from_datasources,
    warehouse_context_with_connection,
)
from datahub.ingestion.source.microstrategy.mapper import MicroStrategyMapper
from datahub.ingestion.source.microstrategy.microstrategy_semantic_model import (
    MicroStrategySemanticModelMapper,
)
from datahub.ingestion.source.microstrategy.models import (
    DashboardDefinition,
    DatasetObject,
    Datasource,
    MetricEnrichment,
    MicroStrategyDict,
    MicroStrategyObject,
    PersonalFolderResolution,
    PredefinedFolderResolution,
    Project,
    ProjectKey,
    ReportDefinition,
    ReportDerivedMetric,
    Visualization,
    extract_document_derived_metric_definitions,
    extract_embedded_metric_definitions,
    extract_folder_parts,
    is_document_object,
    is_personal_folder_object,
    metric_enrichment_from_expression,
    normalize_object_id,
)
from datahub.ingestion.source.microstrategy.parallel import (
    SingleFlight,
    TaskOutcome,
    ordered_parallel_map,
)
from datahub.ingestion.source.microstrategy.report import MicroStrategyReport
from datahub.ingestion.source.microstrategy.spill_cache import (
    SpillCache,
    object_cache_key,
)
from datahub.ingestion.source.microstrategy.telemetry import (
    RunProgress,
    SqlglotWarningFilter,
    timed_generator_phase,
    timed_iter,
    timed_phase,
)
from datahub.ingestion.source.microstrategy.usage import (
    MicroStrategyUsageExtractor,
    UsageBucket,
    UsageTarget,
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
from datahub.ingestion.source_report.ingestion_stage import METADATA_EXTRACTION
from datahub.ingestion.workunit_processors.auto_stale_entity_removal import (
    AutoStaleEntityRemovalProcessor,
)
from datahub.utilities.urns.urn import guess_entity_type
from datahub.utilities.urns.urn_iter import list_urns

logger = logging.getLogger(__name__)

_T = TypeVar("_T")

_ERROR_SUMMARY_MAX_CHARS = 300


# Modeling document answers that mean the whole project lacks access. 404 is
# left out: the endpoint can answer it for a single unmodelled document.
_MODEL_DOCUMENT_PROJECT_WIDE_STATUSES = frozenset({403, 501})


class _PredefinedFoldersUnavailable(Exception):
    """Predefined folders could not be resolved, so the project's dashboards
    and reports are skipped rather than filed under raw folder names."""


class _CandidateEvaluation(BaseModel, Generic[_T]):
    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    candidate: _T
    accepted: bool = True
    filtered_by_pattern: bool = False
    filtered_by_folder_path: Optional[str] = None
    unresolved_folder_path: bool = False
    personal_folder: bool = False


class _PrefetchedDashboard(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    dashboard_object: MicroStrategyObject
    dashboard: Optional[DashboardDefinition] = None
    dossier_sql_rows: Optional[List[Dict[str, object]]] = None
    skipped_report_sql_keys: Set[str] = Field(default_factory=set)
    prefetch_seconds: float = 0.0
    prefetch_api_calls: int = 0
    prefetch_api_seconds: float = 0.0


class _ReportSqlFetchResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    sql: Optional[str] = None
    skipped: bool = False


class _PrefetchedReport(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    report_object: MicroStrategyObject
    change_check: Optional[ChangeCheckResult] = None
    report_definition: Optional[ReportDefinition] = None
    source_dataset: Optional[DatasetObject] = None
    dataset_object: Optional[MicroStrategyObject] = None
    sql_fetch_skipped: bool = False


@platform_name("MicroStrategy")
@config_class(MicroStrategyConfig)
@support_status(SupportStatus.ALPHA)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.CONTAINERS, "Projects and folders emit as containers")
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(
    SourceCapability.TAGS,
    "Metric, attribute, temporal, and derived-metric field tags",
)
@capability(SourceCapability.OWNERSHIP, "Enabled by default via `ingest_owner`")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Visualization and report inputs when resolvable",
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Modeling API field lineage from MicroStrategy metrics and attributes "
    "to source warehouse fields",
)
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
@capability(
    SourceCapability.USAGE_STATS,
    "Dashboard and report usage from the Platform Analytics cube when "
    "`extract_usage_statistics` is enabled",
)
class MicroStrategySource(StatefulIngestionSourceBase, TestableSource):
    config: MicroStrategyConfig
    report: MicroStrategyReport
    platform: str = MICROSTRATEGY_PLATFORM

    def __init__(self, config: MicroStrategyConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = MicroStrategyReport()
        self.client = MicroStrategyClient(config, self.report)
        if self.ctx.graph is not None:
            graph_session = getattr(self.ctx.graph, "_session", None)
            if isinstance(graph_session, requests.Session):
                ensure_session_pool_size(graph_session, max(16, config.max_workers * 2))
        self.mapper = MicroStrategyMapper(config, self.report)
        self.lineage = self.mapper.lineage
        if self.config.extract_lineage and (
            self.config.extract_warehouse_lineage
            or self.config.extract_report_sql_lineage
        ):
            # Fail at construction, not per SQL view: a missing parser module
            # would otherwise be swallowed into per-statement parse warnings
            # and the run would report SUCCESS with zero warehouse lineage.
            ensure_sql_lineage_dependencies()
        self._cache_lock = threading.RLock()
        self._execution_lock = threading.RLock()
        self._report_sql_lock = threading.RLock()
        # Bounds total concurrent intra-dashboard visualization detail GETs
        # across all active dashboard workers to match the HTTP pool size.
        self._http_io_gate = threading.Semaphore(max(2, self.config.max_workers * 2))
        self._model_document_flight: SingleFlight[
            Tuple[str, str], Optional[Dict[str, object]]
        ] = SingleFlight()
        self._predefined_folder_flight: SingleFlight[
            str, PredefinedFolderResolution
        ] = SingleFlight()
        self._personal_folder_flight: SingleFlight[str, PersonalFolderResolution] = (
            SingleFlight()
        )
        self._report_sql_flight: SingleFlight[str, _ReportSqlFetchResult] = (
            SingleFlight()
        )
        self._dashboard_instance_flight: SingleFlight[str, str] = SingleFlight()
        memory_entries = self.config.cache_memory_entries
        # A failed fetch is cached as None and reused, except by the semantic model.
        self._metric_model_cache: SpillCache[Optional[Dict[str, object]]] = SpillCache(
            memory_entries, compress=True
        )
        self._model_document_unavailable_projects: Set[str] = set()
        # (project id, dashboard id) -> Modeling document for in-flight
        # dashboards so binding and document derived metrics share one GET
        # without cross-dashboard overwrites across worker threads.
        self._model_documents_in_flight: Dict[
            Tuple[str, str], Optional[Dict[str, object]]
        ] = {}
        self._predefined_folder_cache: Dict[str, PredefinedFolderResolution] = {}
        self._personal_folder_cache: Dict[str, PersonalFolderResolution] = {}
        # A failed lookup is cached as None, so it is not fetched again.
        self._report_object_cache: SpillCache[Optional[MicroStrategyObject]] = (
            SpillCache(memory_entries)
        )
        # A failed read is cached, so a shared report is not executed again.
        self._report_sql_cache: SpillCache[Optional[str]] = SpillCache(
            memory_entries, compress=True
        )
        self._report_sql_consumed: Set[str] = set()
        self._report_v2_payload_cache: SpillCache[Dict[str, object]] = SpillCache(
            memory_entries, compress=True
        )
        # Released twice per project; both must count as one reset.
        self._report_caches_released = False
        self._project_caches_released = False
        # "project id:report id" -> derived metric definitions, or None once
        # every definition endpoint failed for it.
        self._report_derived_metric_cache: SpillCache[
            Optional[List[ReportDerivedMetric]]
        ] = SpillCache(memory_entries, compress=True)
        # Projects already warned about the Modeling report definition
        # endpoint failing (one structured warning per project, not per
        # report), and projects whose v2 payload shape was already logged.
        self._model_definition_warned_projects: Set[str] = set()
        # Dashboard id -> its one instance (an execution), shared by the
        # visualization-detail and SQL-view consumers, or the error that
        # creating it raised so a timed-out execution is never repeated.
        self._dashboard_instances: Dict[str, Union[str, MicroStrategyAPIError]] = {}
        # Project id -> consecutive instance-creation timeouts, and projects
        # where max_consecutive_execution_timeouts tripped.
        self._consecutive_execution_timeouts: Dict[str, int] = {}
        self._execution_limited_projects: Set[str] = set()
        # One dataset per report per project, shared by every dashboard that embeds it.
        self._per_report = (
            self.config.dataset_identity == MicroStrategyDatasetIdentity.PER_REPORT
        )
        self._report_datasets: Dict[_ReportDatasetKey, _ReportDatasetEntry] = {}
        if self.config.extract_derived_metrics and not (
            self.config.extract_lineage and self.config.extract_visualization_details
        ):
            # Not an error: the flag defaults on and its inputs are simply
            # absent. Make the no-op observable so operators aren't left
            # wondering why no derived fields appeared.
            self.report.info(
                title="Derived metric extraction has no input",
                message=(
                    "extract_derived_metrics is enabled but extract_lineage "
                    "and/or extract_visualization_details are disabled, so no "
                    "runtime grids are fetched and no derived metrics can be "
                    "extracted."
                ),
            )
        # Object GUID (upper) -> usage target for entities ingested this run;
        # usage buckets only attach to these.
        self._usage_targets: Dict[str, UsageTarget] = {}
        self._progress = RunProgress(
            self.report,
            every_dashboards=self.config.progress_log_interval_dashboards,
            every_seconds=self.config.progress_log_interval_seconds,
        )
        # Owned here so entities of objects skipped as unchanged stay in state.
        self.stale_entity_removal_handler = StaleEntityRemovalHandler(
            state_provider=self.state_provider,
            report=self.report,
            config=self.config,
            state_type_class=GenericCheckpointState,
            pipeline_name=ctx.pipeline_name,
            run_id=ctx.run_id,
            platform=self.platform,
        )
        self._object_state = MicroStrategyStateHandler(
            state_provider=self.state_provider,
            config=self.config,
            pipeline_name=ctx.pipeline_name,
            run_id=ctx.run_id,
            stale_job_id=self.stale_entity_removal_handler.job_id
            if self.stale_entity_removal_handler.is_checkpointing_enabled()
            else None,
        )
        self._change_detector = MicroStrategyChangeDetector(
            self.config, self.mapper, self._object_state
        )
        # Per-project skip_unchanged_objects state, keyed by normalized id.
        self._dashboard_candidates: Dict[str, MicroStrategyObject] = {}
        self._read_dashboards: Set[str] = set()
        self._dashboard_fingerprints: Dict[str, str] = {}
        self._report_plan: Optional[ReportDatasetPlan] = None
        if self.config.skip_unchanged_objects and not self._skips_unchanged():
            self.report.warning(
                title="skip_unchanged_objects is ignored",
                message=(
                    "Skipping unchanged objects needs stateful ingestion and a "
                    "DataHub graph connection, so every object is read."
                ),
            )

    @classmethod
    def create(
        cls,
        config_dict: Dict[str, object],
        ctx: PipelineContext,
    ) -> "MicroStrategySource":
        config = MicroStrategyConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_excluded_workunit_processors(
        self,
    ) -> List[Union[str, Type[WorkunitProcessor]]]:
        return [AutoStaleEntityRemovalProcessor]

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        processors = super().get_workunit_processors()
        processors.append(
            partial(auto_stale_entity_removal, self.stale_entity_removal_handler)
        )
        return processors

    def _skips_unchanged(self) -> bool:
        return (
            self.config.skip_unchanged_objects
            and self._object_state.is_checkpointing_enabled()
            and self.ctx.graph is not None
        )

    def _records_checkpoints(self) -> bool:
        return self._object_state.is_checkpointing_enabled()

    @staticmethod
    def test_connection(config_dict: Dict[str, object]) -> TestConnectionReport:
        test_report = TestConnectionReport()
        client: Optional[MicroStrategyClient] = None
        try:
            config = MicroStrategyConfig.parse_obj_allow_extras(config_dict)
            client = MicroStrategyClient(config, MicroStrategyReport())
            client.login()
            client.list_projects()
            test_report.basic_connectivity = CapabilityReport(capable=True)
        except Exception as error:
            test_report.basic_connectivity = CapabilityReport(
                capable=False,
                failure_reason=str(error),
            )
        finally:
            if client is not None:
                client.close()
        return test_report

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        self.client.login()
        auth_lost = False
        # Per-statement sqlglot warnings fill the executor's bounded log.
        sqlglot_filter = SqlglotWarningFilter(self.report)
        sqlglot_logger = logging.getLogger(MSTR_SQLGLOT_LOGGER)
        sqlglot_logger.addFilter(sqlglot_filter)
        try:
            if self.config.emit_chapter_page_structured_properties:
                yield from self._emit_chapter_page_structured_property_definitions()
            projects = self.client.list_projects()
            for index, project in enumerate(projects, start=1):
                if not self.config.project_pattern.allowed(project.name):
                    self.report.report_filtered_project(project.name)
                    continue
                # A project can take minutes (paginated searches plus per-object
                # execution), so show which project is in flight and how far along.
                logger.info(
                    "Processing project %r (%s) [%d/%d]",
                    project.name,
                    project.id,
                    index,
                    len(projects),
                )
                try:
                    with self.report.new_stage(
                        f"{project.name}: {METADATA_EXTRACTION}"
                    ):
                        yield from self._process_project(project)
                    self._progress.log_progress()
                except MicroStrategyAuthError as error:
                    # Auth is dead: every remaining call fails identically, so stop
                    # with one clear failure instead of a per-project cascade.
                    self.report.failure(
                        title="MicroStrategy Authentication Lost",
                        message=(
                            "The session was invalidated mid-run and could not "
                            "be re-established; aborting the remaining projects."
                        ),
                        context=f"project_id={project.id}, project_name={project.name}",
                        exc=error,
                    )
                    auth_lost = True
                    break
                except ConfigurationError:
                    # Environmental (e.g. a missing parser dependency): every
                    # remaining project would fail identically, so surface it
                    # as a run failure instead of per-project noise.
                    raise
                except Exception as error:
                    self.report.failure(
                        title="Failed to Process Project",
                        message=(
                            "Skipping the rest of this project after an unexpected "
                            "error; other projects will still be processed."
                        ),
                        context=f"project_id={project.id}, project_name={project.name}",
                        exc=error,
                    )
                finally:
                    self._reset_project_caches()
            if self.config.extract_usage_statistics and not auth_lost:
                yield from self._process_usage_statistics(projects)
        finally:
            self._finish_checkpoint()
            self._progress.log_progress()
            sqlglot_logger.removeFilter(sqlglot_filter)
            self._warn_if_every_sql_view_failed()
            self.lineage.close()
            for cache in self._spill_caches():
                cache.close()
            self.client.close()

    def _spill_caches(self) -> List[SpillCache]:
        return [
            self._metric_model_cache,
            self._report_object_cache,
            self._report_sql_cache,
            self._report_v2_payload_cache,
            self._report_derived_metric_cache,
        ]

    def _release_report_caches(self) -> None:
        # The semantic model does not read these, so its per-metric work does not stack on them.
        if self._report_caches_released:
            return
        self._report_caches_released = True
        self._report_sql_cache.reset(self.report, MicroStrategyCache.REPORT_SQL)
        with self._report_sql_lock:
            self._report_sql_consumed.clear()
        self._report_object_cache.reset(self.report, MicroStrategyCache.DATASET_OBJECT)
        self._report_derived_metric_cache.reset(
            self.report, MicroStrategyCache.REPORT_DERIVED_METRIC
        )
        self._report_v2_payload_cache.reset(
            self.report, MicroStrategyCache.REPORT_DEFINITION
        )
        with self._cache_lock:
            self._model_documents_in_flight.clear()
        self.lineage.reset_project_caches()

    def _reset_project_caches(self) -> None:
        # Memory and temporary disk track the largest project, not the whole run.
        if self._project_caches_released:
            return
        self._project_caches_released = True
        self._release_report_caches()
        self._metric_model_cache.reset(self.report, MicroStrategyCache.METRIC_MODEL)
        self.report.report_cache_project_reset()

    def _emit_chapter_page_structured_property_definitions(
        self,
    ) -> Iterable[MetadataWorkUnit]:
        """The three chart structured property definitions, once per run and
        ahead of every chart that carries a value. GMS validates a
        structuredProperties assignment against the definition already in
        its database, and the REST sink's default async batching could
        deliver a chart's values before the definition that makes them valid,
        so with a graph at hand the definitions are written synchronously
        first (as the Glue source does for its column parameter properties).
        Without a graph (file sink, tests) they are ordinary work units
        emitted before any other."""
        for definition in self.mapper.chapter_page_structured_property_definitions():
            if self.ctx.graph is not None:
                self.ctx.graph.emit_mcp(definition, emit_mode=EmitMode.SYNC_PRIMARY)
            else:
                yield definition.as_workunit()
            self.report.report_chapter_page_structured_property_definition_emitted()

    def _warn_if_every_sql_view_failed(self) -> None:
        parsed = self.report.sql_views_parsed
        # Cache hits count in sql_views_parsed; a failure counts once, plus its reuses.
        failed = (
            self.report.sql_parse_failure_count
            + self.report.sql_parse_failure_cache_hits
        )
        if parsed == 0 or failed < parsed:
            return
        first_failure = next(iter(self.report.sql_parse_failures), "")
        self.report.warning(
            title="Every MicroStrategy SQL view failed to parse",
            message=(
                f"All {parsed} SQL views submitted for warehouse lineage failed to "
                "parse, so no dataset-to-warehouse lineage was emitted this run. "
                "This usually indicates an environment problem rather than bad "
                "SQL; see the first failure for the cause."
            ),
            context=f"first_failure={first_failure}",
        )

    @timed_generator_phase(MicroStrategyPhase.USAGE)
    def _process_usage_statistics(
        self,
        projects: List[Project],
    ) -> Iterable[MetadataWorkUnit]:
        # The Platform Analytics project is matched against the full project
        # list, not the pattern-filtered one: telemetry lives in its own
        # project that recipes typically do not allow-list for ingestion.
        extractor = MicroStrategyUsageExtractor(self.client, self.config, self.report)
        pa_project = extractor.find_platform_analytics_project(projects)
        if pa_project is None:
            self.report.warning(
                title="Platform Analytics Project Not Found",
                message=(
                    "Skipping usage statistics because the Platform Analytics "
                    "project was not found or is not visible to the principal."
                ),
                context=(
                    "platform_analytics_project_name="
                    f"{self.config.platform_analytics_project_name!r}"
                ),
            )
            return
        try:
            buckets = extractor.fetch_usage_buckets(pa_project.id)
        except Exception as error:
            # Usage is supplementary: a warning (not a failure) so a broken
            # telemetry cube never blocks stale-entity cleanup of the
            # successfully ingested entities.
            self.report.warning(
                title="Failed to Extract Usage Statistics",
                message="Skipping usage statistics after an unexpected error.",
                context=f"project_id={pa_project.id}",
                exc=error,
            )
            return
        yield from self._usage_bucket_workunits(buckets)

    def _usage_bucket_workunits(
        self,
        buckets: List[UsageBucket],
    ) -> Iterable[MetadataWorkUnit]:
        for bucket in buckets:
            target = self._usage_targets.get(bucket.object_guid)
            if target is None:
                # Telemetry covers every object in the environment; rows for
                # objects outside this run's scope are expected.
                self.report.report_usage_object_unmatched()
                continue
            yield from self.mapper.gen_usage_workunits(
                target.kind,
                target.urn,
                bucket,
            )

    def _register_usage_target(
        self,
        object_id: str,
        entity_kind: str,
        entity_urn: str,
    ) -> None:
        if self.config.extract_usage_statistics:
            self._usage_targets[object_id.strip().upper()] = UsageTarget(
                kind=entity_kind, urn=entity_urn
            )

    def _register_recorded_target(self, target: RecordedUsageTarget) -> None:
        self._register_usage_target(target.object_id, target.kind, target.urn)

    def _finish_checkpoint(self) -> None:
        if self.report.failures:
            # Objects a failed run never reached keep their last read.
            self._object_state.keep_unrecorded()
        if self._object_state.is_too_large():
            self.report.warning(
                title="Unchanged-object checkpoint too large",
                message=(
                    "The checkpoint of dashboards and reports is over "
                    "stateful_ingestion.max_checkpoint_state_size, so it is not "
                    "saved and the next run reads every object."
                ),
            )

    def _note_change_check(self, failure: Optional[str]) -> None:
        if failure is not None:
            self.report.report_change_check_failure(failure)

    def _evaluate_dashboard_remote(
        self, project: Project, dashboard_object: MicroStrategyObject
    ) -> ChangeCheckResult:
        return self._change_detector.evaluate_dashboard(
            project.id,
            dashboard_object,
            self.mapper.folder_path(
                project.name, dashboard_object, self._predefined_folders(project.id)
            ),
            self._dataset_object_info,
            self.ctx.graph,
        )

    def _evaluate_dashboard(
        self, project: Project, dashboard_object: MicroStrategyObject
    ) -> ChangeCheckResult:
        result = self._evaluate_dashboard_remote(project, dashboard_object)
        self._note_change_check(result.failure)
        return result

    def _evaluate_report_remote(
        self, project: Project, report_object: MicroStrategyObject
    ) -> ChangeCheckResult:
        return self._change_detector.evaluate_report(
            project.id,
            report_object,
            self.mapper.folder_path(
                project.name, report_object, self._predefined_folders(project.id)
            ),
            self._dataset_object_info,
            self.ctx.graph,
        )

    def _evaluate_report(
        self, project: Project, report_object: MicroStrategyObject
    ) -> ChangeCheckResult:
        result = self._evaluate_report_remote(project, report_object)
        self._note_change_check(result.failure)
        return result

    @staticmethod
    def _tracked_urns(urns: Iterable[str]) -> List[str]:
        return sorted(
            {
                urn
                for urn in urns
                if guess_entity_type(urn) not in STATEFUL_INGESTION_IGNORED_ENTITY_TYPES
            }
        )

    def _keep_entities(
        self, urns: Sequence[str], usage_targets: Sequence[RecordedUsageTarget]
    ) -> None:
        for urn in urns:
            self.stale_entity_removal_handler.add_entity_to_state(
                guess_entity_type(urn), urn
            )
        for target in usage_targets:
            self._register_recorded_target(target)

    def _keep_dashboard(
        self,
        project_id: str,
        dashboard_id: str,
        previous: ObjectCheckpoint,
        linked_report_ids: Optional[Set[str]],
    ) -> None:
        self._keep_entities(previous.urns, previous.usage_targets)
        if linked_report_ids is not None:
            linked_report_ids.update(previous.linked_report_ids)
        if previous.fingerprint is not None:
            self._dashboard_fingerprints[normalize_object_id(dashboard_id)] = (
                previous.fingerprint
            )
        self._object_state.record_dashboard(project_id, dashboard_id, previous)

    def _keep_report(
        self, project_id: str, report_id: str, previous: ObjectCheckpoint
    ) -> None:
        self._keep_entities(previous.urns, previous.usage_targets)
        self._object_state.record_report(project_id, report_id, previous)

    def _folder_containers(
        self,
        project_id: str,
        mstr_object: MicroStrategyObject,
        predefined_folders: Optional[PredefinedFolderResolution],
        checkpoint: Union[ObjectCheckpoint, ReportDatasetCheckpoint, None],
    ) -> Iterable[MetadataWorkUnit]:
        # A skipped object must keep its folders even if already emitted.
        if checkpoint is not None:
            checkpoint.urns.extend(
                self.mapper.folder_container_urns(
                    project_id, mstr_object, predefined_folders
                )
            )
        yield from self.mapper.gen_folder_containers(
            project_id, mstr_object, predefined_folders
        )

    def _start_report_plan(
        self,
        project_id: str,
        candidates: List[str],
        decisions: Dict[str, ChangeCheckResult],
    ) -> ReportDatasetPlan:
        plan = ReportDatasetPlan(
            previous_dashboards=self._object_state.previous_dashboards(project_id),
            previous_reports=self._object_state.previous_report_datasets(project_id),
            candidates=candidates,
            unchanged={
                dashboard_id
                for dashboard_id, decision in decisions.items()
                if decision.unchanged
            },
        )
        plan.mark_reports_of_changed_dashboards()
        pending_reports = [
            (report_id, previous)
            for report_id, previous in plan.previous_reports.items()
            if report_id not in plan.dirty
        ]
        with ordered_parallel_map(
            lambda item: self._change_detector.evaluate_report_dataset(
                project_id,
                item[0],
                item[1],
                {
                    dashboard_id: dashboard.fingerprint
                    if (dashboard := plan.previous_dashboards.get(dashboard_id))
                    else None
                    for dashboard_id in item[1].dashboard_ids
                },
                self._dataset_object_info,
                self.ctx.graph,
            ),
            pending_reports,
            max_workers=self.config.max_workers,
        ) as outcomes:
            for outcome in outcomes:
                report_id = outcome.item[0]
                result = outcome.unwrap()
                self._note_change_check(result.failure)
                if not result.unchanged:
                    plan.dirty.add(report_id)
        return plan

    def _emits_report_dataset(self, entry: "_ReportDatasetEntry") -> bool:
        # Dashboard-only datasets follow extract_cubes; report-pass ones always emit.
        return entry.from_report_pass or bool(
            entry.dashboard_ids and self.config.extract_cubes
        )

    def _mark_bound_reports_changed(self, project_id: str) -> None:
        # Re-emit report datasets a changed dashboard built or the last run missed.
        plan = self._report_plan
        if plan is None:
            return
        for key, entry in self._report_datasets.items():
            if key.project_id != project_id or not self._emits_report_dataset(entry):
                continue
            report_id = key.report_id
            if report_id not in plan.previous_reports or any(
                normalize_object_id(dashboard_id) not in plan.unchanged
                for dashboard_id in entry.dashboard_ids
            ):
                plan.dirty.add(report_id)

    def _read_dashboards_of_changed_reports(
        self,
        project: Project,
        lineage_context: "_LazyProjectLineage",
        linked_report_ids: Optional[Set[str]],
    ) -> Iterable[MetadataWorkUnit]:
        # A re-emitted report dataset is rebuilt from every embedding dashboard.
        plan = self._report_plan
        if plan is None:
            return
        while True:
            self._mark_bound_reports_changed(project.id)
            to_read = plan.dashboards_to_read() - self._read_dashboards
            pending = [
                self._dashboard_candidates[dashboard_id]
                for dashboard_id in plan.candidates
                if dashboard_id in to_read
            ]
            if not pending:
                return
            self.report.report_dashboards_skipped_unchanged(-len(pending))
            yield from self._run_dashboards_parallel(
                project,
                pending,
                lineage_context,
                linked_report_ids,
            )

    def _order_by_search(self, entry: "_ReportDatasetEntry") -> "_ReportDatasetEntry":
        # Restore search order so the fallback dashboard matches a full run.
        order = {
            dashboard_id: index
            for index, dashboard_id in enumerate(self._dashboard_candidates)
        }
        dashboard_ids = sorted(
            entry.dashboard_ids,
            key=lambda dashboard_id: order.get(
                normalize_object_id(dashboard_id), len(order)
            ),
        )
        first = (
            self._dashboard_candidates.get(normalize_object_id(dashboard_ids[0]))
            if dashboard_ids
            else None
        )
        return entry.model_copy(
            update={
                "dashboard_ids": dashboard_ids,
                "first_dashboard_object": first or entry.first_dashboard_object,
            }
        )

    def _process_project(self, project: Project) -> Iterable[MetadataWorkUnit]:
        self._report_caches_released = False
        self._project_caches_released = False
        self._dashboard_candidates = {}
        self._read_dashboards = set()
        self._dashboard_fingerprints = {}
        self._report_plan = None
        source_warehouses = self._get_project_source_warehouses(project.id)
        # Warehouse/model-table lookups are deferred until an allowed dashboard/report
        # needs them, so fully-filtered projects don't pay for those API calls.
        lineage_context = _LazyProjectLineage(self, project.id, source_warehouses)
        # Report ids referenced by allowed dashboards, collected during the
        # dashboard pass so the report pass can scope to them.
        linked_report_ids: Set[str] = set()
        yield from self.mapper.gen_project_container(
            project,
            source_warehouses=source_warehouses,
        )
        if (
            self.config.extract_dashboards or self.config.extract_reports
        ) and not self._resolve_project_folders(project):
            if self.config.emit_semantic_model_entities:
                yield from self._process_project_semantic_model(
                    project, lineage_context
                )
            return
        if self._records_checkpoints():
            self._change_detector.init_project(
                project.id, source_warehouses, self._predefined_folders(project.id)
            )
        if (
            self._per_report
            and self._skips_unchanged()
            and not self.config.extract_dashboards
        ):
            self._report_plan = self._start_report_plan(project.id, [], {})
        if self.config.extract_dashboards:
            yield from self._process_project_dashboards(
                project,
                lineage_context,
                linked_report_ids,
            )
        if self.config.extract_reports:
            yield from self._process_project_reports(
                project,
                lineage_context,
                linked_report_ids,
            )
        if self._report_plan is not None:
            self._report_plan.mark_reports_left_report_pass()
            yield from self._read_dashboards_of_changed_reports(
                project, lineage_context, linked_report_ids
            )
        if self._per_report:
            yield from self._gen_report_dataset_workunits(project.id)
        if self.config.emit_semantic_model_entities:
            yield from self._process_project_semantic_model(project, lineage_context)

    @timed_generator_phase(MicroStrategyPhase.SEMANTIC_MODEL)
    def _process_project_semantic_model(
        self,
        project: Project,
        lineage_context: "_LazyProjectLineage",
    ) -> Iterable[MetadataWorkUnit]:
        # The metric model cache is kept so the semantic model reuses what the dashboards fetched.
        self._release_report_caches()
        model_tables = lineage_context.model_tables_retrying_failure()
        if model_tables is None:
            # [] would let stale removal soft-delete the semantic model.
            self.report.failure(
                title="MicroStrategy Semantic Model Unavailable",
                message=(
                    "Skipping semantic-model emission for this project because "
                    "the model tables API could not be read. Stale-entity "
                    "removal is skipped for this run."
                ),
                context=f"project_id={project.id}",
            )
            return
        if not model_tables:
            self.report.warning(
                title="MicroStrategy Semantic Model Unavailable",
                message=(
                    "Skipping semantic-model emission for this project because no "
                    "logical tables were returned by the model tables API."
                ),
                context=f"project_id={project.id}",
            )
            return
        semantic_model_mapper = MicroStrategySemanticModelMapper(
            config=self.config,
            report=self.report,
            client=self.client,
            lineage=self.lineage,
            metric_model_cache=self._metric_model_cache,
        )
        yield from semantic_model_mapper.emit(
            project_id=project.id,
            project_name=project.name,
            model_tables=model_tables,
            warehouse_context=lineage_context.warehouse_context,
            project_container_key=self.mapper.project_key(project.id),
            table_context_resolver=lineage_context.table_context_resolver,
        )

    @timed_phase(MicroStrategyPhase.SOURCE_WAREHOUSES)
    def _get_project_source_warehouses(self, project_id: str) -> List[Datasource]:
        if not self.config.extract_source_warehouses:
            return []
        try:
            source_warehouses = self.client.list_project_datasources(project_id)
        except MicroStrategyAPIError as error:
            try:
                source_warehouses = self.client.list_datasources(project_id)
            except MicroStrategyAPIError as fallback_error:
                self.report.report_source_warehouse_api_failure()
                self.report.warning(
                    title="Failed to Fetch Source Warehouses",
                    message=(
                        "Skipping project datasource inventory. Dashboard extraction "
                        "will continue without source warehouse summary properties."
                    ),
                    context=f"project_id={project_id}",
                    exc=fallback_error,
                )
                return []
            self.report.warning(
                title="Failed to Fetch Project Source Warehouses",
                message=(
                    "Falling back to the broader datasource inventory endpoint. "
                    "Warehouse lineage context may be less precise."
                ),
                context=f"project_id={project_id}",
                exc=error,
            )

        self.report.report_source_warehouses_scanned(len(source_warehouses))
        return source_warehouses

    def _warehouse_lineage_enabled(self) -> bool:
        return self.config.extract_lineage and (
            self.config.extract_warehouse_lineage
            or self.config.extract_model_lineage
            or self.config.extract_report_sql_lineage
        )

    def _project_has_several_warehouse_contexts(
        self, source_warehouses: List[Datasource]
    ) -> bool:
        return (
            self._warehouse_lineage_enabled()
            and len(
                distinct_warehouse_contexts(
                    source_warehouses,
                    self.config.env,
                    self.config.datasource_platform_mapping,
                )
            )
            > 1
        )

    @staticmethod
    def _source_platform_summary(source_warehouses: Optional[List[Datasource]]) -> str:
        types = Counter(
            datasource.database_type
            or datasource.datasource_type
            or datasource.dbms_name
            or "unknown"
            for datasource in source_warehouses or []
        )
        return "datasource_types=" + ", ".join(
            f"{name}:{count}" for name, count in sorted(types.items())
        )

    def _warn_unresolved_project_warehouse(
        self, project_id: str, source_warehouses: List[Datasource]
    ) -> None:
        if not source_warehouses:
            return
        several = self._project_has_several_warehouse_contexts(source_warehouses)
        self.report.warning(
            title=(
                "MicroStrategy Project Has Several Source Warehouses"
                if several
                else "MicroStrategy Project Has No Supported Source Warehouse"
            ),
            message=(
                "No single project-level warehouse can be chosen, so only "
                "datasets and logical tables that name their own datasource "
                f"get warehouse lineage. {MSTR_DATASOURCE_MAPPING_HINT}"
                if several
                else "None of the project's datasources maps to a supported "
                "platform, so warehouse lineage is skipped. Set "
                "datasource_platform_mapping, keyed by connection or "
                "datasource name, to map them."
            ),
            context=(
                f"project_id={project_id}, "
                f"{self._source_platform_summary(source_warehouses)}"
            ),
        )

    def _get_project_warehouse_context(
        self,
        project_id: str,
        source_warehouses: List[Datasource],
    ) -> Optional[WarehouseLineageContext]:
        if not self._warehouse_lineage_enabled():
            return None

        context = warehouse_context_from_datasources(
            source_warehouses,
            self.config.env,
            self.config.datasource_platform_mapping,
        )
        if not context:
            self._warn_unresolved_project_warehouse(project_id, source_warehouses)
            return None

        datasource = matching_datasource_for_context(
            source_warehouses,
            context,
            self.config.datasource_platform_mapping,
        )
        if datasource:
            context = self._context_with_connection(project_id, context, datasource)
        return context

    def _context_with_connection(
        self,
        project_id: str,
        context: WarehouseLineageContext,
        datasource: Datasource,
    ) -> WarehouseLineageContext:
        if not datasource.connection_id:
            return context
        try:
            connection = self.client.get_datasource_connection(
                datasource.connection_id,
                project_id=project_id,
            )
        except MicroStrategyAPIError as error:
            self.report.report_warehouse_lineage_api_failure()
            self.report.warning(
                title="Failed to Fetch Datasource Connection",
                message=(
                    "Continuing without connection-level database/schema "
                    "context; warehouse lineage may be less precise."
                ),
                context=(
                    f"project_id={project_id}, connection_id={datasource.connection_id}"
                ),
                exc=error,
            )
            return context
        return warehouse_context_with_connection(
            context,
            connection,
            self.config.datasource_platform_mapping,
        )

    def _evaluate_candidate(
        self,
        project: Project,
        mstr_object: MicroStrategyObject,
        name_pattern: AllowDenyPattern,
        remote_check: Optional[Callable[[MicroStrategyObject], bool]] = None,
    ) -> _CandidateEvaluation[MicroStrategyObject]:
        if not name_pattern.allowed(mstr_object.name):
            return _CandidateEvaluation(
                candidate=mstr_object,
                accepted=False,
                filtered_by_pattern=True,
            )
        pattern = self.config.folder_path_pattern
        folder_path = self.mapper.folder_path(
            project.name, mstr_object, self._predefined_folders(project.id)
        )
        unresolved = folder_path is None
        if folder_path is None:
            if not _is_default_pattern(pattern):
                return _CandidateEvaluation(
                    candidate=mstr_object,
                    accepted=False,
                    unresolved_folder_path=True,
                    filtered_by_folder_path=f"{mstr_object.name} (unresolved)",
                )
        elif not pattern.allowed(folder_path):
            return _CandidateEvaluation(
                candidate=mstr_object,
                accepted=False,
                filtered_by_folder_path=f"{mstr_object.name} ({folder_path})",
            )
        if self._is_personal_object(project.id, mstr_object):
            return _CandidateEvaluation(
                candidate=mstr_object,
                accepted=False,
                unresolved_folder_path=unresolved,
                personal_folder=True,
            )
        if remote_check is not None and not remote_check(mstr_object):
            return _CandidateEvaluation(
                candidate=mstr_object,
                accepted=False,
                unresolved_folder_path=unresolved,
            )
        return _CandidateEvaluation(
            candidate=mstr_object,
            accepted=True,
            unresolved_folder_path=unresolved,
        )

    def _evaluate_candidates_parallel(
        self,
        project: Project,
        candidates: Iterable[MicroStrategyObject],
        *,
        name_pattern: AllowDenyPattern,
        on_pattern_skip: Callable[[str], None],
        on_folder_skip: Callable[[str], None],
        remote_check: Optional[Callable[[MicroStrategyObject], bool]] = None,
    ) -> Iterator[MicroStrategyObject]:
        with ordered_parallel_map(
            lambda item: self._evaluate_candidate(
                project, item, name_pattern, remote_check=remote_check
            ),
            candidates,
            max_workers=self.config.max_workers,
        ) as outcomes:
            for outcome in outcomes:
                decision = outcome.unwrap()
                mstr_object = decision.candidate
                if decision.filtered_by_pattern:
                    on_pattern_skip(mstr_object.name)
                    continue
                if decision.unresolved_folder_path:
                    self._record_unresolved_folder_path(project.id, mstr_object)
                if decision.filtered_by_folder_path is not None:
                    on_folder_skip(decision.filtered_by_folder_path)
                    continue
                if decision.personal_folder:
                    self.report.report_personal_folder_object_skipped(mstr_object.name)
                    logger.debug(
                        "Skipping %r (%s) in project %s: filed under a personal folder",
                        mstr_object.name,
                        mstr_object.id,
                        project.id,
                    )
                    continue
                if decision.accepted:
                    yield mstr_object

    def _record_unresolved_folder_path(
        self, project_id: str, mstr_object: MicroStrategyObject
    ) -> None:
        self.report.report_folder_path_unresolved()
        self.report.warning(
            title="Folder path unresolved",
            message=(
                "The search result carried no folder ancestry, so "
                "folder_path_pattern could not be matched against the "
                "object; it is ingested under the default pattern and "
                "skipped once a pattern is configured."
            ),
            context=(
                f"project_id={project_id}, object_id={mstr_object.id}, "
                f"object_name={mstr_object.name}"
            ),
            log=False,
        )

    def _process_project_dashboards(
        self,
        project: Project,
        lineage_context: "_LazyProjectLineage",
        linked_report_ids: Optional[Set[str]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        candidates = self._allowed_dashboards(project)
        if not self._skips_unchanged():
            yield from self._run_dashboards_parallel(
                project, candidates, lineage_context, linked_report_ids
            )
            return
        # Decide every change first so a per_report dataset's dashboards read together.
        self._dashboard_candidates = {
            normalize_object_id(dashboard_object.id): dashboard_object
            for dashboard_object in candidates
        }
        decisions: Dict[str, ChangeCheckResult] = {}
        with ordered_parallel_map(
            lambda item: self._evaluate_dashboard_remote(project, item[1]),
            list(self._dashboard_candidates.items()),
            max_workers=self.config.max_workers,
        ) as check_outcomes:
            for check_outcome in check_outcomes:
                dashboard_id = check_outcome.item[0]
                result = check_outcome.unwrap()
                self._note_change_check(result.failure)
                decisions[dashboard_id] = result
        to_read = {
            dashboard_id
            for dashboard_id, decision in decisions.items()
            if not decision.unchanged
        }
        if self._per_report:
            self._report_plan = self._start_report_plan(
                project.id, list(self._dashboard_candidates), decisions
            )
            to_read |= self._report_plan.dashboards_to_read()
        dashboards_to_read = [
            dashboard_object
            for dashboard_id, dashboard_object in self._dashboard_candidates.items()
            if dashboard_id in to_read or decisions[dashboard_id].previous is None
        ]
        with ordered_parallel_map(
            lambda item: self._prefetch_dashboard(project, item, lineage_context),
            dashboards_to_read,
            max_workers=self.config.max_workers,
        ) as outcomes:
            outcome_iter = iter(outcomes)
            for dashboard_id, dashboard_object in self._dashboard_candidates.items():
                previous = decisions[dashboard_id].previous
                if dashboard_id in to_read or previous is None:
                    yield from self._consume_dashboard_with_boundary(
                        project,
                        next(outcome_iter),
                        lineage_context,
                        linked_report_ids,
                    )
                    continue
                self._keep_dashboard(
                    project.id, dashboard_object.id, previous, linked_report_ids
                )
                self.report.report_dashboards_skipped_unchanged()
        yield from self._read_dashboards_of_changed_reports(
            project, lineage_context, linked_report_ids
        )

    def _allowed_dashboards(self, project: Project) -> Iterable[MicroStrategyObject]:
        return self._evaluate_candidates_parallel(
            project,
            timed_iter(
                self.report,
                MicroStrategyPhase.DASHBOARD_SEARCH,
                self.client.search_dashboards(project.id),
            ),
            name_pattern=self.config.dashboard_pattern,
            on_pattern_skip=self.report.report_filtered_dashboard,
            on_folder_skip=self.report.report_dashboard_filtered_by_folder_path,
        )

    def _run_dashboards_parallel(
        self,
        project: Project,
        dashboards: Iterable[MicroStrategyObject],
        lineage_context: "_LazyProjectLineage",
        linked_report_ids: Optional[Set[str]],
    ) -> Iterable[MetadataWorkUnit]:
        with ordered_parallel_map(
            lambda item: self._prefetch_dashboard(project, item, lineage_context),
            dashboards,
            max_workers=self.config.max_workers,
        ) as outcomes:
            for outcome in outcomes:
                yield from self._consume_dashboard_with_boundary(
                    project,
                    outcome,
                    lineage_context,
                    linked_report_ids,
                )

    def _consume_dashboard_with_boundary(
        self,
        project: Project,
        outcome: TaskOutcome[MicroStrategyObject, _PrefetchedDashboard],
        lineage_context: "_LazyProjectLineage",
        linked_report_ids: Optional[Set[str]],
    ) -> Iterable[MetadataWorkUnit]:
        project_id = project.id
        dashboard_object = outcome.item
        self._read_dashboards.add(normalize_object_id(dashboard_object.id))
        # Progress before the expensive per-dashboard work so the log never goes silent.
        logger.info(
            "Processing dashboard %r (%s) in project %s",
            dashboard_object.name,
            dashboard_object.id,
            project_id,
        )
        before = self._progress.snapshot()
        consumer_calls_before, consumer_secs_before = self.report.thread_api_totals()
        consumer_started = time.perf_counter()
        consumer_elapsed = 0.0
        prefetch_seconds = (
            outcome.value.prefetch_seconds if outcome.value is not None else 0.0
        )
        prefetch_api_calls = (
            outcome.value.prefetch_api_calls if outcome.value is not None else 0
        )
        prefetch_api_seconds = (
            outcome.value.prefetch_api_seconds if outcome.value is not None else 0.0
        )
        checkpoint = ObjectCheckpoint() if self._records_checkpoints() else None
        try:
            # Timed step by step, so sink writes are not charged to it.
            for workunit in _recording_urns(
                timed_iter(
                    self.report,
                    MicroStrategyPhase.DASHBOARD,
                    self._process_dashboard_object(
                        project,
                        dashboard_object,
                        lineage_context,
                        linked_report_ids,
                        checkpoint,
                        prefetched_outcome=outcome,
                    ),
                ),
                checkpoint,
            ):
                consumer_elapsed += time.perf_counter() - consumer_started
                yield workunit
                consumer_started = time.perf_counter()
            consumer_elapsed += time.perf_counter() - consumer_started
        except (MicroStrategyAuthError, ConfigurationError):
            consumer_elapsed += time.perf_counter() - consumer_started
            raise
        except Exception as error:
            consumer_elapsed += time.perf_counter() - consumer_started
            self.report.warning(
                title="Failed to Process Dashboard",
                message=(
                    "Skipping dashboard after an unexpected error; other "
                    "dashboards will still be processed."
                ),
                context=(
                    f"project_id={project_id}, "
                    f"dashboard_id={dashboard_object.id}, "
                    f"dashboard_name={dashboard_object.name}"
                ),
                exc=error,
            )
            checkpoint = None
        finally:
            self._release_model_document(project_id, dashboard_object.id)
            consumer_calls_after, consumer_secs_after = self.report.thread_api_totals()
            self._progress.dashboard_done(
                project.name,
                dashboard_object.name,
                dashboard_object.id,
                before,
                override_seconds=prefetch_seconds + consumer_elapsed,
                override_api_calls=prefetch_api_calls
                + (consumer_calls_after - consumer_calls_before),
                override_api_seconds=prefetch_api_seconds
                + (consumer_secs_after - consumer_secs_before),
            )
        if not self._records_checkpoints():
            return
        if checkpoint is None or checkpoint.fingerprint is None:
            # Not fully read: keep what its last read produced.
            previous = self._object_state.previous_dashboard(
                project_id, dashboard_object.id
            )
            if previous is not None:
                self._keep_dashboard(
                    project_id, dashboard_object.id, previous, linked_report_ids
                )
            return
        checkpoint.urns = self._tracked_urns(checkpoint.urns)
        self._object_state.record_dashboard(project_id, dashboard_object.id, checkpoint)
        self._dashboard_fingerprints[normalize_object_id(dashboard_object.id)] = (
            checkpoint.fingerprint
        )

    def _resolve_project_folders(self, project: Project) -> bool:
        try:
            self._predefined_folders(project.id)
        except _PredefinedFoldersUnavailable as error:
            # A failure (rather than a warning) also stops stale-entity removal
            # from soft-deleting the containers earlier runs emitted.
            self.report.failure(
                title="Predefined folder lookup failed",
                message=(
                    "Could not resolve MicroStrategy's predefined folders, so "
                    "this project's dashboards and reports were skipped rather "
                    "than filed under different folder containers. Set "
                    "use_predefined_folder_names: false if the server does not "
                    "support GET /api/folders/preDefined."
                ),
                context=f"project_id={project.id}, project_name={project.name}",
                exc=error.__cause__ or error,
            )
            return False
        return True

    @timed_phase(MicroStrategyPhase.FOLDER_RESOLUTION)
    def _predefined_folders(self, project_id: str) -> PredefinedFolderResolution:
        """Predefined-folder resolution for a project: MicroStrategy-assigned
        labels (e.g. 'Shared Reports') plus the system containers Strategy Web
        never shows (project root folder, 'Public Objects'), cached per project
        since folder containers are (re)built once per dashboard/report."""
        if not self.config.use_predefined_folder_names:
            return PredefinedFolderResolution.empty()
        with self._cache_lock:
            cached = self._predefined_folder_cache.get(project_id)
        if cached is not None:
            return cached

        def fetch() -> PredefinedFolderResolution:
            with self._cache_lock:
                existing = self._predefined_folder_cache.get(project_id)
            if existing is not None:
                return existing
            labels: Dict[str, str] = {}
            hidden_ids: Set[str] = set()
            try:
                folders = self.client.get_predefined_folders(
                    project_id,
                    sorted(
                        set(MSTR_PREDEFINED_FOLDER_LABELS)
                        | MSTR_PREDEFINED_HIDDEN_FOLDER_TYPES
                    ),
                )
            except MicroStrategyAuthError:
                raise
            except Exception as error:
                # Falling back to raw folder names would mint a different set of
                # folder container URNs and re-parent every object under them, and
                # folder_path_pattern would match against the wrong paths. The
                # caller skips the project's dashboards and reports instead.
                raise _PredefinedFoldersUnavailable(project_id) from error

            if not any(
                folder.folder_type in MSTR_PREDEFINED_FOLDER_LABELS
                or folder.folder_type in MSTR_PREDEFINED_HIDDEN_FOLDER_TYPES
                for folder in folders
            ):
                # Every project has these folders, so an answer without any of them
                # is unrecognized, not empty; using raw names would change URNs.
                raise _PredefinedFoldersUnavailable(project_id)
            for folder in folders:
                if folder.folder_type is None:
                    continue
                if folder.folder_type in MSTR_PREDEFINED_HIDDEN_FOLDER_TYPES:
                    hidden_ids.add(normalize_object_id(folder.id))
                    continue
                label = MSTR_PREDEFINED_FOLDER_LABELS.get(folder.folder_type)
                if label:
                    labels[normalize_object_id(folder.id)] = label
            self.report.report_predefined_folder_labels_resolved(
                len(labels) + len(hidden_ids)
            )
            resolution = PredefinedFolderResolution(
                labels=labels, hidden_ids=hidden_ids
            )
            with self._cache_lock:
                self._predefined_folder_cache[project_id] = resolution
            return resolution

        return self._predefined_folder_flight.do(project_id, fetch)

    @timed_phase(MicroStrategyPhase.FOLDER_RESOLUTION)
    def _personal_folders(self, project_id: str) -> PersonalFolderResolution:
        """How to recognise personal (per-user profile) content in a project,
        resolved once per project. By id when possible: the predefined-folder
        call names the logged-in principal's own profile folder (type 19) and
        My Reports (type 20); the profile folder's immediate parent is the
        project's Profiles system folder, under which every user's profile
        folder lives. Any failure falls back to matching ancestor folder
        names, which is exact but breaks on localized or renamed folders."""
        with self._cache_lock:
            cached = self._personal_folder_cache.get(project_id)
        if cached is not None:
            return cached

        def fetch() -> PersonalFolderResolution:
            with self._cache_lock:
                existing = self._personal_folder_cache.get(project_id)
            if existing is not None:
                return existing
            root_ids: Set[str] = set()
            names: Set[str] = set(MSTR_PERSONAL_FOLDER_NAMES)
            profiles_root_ids: Set[str] = set()
            lookup_failed = False
            try:
                folders = self.client.get_predefined_folders(
                    project_id, list(MSTR_PERSONAL_FOLDER_TYPES)
                )
                for folder in folders:
                    if folder.folder_type not in MSTR_PERSONAL_FOLDER_TYPES:
                        continue
                    root_ids.add(normalize_object_id(folder.id))
                    if folder.folder_type == MSTR_FOLDER_TYPE_PROFILE_REPORTS:
                        names.add(folder.name.strip().lower())
                    if folder.folder_type == MSTR_FOLDER_TYPE_PROFILE_OBJECTS:
                        profiles_root_ids = self._profiles_root_ids(
                            project_id, folder.id
                        )
                        root_ids.update(profiles_root_ids)
            except MicroStrategyAuthError:
                raise
            except Exception as error:
                root_ids.clear()
                profiles_root_ids.clear()
                lookup_failed = True
                self.report.warning(
                    title="Personal folder root not resolved",
                    message=(
                        "Could not resolve the project's Profiles folder by id; "
                        "personal folders are recognised by ancestor folder name "
                        "('Profiles' / 'My Reports', case-insensitive) instead, so "
                        "personal content in localized or renamed folders may be "
                        "ingested."
                    ),
                    context=f"project_id={project_id}",
                    exc=error,
                )
            if profiles_root_ids:
                self.report.report_personal_folder_root_resolved()
            elif not lookup_failed:
                self.report.info(
                    title="Personal folder root not resolved",
                    message=(
                        "The project's Profiles folder could not be identified "
                        "(no profile folder for this principal, or its parent is "
                        "a hidden system folder); personal folders are recognised "
                        "by ancestor folder name instead."
                    ),
                    context=f"project_id={project_id}",
                )
            resolution = PersonalFolderResolution(
                root_ids=root_ids,
                names=names,
                profiles_root_resolved=bool(profiles_root_ids),
            )
            with self._cache_lock:
                self._personal_folder_cache[project_id] = resolution
            return resolution

        return self._personal_folder_flight.do(project_id, fetch)

    def _profiles_root_ids(self, project_id: str, profile_folder_id: str) -> Set[str]:
        """The Profiles system folder: the immediate parent of the principal's
        own profile folder, read from that folder's ancestors. System
        containers hidden from the browse hierarchy (project root, Public
        Objects) are never it, so a profile folder parented directly under
        one of those yields nothing rather than a wrong root."""
        profile_folder = self.client.get_object_info(
            project_id, profile_folder_id, MSTR_OBJECT_TYPE_FOLDER
        )
        if profile_folder is None:
            return set()
        hidden_ids = self._predefined_folders(project_id).hidden_ids
        parents = [
            normalize_object_id(part.id)
            for part in extract_folder_parts(profile_folder.model_dump())
            if part.id
        ]
        if not parents or parents[-1] in hidden_ids:
            return set()
        return {parents[-1]}

    def _is_personal_object(
        self, project_id: str, mstr_object: MicroStrategyObject
    ) -> bool:
        if self.config.include_personal_folders:
            return False
        return is_personal_folder_object(
            mstr_object.model_dump(), self._personal_folders(project_id)
        )

    def _skip_personal_object(
        self, project_id: str, mstr_object: MicroStrategyObject
    ) -> bool:
        """Decide before any definition is fetched, so a skipped personal
        object costs no further API calls. Stateful ingestion soft-deletes
        previously ingested personal content on its own."""
        if not self._is_personal_object(project_id, mstr_object):
            return False
        self.report.report_personal_folder_object_skipped(mstr_object.name)
        logger.debug(
            "Skipping %r (%s) in project %s: filed under a personal folder",
            mstr_object.name,
            mstr_object.id,
            project_id,
        )
        return True

    def _skip_by_folder_path(
        self,
        project: Project,
        mstr_object: MicroStrategyObject,
        on_skip: Callable[[str], None],
    ) -> bool:
        """Apply folder_path_pattern before any definition is fetched, so a
        denied dashboard or report costs no further API calls and nothing
        under it (visualizations, datasets) is emitted. The path comes from
        the same ancestry and relabelling as the browse path, so the two
        always agree. An object whose ancestry is unknown cannot be matched:
        under the default allow-all pattern it is let through with a warning,
        since dropping it would silently shrink the catalog; under a
        configured pattern it is skipped and counted, since letting it
        through would silently widen the scope."""
        pattern = self.config.folder_path_pattern
        folder_path = self.mapper.folder_path(
            project.name, mstr_object, self._predefined_folders(project.id)
        )
        if folder_path is None:
            self.report.report_folder_path_unresolved()
            self.report.warning(
                title="Folder path unresolved",
                message=(
                    "The search result carried no folder ancestry, so "
                    "folder_path_pattern could not be matched against the "
                    "object; it is ingested under the default pattern and "
                    "skipped once a pattern is configured."
                ),
                context=(
                    f"project_id={project.id}, object_id={mstr_object.id}, "
                    f"object_name={mstr_object.name}"
                ),
                log=False,
            )
            if _is_default_pattern(pattern):
                return False
        elif pattern.allowed(folder_path):
            return False
        on_skip(f"{mstr_object.name} ({folder_path or 'unresolved'})")
        logger.debug(
            "Skipping %r (%s) in project %s: folder path %r not allowed",
            mstr_object.name,
            mstr_object.id,
            project.id,
            folder_path,
        )
        return True

    def _report_dataset_entry(
        self, project_id: str, dataset_id: str
    ) -> Optional["_ReportDatasetEntry"]:
        if not self._per_report:
            return None
        with self._cache_lock:
            return self._report_datasets.get(
                _report_dataset_key(project_id, dataset_id)
            )

    def _report_lineage_resolved(self, project_id: str, dataset_id: str) -> bool:
        # A parsed SQL view means this report is not executed again.
        entry = self._report_dataset_entry(project_id, dataset_id)
        return entry is not None and entry.lineage_resolved

    def _bind_report_dataset(
        self,
        project_id: str,
        dashboard_object: MicroStrategyObject,
        dataset: DatasetObject,
    ) -> DatasetObject:
        # Later dashboards share the first one's instance, so enrichment accumulates once.
        cache_key = _report_dataset_key(project_id, dataset.id)
        with self._cache_lock:
            entry = self._report_datasets.get(cache_key)
            if entry is None:
                entry = _ReportDatasetEntry(
                    dataset=dataset, first_dashboard_object=dashboard_object
                )
            else:
                self.report.report_report_deduplicated()
                if entry.dataset.source_warehouse is None:
                    entry.dataset.source_warehouse = dataset.source_warehouse
            if dashboard_object.id not in entry.dashboard_ids:
                entry = entry.model_copy(
                    update={
                        "dashboard_ids": [*entry.dashboard_ids, dashboard_object.id]
                    }
                )
            self._report_datasets[cache_key] = entry
            return entry.dataset

    def _register_report_dataset(
        self,
        project_id: str,
        report_object: MicroStrategyObject,
        report_definition: Optional[ReportDefinition],
    ) -> "_ReportDatasetEntry":
        # Keeps the dashboard's instance; the report definition replaces the schema.
        cache_key = _report_dataset_key(project_id, report_object.id)
        available_objects = (
            report_definition.available_objects if report_definition else {}
        )
        description = (
            report_definition.description if report_definition else None
        ) or report_object.description
        fresh = _report_dataset_object(report_object, available_objects, description)
        with self._cache_lock:
            entry = self._report_datasets.get(cache_key)
            lineage_resolved = False
            if entry is None:
                entry = _ReportDatasetEntry(dataset=fresh)
            elif available_objects:
                entry.dataset.available_objects = fresh.available_objects
                entry.dataset.object_ids = fresh.object_ids
                if description:
                    entry.dataset.description = description
                stale_field_lineage = self._drop_stale_field_lineage(entry.dataset)
                lineage_resolved = entry.lineage_resolved and not stale_field_lineage
            else:
                lineage_resolved = entry.lineage_resolved
            entry = entry.model_copy(
                update={
                    "report_object": report_object,
                    "report_definition": report_definition,
                    "from_report_pass": True,
                    "lineage_resolved": lineage_resolved,
                }
            )
            self._report_datasets[cache_key] = entry
            return entry

    def _drop_stale_field_lineage(self, dataset: DatasetObject) -> bool:
        field_paths = set(self.mapper.dataset_field_paths(dataset))
        stale_paths = [
            path
            for path in dataset.field_warehouse_upstreams
            if path not in field_paths
        ]
        for path in stale_paths:
            del dataset.field_warehouse_upstreams[path]
        return bool(stale_paths)

    def _gen_report_dataset_workunits(
        self, project_id: str
    ) -> Iterable[MetadataWorkUnit]:
        # Once, after both passes, so the entity has everything the run learned.
        predefined_folders = self._predefined_folders(project_id)
        plan = self._report_plan
        with self._cache_lock:
            project_keys = [
                key for key in self._report_datasets if key.project_id == project_id
            ]
            missing_dataset_ids = [
                self._report_datasets[key].dataset.id
                for key in project_keys
                if self._emits_report_dataset(self._report_datasets[key])
                and (plan is None or plan.emits(key.report_id))
                and self._report_datasets[key].report_object is None
            ]
        if len(missing_dataset_ids) > 1 and self.config.max_workers > 1:
            with ordered_parallel_map(
                lambda dataset_id: self._dataset_object_info(project_id, dataset_id),
                missing_dataset_ids,
                max_workers=self.config.max_workers,
            ) as outcomes:
                for outcome in outcomes:
                    outcome.unwrap()
        for cache_key in project_keys:
            with self._cache_lock:
                entry = self._report_datasets.pop(cache_key)
            if not self._emits_report_dataset(entry):
                continue
            if plan is not None and not plan.emits(cache_key.report_id):
                continue
            if plan is not None:
                entry = self._order_by_search(entry)
            checkpoint = (
                ReportDatasetCheckpoint(
                    dashboard_ids=[
                        normalize_object_id(dashboard_id)
                        for dashboard_id in entry.dashboard_ids
                    ],
                    from_report_pass=entry.from_report_pass,
                )
                if self._records_checkpoints()
                else None
            )
            dataset = entry.dataset
            dataset_object = entry.report_object or self._dataset_object_info(
                project_id, dataset.id
            )
            folder_object = self._folder_object(project_id, dataset_object)
            if folder_object is not None:
                yield from self._folder_containers(
                    project_id, folder_object, predefined_folders, checkpoint
                )
            fallback_key = (
                self.mapper.folder_container_for_dashboard(
                    project_id, entry.first_dashboard_object, predefined_folders
                )
                if entry.first_dashboard_object is not None
                else self.mapper.project_key(project_id)
            )
            usage_target = RecordedUsageTarget(
                object_id=dataset.id,
                kind=USAGE_TARGET_DATASET,
                urn=self.lineage.dataset_urn(project_id, None, dataset),
            )
            self._register_recorded_target(usage_target)
            if checkpoint is not None:
                checkpoint.usage_targets = [usage_target]
                checkpoint.referenced_object_ids = sorted(
                    dataset.normalized_object_ids()
                )
                checkpoint.fingerprint = (
                    self._change_detector.report_dataset_fingerprint(
                        project_id,
                        self._dataset_object_info(project_id, dataset.id),
                        {
                            dashboard_id: self._dashboard_fingerprints.get(dashboard_id)
                            for dashboard_id in checkpoint.dashboard_ids
                        },
                        entry.from_report_pass,
                    )
                )
            yield from _recording_urns(
                self.mapper.gen_report_dataset_workunits(
                    project_id,
                    dataset,
                    self.mapper.dataset_folder_parent_key(
                        project_id, folder_object, fallback_key, predefined_folders
                    ),
                    dataset_object=dataset_object,
                    dashboard_ids=entry.dashboard_ids,
                    report_definition=entry.report_definition,
                    fingerprint=_checkpoint_fingerprint(checkpoint),
                ),
                checkpoint,
            )
            if checkpoint is not None:
                checkpoint.urns = self._tracked_urns(checkpoint.urns)
                self._object_state.record_report_dataset(
                    project_id, dataset.id, checkpoint
                )
        if plan is not None:
            for report_id, previous in plan.kept_reports().items():
                self._keep_entities(previous.urns, previous.usage_targets)
                self._object_state.record_report_dataset(
                    project_id, report_id, previous
                )
                self.report.report_report_dataset_skipped_unchanged()

    def _folder_object(
        self, project_id: str, dataset_object: Optional[MicroStrategyObject]
    ) -> Optional[MicroStrategyObject]:
        """The object whose folder ancestry a dataset is filed under: its own,
        unless that ancestry is personal -- then None, so no container under
        Profiles is emitted and the dataset falls back to the folder of the
        dossier/report that embeds it (the same degradation as a failed
        object lookup)."""
        if dataset_object is None or not self._is_personal_object(
            project_id, dataset_object
        ):
            return dataset_object
        return None

    def _prefetch_dashboard(
        self,
        project: Project,
        dashboard_object: MicroStrategyObject,
        lineage_context: "_LazyProjectLineage",
    ) -> _PrefetchedDashboard:
        project_id = project.id
        started = time.perf_counter()
        calls_before, secs_before = self.report.thread_api_totals()
        dossier_sql_rows: Optional[List[Dict[str, object]]] = None
        skipped_report_sql_keys: Set[str] = set()
        with self.report.phase(MicroStrategyPhase.DASHBOARD, count=False):
            model_lineage_index = lineage_context.model_lineage_index
            try:
                dashboard = self._get_dashboard_definition(
                    project_id, dashboard_object, bind_per_report=False
                )
                if dashboard is not None:
                    needs_sql_view_context = (
                        self.config.extract_warehouse_lineage
                        or model_lineage_index is not None
                    )
                    has_warehouse_context = bool(
                        lineage_context.warehouse_context
                    ) or any(dataset.source_warehouse for dataset in dashboard.datasets)
                    if (
                        needs_sql_view_context
                        and has_warehouse_context
                        and dashboard.datasets
                    ):
                        if is_document_object(dashboard_object):
                            # In per_report mode an earlier dossier may settle a shared
                            # report on the consumer thread, so document SQL fetches run there.
                            if (
                                self.config.extract_document_dataset_sql_lineage
                                and not self._per_report
                            ):
                                skipped_report_sql_keys = (
                                    self._prefetch_document_warehouse_sql(
                                        project_id,
                                        dashboard,
                                        lineage_context.warehouse_context,
                                    )
                                )
                        else:
                            dossier_sql_rows = self._fetch_dossier_sql_view_rows(
                                project_id, dashboard_object, dashboard.id
                            )
                    if self.config.extract_derived_metrics:
                        for dataset in dashboard.datasets:
                            dataset_object = self._dataset_object_info(
                                project_id, dataset.id
                            )
                            subtype = (
                                (dataset_object.subtype or "").strip()
                                if dataset_object
                                else ""
                            )
                            if (
                                subtype not in MSTR_CUBE_SUBTYPES
                                and dataset_object is not None
                            ):
                                self._report_derived_metric_definitions(
                                    project_id, dataset.id
                                )
                        if (
                            self.config.resolve_report_metrics_via_instance
                            and dashboard.datasets
                        ):
                            self._model_document(project_id, dashboard.id)
                    if (
                        self.config.extract_reports
                        and not self._per_report
                        and (
                            not self.config.include_personal_folders
                            or not _is_default_pattern(self.config.folder_path_pattern)
                        )
                    ):
                        for dependency in dashboard.dependencies:
                            if _is_report_dependency(
                                dependency
                            ) and self.config.report_pattern.allowed(dependency.name):
                                self._linked_report_object(project_id, dependency.id)
                    if self.config.extract_cubes and not self._per_report:
                        for dataset in dashboard.datasets:
                            self._dataset_object_info(project_id, dataset.id)
            finally:
                self._release_dashboard_instance(project_id, dashboard_object)
        calls_after, secs_after = self.report.thread_api_totals()
        return _PrefetchedDashboard(
            dashboard_object=dashboard_object,
            dashboard=dashboard,
            dossier_sql_rows=dossier_sql_rows,
            skipped_report_sql_keys=skipped_report_sql_keys,
            prefetch_seconds=time.perf_counter() - started,
            prefetch_api_calls=calls_after - calls_before,
            prefetch_api_seconds=secs_after - secs_before,
        )

    def _consume_prefetched_dashboard(
        self,
        project: Project,
        outcome: TaskOutcome[MicroStrategyObject, _PrefetchedDashboard],
        lineage_context: "_LazyProjectLineage",
        linked_report_ids: Optional[Set[str]] = None,
        checkpoint: Optional[ObjectCheckpoint] = None,
    ) -> Iterable[MetadataWorkUnit]:
        project_id = project.id
        dashboard_object = outcome.item
        predefined_folders = self._predefined_folders(project_id)
        yield from self._folder_containers(
            project_id, dashboard_object, predefined_folders, checkpoint
        )
        parent_key = self.mapper.folder_container_for_dashboard(
            project_id, dashboard_object, predefined_folders
        )
        model_lineage_index = lineage_context.model_lineage_index
        prefetched = outcome.unwrap()
        dashboard = prefetched.dashboard
        if dashboard is None:
            return
        if self._per_report:
            bound_datasets: List[DatasetObject] = []
            for dataset in dashboard.datasets:
                bound = self._bind_report_dataset(project_id, dashboard_object, dataset)
                if bound is not dataset and dataset.metric_enrichments:
                    bound.metric_enrichments.update(dataset.metric_enrichments)
                bound_datasets.append(bound)
            dashboard.datasets = bound_datasets

        needs_sql_view_context = (
            self.config.extract_warehouse_lineage or model_lineage_index is not None
        )
        has_warehouse_context = bool(lineage_context.warehouse_context) or any(
            dataset.source_warehouse for dataset in dashboard.datasets
        )
        if needs_sql_view_context and has_warehouse_context and dashboard.datasets:
            if is_document_object(dashboard_object):
                if self.config.extract_document_dataset_sql_lineage:
                    self._enrich_document_warehouse_lineage(
                        project_id,
                        dashboard,
                        lineage_context.warehouse_context,
                        skipped_keys=prefetched.skipped_report_sql_keys,
                    )
            elif prefetched.dossier_sql_rows is not None:
                with self.report.phase(
                    MicroStrategyPhase.WAREHOUSE_LINEAGE, count=False
                ):
                    self._attach_dataset_warehouse_upstreams(
                        prefetched.dossier_sql_rows,
                        dashboard,
                        lineage_context.warehouse_context,
                        project_id=project_id,
                    )

        if model_lineage_index:
            self.mapper.attach_model_lineage(dashboard, model_lineage_index)
        if self.config.extract_derived_metrics:
            self.mapper.attach_derived_metrics(dashboard)
            self._enrich_report_derived_metrics(project_id, dashboard)
            self._enrich_document_derived_metrics(
                project_id, dashboard_object, dashboard
            )
        report_dependency_ids = [
            dependency.id
            for dependency in dashboard.dependencies
            if _is_report_dependency(dependency)
        ]
        if linked_report_ids is not None:
            linked_report_ids.update(
                report_id.upper() for report_id in report_dependency_ids
            )
        extra_chart_urns = self._dashboard_report_chart_urns(project, dashboard)
        usage_target = RecordedUsageTarget(
            object_id=dashboard_object.id,
            kind=USAGE_TARGET_DASHBOARD,
            urn=self.mapper.dashboard_urn(project_id, dashboard_object.id),
        )
        self._register_recorded_target(usage_target)
        if checkpoint is not None:
            dependency_ids = _unique_object_ids(
                [dataset.id for dataset in dashboard.datasets] + report_dependency_ids
            )
            checkpoint.dependency_report_ids = dependency_ids
            checkpoint.linked_report_ids = sorted(
                {report_id.upper() for report_id in report_dependency_ids}
            )
            checkpoint.usage_targets = [usage_target]
            checkpoint.referenced_object_ids = sorted(
                {
                    object_id
                    for dataset in dashboard.datasets
                    for object_id in dataset.normalized_object_ids()
                }
            )
            checkpoint.fingerprint = self._change_detector.object_fingerprint(
                project_id,
                dashboard_object,
                self.mapper.folder_path(
                    project.name, dashboard_object, predefined_folders
                ),
                dependency_ids,
                self._dataset_object_info,
            )
        yield from self._process_dashboard(
            project_id,
            dashboard_object,
            dashboard,
            parent_key,
            extra_chart_urns=extra_chart_urns,
            predefined_folders=predefined_folders,
            checkpoint=checkpoint,
        )

    def _process_dashboard_object(
        self,
        project: Project,
        dashboard_object: MicroStrategyObject,
        lineage_context: "_LazyProjectLineage",
        linked_report_ids: Optional[Set[str]] = None,
        checkpoint: Optional[ObjectCheckpoint] = None,
        *,
        prefetched_outcome: Optional[
            TaskOutcome[MicroStrategyObject, _PrefetchedDashboard]
        ] = None,
    ) -> Iterable[MetadataWorkUnit]:
        if prefetched_outcome is not None:
            yield from self._consume_prefetched_dashboard(
                project,
                prefetched_outcome,
                lineage_context,
                linked_report_ids,
                checkpoint=checkpoint,
            )
            return
        try:
            outcome = TaskOutcome.model_construct(
                item=dashboard_object,
                value=self._prefetch_dashboard(
                    project, dashboard_object, lineage_context
                ),
                error=None,
            )
            yield from self._consume_prefetched_dashboard(
                project,
                outcome,
                lineage_context,
                linked_report_ids,
                checkpoint=checkpoint,
            )
        finally:
            self._release_model_document(project.id, dashboard_object.id)

    @timed_phase(MicroStrategyPhase.DATASET_OBJECT_LOOKUP)
    def _dataset_object_info(
        self,
        project_id: str,
        dataset_id: str,
    ) -> Optional[MicroStrategyObject]:
        """Object info (subtype + folder ancestors) for a dataset backing a
        dossier or report, fetched once per distinct dataset per project.
        Reports and cubes share object type 3, so one lookup covers both. Any
        failure degrades to the pre-lookup behaviour (dataset parented under
        the dossier's folder, linking to the dossier) and is counted."""
        return self._report_object_cache.get_or_load(
            object_cache_key(project_id, dataset_id),
            lambda: self._fetch_dataset_object(project_id, dataset_id),
        )

    def _fetch_dataset_object(
        self, project_id: str, dataset_id: str
    ) -> Optional[MicroStrategyObject]:
        dataset_object: Optional[MicroStrategyObject] = None
        try:
            dataset_object = self.client.get_object_info(
                project_id, dataset_id, MSTR_OBJECT_TYPE_REPORT
            )
        except MicroStrategyAuthError:
            raise
        except Exception as error:
            self.report.report_dataset_object_lookup_failure()
            self.report.warning(
                title="Dataset object info unavailable",
                message=(
                    "Could not fetch a dataset's own object info, so its browse "
                    "path and external URL fall back to the dossier/report that "
                    "embeds it."
                ),
                context=f"project_id={project_id}, dataset_id={dataset_id}",
                exc=error,
                log=False,
            )
        else:
            self.report.report_dataset_object_lookup()
            if dataset_object is None:
                self.report.report_dataset_object_lookup_failure()
        return dataset_object

    @timed_phase(MicroStrategyPhase.REPORT_DERIVED_METRICS)
    def _enrich_report_derived_metrics(
        self,
        project_id: str,
        dashboard: DashboardDefinition,
    ) -> None:
        """Upgrade grid-derived metrics with their report definitions. A dossier
        dataset that is a report can define report-level derived metrics; the
        grid only shows them as `derived: true` elements, but the report's
        definition carries their object names and formulas. Cubes cannot
        define derived metrics and are skipped; a dataset whose type is
        unknown is only consulted when a grid actually showed derived metrics
        for it, so the extra calls stay proportional to the evidence."""
        for dataset in dashboard.datasets:
            dataset_object = self._dataset_object_info(project_id, dataset.id)
            subtype = (dataset_object.subtype or "").strip() if dataset_object else ""
            if subtype in MSTR_CUBE_SUBTYPES:
                continue
            if dataset_object is None and not dataset.derived_metrics:
                continue
            definitions = self._report_derived_metric_definitions(
                project_id, dataset.id
            )
            if definitions:
                self.mapper.attach_report_derived_metrics(dataset, definitions)

    @timed_phase(MicroStrategyPhase.DOCUMENT_DERIVED_METRICS)
    def _enrich_document_derived_metrics(
        self,
        project_id: str,
        dashboard_object: MicroStrategyObject,
        dashboard: DashboardDefinition,
    ) -> None:
        """Document/dossier-level derived metrics - defined in the dashboard on
        top of a dataset rather than in the dataset's report - live only in the
        Modeling document definition, which the report path never reads. Read
        it and attach each definition to the dataset that defines it. Opt-in
        with resolve_report_metrics_via_instance, alongside the report-level
        instance read: the endpoint is undocumented. It is read statically -
        the Modeling service has no document instance endpoint, and it rejects
        a dashboard execution instance with 400."""
        if not self.config.resolve_report_metrics_via_instance:
            return
        if not dashboard.datasets:
            return
        model_document = self._model_document(project_id, dashboard.id)
        if model_document is None:
            return

        by_dataset = extract_document_derived_metric_definitions(model_document)
        found = sum(len(definitions) for definitions in by_dataset.values())
        with_expression = sum(
            1
            for definitions in by_dataset.values()
            for definition in definitions
            if definition.has_expression
        )
        self.report.report_document_derived_metric_definitions(found, with_expression)
        for dataset in dashboard.datasets:
            definitions = by_dataset.get(normalize_object_id(dataset.id))
            if definitions:
                self.mapper.attach_report_derived_metrics(dataset, definitions)

    @timed_phase(MicroStrategyPhase.REPORT_DERIVED_METRIC_DEFINITIONS)
    def _report_derived_metric_definitions(
        self,
        project_id: str,
        report_id: str,
    ) -> Optional[List[ReportDerivedMetric]]:
        """Derived metric definitions of one report, fetched once per project.
        The Modeling endpoint (GET /api/model/reports/{id}) is preferred because
        it carries expressions; the v2 definition (GET /api/v2/reports/{id})
        is the fallback and may name derived metrics without a formula. None
        means both failed (counted in report_definition_failures)."""
        return self._report_derived_metric_cache.get_or_load(
            object_cache_key(project_id, report_id),
            lambda: self._fetch_report_derived_metric_definitions(
                project_id, report_id
            ),
        )

    def _fetch_report_derived_metric_definitions(
        self,
        project_id: str,
        report_id: str,
    ) -> Optional[List[ReportDerivedMetric]]:
        definitions: List[ReportDerivedMetric] = []
        model_error: Optional[Exception] = None
        v2_error: Optional[Exception] = None
        instance_id = self._report_model_instance(project_id, report_id)
        # An auth error can propagate after the instance exists; it is still
        # deleted so the server does not keep it.
        try:
            try:
                model_payload = self.client.get_model_report(
                    project_id, report_id, instance_id=instance_id
                )
            except MicroStrategyAuthError:
                raise
            except Exception as error:
                model_error = error
            else:
                definitions = extract_embedded_metric_definitions(
                    model_payload, endpoint=MSTR_DEFINITION_ENDPOINT_MODEL
                )
                if definitions and not any(d.has_expression for d in definitions):
                    self.report.report_report_definition_without_expressions()
                elif not definitions:
                    self.report.report_report_model_definition_empty()
            # An empty Modeling answer can still have derived metrics in v2.
            if not definitions:
                try:
                    v2_payload = self._report_v2_payload(project_id, report_id)
                except MicroStrategyAuthError:
                    raise
                except Exception as error:
                    v2_error = error
                else:
                    definitions = extract_embedded_metric_definitions(
                        v2_payload, endpoint=MSTR_DEFINITION_ENDPOINT_V2
                    )
                    if model_error is not None:
                        self._record_model_definition_failure(
                            project_id, report_id, model_error
                        )
        finally:
            if instance_id:
                self._delete_model_report_instance(project_id, report_id, instance_id)

        if definitions and self.config.extract_metric_expressions:
            self._resolve_embedded_metric_formulas(project_id, definitions)

        result: Optional[List[ReportDerivedMetric]] = definitions
        if model_error is not None and v2_error is not None:
            result = None
            self.report.report_report_definition_failure()
            self.report.warning(
                title="Report definition unavailable for derived metrics",
                message=(
                    "Neither the Modeling nor the v2 report definition endpoint "
                    "returned this dataset's report, so its derived metrics keep "
                    "only grid-level provenance (no report object name or "
                    "formula)."
                ),
                context=f"project_id={project_id}, report_id={report_id}",
                exc=v2_error,
                log=False,
            )
        elif definitions:
            self.report.report_report_derived_metrics_extracted(len(definitions))
        return result

    def _report_v2_payload(self, project_id: str, report_id: str) -> Dict[str, object]:
        # Failures are not cached; each caller reports them its own way.
        return self._report_v2_payload_cache.get_or_load(
            object_cache_key(project_id, report_id),
            lambda: self.client.get_report_definition(project_id, report_id),
        )

    def _resolve_embedded_metric_formulas(
        self,
        project_id: str,
        definitions: List[ReportDerivedMetric],
    ) -> None:
        """The report definition endpoints name a report-level derived metric
        (Modeling marks it isEmbedded, v2 lists it) but carry no formula. The
        metric has a real object id, so ask the metric model endpoint for it,
        exactly as catalog metrics are enriched; the fetch is cached per
        metric and a refusal is aggregated into the existing metric-model
        warning and failed_metric_model_ids, never fatal."""
        for definition in definitions:
            if definition.has_expression:
                continue
            model = self._get_metric_model(project_id, definition.id)
            if not model:
                continue
            enrichment = _metric_expression_summary(model)
            if enrichment is None:
                continue
            definition.expression_text = enrichment.expression_text
            definition.expression_tokens = enrichment.expression_tokens
            definition.endpoint = MSTR_DEFINITION_ENDPOINT_METRIC_MODEL
            self.report.report_report_derived_metric_model_resolved()

    @timed_phase(MicroStrategyPhase.REPORT_MODEL_INSTANCE)
    def _report_model_instance(
        self,
        project_id: str,
        report_id: str,
    ) -> Optional[str]:
        """Open a Modeling-service report instance so the Modeling definition
        can be read against it; metric expressions only resolve that way.
        Returns None when the option is off or the instance could not be
        created - the caller then reads the definition statically, which is
        the long-standing behaviour."""
        if not self.config.resolve_report_metrics_via_instance:
            return None
        try:
            instance_id = self.client.create_model_report_instance(
                project_id, report_id
            )
        except MicroStrategyAuthError:
            raise
        except Exception as error:
            self.report.report_report_model_instance_failure(
                f"{report_id}: {_error_summary(error)}"
            )
            return None
        self.report.report_report_model_instance_created()
        return instance_id

    def _record_model_definition_failure(
        self,
        project_id: str,
        report_id: str,
        error: Exception,
    ) -> None:
        """The Modeling report definition failed but the v2 definition
        answered: derived metrics keep their report names but have no
        formula. Counted per report (with the HTTP status when the failure
        was an error response) and warned once per project, since the cause
        (privilege, an older server without the Modeling service) is
        project-wide."""
        self.report.report_report_model_definition_failure(
            f"{report_id}: {_error_summary(error)}"
        )
        with self._cache_lock:
            if project_id in self._model_definition_warned_projects:
                return
            self._model_definition_warned_projects.add(project_id)
        self.report.warning(
            title="Modeling report definition unavailable; derived metric formulas omitted",
            message=(
                "GET /api/model/reports/{id} failed, so report derived metrics "
                "were taken from the v2 report definition, which names them "
                "but does not expose their formulas. Check the principal's "
                "Modeling service access and the MicroStrategy version (the "
                "endpoint needs 2021 Update 7 or later)."
            ),
            context=f"project_id={project_id}, first_report_id={report_id}",
            exc=error,
        )

    @timed_generator_phase(MicroStrategyPhase.REPORTS)
    def _process_project_reports(
        self,
        project: Project,
        lineage_context: "_LazyProjectLineage",
        linked_report_ids: Optional[Set[str]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        project_id = project.id
        # Scope reports to those referenced by ingested dashboards when linkage exists;
        # otherwise there's nothing to scope by, so all matching reports are ingested.
        scope_to_dashboards = (
            not self.config.extract_independent_reports
            and self.config.extract_dashboards
            and self.config.extract_dashboard_dependencies
            and linked_report_ids is not None
        )
        if scope_to_dashboards:
            assert linked_report_ids is not None
            if not linked_report_ids:
                # Enumerating a project's report library is expensive (thousands of
                # paginated searches); skip it when no dashboard referenced a report.
                self.report.report_reports_skipped_not_dashboard_linked()
                logger.info(
                    "Skipping report enumeration for project %s: no reports are "
                    "referenced by ingested dashboards",
                    project_id,
                )
                return
            yield from self._process_linked_reports(
                project,
                lineage_context,
                linked_report_ids,
            )
            return
        allowed_reports = self._evaluate_candidates_parallel(
            project,
            self._searched_reports(project_id),
            name_pattern=self.config.report_pattern,
            on_pattern_skip=self.report.report_filtered_report,
            on_folder_skip=self.report.report_report_filtered_by_folder_path,
        )
        yield from self._process_reports_parallel(
            project, allowed_reports, lineage_context
        )

    def _searched_reports(self, project_id: str) -> Iterator[MicroStrategyObject]:
        for report_object in self.client.search_reports(project_id):
            self._remember_searched_report(project_id, report_object)
            yield report_object

    def _remember_searched_report(
        self, project_id: str, report_object: MicroStrategyObject
    ) -> None:
        # A search result carries what an object lookup returns, so it replaces a failed one.
        cache_key = object_cache_key(project_id, report_object.id)
        if self._report_object_cache.get(cache_key) is None:
            self._report_object_cache.put(cache_key, report_object)

    def _process_linked_reports(
        self,
        project: Project,
        lineage_context: "_LazyProjectLineage",
        linked_report_ids: Set[str],
    ) -> Iterable[MetadataWorkUnit]:
        project_id = project.id

        # Dashboard-linked reports are fetched directly by id: enumerating the library
        # to find a handful means paginating tens of thousands of objects (minutes per
        # project) and was the largest exposure window to mid-run session loss.
        def iter_linked_objects() -> Iterator[MicroStrategyObject]:
            with ordered_parallel_map(
                lambda report_id: self._linked_report_object(project_id, report_id),
                sorted(linked_report_ids),
                max_workers=self.config.max_workers,
            ) as outcomes:
                for outcome in outcomes:
                    report_object = outcome.unwrap()
                    if report_object is not None:
                        yield report_object

        allowed_reports = self._evaluate_candidates_parallel(
            project,
            iter_linked_objects(),
            name_pattern=self.config.report_pattern,
            on_pattern_skip=self.report.report_filtered_report,
            on_folder_skip=self.report.report_report_filtered_by_folder_path,
        )
        yield from self._process_reports_parallel(
            project, allowed_reports, lineage_context
        )

    def _process_reports_parallel(
        self,
        project: Project,
        report_objects: Iterable[MicroStrategyObject],
        lineage_context: "_LazyProjectLineage",
    ) -> Iterable[MetadataWorkUnit]:
        def iter_reports_to_process() -> Iterator[MicroStrategyObject]:
            for report_object in report_objects:
                if self._report_plan is not None and not (
                    self._report_plan.read_in_report_pass(
                        normalize_object_id(report_object.id)
                    )
                ):
                    continue
                yield report_object

        with ordered_parallel_map(
            lambda report_object: self._prefetch_report(
                project, report_object, lineage_context
            ),
            iter_reports_to_process(),
            max_workers=self.config.max_workers,
        ) as outcomes:
            for outcome in outcomes:
                yield from self._consume_prefetched_report_with_boundary(
                    project, outcome, lineage_context
                )

    def _prefetch_report(
        self,
        project: Project,
        report_object: MicroStrategyObject,
        lineage_context: "_LazyProjectLineage",
    ) -> _PrefetchedReport:
        project_id = project.id
        change_check: Optional[ChangeCheckResult] = None
        if not self._per_report and self._skips_unchanged():
            change_check = self._evaluate_report_remote(project, report_object)
            if change_check.unchanged and change_check.previous is not None:
                return _PrefetchedReport(
                    report_object=report_object,
                    change_check=change_check,
                )

        report_definition = self._get_report_definition(project_id, report_object)
        if self._per_report:
            available_objects = (
                report_definition.available_objects if report_definition else {}
            )
            preview_dataset = DatasetObject.model_validate(
                {
                    "id": report_object.id,
                    "name": report_object.name,
                    "availableObjects": available_objects,
                }
            )
            if self.config.extract_metric_expressions:
                self._enrich_dataset_metric_expressions(project_id, preview_dataset)
            warehouse_context = lineage_context.warehouse_context
            sql_fetch_skipped = False
            if (
                self.config.extract_report_sql_lineage
                and warehouse_context
                and not self._report_lineage_resolved(project_id, report_object.id)
            ):
                sql_fetch_skipped = self._prefetch_report_sql(
                    project_id,
                    report_object.id,
                    context=(f"project_id={project_id}, report_id={report_object.id}"),
                ).skipped
            # Ensure model_lineage_index is warmed once if enabled.
            _ = lineage_context.model_lineage_index
            return _PrefetchedReport(
                report_object=report_object,
                report_definition=report_definition,
                source_dataset=preview_dataset,
                sql_fetch_skipped=sql_fetch_skipped,
            )

        source_dataset = self._report_source_dataset(report_object, report_definition)
        dataset_object: Optional[MicroStrategyObject] = None
        sql_fetch_skipped = False
        if source_dataset is not None:
            if self.config.extract_metric_expressions:
                self._enrich_dataset_metric_expressions(project_id, source_dataset)
            warehouse_context = lineage_context.warehouse_context
            if (
                self.config.extract_report_sql_lineage
                and warehouse_context
                and not self._report_lineage_resolved(project_id, report_object.id)
            ):
                sql_fetch_skipped = self._prefetch_report_sql(
                    project_id,
                    report_object.id,
                    context=(f"project_id={project_id}, report_id={report_object.id}"),
                ).skipped
            _ = lineage_context.model_lineage_index
            if report_definition and report_definition.source_id:
                dataset_object = self._dataset_object_info(
                    project_id, report_definition.source_id
                )
        return _PrefetchedReport(
            report_object=report_object,
            change_check=change_check,
            report_definition=report_definition,
            source_dataset=source_dataset,
            dataset_object=dataset_object,
            sql_fetch_skipped=sql_fetch_skipped,
        )

    def _consume_prefetched_report_with_boundary(
        self,
        project: Project,
        outcome: TaskOutcome[MicroStrategyObject, _PrefetchedReport],
        lineage_context: "_LazyProjectLineage",
    ) -> Iterable[MetadataWorkUnit]:
        project_id = project.id
        report_object = outcome.item
        # Progress before the expensive per-report work so the log never goes silent.
        logger.info(
            "Processing report %r (%s) in project %s",
            report_object.name,
            report_object.id,
            project_id,
        )
        checkpoint: Optional[ObjectCheckpoint] = None
        if not self._per_report:
            self._register_usage_target(
                report_object.id,
                USAGE_TARGET_CHART,
                self.mapper.report_urn(project_id, report_object.id),
            )
            if self._records_checkpoints():
                checkpoint = ObjectCheckpoint()
        try:
            prefetched = outcome.unwrap()
            if prefetched.change_check is not None:
                self._note_change_check(prefetched.change_check.failure)
                if (
                    prefetched.change_check.unchanged
                    and prefetched.change_check.previous is not None
                ):
                    self._keep_report(
                        project_id, report_object.id, prefetched.change_check.previous
                    )
                    self.report.report_report_skipped_unchanged()
                    return
            yield from _recording_urns(
                self._emit_prefetched_report_object(
                    project,
                    prefetched,
                    lineage_context,
                    checkpoint,
                ),
                checkpoint,
            )
        except (MicroStrategyAuthError, ConfigurationError):
            raise
        except Exception as error:
            self.report.warning(
                title="Failed to Process Report",
                message=(
                    "Skipping report after an unexpected error; other reports "
                    "will still be processed."
                ),
                context=(
                    f"project_id={project_id}, report_id={report_object.id}, "
                    f"report_name={report_object.name}"
                ),
                exc=error,
            )
            if checkpoint is not None:
                checkpoint.fingerprint = None
        if checkpoint is None:
            return
        if checkpoint.fingerprint is None:
            previous = self._object_state.previous_report(project_id, report_object.id)
            if previous is not None:
                self._keep_report(project_id, report_object.id, previous)
            return
        checkpoint.urns = self._tracked_urns(checkpoint.urns)
        self._object_state.record_report(project_id, report_object.id, checkpoint)

    def _emit_prefetched_report_object(
        self,
        project: Project,
        prefetched: _PrefetchedReport,
        lineage_context: "_LazyProjectLineage",
        checkpoint: Optional[ObjectCheckpoint] = None,
    ) -> Iterable[MetadataWorkUnit]:
        project_id = project.id
        report_object = prefetched.report_object
        if self._per_report:
            self._process_report_as_dataset(
                project_id, report_object, lineage_context, prefetched=prefetched
            )
            return

        predefined_folders = self._predefined_folders(project_id)
        yield from self._folder_containers(
            project_id, report_object, predefined_folders, checkpoint
        )
        parent_key = self.mapper.folder_container_for_dashboard(
            project_id,
            report_object,
            predefined_folders,
        )
        report_definition = prefetched.report_definition
        source_dataset = prefetched.source_dataset
        if source_dataset is not None:
            warehouse_context = lineage_context.warehouse_context
            if (
                self.config.extract_report_sql_lineage
                and warehouse_context
                and not prefetched.sql_fetch_skipped
            ):
                self._enrich_report_sql_lineage(
                    project_id=project_id,
                    report_object=report_object,
                    dataset=source_dataset,
                    context=warehouse_context,
                )
            model_lineage_index = lineage_context.model_lineage_index
            if model_lineage_index:
                self.mapper.attach_dataset_model_lineage(
                    source_dataset,
                    model_lineage_index,
                )
            dataset_object = prefetched.dataset_object
            folder_object = self._folder_object(project_id, dataset_object)
            if folder_object is not None:
                yield from self._folder_containers(
                    project_id, folder_object, predefined_folders, checkpoint
                )
            yield from self.mapper.gen_report_source_dataset_workunits(
                project_id,
                report_object,
                source_dataset,
                self.mapper.dataset_folder_parent_key(
                    project_id, folder_object, parent_key, predefined_folders
                ),
                dataset_object=dataset_object,
            )
        if checkpoint is not None:
            dependency_ids = (
                [report_definition.source_id]
                if report_definition and report_definition.source_id
                else []
            )
            checkpoint.dependency_report_ids = dependency_ids
            checkpoint.usage_targets = [
                RecordedUsageTarget(
                    object_id=report_object.id,
                    kind=USAGE_TARGET_CHART,
                    urn=self.mapper.report_urn(project_id, report_object.id),
                )
            ]
            checkpoint.referenced_object_ids = (
                sorted(source_dataset.normalized_object_ids())
                if source_dataset is not None
                else []
            )
            checkpoint.fingerprint = self._change_detector.object_fingerprint(
                project_id,
                report_object,
                self.mapper.folder_path(
                    project.name, report_object, predefined_folders
                ),
                dependency_ids,
                self._dataset_object_info,
            )
        yield from self.mapper.gen_report_workunits(
            project_id,
            report_object,
            report_definition,
            source_dataset,
            parent_key,
            fingerprint=_checkpoint_fingerprint(checkpoint),
        )

    def _process_report_as_dataset(
        self,
        project_id: str,
        report_object: MicroStrategyObject,
        lineage_context: "_LazyProjectLineage",
        *,
        prefetched: Optional[_PrefetchedReport] = None,
    ) -> None:
        # No Report chart: it was a second copy of the report.
        self.report.report_report_scanned()
        report_definition = (
            prefetched.report_definition
            if prefetched is not None
            else self._get_report_definition(project_id, report_object)
        )
        entry = self._register_report_dataset(
            project_id, report_object, report_definition
        )
        dataset = entry.dataset
        if prefetched is not None:
            if (
                prefetched.source_dataset is not None
                and prefetched.source_dataset.metric_enrichments
            ):
                dataset.metric_enrichments.update(
                    prefetched.source_dataset.metric_enrichments
                )
        elif self.config.extract_metric_expressions:
            self._enrich_dataset_metric_expressions(project_id, dataset)
        warehouse_context = lineage_context.warehouse_context
        sql_skipped = prefetched.sql_fetch_skipped if prefetched is not None else False
        if (
            self.config.extract_report_sql_lineage
            and warehouse_context
            and not sql_skipped
        ):
            self._enrich_report_sql_lineage(
                project_id=project_id,
                report_object=report_object,
                dataset=dataset,
                context=warehouse_context,
            )
        model_lineage_index = lineage_context.model_lineage_index
        if model_lineage_index:
            self.mapper.attach_dataset_model_lineage(dataset, model_lineage_index)

    @timed_phase(MicroStrategyPhase.DASHBOARD_DEFINITION)
    def _get_dashboard_definition(
        self,
        project_id: str,
        dashboard_object: MicroStrategyObject,
        *,
        bind_per_report: bool = True,
    ) -> Optional[DashboardDefinition]:
        dashboard_id = dashboard_object.id
        # Dossiers and documents share a subtype; viewMedia tells them apart
        # when the search result carries it. Without it, try the dossier
        # endpoint first: the document endpoint also answers for dossiers but
        # without their chapters, pages and visualizations.
        fetches: List[Tuple[str, Callable[[], Dict[str, object]]]] = [
            (
                "dossier",
                lambda: self.client.get_dossier_definition(project_id, dashboard_id),
            ),
            (
                "document",
                lambda: self.client.get_document_definition(project_id, dashboard_id),
            ),
        ]
        if dashboard_object.view_media is not None and is_document_object(
            dashboard_object
        ):
            fetches.reverse()
        errors: List[str] = []
        response: Optional[Dict[str, object]] = None
        for kind, fetch in fetches:
            try:
                response = fetch()
                break
            except MicroStrategyAPIError as error:
                errors.append(f"{kind}: {error}")
        if response is None:
            self.report.warning(
                title="Failed to Fetch Dashboard Definition",
                message="Skipping dashboard because definition APIs failed.",
                context=(
                    f"project_id={project_id}, dashboard_id={dashboard_id}, "
                    f"errors={errors}"
                ),
            )
            return None

        dashboard = DashboardDefinition.from_api_response(
            object_id=dashboard_id,
            object_name=dashboard_object.name,
            description=dashboard_object.description,
            response=response,
            report=self.report,
        )
        if self._per_report and bind_per_report:
            # Before enrichment, so every dashboard writes onto the one shared instance.
            dashboard.datasets = [
                self._bind_report_dataset(project_id, dashboard_object, dataset)
                for dataset in dashboard.datasets
            ]
        if is_document_object(dashboard_object) and not dashboard.visualizations:
            self.report.report_document_definition_without_visualizations()
        if self.config.extract_lineage and self.config.extract_visualization_details:
            self._enrich_visualization_details(project_id, dashboard_object, dashboard)
        if self.config.extract_lineage:
            self._resolve_visualization_bindings(project_id, dashboard)
        if self.config.extract_dashboard_dependencies:
            self._enrich_dashboard_dependencies(
                project_id=project_id,
                dashboard_object=dashboard_object,
                dashboard=dashboard,
            )
        if self.config.extract_metric_expressions:
            self._enrich_metric_expressions(project_id, dashboard)
        return dashboard

    @timed_phase(MicroStrategyPhase.MODEL_DOCUMENT)
    def _model_document(
        self, project_id: str, dashboard_id: str
    ) -> Optional[Dict[str, object]]:
        """GET /api/model/documents/{id}, read at most once per dashboard. None
        once it has failed in the project: Modeling access is project-scoped,
        so the failure is reported once rather than for every dashboard."""
        key = (project_id, dashboard_id)
        with self._cache_lock:
            if key in self._model_documents_in_flight:
                return self._model_documents_in_flight[key]
            if project_id in self._model_document_unavailable_projects:
                return None

        def fetch() -> Optional[Dict[str, object]]:
            with self._cache_lock:
                if key in self._model_documents_in_flight:
                    return self._model_documents_in_flight[key]
                if project_id in self._model_document_unavailable_projects:
                    return None
            model_document: Optional[Dict[str, object]] = None
            try:
                model_document = self.client.get_model_document(
                    project_id, dashboard_id
                )
            except MicroStrategyAuthError:
                raise
            except Exception as error:
                self.report.report_document_model_definition_failure()
                # Missing privilege or an unsupported endpoint applies to the whole
                # project; other failures (the endpoint answers 500 for some
                # documents) only affect this dashboard.
                project_wide = (
                    isinstance(error, MicroStrategyAPIError)
                    and error.status_code in _MODEL_DOCUMENT_PROJECT_WIDE_STATUSES
                )
                if project_wide:
                    with self._cache_lock:
                        self._model_document_unavailable_projects.add(project_id)
                self.report.warning(
                    title="Modeling document API unavailable",
                    message=(
                        "GET /api/model/documents/{id} failed, so visualizations are "
                        "bound to datasets by object and name inference only, and "
                        "document-level derived metrics are not read"
                        + (" for the rest of this project." if project_wide else ".")
                    ),
                    context=f"project_id={project_id}, dashboard_id={dashboard_id}",
                    exc=error,
                )
            with self._cache_lock:
                self._model_documents_in_flight[key] = model_document
            return model_document

        return self._model_document_flight.do(key, fetch)

    def _release_model_document(self, project_id: str, dashboard_id: str) -> None:
        with self._cache_lock:
            self._model_documents_in_flight.pop((project_id, dashboard_id), None)

    @timed_phase(MicroStrategyPhase.VISUALIZATION_BINDINGS)
    def _resolve_visualization_bindings(
        self,
        project_id: str,
        dashboard: DashboardDefinition,
    ) -> None:
        """Bind visualizations to datasets via dataset-scoped derived objects: the
        definition APIs omit which dataset a visualization reads, but an object defined
        in exactly one dataset pins the visualization to that dataset."""
        unbound = [
            visualization
            for visualization in dashboard.visualizations
            if not visualization.datasets and visualization.object_ids
        ]
        if len(dashboard.datasets) < 2 or not unbound:
            return
        model_document = self._model_document(project_id, dashboard.id)
        if model_document is None:
            return

        owner_by_derived_id = unique_derived_object_owners(model_document)
        if not owner_by_derived_id:
            return
        bound = bind_visualizations_by_derived_objects(dashboard, owner_by_derived_id)
        if bound:
            self.report.report_visualizations_bound_by_derived_objects(bound)

    @timed_phase(MicroStrategyPhase.REPORT_DEFINITION)
    def _get_report_definition(
        self,
        project_id: str,
        report_object: MicroStrategyObject,
    ) -> Optional[ReportDefinition]:
        if not self.config.extract_report_definitions:
            return ReportDefinition.from_search_result(report_object)

        # Last reader, so a miss is fetched without being cached.
        response = self._report_v2_payload_cache.get(
            object_cache_key(project_id, report_object.id)
        )
        try:
            if response is None:
                response = self.client.get_report_definition(
                    project_id, report_object.id
                )
        except MicroStrategyAPIError as error:
            self.report.report_report_definition_api_failure()
            self.report.warning(
                title="Failed to Fetch Report Definition",
                message=(
                    "Continuing with report search metadata only. Report source "
                    "dataset fields may be unavailable."
                ),
                context=f"project_id={project_id}, report_id={report_object.id}",
                exc=error,
            )
            return ReportDefinition.from_search_result(report_object)

        return ReportDefinition.from_api_response(
            object_id=report_object.id,
            object_name=report_object.name,
            description=report_object.description,
            response=response,
        )

    @staticmethod
    def _report_source_dataset(
        report_object: MicroStrategyObject,
        report_definition: Optional[ReportDefinition],
    ) -> Optional[DatasetObject]:
        if report_definition is None:
            return None
        if not report_definition.source_id and not report_definition.available_objects:
            return None
        return DatasetObject.model_validate(
            {
                "id": report_definition.source_id or "source",
                "name": report_definition.source_name or f"{report_object.name} Source",
                "description": report_definition.description,
                "availableObjects": report_definition.available_objects,
            }
        )

    @timed_phase(MicroStrategyPhase.MODEL_TABLES)
    def _fetch_project_model_tables(
        self, project_id: str, include_datasource: bool = False
    ) -> Optional[List[Dict[str, object]]]:
        """Paginate /api/model/tables for a project.

        Returns None on API failure (already reported/warned); an empty list is
        a genuine "no model tables" result, distinct from a failed fetch.
        Shared by the classic per-report model-lineage path and the
        project-level semantic-model path, so it is fetched at most once per
        project regardless of how many consumers need it.
        """
        model_tables: List[Dict[str, object]] = []
        offset = 0
        fields = (
            f"{MSTR_MODEL_TABLE_FIELDS},{MSTR_MODEL_TABLE_DATASOURCE_FIELD}"
            if include_datasource
            else MSTR_MODEL_TABLE_FIELDS
        )
        while True:
            try:
                response = self.client.list_model_tables(
                    project_id,
                    limit=self.config.page_size,
                    offset=offset,
                    fields=fields,
                )
            except MicroStrategyAPIError as error:
                self.report.report_model_lineage_api_failure()
                self.report.warning(
                    title="MicroStrategy Model Lineage Unavailable",
                    message=(
                        "Logical table lineage requires model table access. "
                        "SQL-view physical warehouse lineage may still be emitted "
                        "when dashboard SQL-view APIs are available."
                    ),
                    context=f"project_id={project_id}",
                    exc=error,
                )
                return None

            tables = response.tables
            if tables is None:
                # [] would let stale removal soft-delete the semantic model.
                self.report.warning(
                    title="Unrecognized model tables response shape",
                    message=(
                        "The model tables API returned a payload without a "
                        "'tables' list; model lineage and the semantic model are "
                        "skipped for this project."
                    ),
                    context=f"project_id={project_id}, offset={offset}",
                )
                return None
            model_tables.extend(table for table in tables if isinstance(table, dict))
            total = response.total
            offset += len(tables)
            if not tables:
                break
            # Large projects paginate thousands of model tables; log progress so
            # the lineage-index build does not look hung.
            logger.info(
                "Model tables progress: %d/%s tables (project=%s)",
                offset,
                total if isinstance(total, int) else "?",
                project_id,
            )
            if isinstance(total, int):
                if offset >= total:
                    break
            elif len(tables) < self.config.page_size:
                # Without a total, a short page is the only end-of-results signal.
                break

        self.report.report_model_tables_scanned(len(model_tables))
        return model_tables

    @timed_phase(MicroStrategyPhase.MODEL_LINEAGE_INDEX)
    def _get_project_model_lineage_index(
        self,
        project_id: str,
        warehouse_context: Optional[WarehouseLineageContext],
        model_tables_provider: Callable[[], Optional[List[Dict[str, object]]]],
        source_warehouses: Optional[List[Datasource]] = None,
        table_context_resolver: Optional[TableContextResolver] = None,
    ) -> Optional[ModelLineageIndex]:
        if not self.config.extract_lineage or not self.config.extract_model_lineage:
            return None
        if warehouse_context is None and table_context_resolver is None:
            self.report.warning(
                title="MicroStrategy Model Lineage Unavailable",
                message=(
                    "Skipping logical table lineage because no supported source "
                    "warehouse context was discovered for this project. "
                    f"{MSTR_DATASOURCE_MAPPING_HINT}"
                ),
                context=(
                    f"project_id={project_id}, "
                    f"{self._source_platform_summary(source_warehouses)}"
                ),
            )
            return None

        # Deferred until here (not called unconditionally by the caller) so the
        # fetch stays skipped when these flags are off, exactly as before.
        model_tables = model_tables_provider()
        if not model_tables:
            return None
        return self.lineage.model_lineage_index_from_tables(
            model_tables,
            warehouse_context,
            graph=self.ctx.graph,
            table_context_resolver=table_context_resolver,
        )

    @timed_phase(MicroStrategyPhase.DASHBOARD_DEPENDENCIES)
    def _enrich_dashboard_dependencies(
        self,
        project_id: str,
        dashboard_object: MicroStrategyObject,
        dashboard: DashboardDefinition,
    ) -> None:
        object_type = dashboard_object.type
        if not object_type:
            return
        try:
            dependencies = self.client.get_object_dependencies(
                project_id=project_id,
                object_id=dashboard.id,
                object_type=object_type,
            )
        except MicroStrategyAPIError as error:
            self.report.warning(
                title="Failed to Fetch Dashboard Dependencies",
                message=(
                    "Skipping direct dashboard component metadata from metadata search."
                ),
                context=f"project_id={project_id}, dashboard_id={dashboard.id}",
                exc=error,
            )
            return
        dashboard.dependencies = dependencies
        self.report.report_dashboard_dependencies_scanned(len(dependencies))

    @timed_phase(MicroStrategyPhase.METRIC_EXPRESSIONS)
    def _enrich_metric_expressions(
        self,
        project_id: str,
        dashboard: DashboardDefinition,
    ) -> None:
        for dataset in dashboard.datasets:
            self._enrich_dataset_metric_expressions(project_id, dataset)

    def _enrich_dataset_metric_expressions(
        self,
        project_id: str,
        dataset: DatasetObject,
    ) -> None:
        for metric in _metric_items(dataset.available_objects):
            metric_id = str(metric.get("id") or "")
            if not metric_id:
                continue
            model = self._get_metric_model(project_id, metric_id)
            if not model:
                continue
            enrichment = _metric_expression_summary(model)
            fact_ids = self._metric_model_fact_ids(
                project_id,
                model,
                visited={metric_id.upper()},
            )
            if enrichment is None and not fact_ids:
                continue
            enrichment = enrichment or MetricEnrichment()
            enrichment.fact_ids = fact_ids
            dataset.metric_enrichments[metric_id.strip().upper()] = enrichment

    @timed_phase(MicroStrategyPhase.METRIC_MODEL_FETCH)
    def _get_metric_model(
        self,
        project_id: str,
        metric_id: str,
    ) -> Dict[str, object]:
        model = self._metric_model_cache.get_or_load(
            object_cache_key(project_id, metric_id),
            lambda: self._fetch_metric_model(project_id, metric_id),
        )
        return model if model is not None else {}

    def _fetch_metric_model(
        self, project_id: str, metric_id: str
    ) -> Optional[Dict[str, object]]:
        # None marks a failed fetch, already warned and counted.
        try:
            model = self.client.get_metric_model(project_id, metric_id)
        except MicroStrategyAPIError as error:
            self.report.report_metric_expression_api_failure()
            self.report.report_failed_metric_model(
                f"{metric_id} (HTTP {getattr(error, 'status_code', None)})"
            )
            # Aggregated under one title (log=False avoids one line per metric) so
            # operators see that some metric models were skipped -- most often
            # because the metric lives in another project or the principal lacks
            # modeling access to it.
            self.report.warning(
                title="MicroStrategy metric model unavailable",
                message=(
                    "Could not fetch a metric model, so expression metadata and "
                    "fact-based lineage were skipped for it. The metric may belong "
                    "to another project or the ingestion principal may lack "
                    "modeling access."
                ),
                context=f"project={project_id}, metric={metric_id}",
                log=False,
            )
            logger.debug(
                "Failed to fetch MicroStrategy metric model %s",
                metric_id,
                exc_info=True,
            )
            return None
        self.report.report_metric_expression_scanned()
        return model

    def _metric_model_fact_ids(
        self,
        project_id: str,
        model: Dict[str, object],
        visited: Set[str],
    ) -> List[str]:
        fact_ids = set(metric_fact_ids_from_model(model))
        for nested_metric_id in metric_metric_ids_from_model(model):
            if nested_metric_id in visited:
                continue
            visited.add(nested_metric_id)
            nested_model = self._get_metric_model(project_id, nested_metric_id)
            if nested_model:
                fact_ids.update(
                    self._metric_model_fact_ids(
                        project_id,
                        nested_model,
                        visited,
                    )
                )
        return sorted(fact_ids)

    @timed_phase(MicroStrategyPhase.VISUALIZATION_DETAILS)
    def _enrich_visualization_details(
        self,
        project_id: str,
        dashboard_object: MicroStrategyObject,
        dashboard: DashboardDefinition,
    ) -> None:
        if not dashboard.visualizations:
            return
        if self._execution_limit_reached(project_id):
            return
        try:
            instance_id = self._dashboard_instance(project_id, dashboard_object)
        except MicroStrategyAPIError as error:
            self.report.warning(
                title="Failed to Create Dashboard Instance",
                message=(
                    "Skipping runtime visualization detail extraction for dashboard "
                    "lineage."
                ),
                context=f"project_id={project_id}, dashboard_id={dashboard.id}",
                exc=error,
            )
            return

        def fetch_one(visualization: Visualization) -> Visualization:
            if not visualization.chapter_key:
                return visualization
            try:
                with (
                    self._http_io_gate,
                    self.report.phase(
                        MicroStrategyPhase.VISUALIZATION_DETAILS, count=False
                    ),
                ):
                    detail = self.client.get_dossier_visualization(
                        project_id=project_id,
                        dossier_id=dashboard.id,
                        instance_id=instance_id,
                        chapter_key=visualization.chapter_key,
                        visualization_key=visualization.key,
                    )
            except MicroStrategyAPIError as error:
                self.report.warning(
                    title="Failed to Fetch Visualization Definition",
                    message=(
                        "Keeping visualization without runtime lineage enrichment."
                    ),
                    context=(
                        f"project_id={project_id}, dashboard_id={dashboard.id}, "
                        f"visualization_key={visualization.key}"
                    ),
                    exc=error,
                )
                return visualization

            try:
                return Visualization.model_validate(
                    {
                        **visualization.raw,
                        "chapterKey": visualization.chapter_key,
                        "pageKey": visualization.page_key,
                        "pageName": visualization.page_name,
                        "runtimeDefinition": detail,
                    }
                )
            except ValidationError as error:
                self.report.report_malformed_object(
                    f"visualization runtime detail key={visualization.key}"
                )
                self.report.warning(
                    title="Skipped malformed visualization runtime detail",
                    message=(
                        "Keeping the visualization without runtime lineage enrichment."
                    ),
                    context=(
                        f"project_id={project_id}, dashboard_id={dashboard.id}, "
                        f"visualization_key={visualization.key}"
                    ),
                    exc=error,
                )
                return visualization

        chaptered_count = sum(1 for v in dashboard.visualizations if v.chapter_key)
        workers = self.config.max_workers if chaptered_count > 1 else 1
        enriched: List[Visualization] = []
        with ordered_parallel_map(
            fetch_one,
            dashboard.visualizations,
            max_workers=workers,
        ) as outcomes:
            for outcome in outcomes:
                enriched.append(outcome.unwrap())
        dashboard.visualizations = enriched

    @timed_phase(MicroStrategyPhase.WAREHOUSE_LINEAGE)
    def _fetch_dossier_sql_view_rows(
        self,
        project_id: str,
        dashboard_object: MicroStrategyObject,
        dashboard_id: str,
    ) -> Optional[List[Dict[str, object]]]:
        if self._execution_limit_reached(project_id):
            return None
        try:
            instance_id = self._dashboard_instance(project_id, dashboard_object)
        except MicroStrategyAPIError as error:
            self.report.report_warehouse_lineage_api_failure()
            self.report.warning(
                title="Failed to Create Dashboard Instance for Warehouse Lineage",
                message="Skipping SQL-view warehouse lineage for this dashboard.",
                context=f"project_id={project_id}, dashboard_id={dashboard_id}",
                exc=error,
            )
            return None

        try:
            sql_view_rows = self.client.get_dossier_datasets_sql(
                project_id=project_id,
                dossier_id=dashboard_id,
                instance_id=instance_id,
            )
        except MicroStrategyAPIError as error:
            self.report.report_warehouse_lineage_api_failure()
            self.report.warning(
                title="Failed to Fetch Dataset SQL View",
                message="Skipping source warehouse lineage for this dashboard.",
                context=f"project_id={project_id}, dashboard_id={dashboard_id}",
                exc=error,
            )
            return None

        self.report.report_warehouse_lineage_sql_views_scanned(len(sql_view_rows))
        return sql_view_rows

    def _enrich_warehouse_lineage(
        self,
        project_id: str,
        dashboard_object: MicroStrategyObject,
        dashboard: DashboardDefinition,
        context: Optional[WarehouseLineageContext],
    ) -> None:
        if not dashboard.datasets:
            return
        if is_document_object(dashboard_object):
            if self.config.extract_document_dataset_sql_lineage:
                self._enrich_document_warehouse_lineage(project_id, dashboard, context)
            return
        sql_view_rows = self._fetch_dossier_sql_view_rows(
            project_id, dashboard_object, dashboard.id
        )
        if sql_view_rows is None:
            return
        with self.report.phase(MicroStrategyPhase.WAREHOUSE_LINEAGE, count=False):
            self._attach_dataset_warehouse_upstreams(
                sql_view_rows, dashboard, context, project_id=project_id
            )

    def _prefetch_report_sql(
        self,
        project_id: str,
        report_id: str,
        context: str,
    ) -> _ReportSqlFetchResult:
        cache_key = object_cache_key(project_id, report_id)
        cached = self._report_sql_cache.get_entry(cache_key)
        if cached.found:
            return _ReportSqlFetchResult(sql=cached.value, skipped=False)

        def fetch() -> _ReportSqlFetchResult:
            existing = self._report_sql_cache.get_entry(cache_key)
            if existing.found:
                return _ReportSqlFetchResult(sql=existing.value, skipped=False)
            if self._execution_limit_reached(project_id):
                return _ReportSqlFetchResult(sql=None, skipped=True)
            sql_statement = self._report_sql_statement(
                project_id,
                report_id,
                context=context,
            )
            self._report_sql_cache.put(cache_key, sql_statement)
            return _ReportSqlFetchResult(sql=sql_statement, skipped=False)

        return self._report_sql_flight.do(cache_key, fetch)

    def _prefetch_document_warehouse_sql(
        self,
        project_id: str,
        dashboard: DashboardDefinition,
        context: Optional[WarehouseLineageContext],
    ) -> Set[str]:
        skipped_keys: Set[str] = set()
        with self.report.phase(
            MicroStrategyPhase.DOCUMENT_WAREHOUSE_LINEAGE, count=False
        ):
            for dataset in dashboard.datasets:
                dataset_object = self._dataset_object_info(project_id, dataset.id)
                subtype = (
                    (dataset_object.subtype or "").strip() if dataset_object else ""
                )
                if subtype != MSTR_OBJECT_SUBTYPE_REPORT_GRID:
                    continue
                if self._dataset_warehouse_context(dataset, context) is None:
                    continue
                if self._report_lineage_resolved(project_id, dataset.id):
                    continue
                cache_key = object_cache_key(project_id, dataset.id)
                fetched = self._prefetch_report_sql(
                    project_id,
                    dataset.id,
                    context=(
                        f"project_id={project_id}, document_id={dashboard.id}, "
                        f"dataset_id={dataset.id}"
                    ),
                )
                if fetched.skipped:
                    skipped_keys.add(cache_key)
        return skipped_keys

    @timed_phase(MicroStrategyPhase.DOCUMENT_WAREHOUSE_LINEAGE)
    def _enrich_document_warehouse_lineage(
        self,
        project_id: str,
        dashboard: DashboardDefinition,
        context: Optional[WarehouseLineageContext],
        *,
        skipped_keys: Optional[Set[str]] = None,
    ) -> None:
        """Warehouse lineage for a Report Services document's datasets. They
        are grid reports, so each one's SQL is read through its own report
        instance (the create / sqlView / delete sequence of
        extract_report_sql_lineage, one execution per distinct report per
        run) and attached to the document-scoped dataset. Cubes and datasets
        of unknown type have no report SQL view and are skipped and counted."""
        for dataset in dashboard.datasets:
            dataset_object = self._dataset_object_info(project_id, dataset.id)
            subtype = (dataset_object.subtype or "").strip() if dataset_object else ""
            if subtype != MSTR_OBJECT_SUBTYPE_REPORT_GRID:
                self.report.report_document_dataset_skipped_not_grid_report()
                logger.debug(
                    "Document dataset %s (subtype=%r) is not a grid report; "
                    "no report SQL view to read for document %s",
                    dataset.id,
                    subtype,
                    dashboard.id,
                )
                continue
            dataset_context = self._dataset_warehouse_context(dataset, context)
            if dataset_context is None:
                self.report.report_sql_view_row_without_context()
                continue
            if self._report_lineage_resolved(project_id, dataset.id):
                # Lineage already settled by an earlier dashboard.
                self.report.report_document_dataset_sql_view_reused()
                continue
            cache_key = object_cache_key(project_id, dataset.id)
            if skipped_keys is not None and cache_key in skipped_keys:
                continue
            fetched = self._prefetch_report_sql(
                project_id,
                dataset.id,
                context=(
                    f"project_id={project_id}, document_id={dashboard.id}, "
                    f"dataset_id={dataset.id}"
                ),
            )
            if fetched.skipped:
                continue
            sql_statement = fetched.sql
            with self._report_sql_lock:
                already_consumed = cache_key in self._report_sql_consumed
                self._report_sql_consumed.add(cache_key)
            if already_consumed:
                self.report.report_document_dataset_sql_view_reused()
            elif sql_statement is None:
                self.report.report_document_dataset_sql_view_failure()
            if not sql_statement:
                continue
            self.report.report_document_dataset_sql_view_scanned()
            self.report.report_warehouse_lineage_sql_views_scanned(1)
            lineage = self._attach_sql_view_lineage(
                sql_statement, dataset, dataset_context, project_id=project_id
            )
            if lineage.upstream_urns:
                self.report.report_document_dataset_sql_view_parsed()

    def _dataset_warehouse_context(
        self,
        dataset: DatasetObject,
        context: Optional[WarehouseLineageContext],
    ) -> Optional[WarehouseLineageContext]:
        """The dataset's own source warehouse when its definition names one,
        else the project-level context. A named warehouse that does not map
        to a platform gives None: the project's warehouse may be a different
        platform and database, and its URNs would not be this dataset's."""
        if dataset.source_warehouse:
            return warehouse_context_from_datasource(
                dataset.source_warehouse,
                self.config.env,
                self.config.datasource_platform_mapping,
            )
        return context

    def _attach_dataset_warehouse_upstreams(
        self,
        sql_view_rows: List[Dict[str, object]],
        dashboard: DashboardDefinition,
        context: Optional[WarehouseLineageContext],
        project_id: Optional[str] = None,
    ) -> None:
        dataset_by_id = {
            normalize_object_id(dataset.id): dataset for dataset in dashboard.datasets
        }
        dataset_by_name = {
            dataset.name.strip().lower(): dataset for dataset in dashboard.datasets
        }
        for row in sql_view_rows:
            dataset = None
            dataset_key = sql_view_dataset_key(row)
            if dataset_key:
                dataset = dataset_by_id.get(normalize_object_id(dataset_key))
            if dataset is None:
                dataset_name = sql_view_dataset_name(row)
                if dataset_name:
                    dataset = dataset_by_name.get(dataset_name.strip().lower())
            if dataset is None:
                self.report.report_sql_view_row_unmatched()
                logger.debug(
                    "SQL-view row could not be matched to a dashboard dataset: "
                    "key=%s, name=%s",
                    dataset_key,
                    sql_view_dataset_name(row),
                )
                continue

            dataset_context = self._dataset_warehouse_context(dataset, context)
            if dataset_context is None:
                self.report.report_sql_view_row_without_context()
                continue

            sql_statement = sql_statement_from_sql_view_entry(row)
            if not sql_statement.strip():
                self.report.report_sql_view_without_statement()
                continue

            self._attach_sql_view_lineage(
                sql_statement, dataset, dataset_context, project_id=project_id
            )

    def _attach_sql_view_lineage(
        self,
        sql_statement: str,
        dataset: DatasetObject,
        context: WarehouseLineageContext,
        project_id: Optional[str] = None,
    ) -> SqlViewLineage:
        # Callers are already gated by extract_report_sql_lineage /
        # extract_warehouse_lineage. A single parse yields both the table-level
        # upstreams and the column-level edges on top of them.
        entry = (
            self._report_dataset_entry(project_id, dataset.id) if project_id else None
        )
        if entry is not None and entry.lineage_resolved:
            return SqlViewLineage(
                upstream_urns=list(dataset.warehouse_upstream_urns),
                field_upstreams=dict(dataset.field_warehouse_upstreams),
            )
        result = self.lineage.warehouse_lineage_from_sql(
            sql_statement,
            context,
            self.mapper.dataset_field_paths(dataset),
            graph=self.ctx.graph,
            label=f"{dataset.name} ({dataset.id})",
        )
        if result.upstream_urns:
            dataset.warehouse_upstream_urns = result.upstream_urns
            if entry is not None and project_id:
                with self._cache_lock:
                    self._report_datasets[
                        _report_dataset_key(project_id, dataset.id)
                    ] = entry.model_copy(update={"lineage_resolved": True})
        if result.field_upstreams:
            dataset.field_warehouse_upstreams = result.field_upstreams
        return result

    def _enrich_report_sql_lineage(
        self,
        project_id: str,
        report_object: MicroStrategyObject,
        dataset: DatasetObject,
        context: WarehouseLineageContext,
    ) -> None:
        if self._report_lineage_resolved(project_id, report_object.id):
            # A dashboard already settled this report's lineage.
            return
        fetched = self._prefetch_report_sql(
            project_id,
            report_object.id,
            context=f"project_id={project_id}, report_id={report_object.id}",
        )
        if fetched.skipped or not fetched.sql:
            return

        self.report.report_warehouse_lineage_sql_views_scanned(1)
        self._attach_sql_view_lineage(
            fetched.sql, dataset, context, project_id=project_id
        )

    @timed_phase(MicroStrategyPhase.REPORT_SQL_FETCH)
    def _report_sql_statement(
        self,
        project_id: str,
        report_id: str,
        context: str,
    ) -> Optional[str]:
        """One report's SQL, read through a report execution instance that is
        deleted again afterwards. None when the instance could not be created
        or its SQL view not read (counted in report_sql_view_api_failures and
        warned with `context`); otherwise the statement, empty (and counted
        in sql_views_without_statement) when the view carried none. The
        instance creation is the call that may execute the report, and the
        one that counts towards max_consecutive_execution_timeouts. Callers
        memoize the result in _report_sql_cache."""
        try:
            instance_id = self.client.create_report_instance(project_id, report_id)
        except MicroStrategyAPIError as error:
            self._record_execution(project_id, error)
            self.report.report_report_sql_view_api_failure()
            self.report.warning(
                title="Failed to Create Report Instance for Warehouse Lineage",
                message="Skipping SQL-view warehouse lineage for this report.",
                context=context,
                exc=error,
            )
            return None

        self._record_execution(project_id, None)

        try:
            sql_view = self.client.get_report_sql_view(
                project_id=project_id,
                report_id=report_id,
                instance_id=instance_id,
            )
        except (MicroStrategyAPIError, ValidationError) as error:
            # ValidationError: a response body that does not fit SqlView.
            self.report.report_report_sql_view_api_failure()
            self.report.warning(
                title="Failed to Fetch Report SQL View",
                message="Skipping source warehouse lineage for this report.",
                context=context,
                exc=error,
            )
            return None
        finally:
            self._delete_report_instance(project_id, report_id, instance_id)

        sql_statement = sql_view.get_statement()
        self.report.report_report_sql_fetched(sql_statement)
        if not sql_statement:
            self.report.report_sql_view_without_statement()
        return sql_statement

    @timed_phase(MicroStrategyPhase.DASHBOARD_INSTANCE)
    def _dashboard_instance(
        self,
        project_id: str,
        dashboard_object: MicroStrategyObject,
    ) -> str:
        """The dashboard's one instance, created on first use. Visualization
        details and SQL-view lineage both read from an instance, and creating
        one executes the dashboard, so they share it; a failed creation is
        remembered and re-raised rather than retried, so a dashboard that
        timed out once is not executed (and waited on) a second time."""
        with self._execution_lock:
            cached = self._dashboard_instances.get(dashboard_object.id)
        if isinstance(cached, MicroStrategyAPIError):
            raise cached
        if cached is not None:
            self.report.report_dashboard_instance_reused()
            return cached

        def create() -> str:
            with self._execution_lock:
                existing = self._dashboard_instances.get(dashboard_object.id)
            if isinstance(existing, MicroStrategyAPIError):
                raise existing
            if existing is not None:
                return existing
            try:
                instance_id = self._create_dashboard_instance(
                    project_id, dashboard_object, dashboard_object.id
                )
            except MicroStrategyAPIError as error:
                with self._execution_lock:
                    self._record_execution(project_id, error)
                    self._dashboard_instances[dashboard_object.id] = error
                raise
            with self._execution_lock:
                self._record_execution(project_id, None)
                self._dashboard_instances[dashboard_object.id] = instance_id
            return instance_id

        return self._dashboard_instance_flight.do(dashboard_object.id, create)

    @timed_phase(MicroStrategyPhase.INSTANCE_CLEANUP)
    def _release_dashboard_instance(
        self,
        project_id: str,
        dashboard_object: MicroStrategyObject,
    ) -> None:
        with self._execution_lock:
            cached = self._dashboard_instances.pop(dashboard_object.id, None)
        if isinstance(cached, str):
            self._delete_dashboard_instance(
                project_id, dashboard_object, dashboard_object.id, cached
            )

    def _execution_limit_reached(self, project_id: str) -> bool:
        """True once max_consecutive_execution_timeouts tripped for the
        project; the caller then skips its execution and counts the skip."""
        with self._execution_lock:
            limited = project_id in self._execution_limited_projects
        if not limited:
            return False
        self.report.report_execution_skipped_after_timeouts()
        return True

    def _record_execution(
        self,
        project_id: str,
        error: Optional[MicroStrategyAPIError],
    ) -> None:
        """Track consecutive execution timeouts per project. A success resets
        the run; any other failure (a fast 4xx/5xx) costs no wait and neither
        counts nor resets."""
        with self._execution_lock:
            if error is None:
                self._consecutive_execution_timeouts[project_id] = 0
                return
            if not isinstance(error.__cause__, requests.Timeout):
                return
            self.report.report_execution_timeout()
            count = self._consecutive_execution_timeouts.get(project_id, 0) + 1
            self._consecutive_execution_timeouts[project_id] = count
            limit = self.config.max_consecutive_execution_timeouts
            if not limit or count < limit:
                return
            if project_id in self._execution_limited_projects:
                return
            self._execution_limited_projects.add(project_id)
        self.report.report_execution_timeout_limit_reached(project_id)
        self.report.warning(
            title="Stopped executing content after repeated timeouts",
            message=(
                "Instance creation timed out max_consecutive_execution_timeouts "
                "times in a row, so the rest of this project's dashboards and "
                "reports are ingested without visualization runtime details or "
                "SQL-view warehouse lineage."
            ),
            context=f"project_id={project_id}, consecutive_timeouts={count}",
        )

    def _create_dashboard_instance(
        self,
        project_id: str,
        dashboard_object: MicroStrategyObject,
        dashboard_id: str,
    ) -> str:
        # On the subtype alone, not is_document_object: a dossier is a document
        # underneath, so the document instance endpoint serves both kinds and
        # is the one the connector has always used for dossiers, which share
        # the subtype; the dossier endpoint remains for the other type-55
        # subtypes. The instance cleanup below mirrors this choice.
        if dashboard_object.subtype == MSTR_OBJECT_SUBTYPE_DOCUMENT:
            return self.client.create_document_instance(project_id, dashboard_id)
        return self.client.create_dossier_instance(project_id, dashboard_id)

    def _delete_dashboard_instance(
        self,
        project_id: str,
        dashboard_object: MicroStrategyObject,
        dashboard_id: str,
        instance_id: str,
    ) -> None:
        try:
            deleted = self.client.delete_dossier_instance(
                project_id,
                dashboard_id,
                instance_id,
            )
            if deleted or dashboard_object.subtype != MSTR_OBJECT_SUBTYPE_DOCUMENT:
                return
            self.client.delete_document_instance(project_id, dashboard_id, instance_id)
        except MicroStrategyAPIError as error:
            self._record_instance_cleanup_failure(
                "dashboard instance", project_id, instance_id, error
            )

    def _delete_report_instance(
        self,
        project_id: str,
        report_id: str,
        instance_id: str,
    ) -> None:
        try:
            self.client.delete_report_instance(project_id, report_id, instance_id)
        except MicroStrategyAPIError as error:
            self._record_instance_cleanup_failure(
                "report instance", project_id, instance_id, error
            )

    def _delete_model_report_instance(
        self,
        project_id: str,
        report_id: str,
        instance_id: str,
    ) -> None:
        try:
            self.client.delete_model_report_instance(project_id, report_id, instance_id)
        except MicroStrategyAPIError as error:
            self._record_instance_cleanup_failure(
                "Modeling report instance", project_id, instance_id, error
            )

    def _record_instance_cleanup_failure(
        self, kind: str, project_id: str, instance_id: str, error: Exception
    ) -> None:
        # A leaked instance holds server resources and counts towards the
        # user's instance limits, so it belongs in the report.
        self.report.report_instance_cleanup_failure()
        self.report.warning(
            title="MicroStrategy instance not deleted",
            message="An execution instance could not be deleted after use.",
            context=f"project_id={project_id}, kind={kind}, instance_id={instance_id}",
            exc=error,
            log=False,
        )

    def _linked_report_object(
        self, project_id: str, report_id: str
    ) -> Optional[MicroStrategyObject]:
        return self._report_object_cache.get_or_load(
            object_cache_key(project_id, report_id),
            lambda: self._fetch_linked_report_object(project_id, report_id),
        )

    def _fetch_linked_report_object(
        self, project_id: str, report_id: str
    ) -> Optional[MicroStrategyObject]:
        report_object: Optional[MicroStrategyObject] = None
        try:
            report_object = self.client.get_report_object(project_id, report_id)
        except MicroStrategyAuthError:
            raise
        except Exception as error:
            self.report.warning(
                title="Failed to Fetch Linked Report",
                message=(
                    "Skipping a dashboard-linked report whose object info "
                    "could not be fetched."
                ),
                context=f"project_id={project_id}, report_id={report_id}",
                exc=error,
            )
        return report_object

    def _report_left_out(
        self, project: Project, report_object: MicroStrategyObject
    ) -> bool:
        """Whether the report pass will skip this report for its folder path
        or for being personal content. Mirrors _skip_by_folder_path and
        _skip_personal_object without reporting, which the report pass does."""
        if self._is_personal_object(project.id, report_object):
            return True
        pattern = self.config.folder_path_pattern
        if _is_default_pattern(pattern):
            return False
        folder_path = self.mapper.folder_path(
            project.name, report_object, self._predefined_folders(project.id)
        )
        return folder_path is None or not pattern.allowed(folder_path)

    def _dashboard_report_chart_urns(
        self,
        project: Project,
        dashboard: DashboardDefinition,
    ) -> List[str]:
        if not self.config.extract_reports or self._per_report:
            # No Report chart: the dashboard's visualizations link the dataset.
            return []
        project_id = project.id
        # Personal and folder-path exclusion need the report's ancestry, which
        # the dependency lacks; without it a dashboard would list charts the
        # report pass never emits.
        check_ancestry = not self.config.include_personal_folders or not (
            _is_default_pattern(self.config.folder_path_pattern)
        )
        report_urns: List[str] = []
        seen: Set[str] = set()
        for dependency in dashboard.dependencies:
            if not _is_report_dependency(dependency):
                continue
            if not self.config.report_pattern.allowed(dependency.name):
                continue
            if check_ancestry:
                report_object = self._linked_report_object(project_id, dependency.id)
                if report_object is None or self._report_left_out(
                    project, report_object
                ):
                    continue
            report_urn = self.mapper.report_urn(project_id, dependency.id)
            if report_urn in seen:
                continue
            seen.add(report_urn)
            report_urns.append(report_urn)
        return report_urns

    def _process_dashboard(
        self,
        project_id: str,
        dashboard_object: MicroStrategyObject,
        dashboard: DashboardDefinition,
        parent_key: ProjectKey,
        extra_chart_urns: Sequence[str] = (),
        predefined_folders: Optional[PredefinedFolderResolution] = None,
        checkpoint: Optional[ObjectCheckpoint] = None,
    ) -> Iterable[MetadataWorkUnit]:
        if self.config.extract_cubes and not self._per_report:
            for dataset in dashboard.datasets:
                # A dataset is its own catalog object (report or cube) with its
                # own folder, which is usually not the dossier's folder.
                dataset_object = self._dataset_object_info(project_id, dataset.id)
                folder_object = self._folder_object(project_id, dataset_object)
                if folder_object is not None:
                    yield from self._folder_containers(
                        project_id, folder_object, predefined_folders, checkpoint
                    )
                yield from self.mapper.gen_dataset_workunits(
                    project_id,
                    dashboard,
                    dataset,
                    self.mapper.dataset_folder_parent_key(
                        project_id, folder_object, parent_key, predefined_folders
                    ),
                    dataset_object=dataset_object,
                )

        if self.config.extract_charts:
            for visualization in dashboard.visualizations:
                yield from self.mapper.gen_chart_workunits(
                    project_id, dashboard, visualization, parent_key
                )

        yield from self.mapper.gen_dashboard_workunits(
            project_id,
            dashboard_object,
            dashboard,
            parent_key,
            extra_chart_urns=extra_chart_urns,
            fingerprint=_checkpoint_fingerprint(checkpoint),
        )

    def get_report(self) -> SourceReport:
        return self.report


class _ReportDatasetKey(BaseModel):
    model_config = ConfigDict(frozen=True)

    project_id: str
    report_id: str


class _ReportDatasetEntry(BaseModel):
    model_config = ConfigDict(frozen=True)

    dataset: DatasetObject
    first_dashboard_object: Optional[MicroStrategyObject] = None
    dashboard_ids: List[str] = Field(default_factory=list)
    report_object: Optional[MicroStrategyObject] = None
    report_definition: Optional[ReportDefinition] = None
    from_report_pass: bool = False
    lineage_resolved: bool = False


def _report_dataset_key(project_id: str, dataset_id: str) -> _ReportDatasetKey:
    return _ReportDatasetKey(
        project_id=project_id, report_id=normalize_object_id(dataset_id)
    )


def _report_dataset_object(
    report_object: MicroStrategyObject,
    available_objects: MicroStrategyDict,
    description: Optional[str],
) -> DatasetObject:
    return DatasetObject.model_validate(
        {
            "id": report_object.id,
            "name": report_object.name,
            "description": description,
            "availableObjects": available_objects,
        }
    )


class _LazyProjectLineage:
    """Per-project lineage context resolved on first use: the datasource lookup and
    model-tables pagination cost API calls, so they are deferred until a dashboard or
    report survives filtering, then cached for the project."""

    def __init__(
        self,
        source: MicroStrategySource,
        project_id: str,
        source_warehouses: List[Datasource],
    ):
        self._source = source
        self._project_id = project_id
        self._source_warehouses = source_warehouses
        self._lock = threading.RLock()
        self._context: Optional[WarehouseLineageContext] = None
        self._context_resolved = False
        self._index: Optional[ModelLineageIndex] = None
        self._index_resolved = False
        self._model_tables: Optional[List[Dict[str, object]]] = None
        self._model_tables_resolved = False
        self._table_context_resolver: Optional[TableContextResolver] = None
        self._table_context_resolver_resolved = False
        self._datasource_contexts: Dict[str, Optional[WarehouseLineageContext]] = {}
        # Object identity: the cached table list is shared by both passes.
        self._counted_tables: Set[int] = set()

    def _resolve(self, part: str, fetch: Callable[[], _T]) -> Optional[_T]:
        """Run a project-level lookup once. An unexpected error is reported
        against the project, not the dashboard that happened to trigger it,
        and every later consumer gets None rather than retrying."""
        try:
            return fetch()
        except (MicroStrategyAuthError, ConfigurationError):
            raise
        except Exception as error:
            self._source.report.warning(
                title="Project lineage context unavailable",
                message=(
                    "A project-level lineage lookup failed, so this project's "
                    "dashboards and reports are emitted without it."
                ),
                context=f"project_id={self._project_id}, lookup={part}",
                exc=error,
            )
            return None

    @property
    def warehouse_context(self) -> Optional[WarehouseLineageContext]:
        # Resolved once even on failure (functools.cached_property would retry).
        with self._lock:
            if not self._context_resolved:
                try:
                    self._context = self._resolve(
                        "warehouse_context",
                        lambda: self._source._get_project_warehouse_context(
                            self._project_id, self._source_warehouses
                        ),
                    )
                finally:
                    self._context_resolved = True
            return self._context

    @property
    def table_context_resolver(self) -> Optional[TableContextResolver]:
        # Only for a project whose several datasources give no single context.
        with self._lock:
            if not self._table_context_resolver_resolved:
                self._table_context_resolver_resolved = True
                if (
                    self.warehouse_context is None
                    and self._source._project_has_several_warehouse_contexts(
                        self._source_warehouses
                    )
                ):
                    self._table_context_resolver = self._table_context
            return self._table_context_resolver

    def _table_context(
        self, table: Dict[str, object]
    ) -> Optional[WarehouseLineageContext]:
        with self._lock:
            datasource = datasource_for_model_table(table, self._source_warehouses)
            context = self._datasource_context(datasource) if datasource else None
            if id(table) not in self._counted_tables:
                self._counted_tables.add(id(table))
                if context is not None:
                    self._source.report.report_model_table_resolved_by_datasource()
                else:
                    self._report_unresolved_table()
            return context

    def _report_unresolved_table(self) -> None:
        if not self._source.report.model_tables_without_warehouse_context:
            self._source.report.warning(
                title="MicroStrategy Logical Tables Without Source Warehouse",
                message=(
                    "This project has several source warehouses and some "
                    "logical tables name no primary datasource that matches "
                    "one of them, so they are emitted without warehouse "
                    "lineage. The run report counts them in "
                    "model_tables_without_warehouse_context."
                ),
                context=f"project_id={self._project_id}",
            )
        self._source.report.report_model_table_without_warehouse_context()

    def _datasource_context(
        self, datasource: Datasource
    ) -> Optional[WarehouseLineageContext]:
        key = normalize_object_id(datasource.id)
        with self._lock:
            if key not in self._datasource_contexts:
                context = warehouse_context_from_datasource(
                    datasource,
                    self._source.config.env,
                    self._source.config.datasource_platform_mapping,
                )
                self._datasource_contexts[key] = (
                    self._source._context_with_connection(
                        self._project_id, context, datasource
                    )
                    if context
                    else None
                )
            return self._datasource_contexts[key]

    @property
    def model_tables(self) -> Optional[List[Dict[str, object]]]:
        """Raw /api/model/tables rows, fetched at most once per project.

        Shared by model_lineage_index (below) and the semantic-model pass, so
        toggling both on for the same run costs one fetch, not two.
        """
        with self._lock:
            if not self._model_tables_resolved:
                try:
                    self._model_tables = self._resolve(
                        MicroStrategyPhase.MODEL_TABLES,
                        lambda: self._source._fetch_project_model_tables(
                            self._project_id,
                            include_datasource=self.table_context_resolver is not None,
                        ),
                    )
                finally:
                    self._model_tables_resolved = True
            return self._model_tables

    def model_tables_retrying_failure(self) -> Optional[List[Dict[str, object]]]:
        # The first fetch is hours earlier; one failure drops the semantic model.
        with self._lock:
            if self._model_tables_resolved and self._model_tables is None:
                logger.info(
                    "Retrying the model tables fetch for the semantic model "
                    "(project=%s); the earlier fetch failed",
                    self._project_id,
                )
                self._model_tables_resolved = False
            return self.model_tables

    @property
    def model_lineage_index(self) -> Optional[ModelLineageIndex]:
        with self._lock:
            if not self._index_resolved:
                try:
                    self._index = self._resolve(
                        MicroStrategyPhase.MODEL_LINEAGE_INDEX,
                        lambda: self._source._get_project_model_lineage_index(
                            self._project_id,
                            self.warehouse_context,
                            lambda: self.model_tables,
                            self._source_warehouses,
                            self.table_context_resolver,
                        ),
                    )
                finally:
                    self._index_resolved = True
            return self._index


def _metric_items(available_objects: Dict[str, object]) -> Iterable[Dict[str, object]]:
    metrics = available_objects.get("metrics")
    if isinstance(metrics, list):
        for metric in metrics:
            if isinstance(metric, dict):
                yield metric


def _is_default_pattern(pattern: AllowDenyPattern) -> bool:
    """The field default: everything allowed, nothing denied."""
    default = AllowDenyPattern.allow_all()
    return pattern.allow == default.allow and pattern.deny == default.deny


def _unique_object_ids(object_ids: Iterable[str]) -> List[str]:
    # Keeps the first spelling so lookups reuse the casing the API returned.
    unique: Dict[str, str] = {}
    for object_id in object_ids:
        unique.setdefault(normalize_object_id(object_id), object_id)
    return [unique[key] for key in sorted(unique)]


def _recording_urns(
    workunits: Iterable[MetadataWorkUnit],
    checkpoint: Union[ObjectCheckpoint, ReportDatasetCheckpoint, None],
) -> Iterable[MetadataWorkUnit]:
    for workunit in workunits:
        if checkpoint is not None and workunit.is_primary_source:
            checkpoint.urns.extend(_checkpoint_urns(workunit))
        yield workunit


def _checkpoint_fingerprint(
    checkpoint: Union[ObjectCheckpoint, ReportDatasetCheckpoint, None],
) -> Optional[str]:
    return checkpoint.fingerprint if checkpoint is not None else None


def _checkpoint_urns(workunit: MetadataWorkUnit) -> Iterable[str]:
    yield workunit.get_urn()
    # Tags and terms are emitted at stream end; a skipped object must keep them.
    for urn in list_urns(workunit.metadata):
        if guess_entity_type(urn) in SHARED_REFERENCE_ENTITY_TYPES:
            yield urn


def _is_report_dependency(dependency: MicroStrategyObject) -> bool:
    # Match on the MicroStrategy object type only; a name-substring heuristic
    # would fabricate report chart URNs for anything named "...Report...".
    return (dependency.type or "").strip() == str(MSTR_OBJECT_TYPE_REPORT)


def _error_summary(error: Exception) -> str:
    """One-line error for report samples: HTTP status first when the client
    recorded one, then the message, bounded so a long server body cannot
    bloat the report."""
    if isinstance(error, MicroStrategyAPIError):
        text = error.summary()
    else:
        text = f"{type(error).__name__}: {error}"
    return text[:_ERROR_SUMMARY_MAX_CHARS]


def _metric_expression_summary(model: Dict[str, object]) -> Optional[MetricEnrichment]:
    expression = model.get("expression")
    if not isinstance(expression, dict):
        return None
    return metric_enrichment_from_expression(expression)
