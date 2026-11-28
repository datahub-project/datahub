import contextlib
import logging
from collections import Counter
from dataclasses import replace
from functools import partial
from typing import (
    Callable,
    Dict,
    FrozenSet,
    Generator,
    Iterable,
    List,
    Mapping,
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
from datahub.ingestion.api.closeable import Closeable
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
    CheckpointDraft,
    CheckpointRecord,
    MicroStrategyStateHandler,
    ObjectCheckpoint,
    RecordedUsageTarget,
    ReportDatasetCheckpoint,
)
from datahub.ingestion.source.microstrategy.client import (
    MicroStrategyAPIError,
    MicroStrategyAuthError,
    MicroStrategyClient,
)
from datahub.ingestion.source.microstrategy.config import MicroStrategyConfig
from datahub.ingestion.source.microstrategy.constants import (
    MICROSTRATEGY_PLATFORM,
    MSTR_CACHE_MEMORY_ENTRIES,
    MSTR_CUBE_SUBTYPES,
    MSTR_DATASOURCE_MAPPING_HINT,
    MSTR_DEFINITION_ENDPOINT_METRIC_MODEL,
    MSTR_DEFINITION_ENDPOINT_MODEL,
    MSTR_DEFINITION_ENDPOINT_V2,
    MSTR_FOLDER_TYPE_PROFILE_OBJECTS,
    MSTR_FOLDER_TYPE_PROFILE_REPORTS,
    MSTR_MODEL_TABLE_FIELDS,
    MSTR_MODEL_TABLE_FIELDS_WITH_DATASOURCE,
    MSTR_OBJECT_SUBTYPE_DOCUMENT,
    MSTR_OBJECT_SUBTYPE_REPORT_GRID,
    MSTR_OBJECT_TYPE_FOLDER,
    MSTR_OBJECT_TYPE_REPORT,
    MSTR_PAYLOAD_CACHE_MEMORY_ENTRIES,
    MSTR_PERSONAL_FOLDER_NAMES,
    MSTR_PERSONAL_FOLDER_TYPES,
    MSTR_PREDEFINED_FOLDER_LABELS,
    MSTR_PREDEFINED_HIDDEN_FOLDER_TYPES,
    MSTR_PROGRESS_LOG_INTERVAL_DASHBOARDS,
    MSTR_UNKNOWN_DATASOURCE_TYPE,
    SHARED_REFERENCE_ENTITY_TYPES,
    MicroStrategyReadOutcome,
    ReportObjectLookupKind,
    UsageTargetKind,
)
from datahub.ingestion.source.microstrategy.incremental import (
    MicroStrategyChangeDetector,
    ProjectFingerprinter,
    ReportDatasetPlan,
    StampedAspect,
    StampedUrnBuilder,
)
from datahub.ingestion.source.microstrategy.lineage import (
    ModelLineageIndex,
    ProjectWarehouseContexts,
    SqlViewLineage,
    TableContextResolver,
    WarehouseLineageContext,
    bind_visualizations_by_derived_objects,
    datasource_for_model_table,
    matching_datasource,
    metric_fact_ids_from_model,
    metric_metric_ids_from_model,
    object_id,
    project_warehouse_contexts,
    sql_statement_from_sql_view_entry,
    sql_view_dataset_key,
    sql_view_dataset_name,
    unique_derived_object_owners,
    warehouse_context_from_datasource,
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
    DatasourceConnection,
    DatasourceReference,
    MetricEnrichment,
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
    object_cache_key,
)
from datahub.ingestion.source.microstrategy.report import MicroStrategyReport
from datahub.ingestion.source.microstrategy.spill_cache import (
    SpillCache,
    SpillConnection,
)
from datahub.ingestion.source.microstrategy.sqlglot_warnings import (
    suppress_sqlglot_warnings,
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
from datahub.metadata.schema_classes import ChartInfoClass, DashboardInfoClass
from datahub.utilities.sentinels import Unset
from datahub.utilities.urns.urn import guess_entity_type
from datahub.utilities.urns.urn_iter import list_urns

logger = logging.getLogger(__name__)

_T = TypeVar("_T")
_Record = TypeVar("_Record", ObjectCheckpoint, ReportDatasetCheckpoint)


def _without_fingerprint(previous: _Record) -> _Record:
    # A failed read may have written the object's stamp and entities before
    # failing, so the old fingerprint would let the next run skip it.
    return previous.model_copy(update={"fingerprint": None})


_ERROR_SUMMARY_MAX_CHARS = 300


# Modeling document answers that mean the whole project lacks access. 404 is
# left out: the endpoint can answer it for a single unmodelled document.
_MODEL_DOCUMENT_PROJECT_WIDE_STATUSES = frozenset({403, 501})

# Model tables answers that a second request will not change.
_MODEL_TABLES_NON_RETRYABLE_STATUSES = frozenset({403, 501})

# Enrichment failures a later read can recover from. Other 4xx and 500 repeat
# on every read (the Modeling service answers 500 for a metric in another
# project), so they must not keep an unchanged object from being skipped.
_TRANSIENT_FAILURE_STATUSES = frozenset({429, 502, 503, 504})


class _PredefinedFoldersUnavailable(Exception):
    """Predefined folders could not be resolved, so the project's dashboards
    and reports are skipped rather than filed under raw folder names."""


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
        self._spill_connection = SpillConnection()
        self.mapper = MicroStrategyMapper(config, self.report, self._spill_connection)
        self.lineage = self.mapper.lineage
        # A failed fetch is cached as None and reused, except by the semantic model.
        self._metric_models: SpillCache[Optional[Dict[str, object]]] = SpillCache(
            self._spill_connection,
            "metric_models",
            MSTR_PAYLOAD_CACHE_MEMORY_ENTRIES,
            compress=True,
        )
        self._model_document_unavailable_projects: Set[str] = set()
        self._model_tables_non_retryable_projects: Set[str] = set()
        # object_cache_key of enrichment reads that failed transiently.
        self._transient_failure_keys: Set[str] = set()
        self._project_datasources: List[Datasource] = []
        # The Modeling document of the dashboard in flight, so binding and
        # document derived metrics share one GET. Dashboards are processed one
        # at a time, so only the latest is kept.
        self._model_document_key: Optional[Tuple[str, str]] = None
        self._model_document_value: Optional[Dict[str, object]] = None
        self._predefined_folder_cache: Dict[str, PredefinedFolderResolution] = {}
        self._personal_folder_cache: Dict[str, PersonalFolderResolution] = {}
        # Object info of type-3 objects (reports and cubes), shared by dataset
        # and linked-report lookups.
        self._type3_objects: SpillCache[_ReportObjectLookup] = SpillCache(
            self._spill_connection, "type3_objects", MSTR_CACHE_MEMORY_ENTRIES
        )
        # Fingerprints read the object endpoint only: search results seeded into
        # _type3_objects carry date_modified one second earlier.
        self._fingerprint_objects: SpillCache[_ReportObjectLookup] = SpillCache(
            self._spill_connection, "fingerprint_objects", MSTR_CACHE_MEMORY_ENTRIES
        )
        # A failed read is cached, so a shared report is not executed again.
        self._report_sql_statements: SpillCache[_ReportSqlOutcome] = SpillCache(
            self._spill_connection,
            "report_sql_statements",
            MSTR_CACHE_MEMORY_ENTRIES,
            compress=True,
        )
        self._report_v2_payloads: SpillCache[Dict[str, object]] = SpillCache(
            self._spill_connection,
            "report_v2_payloads",
            MSTR_PAYLOAD_CACHE_MEMORY_ENTRIES,
            compress=True,
        )
        # Derived metric definitions per report, or None once every definition
        # endpoint failed for it.
        self._report_derived_metrics: SpillCache[
            Optional[List[ReportDerivedMetric]]
        ] = SpillCache(
            self._spill_connection,
            "report_derived_metrics",
            MSTR_CACHE_MEMORY_ENTRIES,
            compress=True,
        )
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
        # (project id, normalized connection id) -> connection, or None once a
        # fetch failed. Keyed by project because the request is sent under one
        # project and a later project must report its own failure.
        self._datasource_connections: Dict[
            Tuple[str, str], Optional[DatasourceConnection]
        ] = {}
        self._failed_connection_keys: Set[Tuple[str, str]] = set()
        self._dashboards_processed = 0
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
            report=self.report,
            pipeline_name=ctx.pipeline_name,
            run_id=ctx.run_id,
            stale_job_id=self.stale_entity_removal_handler.job_id
            if self.stale_entity_removal_handler.is_checkpointing_enabled()
            else None,
        )
        self._change_detector = MicroStrategyChangeDetector(self.config, self.report)
        self._changes = _ProjectChangeState()
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
        try:
            with suppress_sqlglot_warnings(self.report):
                yield from self._process_projects()
        finally:
            self._cleanup()

    def _process_projects(self) -> Iterable[MetadataWorkUnit]:
        auth_lost = False
        if self.config.emit_chapter_page_structured_properties:
            yield from self._emit_chapter_page_structured_property_definitions()
        projects = self.client.list_projects()
        for index, project in enumerate(projects, start=1):
            if not self.config.project_pattern.allowed(project.name):
                self.report.filtered_projects.append(project.name)
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
                with self.report.new_stage(f"{project.name}: {METADATA_EXTRACTION}"):
                    yield from self._process_project(project)
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

    def _cleanup(self) -> None:
        # Callbacks run last-registered first, each even if an earlier one raises.
        with contextlib.ExitStack() as cleanup:
            cleanup.callback(self.client.close)
            for cache in self._spill_caches():
                cleanup.callback(cache.close)
            cleanup.callback(self.lineage.close)
            cleanup.callback(self._warn_if_every_sql_view_failed)
            cleanup.callback(self._finish_checkpoint)

    def _spill_caches(self) -> List[Closeable]:
        return [
            self._metric_models,
            self._type3_objects,
            self._fingerprint_objects,
            self._report_sql_statements,
            self._report_v2_payloads,
            self._report_derived_metrics,
            self._spill_connection,
        ]

    def _release_report_caches(self) -> None:
        # The semantic model does not read these, so its per-metric work does not
        # stack on them. Each reset runs even if an earlier one raises.
        with contextlib.ExitStack() as resets:
            resets.callback(self.lineage.reset_project_caches)
            resets.callback(self._report_datasets.clear)
            resets.callback(self._clear_model_document)
            for cache in (
                self._report_sql_statements,
                self._type3_objects,
                self._fingerprint_objects,
                self._report_derived_metrics,
                self._report_v2_payloads,
            ):
                resets.callback(cache.reset)

    def _clear_model_document(self) -> None:
        self._model_document_key = None
        self._model_document_value = None

    def _reset_project_caches(self) -> None:
        # Memory and temporary disk track the largest project, not the whole run.
        with contextlib.ExitStack() as resets:
            resets.callback(self._count_project_reset)
            resets.callback(self._spill_connection.close)
            resets.callback(self._metric_models.reset)
            resets.callback(self._release_report_caches)

    def _count_project_reset(self) -> None:
        self.report.cache_project_resets += 1

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
        entity_kind: UsageTargetKind,
        entity_urn: str,
    ) -> None:
        if self.config.extract_usage_statistics:
            self._usage_targets[object_id.strip().upper()] = UsageTarget(
                kind=entity_kind, urn=entity_urn
            )

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
                    if self._skips_unchanged()
                    else "The checkpoint of dashboards and reports is over "
                    "stateful_ingestion.max_checkpoint_state_size, so it is not "
                    "saved and the next run cannot keep the entities of an "
                    "object that fails to read."
                ),
            )

    def _folder_path_of(
        self, project: Project, mstr_object: MicroStrategyObject
    ) -> Optional[str]:
        return self.mapper.folder_path(
            project.name, mstr_object, self._predefined_folders(project.id)
        )

    def _object_fingerprint(
        self,
        project: Project,
        mstr_object: MicroStrategyObject,
        dependency_ids: Sequence[str],
        checkpoint: CheckpointDraft,
    ) -> Optional[str]:
        # Stamped only for skipping, and never on a read that missed something.
        fingerprinter = self._changes.fingerprinter
        if (
            not self._skips_unchanged()
            or fingerprinter is None
            or checkpoint.outcome != MicroStrategyReadOutcome.COMPLETE
        ):
            return None
        return fingerprinter.object_fingerprint(
            mstr_object,
            self._folder_path_of(project, mstr_object),
            dependency_ids,
            self._fingerprint_object_info,
        )

    def _evaluate_object(
        self,
        project: Project,
        mstr_object: MicroStrategyObject,
        previous: Optional[ObjectCheckpoint],
        stamped_urn: StampedUrnBuilder,
        stamped_aspect: StampedAspect,
    ) -> bool:
        fingerprinter = self._changes.fingerprinter
        if fingerprinter is None:
            return False
        return self._change_detector.evaluate_object(
            fingerprinter,
            mstr_object,
            self._folder_path_of(project, mstr_object),
            previous,
            self._fingerprint_object_info,
            self.ctx.graph,
            stamped_urn,
            stamped_aspect,
        )

    def _new_draft(self) -> Optional[CheckpointDraft]:
        return (
            CheckpointDraft() if self._object_state.is_checkpointing_enabled() else None
        )

    @staticmethod
    def _tracked_urns(urns: Iterable[str]) -> List[str]:
        return sorted(
            {
                urn
                for urn in urns
                if guess_entity_type(urn) not in STATEFUL_INGESTION_IGNORED_ENTITY_TYPES
            }
        )

    def _keep_entities(self, previous: CheckpointRecord) -> None:
        for urn in previous.urns:
            self.stale_entity_removal_handler.add_entity_to_state(
                guess_entity_type(urn), urn
            )
        for target in previous.usage_targets:
            self._register_usage_target(target.object_id, target.kind, target.urn)

    def _keep_dashboard(
        self,
        project_id: str,
        dashboard_id: str,
        previous: ObjectCheckpoint,
        linked_report_ids: Optional[Set[str]],
        unchanged: bool,
    ) -> None:
        # A read that failed keeps the last good record but is not unchanged: its
        # fingerprint must not let a dataset built from it be skipped.
        if linked_report_ids is not None:
            linked_report_ids.update(previous.linked_report_ids)
        if unchanged and previous.fingerprint is not None:
            self._changes.dashboard_fingerprints[normalize_object_id(dashboard_id)] = (
                previous.fingerprint
            )
        self._keep_entities(previous)
        self._object_state.record_dashboard(project_id, dashboard_id, previous)

    def _keep_report(
        self, project_id: str, report_id: str, previous: ObjectCheckpoint
    ) -> None:
        self._keep_entities(previous)
        self._object_state.record_report(project_id, report_id, previous)

    def _keep_report_dataset(
        self, project_id: str, report_id: str, previous: ReportDatasetCheckpoint
    ) -> None:
        self._keep_entities(previous)
        self._object_state.record_report_dataset(project_id, report_id, previous)

    def _keep_previous_urns(
        self, checkpoint: CheckpointDraft, previous: Optional[CheckpointRecord]
    ) -> None:
        # The partial read may have missed entities; keep them and read it again.
        checkpoint.fingerprint = None
        if previous is not None:
            checkpoint.urns.extend(previous.urns)
            self._keep_entities(previous)

    def _finish_read(
        self,
        checkpoint: CheckpointDraft,
        previous: Optional[_Record],
        keep: Callable[[_Record], None],
        record: Callable[[CheckpointDraft], None],
    ) -> bool:
        """A failed read keeps the object's last record, a partial one keeps
        its last entities too. False when nothing new was recorded."""
        if checkpoint.outcome == MicroStrategyReadOutcome.FAILED:
            if previous is not None:
                keep(_without_fingerprint(previous))
            return False
        if checkpoint.outcome == MicroStrategyReadOutcome.INCOMPLETE:
            self._keep_previous_urns(checkpoint, previous)
        checkpoint.urns = self._tracked_urns(checkpoint.urns)
        record(checkpoint)
        return True

    def _folder_containers(
        self,
        project_id: str,
        mstr_object: MicroStrategyObject,
        predefined_folders: Optional[PredefinedFolderResolution],
        checkpoint: Optional[CheckpointDraft],
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
        fingerprinter: ProjectFingerprinter,
        candidates: List[str],
        unchanged: Set[str],
    ) -> ReportDatasetPlan:
        plan = ReportDatasetPlan(
            previous_dashboards=self._object_state.previous_dashboards(project_id),
            previous_reports=self._object_state.previous_report_datasets(project_id),
            candidates=candidates,
            unchanged=unchanged,
        )
        plan.mark_reports_of_changed_dashboards()
        for report_id, previous in plan.previous_reports.items():
            if report_id in plan.dirty:
                continue
            dashboard_fingerprints: Dict[str, Optional[str]] = {}
            for dashboard_id in previous.dashboard_ids:
                dashboard = plan.previous_dashboards.get(dashboard_id)
                dashboard_fingerprints[dashboard_id] = (
                    dashboard.fingerprint if dashboard is not None else None
                )
            if not self._change_detector.evaluate_report_dataset(
                fingerprinter,
                report_id,
                previous,
                dashboard_fingerprints,
                self._fingerprint_object_info,
                self.ctx.graph,
            ):
                plan.dirty.add(report_id)
        return plan

    def _mark_bound_reports_changed(self, project_id: str) -> None:
        # Re-emit report datasets a changed dashboard built or the last run missed.
        plan = self._changes.report_plan
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
        plan = self._changes.report_plan
        if plan is None:
            return
        while True:
            self._mark_bound_reports_changed(project.id)
            to_read = plan.dashboards_to_read() - self._changes.read_dashboards
            pending = [
                dashboard_id
                for dashboard_id in plan.candidates
                if dashboard_id in to_read
            ]
            if not pending:
                return
            for dashboard_id in pending:
                yield from self._process_dashboard_with_boundary(
                    project,
                    self._changes.candidates[dashboard_id],
                    lineage_context,
                    linked_report_ids,
                )

    def _order_by_search(self, entry: "_ReportDatasetEntry") -> None:
        # Restore search order so the fallback dashboard matches a full run.
        candidates = self._changes.candidates
        order = {dashboard_id: index for index, dashboard_id in enumerate(candidates)}
        entry.dashboard_ids.sort(
            key=lambda dashboard_id: order.get(
                normalize_object_id(dashboard_id), len(order)
            )
        )
        first = (
            candidates.get(normalize_object_id(entry.dashboard_ids[0]))
            if entry.dashboard_ids
            else None
        )
        entry.first_dashboard_object = first or entry.first_dashboard_object

    def _process_project(self, project: Project) -> Iterable[MetadataWorkUnit]:
        self._changes = _ProjectChangeState()
        source_warehouses = self._get_project_source_warehouses(project.id)
        self._project_datasources = source_warehouses
        # Warehouse/model-table lookups are deferred until an allowed dashboard/report
        # needs them, so fully-filtered projects don't pay for those API calls.
        lineage_context = _LazyProjectLineage(self, project.id, source_warehouses)
        yield from self.mapper.gen_project_container(
            project,
            source_warehouses=source_warehouses,
        )
        try:
            yield from self._process_project_content(project, lineage_context)
        finally:
            self._release_report_caches()
        if self.config.emit_semantic_model_entities:
            yield from self._process_project_semantic_model(project, lineage_context)

    def _start_change_tracking(
        self,
        project_id: str,
        source_warehouses: List[Datasource],
        lineage_context: "_LazyProjectLineage",
    ) -> None:
        if not self._skips_unchanged():
            return
        fingerprinter = self._change_detector.init_project(
            project_id,
            source_warehouses,
            self._predefined_folders(project_id),
            # Model lineage of every object comes from these project-wide rows.
            lineage_context.model_tables
            if self.config.extract_lineage and self.config.extract_model_lineage
            else None,
        )
        self._changes.fingerprinter = fingerprinter
        if self.config.per_report and not self.config.extract_dashboards:
            self._changes.report_plan = self._start_report_plan(
                project_id, fingerprinter, [], set()
            )

    def _process_project_content(
        self,
        project: Project,
        lineage_context: "_LazyProjectLineage",
    ) -> Iterable[MetadataWorkUnit]:
        if not (self.config.extract_dashboards or self.config.extract_reports):
            return
        if not self._resolve_project_folders(project):
            return
        self._start_change_tracking(
            project.id, lineage_context.source_warehouses, lineage_context
        )
        # Report ids referenced by allowed dashboards, collected during the
        # dashboard pass so the report pass can scope to them.
        linked_report_ids: Set[str] = set()
        try:
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
            if self._changes.report_plan is not None:
                self._changes.report_plan.mark_reports_left_report_pass()
                yield from self._read_dashboards_of_changed_reports(
                    project, lineage_context, linked_report_ids
                )
            self.report.dashboards_skipped_unchanged += len(
                self._changes.candidates.keys() - self._changes.read_dashboards
            )
        except Exception:
            # Charts and dashboards already emitted point at these datasets. Not
            # a finally: yielding while the generator closes raises RuntimeError.
            if self.config.per_report:
                yield from self._gen_report_datasets_after_error(project.id)
            raise
        if self.config.per_report:
            yield from self._gen_report_dataset_workunits(project.id)

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
            cached_metric_model=self._cached_metric_model,
            graph=self.ctx.graph,
        )
        yield from semantic_model_mapper.emit(
            project_id=project.id,
            project_name=project.name,
            model_tables=model_tables,
            table_context_resolver=lineage_context.table_context_resolver,
            project_container_key=self.mapper.project_key(project.id),
        )

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

    def warehouse_lineage_enabled(self) -> bool:
        return self.config.extract_lineage and (
            self.config.extract_warehouse_lineage
            or self.config.extract_model_lineage
            or self.config.extract_report_sql_lineage
        )

    def warn_unresolved_project_warehouse(
        self,
        project_id: str,
        source_warehouses: List[Datasource],
        contexts: ProjectWarehouseContexts,
    ) -> None:
        if not source_warehouses:
            if self.config.extract_model_lineage:
                self.report.warning(
                    title="MicroStrategy Model Lineage Unavailable",
                    message=(
                        "Skipping logical table lineage because no source "
                        "warehouse was listed for this project. It needs "
                        "extract_source_warehouses and access to the project's "
                        "datasources."
                    ),
                    context=f"project_id={project_id}",
                )
            return
        context = (
            f"project_id={project_id}, "
            f"{_source_platform_summary(source_warehouses)}, "
            f"{MSTR_DATASOURCE_MAPPING_HINT}"
        )
        if contexts.has_several:
            self.report.warning(
                title="MicroStrategy Project Has Several Source Warehouses",
                message=(
                    "No single project-level warehouse can be chosen, so "
                    "warehouse lineage only covers datasets and logical tables "
                    "whose own datasource maps to a platform."
                ),
                context=context,
            )
            return
        self.report.warning(
            title="MicroStrategy Project Has No Supported Source Warehouse",
            message=(
                "None of the project's datasources maps to a supported "
                "platform, so warehouse lineage is skipped."
            ),
            context=context,
        )

    def datasource_warehouse_context(
        self,
        project_id: str,
        datasource: DatasourceReference,
    ) -> Optional[WarehouseLineageContext]:
        # Shared by SQL-view and model-table lineage: both must build the same
        # table URNs or model field edges are filtered out of SQL upstreams.
        context = warehouse_context_from_datasource(
            datasource,
            self.config.env,
            self.config.datasource_platform_mapping,
        )
        if context is None:
            return None
        return self._context_with_connection(project_id, context, datasource)

    def _context_with_connection(
        self,
        project_id: str,
        context: WarehouseLineageContext,
        datasource: DatasourceReference,
    ) -> Optional[WarehouseLineageContext]:
        if not datasource.connection_id:
            return context
        connection = self._datasource_connection(project_id, datasource.connection_id)
        if connection is None:
            if context.database:
                return (
                    replace(context, connection_unavailable=True)
                    if (project_id, normalize_object_id(datasource.connection_id))
                    in self._failed_connection_keys
                    else context
                )
            # Without a database the table URNs would be guesses.
            self.report.report_datasource_skipped_without_database()
            return None
        return warehouse_context_with_connection(
            context,
            connection,
            self.config.datasource_platform_mapping,
        )

    def connection_lookup_failed(self, project_id: str) -> bool:
        return any(
            failed_project == project_id
            for failed_project, _ in self._failed_connection_keys
        )

    def _datasource_connection(
        self, project_id: str, connection_id: str
    ) -> Optional[DatasourceConnection]:
        key = (project_id, normalize_object_id(connection_id))
        if key in self._datasource_connections:
            return self._datasource_connections[key]
        connection: Optional[DatasourceConnection] = None
        try:
            connection = self.client.get_datasource_connection(
                connection_id,
                project_id=project_id,
            )
        except MicroStrategyAPIError as error:
            self._failed_connection_keys.add(key)
            self.report.report_warehouse_lineage_api_failure()
            self.report.warning(
                title="Failed to Fetch Datasource Connection",
                message=(
                    "Continuing without connection-level database/schema "
                    "context; warehouse lineage may be less precise."
                ),
                context=f"project_id={project_id}, connection_id={connection_id}",
                exc=error,
            )
        self._datasource_connections[key] = connection
        return connection

    def _process_project_dashboards(
        self,
        project: Project,
        lineage_context: "_LazyProjectLineage",
        linked_report_ids: Optional[Set[str]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        candidates = self._allowed_dashboards(project)
        if not self._skips_unchanged():
            for dashboard_object in candidates:
                yield from self._process_dashboard_with_boundary(
                    project, dashboard_object, lineage_context, linked_report_ids
                )
            return
        # Decide every change first so a per_report dataset's dashboards read together.
        self._changes.candidates = {
            normalize_object_id(dashboard_object.id): dashboard_object
            for dashboard_object in candidates
        }
        previous_checkpoints = {
            dashboard_id: self._object_state.previous_dashboard(
                project.id, dashboard_object.id
            )
            for dashboard_id, dashboard_object in self._changes.candidates.items()
        }
        unchanged = {
            dashboard_id
            for dashboard_id, dashboard_object in self._changes.candidates.items()
            if self._evaluate_object(
                project,
                dashboard_object,
                previous_checkpoints[dashboard_id],
                self.mapper.dashboard_urn,
                DashboardInfoClass,
            )
        }
        to_read = set(self._changes.candidates) - unchanged
        fingerprinter = self._changes.fingerprinter
        if self.config.per_report and fingerprinter is not None:
            plan = self._start_report_plan(
                project.id, fingerprinter, list(self._changes.candidates), unchanged
            )
            self._changes.report_plan = plan
            to_read |= plan.dashboards_to_read()
        for dashboard_id, dashboard_object in self._changes.candidates.items():
            previous = previous_checkpoints[dashboard_id]
            if dashboard_id in to_read or previous is None:
                yield from self._process_dashboard_with_boundary(
                    project, dashboard_object, lineage_context, linked_report_ids
                )
                continue
            self._keep_dashboard(
                project.id,
                dashboard_object.id,
                previous,
                linked_report_ids,
                unchanged=True,
            )
        yield from self._read_dashboards_of_changed_reports(
            project, lineage_context, linked_report_ids
        )

    def _allowed_dashboards(self, project: Project) -> Iterable[MicroStrategyObject]:
        project_id = project.id
        for dashboard_object in self.client.search_dashboards(project_id):
            if not self.config.dashboard_pattern.allowed(dashboard_object.name):
                self.report.filtered_dashboards.append(dashboard_object.name)
                continue
            if self._skip_by_folder_path(
                project,
                dashboard_object,
                self.report.report_dashboard_filtered_by_folder_path,
            ):
                continue
            if self._skip_personal_object(project_id, dashboard_object):
                continue
            yield dashboard_object

    def _process_dashboard_with_boundary(
        self,
        project: Project,
        dashboard_object: MicroStrategyObject,
        lineage_context: "_LazyProjectLineage",
        linked_report_ids: Optional[Set[str]],
    ) -> Iterable[MetadataWorkUnit]:
        project_id = project.id
        self._changes.read_dashboards.add(normalize_object_id(dashboard_object.id))
        # Progress before the expensive per-dashboard work so the log never goes silent.
        logger.info(
            "Processing dashboard %r (%s) in project %s",
            dashboard_object.name,
            dashboard_object.id,
            project_id,
        )
        checkpoint = self._new_draft()
        try:
            yield from _recording_urns(
                self._process_dashboard_object(
                    project,
                    dashboard_object,
                    lineage_context,
                    linked_report_ids,
                    checkpoint,
                ),
                checkpoint,
            )
        except (MicroStrategyAuthError, ConfigurationError):
            # Not a per-dashboard problem; must abort the whole run.
            raise
        except Exception as error:
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
            if checkpoint is not None:
                checkpoint.outcome = MicroStrategyReadOutcome.FAILED
        finally:
            self._log_dashboard_progress()
        if checkpoint is None:
            return
        dashboard_id = normalize_object_id(dashboard_object.id)
        if checkpoint.outcome == MicroStrategyReadOutcome.FAILED:
            self._changes.failed_dashboards.add(dashboard_id)
        recorded = self._finish_read(
            checkpoint,
            self._object_state.previous_dashboard(project_id, dashboard_object.id),
            lambda previous: self._keep_dashboard(
                project_id,
                dashboard_object.id,
                previous,
                linked_report_ids,
                unchanged=False,
            ),
            lambda draft: self._object_state.record_dashboard(
                project_id, dashboard_object.id, draft.to_object_checkpoint()
            ),
        )
        if recorded and checkpoint.fingerprint is not None:
            self._changes.dashboard_fingerprints[dashboard_id] = checkpoint.fingerprint

    def _log_dashboard_progress(self) -> None:
        self._dashboards_processed += 1
        if self._dashboards_processed % MSTR_PROGRESS_LOG_INTERVAL_DASHBOARDS == 0:
            logger.info(
                "MicroStrategy progress: %d dashboards processed, %d datasets scanned",
                self._dashboards_processed,
                self.report.datasets_scanned,
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

    def _predefined_folders(self, project_id: str) -> PredefinedFolderResolution:
        """Predefined-folder resolution for a project: MicroStrategy-assigned
        labels (e.g. 'Shared Reports') plus the system containers Strategy Web
        never shows (project root folder, 'Public Objects'), cached per project
        since folder containers are (re)built once per dashboard/report."""
        if not self.config.use_predefined_folder_names:
            return PredefinedFolderResolution.empty()
        if project_id in self._predefined_folder_cache:
            return self._predefined_folder_cache[project_id]

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
        resolution = PredefinedFolderResolution(labels=labels, hidden_ids=hidden_ids)
        self._predefined_folder_cache[project_id] = resolution
        return resolution

    def _personal_folders(self, project_id: str) -> PersonalFolderResolution:
        """How to recognise personal (per-user profile) content in a project,
        resolved once per project. By id when possible: the predefined-folder
        call names the logged-in principal's own profile folder (type 19) and
        My Reports (type 20); the profile folder's immediate parent is the
        project's Profiles system folder, under which every user's profile
        folder lives. Any failure falls back to matching ancestor folder
        names, which is exact but breaks on localized or renamed folders."""
        if project_id in self._personal_folder_cache:
            return self._personal_folder_cache[project_id]

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
                    profiles_root_ids = self._profiles_root_ids(project_id, folder.id)
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
        self._personal_folder_cache[project_id] = resolution
        return resolution

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
        if not self.config.per_report:
            return None
        return self._report_datasets.get(_report_dataset_key(project_id, dataset_id))

    def _shared_report_dataset(
        self,
        project_id: str,
        dashboard_object: MicroStrategyObject,
        dataset: DatasetObject,
    ) -> DatasetObject:
        # Later dashboards share the first one's instance, so enrichment accumulates once.
        cache_key = _report_dataset_key(project_id, dataset.id)
        entry = self._report_datasets.get(cache_key)
        if entry is None:
            entry = _ReportDatasetEntry(dataset=dataset)
            self._report_datasets[cache_key] = entry
        elif entry.dataset.source_warehouse is None:
            entry.dataset.source_warehouse = dataset.source_warehouse
        # Set before the dashboard's charts are emitted, so a dashboard that fails
        # part-way cannot leave charts pointing at a dataset that is never emitted.
        entry.referenced_by_dashboard = True
        entry.bound_dashboard_ids.add(normalize_object_id(dashboard_object.id))
        return entry.dataset

    def _record_report_dataset_dashboard(
        self,
        project_id: str,
        dashboard_object: MicroStrategyObject,
        dashboard: DashboardDefinition,
    ) -> None:
        # Only once the dashboard is emitted, so a failed one is never listed in the
        # dataset's dashboard ids. Its warehouse lineage, derived metrics and
        # first-sighting schema stay on the shared dataset.
        for dataset in dashboard.datasets:
            entry = self._report_dataset_entry(project_id, dataset.id)
            if entry is None:
                self.report.warning(
                    title="Report Dataset Not Registered",
                    message=(
                        "A dashboard's dataset has no shared report dataset, so "
                        "the dashboard is not listed on it."
                    ),
                    context=(
                        f"project_id={project_id}, "
                        f"dashboard_id={dashboard_object.id}, dataset_id={dataset.id}"
                    ),
                )
                continue
            if entry.add_dashboard(dashboard_object):
                self.report.report_report_deduplicated()

    def _register_report_dataset(
        self,
        project_id: str,
        report_object: MicroStrategyObject,
        report_definition: Optional[ReportDefinition],
    ) -> "_ReportDatasetEntry":
        cache_key = _report_dataset_key(project_id, report_object.id)
        available_objects = (
            report_definition.available_objects if report_definition else {}
        )
        description = (
            report_definition.description if report_definition else None
        ) or report_object.description
        fresh = DatasetObject.from_available_objects(
            report_object.id, report_object.name, description, available_objects
        )
        entry = self._report_datasets.get(cache_key)
        if entry is None:
            entry = _ReportDatasetEntry(dataset=fresh)
            self._report_datasets[cache_key] = entry
        else:
            if available_objects:
                # Merged, not replaced: charts already emitted reference the dossier's fields.
                entry.dataset.available_objects = self.mapper.merge_dataset_fields(
                    fresh, entry.dataset
                )
                entry.dataset.object_ids = sorted(
                    {*fresh.object_ids, *entry.dataset.object_ids}
                )
                if self._drop_stale_field_lineage(entry.dataset):
                    entry.lineage_resolved = False
            if description:
                entry.dataset.description = description
        entry.report_object = report_object
        entry.report_definition = report_definition
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

    def _emits_report_dataset(self, entry: "_ReportDatasetEntry") -> bool:
        return entry.from_report_pass or (
            entry.referenced_by_dashboard and self.config.extract_cubes
        )

    def _gen_report_datasets_after_error(
        self, project_id: str
    ) -> Iterable[MetadataWorkUnit]:
        # The original error is re-raised by the caller, so a failure here is
        # reported rather than raised, or it would replace the original cause.
        try:
            yield from self._gen_report_dataset_workunits(project_id, cached_only=True)
        except Exception as emit_error:
            self.report.failure(
                title="Failed to Emit Report Datasets After Error",
                message=(
                    "Report datasets could not be emitted after the project failed; "
                    "stale-entity removal is skipped for this run."
                ),
                context=f"project_id={project_id}",
                exc=emit_error,
            )

    def _gen_report_dataset_workunits(
        self, project_id: str, cached_only: bool = False
    ) -> Iterable[MetadataWorkUnit]:
        # Once, after both passes, so the entity has everything the run learned.
        predefined_folders = self._predefined_folders(project_id)
        plan = self._changes.report_plan
        settled: Set[str] = set()
        project_keys = [
            key for key in self._report_datasets if key.project_id == project_id
        ]
        for cache_key in project_keys:
            entry = self._report_datasets.pop(cache_key)
            if not self._emits_report_dataset(entry):
                continue
            if plan is not None and not plan.emits(cache_key.report_id):
                continue
            settled.add(cache_key.report_id)
            if plan is not None:
                self._order_by_search(entry)
            checkpoint = self._new_draft()
            if checkpoint is not None:
                checkpoint.dashboard_ids = [
                    normalize_object_id(dashboard_id)
                    for dashboard_id in entry.dashboard_ids
                ]
                checkpoint.from_report_pass = entry.from_report_pass
            workunits = self._build_report_dataset_workunits(
                project_id, entry, predefined_folders, checkpoint, cached_only
            )
            if workunits is None:
                previous = self._object_state.previous_report_dataset(
                    project_id, entry.dataset.id
                )
                if checkpoint is not None and previous is not None:
                    self._keep_report_dataset(
                        project_id, entry.dataset.id, _without_fingerprint(previous)
                    )
                continue
            self.report.report_report_dataset_emitted()
            usage_target = RecordedUsageTarget(
                object_id=entry.dataset.id,
                kind=UsageTargetKind.DATASET,
                urn=self.lineage.report_dataset_urn(project_id, entry.dataset),
            )
            self._register_usage_target(
                usage_target.object_id, usage_target.kind, usage_target.urn
            )
            yield from workunits
            if checkpoint is not None:
                checkpoint.usage_targets = [usage_target]
                checkpoint.urns = self._tracked_urns(checkpoint.urns)
                self._object_state.record_report_dataset(
                    project_id,
                    entry.dataset.id,
                    checkpoint.to_report_dataset_checkpoint(),
                )
        self._keep_unemitted_report_datasets(project_id, plan, settled)

    def _keep_unemitted_report_datasets(
        self, project_id: str, plan: Optional[ReportDatasetPlan], settled: Set[str]
    ) -> None:
        unchanged = plan.kept_reports() if plan is not None else {}
        for report_id, previous in unchanged.items():
            self._keep_report_dataset(project_id, report_id, previous)
            self.report.report_datasets_skipped_unchanged += 1
        # A dashboard that failed to read may have been a dataset's only embedder.
        failed = self._changes.failed_dashboards
        for report_id, previous in self._object_state.previous_report_datasets(
            project_id
        ).items():
            if (
                report_id not in settled
                and report_id not in unchanged
                and failed.intersection(previous.dashboard_ids)
            ):
                self._keep_report_dataset(
                    project_id, report_id, _without_fingerprint(previous)
                )

    def _build_report_dataset_workunits(
        self,
        project_id: str,
        entry: "_ReportDatasetEntry",
        predefined_folders: PredefinedFolderResolution,
        checkpoint: Optional[CheckpointDraft],
        cached_only: bool,
    ) -> Optional[List[MetadataWorkUnit]]:
        # Built whole so a failure part-way emits nothing for this report.
        try:
            return list(
                self._report_dataset_entry_workunits(
                    project_id, entry, predefined_folders, checkpoint, cached_only
                )
            )
        except (MicroStrategyAuthError, ConfigurationError):
            # Swallowing these would let stale removal delete datasets charts use.
            raise
        except Exception as error:
            # A failure, not a warning, so stale removal skips this run instead of
            # soft-deleting a dataset that charts still point at.
            self.report.failure(
                title="Failed to Emit Report Dataset",
                message=(
                    "Skipping this report dataset after an unexpected error; "
                    "dashboards and charts that use it will show a missing "
                    "upstream, and stale-entity removal is skipped for this run. "
                    "Other report datasets will still be emitted."
                ),
                context=(
                    f"project_id={project_id}, report_id={entry.dataset.id}, "
                    f"report_name={entry.dataset.name}"
                ),
                exc=error,
            )
            return None

    def _report_dataset_entry_workunits(
        self,
        project_id: str,
        entry: "_ReportDatasetEntry",
        predefined_folders: PredefinedFolderResolution,
        checkpoint: Optional[CheckpointDraft],
        cached_only: bool,
    ) -> Iterable[MetadataWorkUnit]:
        dataset = entry.dataset
        dataset_object = entry.report_object
        if dataset_object is None and cached_only:
            # The session is gone, so a lookup would only fail again.
            cached = self._type3_objects.get(object_cache_key(project_id, dataset.id))
            if not isinstance(cached, Unset):
                dataset_object = cached.report_object
        elif dataset_object is None:
            dataset_object = self._dataset_object_info(project_id, dataset.id)
        fallback_key = (
            self.mapper.folder_container_for_dashboard(
                project_id, entry.first_dashboard_object, predefined_folders
            )
            if entry.first_dashboard_object is not None
            else self.mapper.project_key(project_id)
        )
        parent_key = yield from self._dataset_folder_workunits(
            project_id, dataset_object, fallback_key, predefined_folders, checkpoint
        )
        if checkpoint is not None:
            # No fingerprint once the session is gone, so the next run reads it again.
            previous = self._object_state.previous_report_dataset(
                project_id, dataset.id
            )
            fingerprinter = self._changes.fingerprinter
            if cached_only or self._report_dataset_incomplete(entry, previous):
                self._keep_previous_urns(checkpoint, previous)
            elif self._skips_unchanged() and fingerprinter is not None:
                checkpoint.fingerprint = fingerprinter.report_dataset_fingerprint(
                    self._fingerprint_object_info(project_id, dataset.id),
                    {
                        dashboard_id: self._changes.dashboard_fingerprints.get(
                            dashboard_id
                        )
                        for dashboard_id in checkpoint.dashboard_ids
                    },
                    entry.from_report_pass,
                )
        yield from _recording_urns(
            self.mapper.gen_report_dataset_workunits(
                project_id,
                dataset,
                parent_key,
                dataset_object=dataset_object,
                dashboard_ids=entry.dashboard_ids,
                report_definition=entry.report_definition,
                fingerprint=checkpoint.fingerprint if checkpoint is not None else None,
            ),
            checkpoint,
        )

    def _report_dataset_incomplete(
        self,
        entry: "_ReportDatasetEntry",
        previous: Optional[ReportDatasetCheckpoint],
    ) -> bool:
        # A dashboard read that missed lineage also missed it for the shared dataset.
        changes = self._changes
        failed_dashboards = changes.failed_dashboards
        embedders = set(entry.bound_dashboard_ids)
        if previous is not None:
            embedders.update(previous.dashboard_ids)
        return (
            entry.incomplete
            or normalize_object_id(entry.dataset.id) in changes.failed_reports
            or bool(failed_dashboards.intersection(embedders))
            or (
                self._skips_unchanged()
                and any(
                    normalize_object_id(dashboard_id)
                    not in changes.dashboard_fingerprints
                    for dashboard_id in entry.dashboard_ids
                )
            )
        )

    def _dataset_folder_workunits(
        self,
        project_id: str,
        dataset_object: Optional[MicroStrategyObject],
        fallback_key: ProjectKey,
        predefined_folders: Optional[PredefinedFolderResolution],
        checkpoint: Optional[CheckpointDraft] = None,
    ) -> Generator[MetadataWorkUnit, None, ProjectKey]:
        folder_object = self._folder_object(project_id, dataset_object)
        if folder_object is not None:
            yield from self._folder_containers(
                project_id, folder_object, predefined_folders, checkpoint
            )
        return self.mapper.dataset_folder_parent_key(
            project_id, folder_object, fallback_key, predefined_folders
        )

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

    def _process_dashboard_object(
        self,
        project: Project,
        dashboard_object: MicroStrategyObject,
        lineage_context: "_LazyProjectLineage",
        linked_report_ids: Optional[Set[str]] = None,
        checkpoint: Optional[CheckpointDraft] = None,
    ) -> Iterable[MetadataWorkUnit]:
        project_id = project.id
        predefined_folders = self._predefined_folders(project_id)
        yield from self._folder_containers(
            project_id, dashboard_object, predefined_folders, checkpoint
        )
        parent_key = self.mapper.folder_container_for_dashboard(
            project_id, dashboard_object, predefined_folders
        )
        model_lineage_index = lineage_context.model_lineage_index
        try:
            dashboard = self._get_dashboard_definition(
                project_id, dashboard_object, checkpoint
            )
            if dashboard is None:
                if checkpoint is not None:
                    checkpoint.outcome = MicroStrategyReadOutcome.FAILED
                return
            needs_sql_view_context = (
                self.config.extract_warehouse_lineage or model_lineage_index is not None
            )
            has_warehouse_context = lineage_context.warehouse_context or any(
                dataset.source_warehouse for dataset in dashboard.datasets
            )
            if (
                needs_sql_view_context
                and has_warehouse_context
                and not self._enrich_warehouse_lineage(
                    project_id=project_id,
                    dashboard_object=dashboard_object,
                    dashboard=dashboard,
                    context=lineage_context.warehouse_context,
                )
            ):
                _mark_incomplete(checkpoint)
        finally:
            # Last consumer of the shared instance is done.
            self._release_dashboard_instance(project_id, dashboard_object)
        if model_lineage_index:
            self.mapper.attach_model_lineage(dashboard, model_lineage_index)
        if lineage_context.lineage_degraded:
            _mark_incomplete(checkpoint)
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
        if self._dashboard_failed_transiently(project_id, dashboard):
            _mark_incomplete(checkpoint)
        extra_chart_urns = self._dashboard_report_chart_urns(project, dashboard)
        usage_target = RecordedUsageTarget(
            object_id=dashboard_object.id,
            kind=UsageTargetKind.DASHBOARD,
            urn=self.mapper.dashboard_urn(project_id, dashboard_object.id),
        )
        self._register_usage_target(
            usage_target.object_id, usage_target.kind, usage_target.urn
        )
        if checkpoint is not None:
            dependency_ids = _unique_object_ids(
                [dataset.id for dataset in dashboard.datasets] + report_dependency_ids
            )
            checkpoint.dependency_ids = dependency_ids
            checkpoint.linked_report_ids = sorted(
                {report_id.upper() for report_id in report_dependency_ids}
            )
            checkpoint.usage_targets = [usage_target]
            checkpoint.fingerprint = self._object_fingerprint(
                project, dashboard_object, dependency_ids, checkpoint
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
        if self.config.per_report:
            self._record_report_dataset_dashboard(
                project_id, dashboard_object, dashboard
            )

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
        return self._report_object(
            project_id,
            dataset_id,
            ReportObjectLookupKind.DATASET,
            lambda: self._fetch_dataset_object(project_id, dataset_id),
        )

    def _fingerprint_object_info(
        self, project_id: str, object_id: str
    ) -> Optional[MicroStrategyObject]:
        return self._fingerprint_objects.get_or_load(
            object_cache_key(project_id, object_id),
            lambda: _ReportObjectLookup.from_fetch(
                ReportObjectLookupKind.DATASET,
                self._fetch_dataset_object(project_id, object_id),
            ),
        ).report_object

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

    def _note_failure(
        self, project_id: str, object_id: str, *errors: Optional[Exception]
    ) -> None:
        if any(_is_transient_failure(error) for error in errors):
            self._transient_failure_keys.add(object_cache_key(project_id, object_id))

    def _failed_transiently(self, project_id: str, object_ids: Iterable[str]) -> bool:
        # Membership checks only: other workers add keys concurrently.
        return any(
            object_cache_key(project_id, object_id) in self._transient_failure_keys
            for object_id in object_ids
        )

    def _dashboard_failed_transiently(
        self, project_id: str, dashboard: DashboardDefinition
    ) -> bool:
        object_ids = [dashboard.id]
        for dataset in dashboard.datasets:
            object_ids.extend(_enrichment_ids(dataset))
        return self._failed_transiently(project_id, object_ids)

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
        return self._report_derived_metrics.get_or_load(
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
            self._note_failure(project_id, report_id, model_error, v2_error)
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
        return self._report_v2_payloads.get_or_load(
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
        for report_object in self.client.search_reports(project_id):
            self._remember_searched_report(project_id, report_object)
            if not self.config.report_pattern.allowed(report_object.name):
                self.report.filtered_reports.append(report_object.name)
                continue
            if self._skip_by_folder_path(
                project,
                report_object,
                self.report.report_report_filtered_by_folder_path,
            ):
                continue
            if self._skip_personal_object(project_id, report_object):
                continue
            yield from self._process_report_with_boundary(
                project,
                report_object,
                lineage_context,
            )

    def _remember_searched_report(
        self, project_id: str, report_object: MicroStrategyObject
    ) -> None:
        # A search result carries what an object lookup returns, so it replaces a failed one.
        self._type3_objects.put(
            object_cache_key(project_id, report_object.id),
            _ReportObjectLookup(report_object=report_object),
        )

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
        for report_id in sorted(linked_report_ids):
            report_object = self._linked_report_object(project_id, report_id)
            if report_object is None:
                continue
            if not self.config.report_pattern.allowed(report_object.name):
                self.report.filtered_reports.append(report_object.name)
                continue
            if self._skip_by_folder_path(
                project,
                report_object,
                self.report.report_report_filtered_by_folder_path,
            ):
                continue
            if self._skip_personal_object(project_id, report_object):
                continue
            yield from self._process_report_with_boundary(
                project,
                report_object,
                lineage_context,
            )

    def _process_report_with_boundary(
        self,
        project: Project,
        report_object: MicroStrategyObject,
        lineage_context: "_LazyProjectLineage",
    ) -> Iterable[MetadataWorkUnit]:
        project_id = project.id
        if self._changes.report_plan is not None and not (
            self._changes.report_plan.read_in_report_pass(
                normalize_object_id(report_object.id)
            )
        ):
            return
        # Progress before the expensive per-report work so the log never goes silent.
        logger.info(
            "Processing report %r (%s) in project %s",
            report_object.name,
            report_object.id,
            project_id,
        )
        checkpoint: Optional[CheckpointDraft] = None
        if not self.config.per_report:
            self._register_usage_target(
                report_object.id,
                UsageTargetKind.CHART,
                self.mapper.report_urn(project_id, report_object.id),
            )
            if self._skips_unchanged():
                previous = self._object_state.previous_report(
                    project_id, report_object.id
                )
                if previous is not None and self._evaluate_object(
                    project,
                    report_object,
                    previous,
                    self.mapper.report_urn,
                    ChartInfoClass,
                ):
                    self._keep_report(project_id, report_object.id, previous)
                    self.report.reports_skipped_unchanged += 1
                    return
            checkpoint = self._new_draft()
        try:
            yield from _recording_urns(
                self._process_report_object(
                    project,
                    report_object,
                    lineage_context,
                    checkpoint,
                ),
                checkpoint,
            )
        except (MicroStrategyAuthError, ConfigurationError):
            # Not a per-report problem; must abort the whole run.
            raise
        except Exception as error:
            entry = self._report_dataset_entry(project_id, report_object.id)
            self.report.warning(
                title="Failed to Process Report",
                message=(
                    "Report dataset will still be emitted, possibly without "
                    "lineage, after an unexpected error; other reports will "
                    "still be processed."
                    if entry is not None and self._emits_report_dataset(entry)
                    else "Skipping report after an unexpected error; other "
                    "reports will still be processed."
                ),
                context=(
                    f"project_id={project_id}, report_id={report_object.id}, "
                    f"report_name={report_object.name}"
                ),
                exc=error,
            )
            if checkpoint is not None:
                checkpoint.outcome = MicroStrategyReadOutcome.FAILED
            elif self.config.per_report:
                self._keep_failed_report_dataset(project_id, report_object.id)
        if checkpoint is None:
            return
        self._finish_read(
            checkpoint,
            self._object_state.previous_report(project_id, report_object.id),
            lambda previous: self._keep_report(project_id, report_object.id, previous),
            lambda draft: self._object_state.record_report(
                project_id, report_object.id, draft.to_object_checkpoint()
            ),
        )

    def _keep_failed_report_dataset(self, project_id: str, report_id: str) -> None:
        # A per_report report read records nothing itself: its dataset is built
        # once at the end of the project from the shared entry.
        self._changes.failed_reports.add(normalize_object_id(report_id))
        entry = self._report_dataset_entry(project_id, report_id)
        if entry is not None:
            entry.incomplete = True
            return
        previous = self._object_state.previous_report_dataset(project_id, report_id)
        if previous is not None:
            self._keep_report_dataset(
                project_id, report_id, _without_fingerprint(previous)
            )

    def _process_report_object(
        self,
        project: Project,
        report_object: MicroStrategyObject,
        lineage_context: "_LazyProjectLineage",
        checkpoint: Optional[CheckpointDraft] = None,
    ) -> Iterable[MetadataWorkUnit]:
        project_id = project.id
        if self.config.per_report:
            self._process_report_as_dataset(project_id, report_object, lineage_context)
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
        report_definition = self._fetch_report_definition(project_id, report_object)
        if report_definition is None:
            if self.config.extract_report_definitions:
                _mark_incomplete(checkpoint)
            report_definition = ReportDefinition.from_search_result(report_object)
        source_dataset = self._report_source_dataset(
            report_object,
            report_definition,
        )
        if source_dataset is not None:
            if not self._enrich_report_dataset(
                project_id, report_object, source_dataset, lineage_context
            ):
                _mark_incomplete(checkpoint)
            # A report without a source id gets a placeholder dataset id, which
            # names no catalog object to look up.
            dataset_object = (
                self._dataset_object_info(project_id, report_definition.source_id)
                if report_definition and report_definition.source_id
                else None
            )
            dataset_parent_key = yield from self._dataset_folder_workunits(
                project_id, dataset_object, parent_key, predefined_folders, checkpoint
            )
            yield from self.mapper.gen_report_source_dataset_workunits(
                project_id,
                report_object,
                source_dataset,
                dataset_parent_key,
                dataset_object=dataset_object,
            )
        if checkpoint is not None:
            dependency_ids = (
                [report_definition.source_id]
                if report_definition and report_definition.source_id
                else []
            )
            checkpoint.dependency_ids = dependency_ids
            checkpoint.usage_targets = [
                RecordedUsageTarget(
                    object_id=report_object.id,
                    kind=UsageTargetKind.CHART,
                    urn=self.mapper.report_urn(project_id, report_object.id),
                )
            ]
            checkpoint.fingerprint = self._object_fingerprint(
                project, report_object, dependency_ids, checkpoint
            )
        yield from self.mapper.gen_report_workunits(
            project_id,
            report_object,
            report_definition,
            source_dataset,
            parent_key,
            fingerprint=checkpoint.fingerprint if checkpoint is not None else None,
        )

    def _process_report_as_dataset(
        self,
        project_id: str,
        report_object: MicroStrategyObject,
        lineage_context: "_LazyProjectLineage",
    ) -> None:
        self.report.report_report_scanned()
        # Only a definition the API returned: the search-result fallback carries
        # prompt and filter values that were never read.
        report_definition = self._fetch_report_definition(project_id, report_object)
        complete = (
            report_definition is not None or not self.config.extract_report_definitions
        )
        entry = self._register_report_dataset(
            project_id, report_object, report_definition
        )
        complete = (
            self._enrich_report_dataset(
                project_id, report_object, entry.dataset, lineage_context, entry
            )
            and complete
        )
        if (
            self.config.extract_derived_metrics
            and (report_object.subtype or "").strip() not in MSTR_CUBE_SUBTYPES
        ):
            definitions = self._report_derived_metric_definitions(
                project_id, report_object.id
            )
            if definitions:
                self.mapper.attach_report_derived_metrics(entry.dataset, definitions)
        if self._failed_transiently(project_id, _enrichment_ids(entry.dataset)):
            complete = False
        if not complete:
            entry.incomplete = True

    def _enrich_report_dataset(
        self,
        project_id: str,
        report_object: MicroStrategyObject,
        dataset: DatasetObject,
        lineage_context: "_LazyProjectLineage",
        entry: Optional["_ReportDatasetEntry"] = None,
    ) -> bool:
        if self.config.extract_metric_expressions:
            self._enrich_dataset_metric_expressions(project_id, dataset)
        complete = self._enrich_report_dataset_sql_lineage(
            project_id, report_object, dataset, lineage_context, entry
        )
        model_lineage_index = lineage_context.model_lineage_index
        if model_lineage_index:
            self.mapper.attach_dataset_model_lineage(dataset, model_lineage_index)
        return complete and not lineage_context.lineage_degraded

    def _enrich_report_dataset_sql_lineage(
        self,
        project_id: str,
        report_object: MicroStrategyObject,
        dataset: DatasetObject,
        lineage_context: "_LazyProjectLineage",
        entry: Optional["_ReportDatasetEntry"],
    ) -> bool:
        # False when the report's SQL view failed or the execution limit skipped it.
        if not self.config.extract_report_sql_lineage:
            return True
        context = self._dataset_warehouse_context(
            project_id, dataset, lineage_context.warehouse_context
        )
        if not context:
            return True
        complete = self._enrich_report_sql_lineage(
            project_id=project_id,
            report_object=report_object,
            dataset=dataset,
            context=context,
            entry=entry,
        )
        return complete and not context.connection_unavailable

    def _get_dashboard_definition(
        self,
        project_id: str,
        dashboard_object: MicroStrategyObject,
        checkpoint: Optional[CheckpointDraft] = None,
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
        if self.config.per_report:
            # Before enrichment, so every dashboard writes onto the one shared instance.
            dashboard.datasets = [
                self._shared_report_dataset(project_id, dashboard_object, dataset)
                for dataset in dashboard.datasets
            ]
        if is_document_object(dashboard_object) and not dashboard.visualizations:
            self.report.report_document_definition_without_visualizations()
        if (
            self.config.extract_lineage
            and self.config.extract_visualization_details
            and not self._enrich_visualization_details(
                project_id, dashboard_object, dashboard
            )
        ):
            _mark_incomplete(checkpoint)
        if self.config.extract_lineage:
            self._resolve_visualization_bindings(project_id, dashboard)
        if (
            self.config.extract_dashboard_dependencies
            and not self._enrich_dashboard_dependencies(
                project_id=project_id,
                dashboard_object=dashboard_object,
                dashboard=dashboard,
            )
        ):
            _mark_incomplete(checkpoint)
        if self.config.extract_metric_expressions:
            self._enrich_metric_expressions(project_id, dashboard)
        return dashboard

    def _model_document(
        self, project_id: str, dashboard_id: str
    ) -> Optional[Dict[str, object]]:
        """GET /api/model/documents/{id}, read at most once per dashboard. None
        once it has failed in the project: Modeling access is project-scoped,
        so the failure is reported once rather than for every dashboard."""
        key = (project_id, dashboard_id)
        if self._model_document_key == key:
            return self._model_document_value
        if project_id in self._model_document_unavailable_projects:
            return None
        model_document: Optional[Dict[str, object]] = None
        try:
            model_document = self.client.get_model_document(project_id, dashboard_id)
        except MicroStrategyAuthError:
            raise
        except Exception as error:
            self._note_failure(project_id, dashboard_id, error)
            self.report.report_document_model_definition_failure()
            # Missing privilege or an unsupported endpoint applies to the whole
            # project; other failures (the endpoint answers 500 for some
            # documents) only affect this dashboard.
            project_wide = (
                isinstance(error, MicroStrategyAPIError)
                and error.status_code in _MODEL_DOCUMENT_PROJECT_WIDE_STATUSES
            )
            if project_wide:
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
        self._model_document_key = key
        self._model_document_value = model_document
        return model_document

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

    def _fetch_report_definition(
        self,
        project_id: str,
        report_object: MicroStrategyObject,
    ) -> Optional[ReportDefinition]:
        """The definition API's answer, None when it is off or failed."""
        if not self.config.extract_report_definitions:
            return None

        try:
            response = self._report_v2_payload(project_id, report_object.id)
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
            return None

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
        return DatasetObject.from_available_objects(
            report_definition.source_id or "source",
            report_definition.source_name or f"{report_object.name} Source",
            report_definition.description,
            report_definition.available_objects,
        )

    def fetch_project_model_tables(
        self, project_id: str, include_datasource: bool = False
    ) -> Optional[List[Dict[str, object]]]:
        """Paginate /api/model/tables for a project, also requesting each table's
        primary datasource when include_datasource is set.

        Returns None on API failure (already reported/warned); an empty list is
        a genuine "no model tables" result, distinct from a failed fetch.
        Shared by the classic per-report model-lineage path and the
        project-level semantic-model path, so it is fetched at most once per
        project regardless of how many consumers need it.
        """
        model_tables: List[Dict[str, object]] = []
        offset = 0
        fields = (
            MSTR_MODEL_TABLE_FIELDS_WITH_DATASOURCE
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
                if error.status_code in _MODEL_TABLES_NON_RETRYABLE_STATUSES:
                    self._model_tables_non_retryable_projects.add(project_id)
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
                # A second request returns the same payload.
                self._model_tables_non_retryable_projects.add(project_id)
                # [] would let stale removal soft-delete the semantic model.
                self.report.failure(
                    title="Unrecognized model tables response shape",
                    message=(
                        "The model tables API returned a payload without a "
                        "'tables' list; model lineage and the semantic model are "
                        "skipped for this project."
                    ),
                    context=f"project_id={project_id}, offset={offset}",
                )
                return None
            total = response.total
            if not tables:
                if isinstance(total, int) and offset < total:
                    # A partial list would drop the missing tables' lineage
                    # and let stale removal soft-delete their semantic model.
                    self.report.failure(
                        title="MicroStrategy Model Tables Truncated",
                        message=(
                            "The model tables API returned an empty page before "
                            "the reported total; model lineage and the semantic "
                            "model are skipped for this project."
                        ),
                        context=f"project_id={project_id}, offset={offset}, total={total}",
                    )
                    return None
                break
            model_tables.extend(table for table in tables if isinstance(table, dict))
            offset += len(tables)
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

    def build_project_model_lineage_index(
        self, lineage_context: "_LazyProjectLineage"
    ) -> Optional[ModelLineageIndex]:
        if not self.config.extract_lineage or not self.config.extract_model_lineage:
            return None
        # The unresolved warehouse was warned about when the contexts resolved;
        # with no platform there is nothing to resolve the tables against.
        if not lineage_context.contexts.supported:
            return None

        # Deferred until here (not called unconditionally by the caller) so the
        # fetch stays skipped when these flags are off, exactly as before.
        model_tables = lineage_context.model_tables
        if not model_tables:
            return None
        return self.lineage.model_lineage_index_from_tables(
            model_tables,
            lineage_context.table_context_resolver,
            graph=self.ctx.graph,
        )

    def _enrich_dashboard_dependencies(
        self,
        project_id: str,
        dashboard_object: MicroStrategyObject,
        dashboard: DashboardDefinition,
    ) -> bool:
        object_type = dashboard_object.type
        if not object_type:
            return True
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
            return False
        dashboard.dependencies = dependencies
        self.report.report_dashboard_dependencies_scanned(len(dependencies))
        return True

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

    def _get_metric_model(
        self,
        project_id: str,
        metric_id: str,
    ) -> Dict[str, object]:
        model = self._metric_models.get_or_load(
            object_cache_key(project_id, metric_id),
            lambda: self._fetch_metric_model(project_id, metric_id),
        )
        return model if model is not None else {}

    def _cached_metric_model(
        self, project_id: str, metric_id: str
    ) -> Optional[Dict[str, object]]:
        model = self._metric_models.get(object_cache_key(project_id, metric_id))
        return None if isinstance(model, Unset) else model

    def _fetch_metric_model(
        self, project_id: str, metric_id: str
    ) -> Optional[Dict[str, object]]:
        # None marks a failed fetch, already warned and counted.
        try:
            model = self.client.get_metric_model(project_id, metric_id)
        except MicroStrategyAPIError as error:
            self._note_failure(project_id, metric_id, error)
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

    def _enrich_visualization_details(
        self,
        project_id: str,
        dashboard_object: MicroStrategyObject,
        dashboard: DashboardDefinition,
    ) -> bool:
        # False when an API failure or the execution limit left details out.
        if not dashboard.visualizations:
            return True
        if self._execution_limit_reached(project_id):
            return False
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
            return False

        complete = True
        enriched = []
        for visualization in dashboard.visualizations:
            if not visualization.chapter_key:
                enriched.append(visualization)
                continue
            try:
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
                enriched.append(visualization)
                complete = False
                continue

            try:
                enriched.append(
                    Visualization.model_validate(
                        {
                            **visualization.raw,
                            "chapterKey": visualization.chapter_key,
                            "pageKey": visualization.page_key,
                            "pageName": visualization.page_name,
                            "runtimeDefinition": detail,
                        }
                    )
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
                enriched.append(visualization)
                complete = False
        dashboard.visualizations = enriched
        return complete

    def _enrich_warehouse_lineage(
        self,
        project_id: str,
        dashboard_object: MicroStrategyObject,
        dashboard: DashboardDefinition,
        context: Optional[WarehouseLineageContext],
    ) -> bool:
        # False when an API failure or the execution limit left lineage out.
        if not dashboard.datasets:
            return True
        if is_document_object(dashboard_object):
            # The dossier SQL-view endpoint answers a document instance with
            # views that carry no SQL (or a 406), so calling it would only
            # spend an execution; a document's datasets are read through
            # report instances instead, when enabled.
            if self.config.extract_document_dataset_sql_lineage:
                return self._enrich_document_warehouse_lineage(
                    project_id, dashboard, context
                )
            return True
        if self._execution_limit_reached(project_id):
            return False
        try:
            instance_id = self._dashboard_instance(project_id, dashboard_object)
        except MicroStrategyAPIError as error:
            self.report.report_warehouse_lineage_api_failure()
            self.report.warning(
                title="Failed to Create Dashboard Instance for Warehouse Lineage",
                message="Skipping SQL-view warehouse lineage for this dashboard.",
                context=f"project_id={project_id}, dashboard_id={dashboard.id}",
                exc=error,
            )
            return False

        try:
            sql_view_rows = self.client.get_dossier_datasets_sql(
                project_id=project_id,
                dossier_id=dashboard.id,
                instance_id=instance_id,
            )
        except MicroStrategyAPIError as error:
            self.report.report_warehouse_lineage_api_failure()
            self.report.warning(
                title="Failed to Fetch Dataset SQL View",
                message="Skipping source warehouse lineage for this dashboard.",
                context=f"project_id={project_id}, dashboard_id={dashboard.id}",
                exc=error,
            )
            return False

        self.report.report_warehouse_lineage_sql_views_scanned(len(sql_view_rows))
        entries = {
            normalize_object_id(dataset.id): self._report_dataset_entry(
                project_id, dataset.id
            )
            for dataset in dashboard.datasets
        }
        return self._attach_dataset_warehouse_upstreams(
            sql_view_rows, dashboard, context, entries, project_id
        )

    def _enrich_document_warehouse_lineage(
        self,
        project_id: str,
        dashboard: DashboardDefinition,
        context: Optional[WarehouseLineageContext],
    ) -> bool:
        """Warehouse lineage for a Report Services document's datasets. They
        are grid reports, so each one's SQL is read through its own report
        instance (the create / sqlView / delete sequence of
        extract_report_sql_lineage, one execution per distinct report per
        run) and attached to the document-scoped dataset. Cubes and datasets
        of unknown type have no report SQL view and are skipped and counted.
        False when a SQL view failed or the execution limit skipped one."""
        complete = True
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
            dataset_context = self._dataset_warehouse_context(
                project_id, dataset, context
            )
            if dataset_context is None:
                self.report.report_sql_view_row_without_context()
                continue
            if dataset_context.connection_unavailable:
                complete = False
            entry = self._report_dataset_entry(project_id, dataset.id)
            # A parsed SQL view means this report is not executed again.
            if entry is not None and entry.lineage_resolved:
                self.report.report_document_dataset_sql_view_reused()
                continue
            cache_key = object_cache_key(project_id, dataset.id)
            cached = self._report_sql_statements.get(cache_key)
            if not isinstance(cached, Unset):
                # Dataset URNs are document-scoped but the report's SQL is
                # not, so a report shared by many documents is read once and
                # its lineage attached to each; a remembered failure is not
                # retried and not counted again.
                sql_statement = cached.sql
                self.report.report_document_dataset_sql_view_reused()
            else:
                if self._execution_limit_reached(project_id):
                    complete = False
                    continue
                sql_statement = self._report_sql_statement(
                    project_id,
                    dataset.id,
                    context=(
                        f"project_id={project_id}, document_id={dashboard.id}, "
                        f"dataset_id={dataset.id}"
                    ),
                )
                self._report_sql_statements.put(
                    cache_key, _ReportSqlOutcome(sql=sql_statement)
                )
                if sql_statement is None:
                    self.report.report_document_dataset_sql_view_failure()
            if sql_statement is None:
                complete = False
            if not sql_statement:
                continue
            self.report.report_document_dataset_sql_view_scanned()
            self.report.report_warehouse_lineage_sql_views_scanned(1)
            lineage = self._attach_sql_view_lineage(
                sql_statement, dataset, dataset_context, entry
            )
            if lineage.upstream_urns:
                self.report.report_document_dataset_sql_view_parsed()
        return complete

    def _dataset_warehouse_context(
        self,
        project_id: str,
        dataset: DatasetObject,
        context: Optional[WarehouseLineageContext],
    ) -> Optional[WarehouseLineageContext]:
        """The dataset's own source warehouse when its definition names one,
        else the project-level context. A named warehouse that does not map
        to a platform gives None: the project's warehouse may be a different
        platform and database, and its URNs would not be this dataset's."""
        reference = dataset.source_warehouse
        if reference:
            # The project's entry carries the connection a dataset reference
            # can omit; model tables resolve through it, so must SQL views.
            datasource = matching_datasource(
                reference.id, reference.name, self._project_datasources
            )
            return self.datasource_warehouse_context(
                project_id, datasource or reference
            )
        return context

    def _attach_dataset_warehouse_upstreams(
        self,
        sql_view_rows: List[Dict[str, object]],
        dashboard: DashboardDefinition,
        context: Optional[WarehouseLineageContext],
        entries: Mapping[str, Optional["_ReportDatasetEntry"]],
        project_id: str,
    ) -> bool:
        # False when a connection lookup failed, leaving a context without its schema.
        complete = True
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

            dataset_context = self._dataset_warehouse_context(
                project_id, dataset, context
            )
            if dataset_context is None:
                self.report.report_sql_view_row_without_context()
                continue
            if dataset_context.connection_unavailable:
                complete = False

            sql_statement = sql_statement_from_sql_view_entry(row)
            if not sql_statement.strip():
                self.report.report_sql_view_without_statement()
                continue

            self._attach_sql_view_lineage(
                sql_statement,
                dataset,
                dataset_context,
                entries.get(normalize_object_id(dataset.id)),
            )
        return complete

    def _attach_sql_view_lineage(
        self,
        sql_statement: str,
        dataset: DatasetObject,
        context: WarehouseLineageContext,
        entry: Optional["_ReportDatasetEntry"],
    ) -> SqlViewLineage:
        # Callers are already gated by extract_report_sql_lineage /
        # extract_warehouse_lineage. A single parse yields both the table-level
        # upstreams and the column-level edges on top of them.
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
            if entry is not None:
                entry.lineage_resolved = True
        if result.field_upstreams:
            dataset.field_warehouse_upstreams = result.field_upstreams
        return result

    def _enrich_report_sql_lineage(
        self,
        project_id: str,
        report_object: MicroStrategyObject,
        dataset: DatasetObject,
        context: WarehouseLineageContext,
        entry: Optional["_ReportDatasetEntry"],
    ) -> bool:
        # False when the SQL view failed or the execution limit skipped it.
        if entry is not None and entry.lineage_resolved:
            # A dashboard already settled this report's lineage.
            return True
        cache_key = object_cache_key(project_id, report_object.id)
        cached = self._report_sql_statements.get(cache_key)
        if not isinstance(cached, Unset):
            # Already read this run as the dataset of a document.
            sql_statement = cached.sql
        else:
            if self._execution_limit_reached(project_id):
                return False
            sql_statement = self._report_sql_statement(
                project_id,
                report_object.id,
                context=f"project_id={project_id}, report_id={report_object.id}",
            )
            self._report_sql_statements.put(
                cache_key, _ReportSqlOutcome(sql=sql_statement)
            )
        if sql_statement is None:
            return False
        if not sql_statement:
            return True

        self.report.report_warehouse_lineage_sql_views_scanned(1)
        self._attach_sql_view_lineage(sql_statement, dataset, context, entry)
        return True

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
        memoize the result in _report_sql_statements."""
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
        self.report.report_sql_view_fetched()
        if not sql_statement:
            self.report.report_sql_view_without_statement()
        return sql_statement

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
        cached = self._dashboard_instances.get(dashboard_object.id)
        if isinstance(cached, MicroStrategyAPIError):
            raise cached
        if cached is not None:
            self.report.report_dashboard_instance_reused()
            return cached
        try:
            instance_id = self._create_dashboard_instance(
                project_id, dashboard_object, dashboard_object.id
            )
        except MicroStrategyAPIError as error:
            self._record_execution(project_id, error)
            self._dashboard_instances[dashboard_object.id] = error
            raise
        self._record_execution(project_id, None)
        self._dashboard_instances[dashboard_object.id] = instance_id
        return instance_id

    def _release_dashboard_instance(
        self,
        project_id: str,
        dashboard_object: MicroStrategyObject,
    ) -> None:
        cached = self._dashboard_instances.pop(dashboard_object.id, None)
        if isinstance(cached, str):
            self._delete_dashboard_instance(
                project_id, dashboard_object, dashboard_object.id, cached
            )

    def _execution_limit_reached(self, project_id: str) -> bool:
        """True once max_consecutive_execution_timeouts tripped for the
        project; the caller then skips its execution and counts the skip."""
        if project_id not in self._execution_limited_projects:
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
        return self._report_object(
            project_id,
            report_id,
            ReportObjectLookupKind.LINKED_REPORT,
            lambda: self._fetch_linked_report_object(project_id, report_id),
        )

    def _report_object(
        self,
        project_id: str,
        object_id: str,
        kind: ReportObjectLookupKind,
        fetch: Callable[[], Optional[MicroStrategyObject]],
    ) -> Optional[MicroStrategyObject]:
        cache_key = object_cache_key(project_id, object_id)
        lookup = self._type3_objects.get_or_load(
            cache_key, lambda: _ReportObjectLookup.from_fetch(kind, fetch())
        )
        if lookup.report_object is not None or kind in lookup.failed_kinds:
            return lookup.report_object
        # The other lookup kind failed. This one warns differently, so it
        # tries on its own rather than reusing that failure.
        retried = _ReportObjectLookup.from_fetch(kind, fetch())
        self._type3_objects.put(cache_key, retried.merged_with(lookup))
        return retried.report_object

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
        if not self.config.extract_reports or self.config.per_report:
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
        checkpoint: Optional[CheckpointDraft] = None,
    ) -> Iterable[MetadataWorkUnit]:
        if self.config.extract_cubes and not self.config.per_report:
            for dataset in dashboard.datasets:
                # A dataset is its own catalog object (report or cube) with its
                # own folder, which is usually not the dossier's folder.
                dataset_object = self._dataset_object_info(project_id, dataset.id)
                dataset_parent_key = yield from self._dataset_folder_workunits(
                    project_id,
                    dataset_object,
                    parent_key,
                    predefined_folders,
                    checkpoint,
                )
                yield from self.mapper.gen_dataset_workunits(
                    project_id,
                    dashboard,
                    dataset,
                    dataset_parent_key,
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
            fingerprint=checkpoint.fingerprint if checkpoint is not None else None,
        )

    def get_report(self) -> SourceReport:
        return self.report


class _ProjectChangeState(BaseModel):
    """What skip_unchanged_objects tracks within one project, keyed by
    normalized object id; filled in place while the project is processed."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    fingerprinter: Optional[ProjectFingerprinter] = None
    candidates: Dict[str, MicroStrategyObject] = Field(default_factory=dict)
    read_dashboards: Set[str] = Field(default_factory=set)
    dashboard_fingerprints: Dict[str, str] = Field(default_factory=dict)
    failed_dashboards: Set[str] = Field(default_factory=set)
    failed_reports: Set[str] = Field(default_factory=set)
    report_plan: Optional[ReportDatasetPlan] = None


class _ReportDatasetKey(BaseModel):
    model_config = ConfigDict(frozen=True)

    project_id: str
    report_id: str


class _ReportDatasetEntry(BaseModel):
    dataset: DatasetObject
    first_dashboard_object: Optional[MicroStrategyObject] = None
    dashboard_ids: List[str] = Field(default_factory=list)
    report_object: Optional[MicroStrategyObject] = None
    report_definition: Optional[ReportDefinition] = None
    lineage_resolved: bool = False
    referenced_by_dashboard: bool = False
    # Every dashboard that read this dataset, including ones that then failed.
    bound_dashboard_ids: Set[str] = Field(default_factory=set)
    # An API failure left part of this run's read out.
    incomplete: bool = False

    @property
    def from_report_pass(self) -> bool:
        return self.report_object is not None

    def add_dashboard(self, dashboard_object: MicroStrategyObject) -> bool:
        """Records a dashboard that uses the report. True when another dashboard
        already did, so this one is a repeat sighting of the same report."""
        if dashboard_object.id in self.dashboard_ids:
            return False
        repeat = bool(self.dashboard_ids)
        if not repeat:
            self.first_dashboard_object = dashboard_object
        self.dashboard_ids.append(dashboard_object.id)
        return repeat


def _report_dataset_key(project_id: str, dataset_id: str) -> _ReportDatasetKey:
    return _ReportDatasetKey(
        project_id=project_id, report_id=normalize_object_id(dataset_id)
    )


class _ReportObjectLookup(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    report_object: Optional[MicroStrategyObject] = None
    failed_kinds: FrozenSet[ReportObjectLookupKind] = frozenset()

    @classmethod
    def from_fetch(
        cls,
        kind: ReportObjectLookupKind,
        report_object: Optional[MicroStrategyObject],
    ) -> "_ReportObjectLookup":
        if report_object is not None:
            return cls(report_object=report_object)
        return cls(failed_kinds=frozenset({kind}))

    def merged_with(self, earlier: "_ReportObjectLookup") -> "_ReportObjectLookup":
        if self.report_object is not None:
            return self
        return _ReportObjectLookup(
            failed_kinds=self.failed_kinds | earlier.failed_kinds
        )


class _ReportSqlOutcome(BaseModel):
    """A report's SQL read; sql is None when the read failed, so the failure is
    remembered rather than retried."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    sql: Optional[str] = None


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
        self._contexts: Optional[ProjectWarehouseContexts] = None
        self._index: Optional[ModelLineageIndex] = None
        self._index_resolved = False
        self._model_tables: Optional[List[Dict[str, object]]] = None
        self._model_tables_resolved = False
        self._context_by_datasource_id: Dict[
            str, Optional[WarehouseLineageContext]
        ] = {}
        self._warned_unresolved_table = False
        # Logical table ids already counted: the cached table list is read by
        # both the lineage index and the semantic model, and refetched on retry.
        self._counted_tables: Set[str] = set()
        self._table_connection_unavailable = False
        self._lookup_failed = False

    def _resolve(self, part: str, fetch: Callable[[], _T]) -> Optional[_T]:
        """Run a project-level lookup once. An unexpected error is reported
        against the project, not the dashboard that happened to trigger it,
        and every later consumer gets None rather than retrying."""
        try:
            return fetch()
        except (MicroStrategyAuthError, ConfigurationError):
            raise
        except Exception as error:
            self._lookup_failed = True
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
    def source_warehouses(self) -> List[Datasource]:
        return self._source_warehouses

    @property
    def contexts(self) -> ProjectWarehouseContexts:
        # Resolved once even on failure (functools.cached_property would retry).
        if self._contexts is None:
            self._contexts = self._resolve(
                "warehouse_context", self._resolve_contexts
            ) or project_warehouse_contexts([])
        return self._contexts

    def _resolve_contexts(self) -> ProjectWarehouseContexts:
        if not self._source.warehouse_lineage_enabled():
            return project_warehouse_contexts([])
        contexts = project_warehouse_contexts(
            self._datasource_context(datasource)
            for datasource in self._source_warehouses
        )
        if contexts.single is None:
            self._source.warn_unresolved_project_warehouse(
                self._project_id, self._source_warehouses, contexts
            )
        return contexts

    def _datasource_context(
        self, datasource: Datasource
    ) -> Optional[WarehouseLineageContext]:
        key = normalize_object_id(datasource.id)
        if key not in self._context_by_datasource_id:
            self._context_by_datasource_id[key] = (
                self._source.datasource_warehouse_context(self._project_id, datasource)
            )
        return self._context_by_datasource_id[key]

    @property
    def warehouse_context(self) -> Optional[WarehouseLineageContext]:
        return self.contexts.single

    @property
    def table_context_resolver(self) -> TableContextResolver:
        single = self.contexts.single
        if single is not None:
            return lambda _: single
        if self.contexts.has_several:
            return self._table_context
        return lambda _: None

    def _table_context(
        self, table: Dict[str, object]
    ) -> Optional[WarehouseLineageContext]:
        datasource = datasource_for_model_table(table, self._source_warehouses)
        context = self._datasource_context(datasource) if datasource else None
        if context is not None and context.connection_unavailable:
            self._table_connection_unavailable = True
        return context

    def _count_table_contexts(self, tables: List[Dict[str, object]]) -> None:
        for table in tables:
            table_id = object_id(table.get("information"))
            if table_id:
                key = normalize_object_id(table_id)
                if key in self._counted_tables:
                    continue
                self._counted_tables.add(key)
            if self._table_context(table) is not None:
                self._source.report.report_model_table_resolved_by_datasource()
            else:
                self._report_unresolved_table()

    def _report_unresolved_table(self) -> None:
        if not self._warned_unresolved_table:
            self._warned_unresolved_table = True
            self._source.report.warning(
                title="MicroStrategy Logical Tables Without Source Warehouse",
                message=(
                    "Some logical tables name no matching primary datasource, "
                    "or one with no supported platform, so they are emitted "
                    "without warehouse lineage."
                ),
                context=(
                    f"project_id={self._project_id}, {MSTR_DATASOURCE_MAPPING_HINT}"
                ),
            )
        self._source.report.report_model_table_without_warehouse_context()

    @property
    def model_tables(self) -> Optional[List[Dict[str, object]]]:
        """Raw /api/model/tables rows, fetched at most once per project.

        Shared by model_lineage_index (below) and the semantic-model pass, so
        toggling both on for the same run costs one fetch, not two.
        """
        if not self._model_tables_resolved:
            self._model_tables_resolved = True
            self._model_tables = self._resolve("model_tables", self._fetch_tables)
        return self._model_tables

    def _fetch_tables(self) -> Optional[List[Dict[str, object]]]:
        several = self.contexts.has_several
        tables = self._source.fetch_project_model_tables(
            self._project_id, include_datasource=several
        )
        if tables and several:
            self._count_table_contexts(tables)
        return tables

    def model_tables_retrying_failure(self) -> Optional[List[Dict[str, object]]]:
        # The first fetch is hours earlier; one failure drops the semantic model.
        # A refusal such as a 403 is not worth a second request.
        if (
            self._model_tables_resolved
            and self._model_tables is None
            and self._project_id
            not in self._source._model_tables_non_retryable_projects
        ):
            logger.info(
                "Retrying the model tables fetch for the semantic model "
                "(project=%s); the earlier fetch failed",
                self._project_id,
            )
            self._model_tables_resolved = False
        return self.model_tables

    @property
    def model_lineage_index(self) -> Optional[ModelLineageIndex]:
        if not self._index_resolved:
            self._index_resolved = True
            self._index = self._resolve(
                "model_lineage_index",
                lambda: self._source.build_project_model_lineage_index(self),
            )
        return self._index

    @property
    def lineage_degraded(self) -> bool:
        """True when a project-level lookup failed, so an object read after the
        model lineage index was asked for is missing lineage even though the
        read itself raised nothing."""
        return (
            self._lookup_failed
            or (self._model_tables_resolved and self._model_tables is None)
            or self._table_connection_unavailable
            or self._source.connection_lookup_failed(self._project_id)
        )


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
    for candidate in object_ids:
        unique.setdefault(normalize_object_id(candidate), candidate)
    return [unique[key] for key in sorted(unique)]


def _recording_urns(
    workunits: Iterable[MetadataWorkUnit],
    checkpoint: Optional[CheckpointDraft],
) -> Iterable[MetadataWorkUnit]:
    for workunit in workunits:
        if checkpoint is not None and workunit.is_primary_source:
            checkpoint.urns.extend(_checkpoint_urns(workunit))
        yield workunit


def _mark_incomplete(checkpoint: Optional[CheckpointDraft]) -> None:
    if checkpoint is not None:
        checkpoint.mark_incomplete()


def _enrichment_ids(dataset: DatasetObject) -> List[str]:
    # The dataset's report definition and its metrics' models.
    return [
        dataset.id,
        *(
            str(metric.get("id") or "")
            for metric in _metric_items(dataset.available_objects)
        ),
    ]


def _is_transient_failure(error: Optional[Exception]) -> bool:
    # A timeout or dropped connection reaches callers without a status code.
    return isinstance(error, MicroStrategyAPIError) and (
        error.status_code is None or error.status_code in _TRANSIENT_FAILURE_STATUSES
    )


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


def _source_platform_summary(source_warehouses: List[Datasource]) -> str:
    types = Counter(
        datasource.database_type
        or datasource.datasource_type
        or datasource.dbms_name
        or MSTR_UNKNOWN_DATASOURCE_TYPE
        for datasource in source_warehouses
    )
    return "datasource_types=" + ", ".join(
        f"{name}:{count}" for name, count in sorted(types.items())
    )


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
