import logging
from contextlib import AbstractContextManager, nullcontext
from typing import Dict, Iterable, List, Literal, Optional, Union

from google.api_core.exceptions import GoogleAPICallError
from google.cloud import aiplatform, aiplatform_v1
from google.cloud.aiplatform.base import VertexAiResourceNoun
from google.cloud.aiplatform.models import Model
from google.cloud.aiplatform_v1 import MetadataServiceClient
from google.cloud.resourcemanager_v3 import ProjectsClient
from google.oauth2 import service_account

from datahub.configuration.env_vars import get_vertexai_disable_parallelism
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ProjectIdKey, gen_containers
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import SourceCapability
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.gcp_project_filter import (
    GcpProjectFilterConfig,
    resolve_gcp_projects,
)
from datahub.ingestion.source.common.subtypes import MLAssetSubTypes
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source.vertexai.vertexai_builder import (
    VertexAIExternalURLBuilder,
    VertexAINameFormatter,
    VertexAIURIParser,
    VertexAIUrnBuilder,
)
from datahub.ingestion.source.vertexai.vertexai_config import VertexAIConfig
from datahub.ingestion.source.vertexai.vertexai_constants import (
    PLATFORM,
    DetailCalls,
    ExternalURLs,
    MLMetadataDefaults,
    ResourceCategory,
    ResourceCategoryType,
    ResourceKinds,
    ResourceReads,
)
from datahub.ingestion.source.vertexai.vertexai_experiment_extractor import (
    VertexAIExperimentExtractor,
)
from datahub.ingestion.source.vertexai.vertexai_ml_metadata_helper import (
    MLMetadataHelper,
)
from datahub.ingestion.source.vertexai.vertexai_model_extractor import (
    VertexAIModelExtractor,
)
from datahub.ingestion.source.vertexai.vertexai_models import (
    LineageMetadata,
    MLMetadataConfig,
    ModelMetadata,
    ModelUsageTracker,
    VertexAIResourceCategoryKey,
)
from datahub.ingestion.source.vertexai.vertexai_pipeline_extractor import (
    VertexAIPipelineExtractor,
)
from datahub.ingestion.source.vertexai.vertexai_read_ledger import ReadLedger
from datahub.ingestion.source.vertexai.vertexai_report import VertexAISourceReport
from datahub.ingestion.source.vertexai.vertexai_scope import (
    CarryForward,
    DetailResult,
    ErrorKind,
    Scope,
    StoppedScopes,
    classify_error,
    scope_context,
    urn_entity_name,
)
from datahub.ingestion.source.vertexai.vertexai_state import VertexAIStateHandler
from datahub.ingestion.source.vertexai.vertexai_training_extractor import (
    VertexAITrainingExtractor,
)
from datahub.ingestion.source.vertexai.vertexai_utils import (
    create_vertex_retry_without_429,
    get_project_container,
    get_resource_category_container,
    patch_vertex_sdk_retry,
)
from datahub.metadata.schema_classes import (
    ContainerClass,
    DataPlatformInstanceClass,
    SubTypesClass,
)
from datahub.metadata.urns import DataPlatformUrn, VersionSetUrn
from datahub.utilities.ratelimiter import RateLimiter
from datahub.utilities.threaded_iterator_executor import ThreadedIteratorExecutor

logger = logging.getLogger(__name__)

_MODEL_STEP_KINDS = [
    ResourceKinds.MODEL,
    ResourceKinds.MODEL_GROUP,
    ResourceKinds.ENDPOINT,
    ResourceKinds.MODEL_EVALUATION,
]


@platform_name("Vertex AI", id=PLATFORM)
@config_class(VertexAIConfig)
@support_status(SupportStatus.BETA)
@capability(
    SourceCapability.DESCRIPTIONS,
    "Extract descriptions for Vertex AI Registered Models and Model Versions",
)
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Enabled by default: training job → model, dataset → training job, and cross-platform lineage to GCS, BigQuery, S3, Snowflake, and Azure Blob Storage",
)
@capability(
    SourceCapability.CONTAINERS,
    "Enabled by default: Experiments are modeled as Container entities; model groups and pipeline templates create folder containers",
)
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled by default via stateful ingestion",
)
@capability(
    SourceCapability.PLATFORM_INSTANCE,
    "Optionally set via the `platform_instance` config field to namespace resources when ingesting from multiple Vertex AI setups",
)
class VertexAISource(StatefulIngestionSourceBase):
    platform: Literal["vertexai"] = PLATFORM

    def __init__(self, ctx: PipelineContext, config: VertexAIConfig):
        super().__init__(config, ctx)
        self.config = config
        self.report: VertexAISourceReport = VertexAISourceReport()
        self.model_usage_tracker = ModelUsageTracker()

        # Initialize state handler for incremental ingestion
        self.state_handler = VertexAIStateHandler(
            source=self,
            stateful_ingestion_config=config.stateful_ingestion,
        )

        self._credentials = self._setup_credentials()
        self._stopped = StoppedScopes(rate_limit_enabled=config.rate_limit)
        self._carry = CarryForward(
            state_provider=self.state_provider,
            platform=self.platform,
            platform_instance=config.platform_instance,
            env=config.env,
            report=self.report,
        )
        self._ledger = ReadLedger(report=self.report, stopped=self._stopped)
        self._projects = self._resolve_target_projects()
        # Resolved when the run starts: the stale-removal checkpoint that
        # carry-forward writes to does not exist yet during __init__.
        self._project_to_regions: Dict[str, List[str]] = {}

        self._current_project_id: Optional[str] = None
        self._current_region: Optional[str] = None

        self.client = aiplatform
        self._metadata_client: Optional[MetadataServiceClient] = None
        self._ml_metadata_helper: Optional[MLMetadataHelper] = None

        self._rate_limiter: Union[RateLimiter, AbstractContextManager[None]] = (
            RateLimiter(max_calls=config.requests_per_min, period=60)
            if config.rate_limit
            else nullcontext()
        )

        self._custom_retry = create_vertex_retry_without_429()
        patch_vertex_sdk_retry(self._custom_retry)

        self._initialize_builders_and_extractors()

    def _setup_credentials(self) -> Optional[service_account.Credentials]:
        """Setup GCP service account credentials from config."""
        creds = self.config.get_credentials()
        return (
            service_account.Credentials.from_service_account_info(creds)
            if creds
            else None
        )

    def _resolve_target_projects(self) -> List[str]:
        """Resolve target GCP projects from config (single project or multi-project mode).

        After config validation, project_id is always migrated to project_ids, so
        project_id will be None in normal usage. Multi-project mode is always active.
        """
        filter_cfg = GcpProjectFilterConfig(
            project_ids=self.config.project_ids,
            project_labels=self.config.project_labels,
            project_id_pattern=self.config.project_id_pattern,
        )
        resolved_projects = resolve_gcp_projects(
            filter_cfg,
            self.report,
            projects_client=(
                ProjectsClient(credentials=self._credentials)
                if not self.config.project_ids
                else None
            ),
        )
        return [p.id for p in resolved_projects]

    def _resolve_project_regions(self) -> Dict[str, List[str]]:
        """Resolve regions for each project (either discover via API or use config)."""
        project_to_regions: Dict[str, List[str]] = {}

        if self.config.discover_regions:
            for project_id in self._projects:
                regions = self._discover_regions_for_project(project_id)
                project_to_regions[project_id] = (
                    regions
                    if regions
                    else ([self.config.region] if self.config.region else [])
                )
        else:
            if self.config.regions:
                regions_from_config = self.config.regions
            elif self.config.region:
                regions_from_config = [self.config.region]
            else:
                regions_from_config = []
            for project_id in self._projects:
                project_to_regions[project_id] = regions_from_config

        return project_to_regions

    def _initialize_builders_and_extractors(self) -> None:
        """Initialize URN builders and resource extractors."""
        self.urn_builder = VertexAIUrnBuilder(
            platform=self.platform,
            env=self.config.env,
            get_project_id_fn=self._get_project_id,
            platform_instance=self.config.platform_instance,
        )
        self.name_formatter = VertexAINameFormatter(
            get_project_id_fn=self._get_project_id
        )
        self.url_builder = VertexAIExternalURLBuilder(
            base_url=self.config.vertexai_url or ExternalURLs.BASE_URL,
            get_project_id_fn=self._get_project_id,
            get_region_fn=self._get_region,
        )
        self.uri_parser = VertexAIURIParser(
            env=self.config.env,
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            platform_instance_map=self.config.platform_instance_map,
        )

        self.pipeline_extractor = VertexAIPipelineExtractor(
            config=self.config,
            client=self.client,
            urn_builder=self.urn_builder,
            name_formatter=self.name_formatter,
            url_builder=self.url_builder,
            uri_parser=self.uri_parser,
            get_project_id_fn=self._get_project_id,
            yield_common_aspects_fn=self._yield_common_aspects,
            model_usage_tracker=self.model_usage_tracker,
            platform=self.platform,
            state_handler=self.state_handler,
            report=self.report,
            get_scope_fn=self._current_scope,
            stopped_scopes=self._stopped,
            carry_forward=self._carry,
            rate_limiter=self._rate_limiter,
        )
        self.experiment_extractor = VertexAIExperimentExtractor(
            config=self.config,
            urn_builder=self.urn_builder,
            name_formatter=self.name_formatter,
            url_builder=self.url_builder,
            uri_parser=self.uri_parser,
            get_project_id_fn=self._get_project_id,
            yield_common_aspects_fn=self._yield_common_aspects,
            model_usage_tracker=self.model_usage_tracker,
            platform=self.platform,
            report=self.report,
            state_handler=self.state_handler,
            get_scope_fn=self._current_scope,
            stopped_scopes=self._stopped,
            rate_limiter=self._rate_limiter,
        )
        self.training_extractor = VertexAITrainingExtractor(
            config=self.config,
            client=self.client,
            urn_builder=self.urn_builder,
            name_formatter=self.name_formatter,
            url_builder=self.url_builder,
            uri_parser=self.uri_parser,
            get_project_id_fn=self._get_project_id,
            yield_common_aspects_fn=self._yield_common_aspects,
            gen_ml_model_mcps_fn=self._gen_ml_model_mcps,
            get_job_lineage_from_ml_metadata_fn=self._get_job_lineage_from_ml_metadata,
            get_scope_fn=self._current_scope,
            platform=self.platform,
            report=self.report,
            state_handler=self.state_handler,
            model_usage_tracker=self.model_usage_tracker,
            stopped_scopes=self._stopped,
            carry_forward=self._carry,
            rate_limiter=self._rate_limiter,
        )
        self.model_extractor = VertexAIModelExtractor(
            config=self.config,
            client=self.client,
            urn_builder=self.urn_builder,
            name_formatter=self.name_formatter,
            url_builder=self.url_builder,
            get_project_id_fn=self._get_project_id,
            yield_common_aspects_fn=self._yield_common_aspects,
            model_usage_tracker=self.model_usage_tracker,
            platform=self.platform,
            state_handler=self.state_handler,
            report=self.report,
            get_scope_fn=self._current_scope,
            stopped_scopes=self._stopped,
            carry_forward=self._carry,
            rate_limiter=self._rate_limiter,
        )

    def get_report(self) -> VertexAISourceReport:
        return self.report

    def _yield_common_aspects(
        self,
        entity_urn: str,
        subtype: str,
        include_container: bool = True,
        include_platform: bool = True,
        resource_category: Optional[ResourceCategoryType] = None,
        include_subtypes: bool = True,
    ) -> Iterable[MetadataWorkUnit]:
        if include_container:
            if resource_category:
                container_urn = self._get_resource_category_container(
                    resource_category
                ).as_urn()
            else:
                container_urn = self._get_project_container().as_urn()

            yield MetadataChangeProposalWrapper(
                entityUrn=entity_urn,
                aspect=ContainerClass(container=container_urn),
            ).as_workunit()

        if include_subtypes:
            yield MetadataChangeProposalWrapper(
                entityUrn=entity_urn,
                aspect=SubTypesClass(typeNames=[subtype]),
            ).as_workunit()

        if include_platform:
            yield MetadataChangeProposalWrapper(
                entityUrn=entity_urn,
                aspect=DataPlatformInstanceClass(
                    platform=DataPlatformUrn(self.platform).urn(),
                    instance=self.config.platform_instance,
                ),
            ).as_workunit()

    def _current_scope(self) -> Scope:
        return Scope(project=self._get_project_id(), region=self._get_region())

    def _keep_project_entities(self, project_id: str) -> None:
        self._carry.keep_project(project_id)
        # Only these kinds name checkpointed entities: process instances never are.
        self._carry.keep_kind(project_id, [*_MODEL_STEP_KINDS, ResourceKinds.DATASET])
        self._keep_unscoped_entities(project_id)

    def _keep_unscoped_entities(self, project_id: str) -> None:
        # GUID containers, flows and jobs cannot be tied to a project, so a
        # project that was ingested before keeps all of them.
        self._carry.keep_all_containers(project_id)
        self._carry.keep_all_flows(project_id)

    def _report_project_unreadable(
        self, project_id: str, exc: Optional[BaseException]
    ) -> None:
        self.report.failure(
            title="Project could not be read",
            message="Nothing could be read from this project. Its earlier entities are kept and not soft-deleted.",
            context=scope_context(Scope(project=project_id), exc),
            exc=exc,
        )
        self._keep_project_entities(project_id)

    def _handle_project_error(self, project_id: str, exc: Exception) -> None:
        # Discovery returns every project the account can see, with no check
        # that the API is on or that a Vertex AI role is held, so nobody chose
        # to ingest a discovered project: no error of it fails the run.
        kind = classify_error(exc)
        listed = bool(self.config.project_ids)
        self.report.report_lost_read(
            project_id,
            scope_context(Scope(project=project_id), exc),
            temporary=kind is ErrorKind.TRANSIENT,
        )
        if not listed and kind is ErrorKind.PROJECT_WIDE:
            self.report.warning(
                title="Project skipped: API not enabled or project invalid",
                message="The Vertex AI API is disabled for this project, or the project is invalid; skipping it. Its earlier entities are kept.",
                context=scope_context(Scope(project=project_id), exc),
                exc=exc,
            )
            self._keep_project_entities(project_id)
        elif not listed and kind is ErrorKind.REFUSED:
            self.report.warning(
                title="Project skipped: no Vertex AI access",
                message="The service account has no Vertex AI role on this project; skipping it. Its earlier entities are kept. Grant a Vertex AI role or exclude the project with project_id_pattern.",
                context=scope_context(Scope(project=project_id), exc),
                exc=exc,
            )
            self._keep_project_entities(project_id)
        elif not listed and kind is ErrorKind.TRANSIENT:
            self.report.warning(
                title="Project skipped: could not be read",
                message="This discovered project could not be read because of a temporary error, so it is skipped for this run and read again on the next. Its earlier entities are kept.",
                context=scope_context(Scope(project=project_id), exc),
                exc=exc,
            )
            self._keep_project_entities(project_id)
        elif not listed:
            self.report.warning(
                title="Project skipped: could not be read",
                message="This discovered project could not be read, and the error repeats on every run. Its earlier entities are kept. Exclude the project with project_id_pattern if it should not be ingested.",
                context=scope_context(Scope(project=project_id), exc),
                exc=exc,
            )
            self._keep_project_entities(project_id)
        else:
            self._report_project_unreadable(project_id, exc)

    def _keep_phase1_kind(self, project_id: str, resource_type: str) -> None:
        if resource_type == ResourceReads.TRAINING_JOBS:
            self._carry.keep_kind(project_id, [ResourceKinds.DATASET])
        elif resource_type == ResourceReads.EXPERIMENTS:
            self._carry.keep_all_containers(project_id)
        elif resource_type == ResourceReads.PIPELINES:
            self._carry.keep_all_flows(project_id)
            self._carry.keep_all_containers(project_id)
        else:
            raise ValueError(f"Unknown phase 1 read: {resource_type}")

    def _keep_lost_phase1(self, project_id: str, region: str) -> None:
        for resource_type in self._ledger.lost_phase1_kinds(project_id, region):
            self._keep_phase1_kind(project_id, resource_type)

    def _hold_back_pending_lineage(self, project_id: str) -> None:
        """Links tracked for a held-back model stay in the tracker, but no
        lineage-only write may replace the model's stored properties."""
        prefix = f"{project_id}."
        self.model_usage_tracker.skip_pending_lineage(
            [
                urn
                for urn in self.model_usage_tracker.get_pending_lineage_urns()
                if (urn_entity_name(urn) or "").startswith(prefix)
            ]
        )

    def _keep_model_step(self, project_id: str) -> None:
        self._carry.keep_kind(project_id, _MODEL_STEP_KINDS)
        self._carry.keep_all_containers(project_id)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        self._project_to_regions = self._resolve_project_regions()
        for project_id in self._projects:
            if not self._project_to_regions.get(project_id):
                continue
            self._current_project_id = project_id
            try:
                yield from self._gen_workunits_for_project(project_id)
            except Exception as e:
                self._handle_project_error(project_id, e)
                self._ledger.record_lost_reads(project_id)
        # Flushing runs on this thread and a pass costs every earlier URN, so it
        # runs once, before the stale-removal diff that follows the stream.
        self._carry.flush()

    def _gen_workunits_for_project(self, project_id: str) -> Iterable[MetadataWorkUnit]:
        """Emit all workunits for a single project across its configured regions.

        The first resource is fetched before any container is emitted, so a project
        whose every read failed emits no orphan containers.
        """
        resources = iter(self._gen_project_resource_workunits(project_id))
        first = next(resources, None)
        # With nothing yielded, the reads are already all done.
        if first is not None or not self._ledger.every_read_failed(project_id):
            yield from self._gen_project_workunits()
        if first is not None:
            yield first
            yield from resources
        if self._ledger.every_read_failed(project_id):
            self._handle_project_error(
                project_id, self._ledger.representative_error(project_id)
            )
        else:
            self._ledger.report_lost_reads(
                project_id, listed=bool(self.config.project_ids)
            )
        self._ledger.record_lost_reads(project_id)

    def _gen_project_resource_workunits(
        self, project_id: str
    ) -> Iterable[MetadataWorkUnit]:
        """Fetch resource workunits for a project across its configured regions."""
        regions = self._project_to_regions.get(project_id, [self._get_region()])
        first_region = True
        for region in regions:
            logger.info(
                f"Initializing Vertex AI client for project={project_id} region={region}"
            )
            aiplatform.init(
                project=project_id,
                location=region,
                credentials=self._credentials,
            )
            self._current_region = region

            # Vertex resources are regional; reset before this region's fetch.
            self.training_extractor.reset_regional_caches()
            self.model_extractor.reset_regional_caches()

            if (
                self.config.use_ml_metadata_for_lineage
                or self.config.extract_execution_metrics
            ):
                try:
                    self._metadata_client = MetadataServiceClient(
                        client_options={
                            "api_endpoint": f"{region}-aiplatform.googleapis.com"
                        },
                        credentials=self._credentials,
                    )

                    ml_metadata_config = MLMetadataConfig(
                        project_id=project_id,
                        region=region,
                        metadata_store=MLMetadataDefaults.DEFAULT_METADATA_STORE,
                        enable_lineage_extraction=self.config.use_ml_metadata_for_lineage,
                        enable_metrics_extraction=self.config.extract_execution_metrics,
                        max_execution_search_limit=self.config.ml_metadata_max_execution_search_limit,
                    )

                    self._ml_metadata_helper = MLMetadataHelper(
                        metadata_client=self._metadata_client,
                        config=ml_metadata_config,
                        uri_parser=self.uri_parser,
                        report=self.report,
                        stopped_scopes=self._stopped,
                        scope=self._current_scope,
                        rate_limiter=self._rate_limiter,
                    )
                except (GoogleAPICallError, ValueError) as e:
                    self.report.warning(
                        title="ML Metadata client could not be created",
                        message="The ML Metadata client could not be created, so training jobs in this region are written without hyperparameters, metrics or ML Metadata lineage.",
                        context=scope_context(
                            Scope(
                                project=project_id,
                                region=region,
                                kind=DetailCalls.ML_METADATA,
                            ),
                            e,
                        ),
                        exc=e,
                    )
                    self._stopped.lost(
                        Scope(project_id, region, kind=DetailCalls.ML_METADATA),
                        classify_error(e),
                        e,
                    )
                    self._metadata_client = None
                    self._ml_metadata_helper = None

            # Phase 1: Fetch training jobs, pipelines, and experiments first.
            # These write to model_usage_tracker; models read from it in Phase 2.
            phase1_resources = []
            if self.config.include_training_jobs:
                phase1_resources.append((ResourceReads.TRAINING_JOBS,))
            # Experiments are project-scoped, so only the first region reads them.
            if first_region and self.config.include_experiments:
                phase1_resources.append((ResourceReads.EXPERIMENTS,))
            if self.config.include_pipelines:
                phase1_resources.append((ResourceReads.PIPELINES,))

            phase1_args = [
                (resource_type, project_id, region)
                for (resource_type,) in phase1_resources
            ]
            if not get_vertexai_disable_parallelism() and len(phase1_resources) > 1:
                max_workers = len(phase1_resources)
                logger.info(
                    "Fetching resources for project=%s region=%s with %d threads",
                    project_id,
                    region,
                    max_workers,
                )
                yield from ThreadedIteratorExecutor.process(
                    worker_func=self._read_phase1_resource,
                    args_list=phase1_args,
                    max_workers=max_workers,
                )
            else:
                for args in phase1_args:
                    yield from self._read_phase1_resource(*args)
            self._keep_lost_phase1(project_id, region)

            # Phase 2: Models must come AFTER Phase 1 (reads model_usage_tracker).
            if self.config.include_models or self.config.include_evaluations:
                held_back_by = self._ledger.hold_back_error(project_id, region)
                if held_back_by is not None:
                    self._lose_model_step(project_id, region, held_back_by)
                else:
                    yield from self._gen_model_step_workunits(project_id, region)

            if self._ledger.project_wide_error(project_id) is not None:
                # No region can get past this, so the rest are not read at all.
                if region != regions[-1]:
                    self._keep_unread_regions(project_id, first_region=first_region)
                break

            first_region = False

    def _keep_unread_regions(self, project_id: str, first_region: bool) -> None:
        if self.config.include_training_jobs:
            self._keep_phase1_kind(project_id, ResourceReads.TRAINING_JOBS)
        if first_region and self.config.include_experiments:
            self._keep_phase1_kind(project_id, ResourceReads.EXPERIMENTS)
        if self.config.include_pipelines:
            self._keep_phase1_kind(project_id, ResourceReads.PIPELINES)
        if self.config.include_models or self.config.include_evaluations:
            self._keep_model_step(project_id)

    def _lose_model_step(self, project_id: str, region: str, exc: Exception) -> None:
        self._ledger.record_read(project_id, region, ResourceReads.MODELS, error=exc)
        self._hold_back_pending_lineage(project_id)
        self._keep_model_step(project_id)

    def _gen_model_step_workunits(
        self, project_id: str, region: str
    ) -> Iterable[MetadataWorkUnit]:
        # Runs on the main thread, so a failure here costs only this region's
        # model step. Item and detail failures are decided inside the extractor.
        try:
            if self.config.include_models:
                yield from auto_workunit(self.model_extractor.get_model_workunits())

                # Emit lineage updates for models that have tracked lineage but
                # weren't re-processed (e.g., in incremental mode when a new pipeline
                # references an existing model that hasn't changed)
                yield from auto_workunit(
                    self.model_extractor.emit_pending_lineage_updates()
                )

            if self.config.include_evaluations:
                yield from auto_workunit(
                    self.model_extractor.get_evaluation_workunits()
                )
        except Exception as e:
            self._lose_model_step(project_id, region, e)
        else:
            self._ledger.record_read(project_id, region, ResourceReads.MODELS)

    def _read_phase1_resource(
        self, resource_type: str, project_id: str, region: str
    ) -> Iterable[MetadataWorkUnit]:
        # Runs in a worker thread: it must never let an exception escape, or the
        # executor drops the sibling workers' output.
        try:
            for workunit in self._fetch_phase1_resource(resource_type):
                self._ledger.record_wrote_output(project_id, region, resource_type)
                yield workunit
        except Exception as e:
            self._ledger.record_read(project_id, region, resource_type, error=e)
        else:
            self._ledger.record_read(project_id, region, resource_type)

    def _fetch_phase1_resource(self, resource_type: str) -> Iterable[MetadataWorkUnit]:
        """Fetch a single Phase 1 resource type (training jobs, experiments, or pipelines).

        Experiments and experiment runs are combined into one call because
        get_experiment_workunits() sets state that get_experiment_run_workunits() reads,
        so they must run sequentially in the same thread.
        """
        if resource_type == ResourceReads.TRAINING_JOBS:
            yield from auto_workunit(self.training_extractor.get_workunits())
        elif resource_type == ResourceReads.EXPERIMENTS:
            yield from self.experiment_extractor.get_experiment_workunits()
            yield from auto_workunit(
                self.experiment_extractor.get_experiment_run_workunits()
            )
        elif resource_type == ResourceReads.PIPELINES:
            yield from auto_workunit(self.pipeline_extractor.get_workunits())

    def _gen_project_workunits(self) -> Iterable[MetadataWorkUnit]:
        yield from gen_containers(
            container_key=self._get_project_container(),
            name=self._get_project_id(),
            sub_types=[MLAssetSubTypes.VERTEX_PROJECT],
        )
        yield from self._generate_resource_category_containers()

    def _gen_ml_model_mcps(
        self, model_metadata: ModelMetadata
    ) -> Iterable[MetadataWorkUnit]:
        return self.model_extractor._gen_ml_model_mcps(model_metadata)

    def _get_version_set_urn(self, model: Model) -> VersionSetUrn:
        return self.model_extractor._get_version_set_urn(model)

    def _get_project_container(self) -> ProjectIdKey:
        return get_project_container(
            self._get_project_id(),
            self.platform,
            self.config.platform_instance,
            self.config.env,
        )

    def _get_resource_category_container(
        self, category: ResourceCategoryType
    ) -> VertexAIResourceCategoryKey:
        return get_resource_category_container(
            self._get_project_id(),
            self.platform,
            self.config.platform_instance,
            self.config.env,
            category,
        )

    def _generate_resource_category_containers(self) -> Iterable[MetadataWorkUnit]:
        for category in ResourceCategory.all():
            category_key = self._get_resource_category_container(category)
            yield from gen_containers(
                parent_container_key=self._get_project_container(),
                container_key=category_key,
                name=category,
                sub_types=[MLAssetSubTypes.FOLDER],
            )

    def _get_project_id(self) -> str:
        if self._current_project_id:
            return self._current_project_id
        if self.config.project_ids:
            return self.config.project_ids[0]
        raise RuntimeError("No project ID configured")

    def _get_region(self) -> str:
        return self._current_region or self.config.region or ""

    def _discover_regions_for_project(self, project_id: str) -> List[str]:
        """Discover available Vertex AI regions for a project using Locations API."""
        try:
            client = aiplatform_v1.PipelineServiceClient(
                client_options={"api_endpoint": "aiplatform.googleapis.com"},
                credentials=self._credentials,
            )
            response = client.list_locations(request={"name": f"projects/{project_id}"})
            discovered: List[str] = []
            for loc in response.locations:
                # loc.name like projects/{project}/locations/{region}
                try:
                    parts = loc.name.split("/")
                    region = parts[parts.index("locations") + 1]
                    if region and region not in discovered:
                        discovered.append(region)
                except (ValueError, IndexError, AttributeError) as e:
                    logger.debug(
                        f"Failed to parse region from location name '{loc.name}' for project {project_id} | cause={type(e).__name__}: {e}"
                    )
                    continue
            if not discovered:
                raise ValueError("The location list holds no usable region")
            return discovered
        except Exception as e:
            if self.config.region:
                self.report.warning(
                    title="Region discovery failed",
                    message="Could not list regions (needs aiplatform.locations.list); using the configured region instead. List `regions` in the recipe to skip discovery.",
                    context=scope_context(Scope(project=project_id), e),
                    exc=e,
                )
                self._stopped.lost(
                    Scope(project_id, kind=DetailCalls.REGION_DISCOVERY),
                    classify_error(e),
                    e,
                )
                self._keep_project_entities(project_id)
            else:
                self._handle_project_error(project_id, e)
            return []

    def _get_job_lineage_from_ml_metadata(
        self, job: VertexAiResourceNoun
    ) -> Optional[DetailResult[Optional[LineageMetadata]]]:
        if not self._ml_metadata_helper:
            return None
        return self._ml_metadata_helper.get_job_lineage_metadata(job)
