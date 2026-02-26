import logging
from contextlib import AbstractContextManager, nullcontext
from typing import Dict, Iterable, List, Literal, Optional, Union

from google.api_core.exceptions import (
    DeadlineExceeded,
    GoogleAPICallError,
    InvalidArgument,
    PermissionDenied,
    ServiceUnavailable,
    Unauthenticated,
)
from google.cloud import aiplatform, aiplatform_v1
from google.cloud.aiplatform.base import VertexAiResourceNoun
from google.cloud.aiplatform.models import Model
from google.cloud.aiplatform_v1 import MetadataServiceClient
from google.oauth2 import service_account

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
from datahub.ingestion.api.source import MetadataWorkUnitProcessor, SourceCapability
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.gcp_project_filter import (
    GcpProjectFilterConfig,
    resolve_gcp_projects,
)
from datahub.ingestion.source.common.subtypes import MLAssetSubTypes
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
)
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
    ExternalURLs,
    MLMetadataDefaults,
    ResourceCategory,
    ResourceCategoryType,
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
from datahub.ingestion.source.vertexai.vertexai_state import VertexAIStateHandler
from datahub.ingestion.source.vertexai.vertexai_training_extractor import (
    VertexAITrainingExtractor,
)
from datahub.ingestion.source.vertexai.vertexai_utils import (
    format_api_error_message,
    get_project_container,
    get_resource_category_container,
)
from datahub.metadata.schema_classes import (
    ContainerClass,
    DataPlatformInstanceClass,
    SubTypesClass,
)
from datahub.metadata.urns import DataPlatformUrn, VersionSetUrn
from datahub.utilities.ratelimiter import RateLimiter

logger = logging.getLogger(__name__)


@platform_name("Vertex AI", id=PLATFORM)
@config_class(VertexAIConfig)
@support_status(SupportStatus.INCUBATING)
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
        self.report: StaleEntityRemovalSourceReport = StaleEntityRemovalSourceReport()
        self.model_usage_tracker = ModelUsageTracker()

        # Initialize state handler for incremental ingestion
        self.state_handler = VertexAIStateHandler(
            source=self,
            stateful_ingestion_config=config.stateful_ingestion or config,
        )

        self._credentials = self._setup_credentials()
        self._projects = self._resolve_target_projects()
        self._project_to_regions = self._resolve_project_regions()

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
        """Resolve target GCP projects from config (single project or multi-project mode)."""
        wants_multi_project = bool(
            self.config.project_ids
            or self.config.project_labels
            or not self.config.project_id
        )
        if wants_multi_project:
            filter_cfg = GcpProjectFilterConfig(
                project_ids=self.config.project_ids,
                project_labels=self.config.project_labels,
                project_id_pattern=self.config.project_id_pattern,
            )
            resolved_projects = resolve_gcp_projects(filter_cfg, self.report)
            projects = [p.id for p in resolved_projects]
            if not projects and self.config.project_id:
                return [self.config.project_id]
            return projects
        return [self.config.project_id]

    def _resolve_project_regions(self) -> Dict[str, List[str]]:
        """Resolve regions for each project (either discover via API or use config)."""
        project_to_regions: Dict[str, List[str]] = {}

        if self.config.discover_regions:
            for project_id in self._projects:
                regions = self._discover_regions_for_project(project_id)
                project_to_regions[project_id] = (
                    regions if regions else [self.config.region]
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
            state_handler=self.state_handler,
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
            platform=self.platform,
            report=self.report,
            state_handler=self.state_handler,
            model_usage_tracker=self.model_usage_tracker,
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
            rate_limiter=self._rate_limiter,
        )

    def get_report(self) -> StaleEntityRemovalSourceReport:
        return self.report

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

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

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        for project_id in self._projects:
            self._current_project_id = project_id

            yield from self._gen_project_workunits()

            regions = self._project_to_regions.get(project_id, [self._get_region()])
            # Experiments are project-scoped, so extract them only for the first region
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
                            rate_limiter=self._rate_limiter,
                        )
                    except (
                        PermissionDenied,
                        Unauthenticated,
                        DeadlineExceeded,
                        ServiceUnavailable,
                        InvalidArgument,
                        ValueError,
                        GoogleAPICallError,
                    ) as e:
                        error_type = type(e).__name__
                        if isinstance(e, (PermissionDenied, Unauthenticated)):
                            reason = "permission issue"
                        elif isinstance(e, (DeadlineExceeded, ServiceUnavailable)):
                            reason = "timeout or service unavailable"
                        elif isinstance(e, InvalidArgument):
                            reason = "invalid configuration"
                        elif isinstance(e, ValueError):
                            reason = "configuration error"
                        else:
                            reason = "API error"
                        logger.warning(
                            f"Failed to initialize ML Metadata client for project {project_id} region {region} due to {reason} | cause={error_type}: {e}"
                        )
                        self._metadata_client = None
                        self._ml_metadata_helper = None

                # Process jobs/pipelines/experiments FIRST to track model usage
                # (for downstream lineage)
                if self.config.include_training_jobs:
                    yield from auto_workunit(self.training_extractor.get_workunits())

                # Experiments are project-scoped, not region-scoped, so only extract once
                if first_region and self.config.include_experiments:
                    yield from self.experiment_extractor.get_experiment_workunits()
                    yield from auto_workunit(
                        self.experiment_extractor.get_experiment_run_workunits()
                    )

                if self.config.include_pipelines:
                    yield from auto_workunit(self.pipeline_extractor.get_workunits())

                # Process models AFTER jobs so we can add downstream lineage
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

                # Mark that we've processed the first region
                first_region = False

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
        categories = [
            ResourceCategory.MODELS,
            ResourceCategory.TRAINING_JOBS,
            ResourceCategory.DATASETS,
            ResourceCategory.ENDPOINTS,
            ResourceCategory.PIPELINES,
            ResourceCategory.EXPERIMENTS,
            ResourceCategory.EVALUATIONS,
        ]

        for category in categories:
            category_key = self._get_resource_category_container(category)
            yield from gen_containers(
                parent_container_key=self._get_project_container(),
                container_key=category_key,
                name=category,
                sub_types=[MLAssetSubTypes.FOLDER],
            )

    def _get_project_id(self) -> str:
        return self._current_project_id or self.config.project_id

    def _get_region(self) -> str:
        return self._current_region or self.config.region

    def _discover_regions_for_project(self, project_id: str) -> List[str]:
        """Discover available Vertex AI regions for a project using Locations API."""
        try:
            client = aiplatform_v1.PipelineServiceClient(
                client_options={"api_endpoint": "aiplatform.googleapis.com"}
            )
            locations = client.list_locations(
                request={"name": f"projects/{project_id}"}
            )
            discovered: List[str] = []
            for loc in locations:
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
            return discovered
        except GoogleAPICallError as e:
            logger.warning(
                f"{format_api_error_message(e, f'discovering Vertex AI regions for project {project_id}', 'project', project_id)}; falling back to configured region(s)"
            )
            return []

    def _get_job_lineage_from_ml_metadata(
        self, job: VertexAiResourceNoun
    ) -> Optional[LineageMetadata]:
        if not self._ml_metadata_helper:
            return None
        return self._ml_metadata_helper.get_job_lineage_metadata(job)
