import logging
from typing import Dict, Iterable, List, Optional

from google.api_core.exceptions import (
    DeadlineExceeded,
    GoogleAPICallError,
    InvalidArgument,
    PermissionDenied,
    ServiceUnavailable,
    Unauthenticated,
)
from google.cloud import aiplatform, aiplatform_v1
from google.cloud.aiplatform import (
    Endpoint,
)
from google.cloud.aiplatform.base import VertexAiResourceNoun
from google.cloud.aiplatform.metadata.experiment_resources import Experiment
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
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source.vertexai.ml_metadata_helper import MLMetadataHelper
from datahub.ingestion.source.vertexai.vertexai_builder import (
    VertexAIExternalURLBuilder,
    VertexAINameFormatter,
    VertexAIURIParser,
    VertexAIUrnBuilder,
)
from datahub.ingestion.source.vertexai.vertexai_config import VertexAIConfig
from datahub.ingestion.source.vertexai.vertexai_constants import (
    ExternalURLs,
    MLMetadataDefaults,
    ResourceCategory,
    VertexAISubTypes,
)
from datahub.ingestion.source.vertexai.vertexai_experiment_extractor import (
    VertexAIExperimentExtractor,
)
from datahub.ingestion.source.vertexai.vertexai_model_extractor import (
    VertexAIModelExtractor,
)
from datahub.ingestion.source.vertexai.vertexai_models import (
    LineageMetadata,
    MLMetadataConfig,
    ModelMetadata,
    VertexAIResourceCategoryKey,
)
from datahub.ingestion.source.vertexai.vertexai_pipeline_extractor import (
    VertexAIPipelineExtractor,
)
from datahub.ingestion.source.vertexai.vertexai_state import ModelUsageTracker
from datahub.ingestion.source.vertexai.vertexai_training_extractor import (
    VertexAITrainingExtractor,
)
from datahub.ingestion.source.vertexai.vertexai_utils import (
    get_project_container,
    get_resource_category_container,
)
from datahub.metadata.schema_classes import (
    ContainerClass,
    DataPlatformInstanceClass,
    SubTypesClass,
)
from datahub.metadata.urns import DataPlatformUrn, VersionSetUrn

logger = logging.getLogger(__name__)


@platform_name("Vertex AI", id="vertexai")
@config_class(VertexAIConfig)
@support_status(SupportStatus.INCUBATING)
@capability(
    SourceCapability.DESCRIPTIONS,
    "Extract descriptions for Vertex AI Registered Models and Model Versions",
)
class VertexAISource(StatefulIngestionSourceBase):
    platform: str = "vertexai"

    def __init__(self, ctx: PipelineContext, config: VertexAIConfig):
        super().__init__(config, ctx)
        self.config = config
        self.report: StaleEntityRemovalSourceReport = StaleEntityRemovalSourceReport()
        self.model_usage_tracker = ModelUsageTracker()

        creds = self.config.get_credentials()
        credentials = (
            service_account.Credentials.from_service_account_info(creds)
            if creds
            else None
        )
        self._credentials = credentials

        # Determine target projects
        self._projects: List[str]
        wants_multi_project = bool(
            config.project_ids or config.project_labels or not config.project_id
        )
        if wants_multi_project:
            filter_cfg = GcpProjectFilterConfig(
                project_ids=config.project_ids,
                project_labels=config.project_labels,
                project_id_pattern=config.project_id_pattern,
            )
            resolved_projects = resolve_gcp_projects(filter_cfg, self.report)
            self._projects = [p.id for p in resolved_projects]
            if not self._projects and config.project_id:
                self._projects = [config.project_id]
        else:
            self._projects = [config.project_id]

        self._project_to_regions: Dict[str, List[str]] = {}
        if self.config.discover_regions:
            for project_id in self._projects:
                regions = self._discover_regions_for_project(project_id)
                self._project_to_regions[project_id] = (
                    regions if regions else [self.config.region]
                )
        else:
            regions_from_config = (
                self.config.regions
                if self.config.regions
                else ([self.config.region] if self.config.region else [])
            )
            for project_id in self._projects:
                self._project_to_regions[project_id] = regions_from_config

        # dynamic context for current project/region during iteration
        self._current_project_id: Optional[str] = None
        self._current_region: Optional[str] = None

        self.client = aiplatform
        self.endpoints: Optional[Dict[str, List[Endpoint]]] = None
        self.datasets: Optional[Dict[str, VertexAiResourceNoun]] = None
        self.experiments: Optional[List[Experiment]] = None

        self._metadata_client: Optional[MetadataServiceClient] = None
        self._ml_metadata_helper: Optional[MLMetadataHelper] = None

        self.urn_builder = VertexAIUrnBuilder(
            platform=self.platform,
            env=self.config.env,
            project_id=self._get_project_id(),
            platform_instance=self.config.platform_instance,
        )
        self.name_formatter = VertexAINameFormatter(project_id=self._get_project_id())
        self.url_builder = VertexAIExternalURLBuilder(
            base_url=self.config.vertexai_url or ExternalURLs.BASE_URL,
            project_id=self._get_project_id(),
            region=self._get_region(),
        )
        self.uri_parser = VertexAIURIParser(
            env=self.config.env,
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            platform_to_instance_map=self.config.platform_to_instance_map,
        )

        self.pipeline_extractor = VertexAIPipelineExtractor(
            config=self.config,
            client=self.client,
            urn_builder=self.urn_builder,
            name_formatter=self.name_formatter,
            url_builder=self.url_builder,
            uri_parser=self.uri_parser,
            project_id=self._get_project_id(),
            yield_common_aspects_fn=self._yield_common_aspects,
            model_usage_tracker=self.model_usage_tracker,
            platform=self.platform,
        )
        self.experiment_extractor = VertexAIExperimentExtractor(
            config=self.config,
            urn_builder=self.urn_builder,
            name_formatter=self.name_formatter,
            url_builder=self.url_builder,
            uri_parser=self.uri_parser,
            project_id=self._get_project_id(),
            yield_common_aspects_fn=self._yield_common_aspects,
            model_usage_tracker=self.model_usage_tracker,
            platform=self.platform,
        )
        self.training_extractor = VertexAITrainingExtractor(
            config=self.config,
            client=self.client,
            urn_builder=self.urn_builder,
            name_formatter=self.name_formatter,
            url_builder=self.url_builder,
            uri_parser=self.uri_parser,
            project_id=self._get_project_id(),
            yield_common_aspects_fn=self._yield_common_aspects,
            gen_ml_model_mcps_fn=self._gen_ml_model_mcps,
            get_job_lineage_from_ml_metadata_fn=self._get_job_lineage_from_ml_metadata,
            platform=self.platform,
            report=self.report,
        )
        self.model_extractor = VertexAIModelExtractor(
            config=self.config,
            client=self.client,
            urn_builder=self.urn_builder,
            name_formatter=self.name_formatter,
            url_builder=self.url_builder,
            project_id=self._get_project_id(),
            yield_common_aspects_fn=self._yield_common_aspects,
            model_usage_tracker=self.model_usage_tracker,
            platform=self.platform,
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
        resource_category: Optional[str] = None,
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
        """
        Main Function to fetch and yields mcps for various VertexAI resources.
        - Models and Model Versions from the Model Registry
        - Training Jobs
        """
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
                self.endpoints = None
                self.datasets = None
                self.experiments = None

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
                        )
                    except (PermissionDenied, Unauthenticated) as e:
                        logger.warning(
                            f"Failed to initialize ML Metadata client for project {project_id} region {region} due to permission issue | cause={type(e).__name__}: {e}"
                        )
                        self._metadata_client = None
                        self._ml_metadata_helper = None
                    except (DeadlineExceeded, ServiceUnavailable) as e:
                        logger.warning(
                            f"Failed to initialize ML Metadata client for project {project_id} region {region} due to timeout or service unavailable | cause={type(e).__name__}: {e}"
                        )
                        self._metadata_client = None
                        self._ml_metadata_helper = None
                    except InvalidArgument as e:
                        logger.warning(
                            f"Failed to initialize ML Metadata client for project {project_id} region {region} due to invalid configuration | cause={type(e).__name__}: {e}"
                        )
                        self._metadata_client = None
                        self._ml_metadata_helper = None
                    except ValueError as e:
                        logger.warning(
                            f"Failed to initialize ML Metadata client for project {project_id} region {region} due to configuration error | cause={type(e).__name__}: {e}"
                        )
                        self._metadata_client = None
                        self._ml_metadata_helper = None
                    except GoogleAPICallError as e:
                        logger.warning(
                            f"Failed to initialize ML Metadata client for project {project_id} region {region} | cause={type(e).__name__}: {e}"
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
            sub_types=[VertexAISubTypes.PROJECT],
        )
        yield from self._generate_resource_category_containers()

    def _gen_ml_model_mcps(
        self, ModelMetadata: ModelMetadata
    ) -> Iterable[MetadataWorkUnit]:
        """Delegate to model extractor."""
        return self.model_extractor._gen_ml_model_mcps(ModelMetadata)

    def _get_version_set_urn(self, model: Model) -> VersionSetUrn:
        """Delegate to model extractor."""
        return self.model_extractor._get_version_set_urn(model)

    def _get_project_container(self) -> ProjectIdKey:
        """Get project container key."""
        return get_project_container(
            self._get_project_id(),
            self.platform,
            self.config.platform_instance,
            self.config.env,
        )

    def _get_resource_category_container(
        self, category: str
    ) -> VertexAIResourceCategoryKey:
        """Get resource category container key."""
        return get_resource_category_container(
            self._get_project_id(),
            self.platform,
            self.config.platform_instance,
            self.config.env,
            category,
        )

    def _generate_resource_category_containers(self) -> Iterable[MetadataWorkUnit]:
        """Generate all resource category containers for the current project."""
        categories = [
            ResourceCategory.MODELS,
            ResourceCategory.TRAINING_JOBS,
            ResourceCategory.DATASETS,
            ResourceCategory.ENDPOINTS,
            ResourceCategory.PIPELINES,
            ResourceCategory.EVALUATIONS,
        ]

        for category in categories:
            category_key = self._get_resource_category_container(category)
            yield from gen_containers(
                parent_container_key=self._get_project_container(),
                container_key=category_key,
                name=category,
                sub_types=[DatasetContainerSubTypes.FOLDER],
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
        except (PermissionDenied, Unauthenticated) as e:
            logger.warning(
                f"Failed to discover Vertex AI regions for project {project_id} due to permission issue; falling back to configured region(s) | cause={type(e).__name__}: {e}"
            )
            return []
        except (DeadlineExceeded, ServiceUnavailable) as e:
            logger.warning(
                f"Failed to discover Vertex AI regions for project {project_id} due to timeout or service unavailable; falling back to configured region(s) | cause={type(e).__name__}: {e}"
            )
            return []
        except GoogleAPICallError as e:
            logger.warning(
                f"Failed to discover Vertex AI regions for project {project_id}; falling back to configured region(s) | cause={type(e).__name__}: {e}"
            )
            return []

    def _get_job_lineage_from_ml_metadata(
        self, job: VertexAiResourceNoun
    ) -> Optional[LineageMetadata]:
        if not self._ml_metadata_helper:
            return None
        return self._ml_metadata_helper.get_job_lineage_metadata(job)
