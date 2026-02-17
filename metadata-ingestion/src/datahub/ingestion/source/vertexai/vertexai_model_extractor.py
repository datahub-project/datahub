import logging
from numbers import Real
from operator import attrgetter
from typing import Any, Dict, Iterable, List, Optional

from google.api_core.exceptions import (
    DeadlineExceeded,
    GoogleAPICallError,
    NotFound,
    PermissionDenied,
    ResourceExhausted,
    ServiceUnavailable,
    Unauthenticated,
)
from google.cloud.aiplatform import Endpoint, ModelEvaluation
from google.cloud.aiplatform.models import Model, VersionInfo

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import gen_containers
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.vertexai.vertexai_builder import (
    VertexAIExternalURLBuilder,
    VertexAINameFormatter,
    VertexAIUrnBuilder,
)
from datahub.ingestion.source.vertexai.vertexai_config import VertexAIConfig
from datahub.ingestion.source.vertexai.vertexai_constants import (
    MLModelType,
    ResourceCategory,
    VertexAISubTypes,
)
from datahub.ingestion.source.vertexai.vertexai_models import (
    EndpointDeploymentCustomProperties,
    MLModelCustomProperties,
    ModelEvaluationCustomProperties,
    ModelGroupKey,
    ModelMetadata,
    YieldCommonAspectsProtocol,
)
from datahub.ingestion.source.vertexai.vertexai_state import ModelUsageTracker
from datahub.ingestion.source.vertexai.vertexai_utils import (
    get_actor_from_labels,
    get_resource_category_container,
)
from datahub.metadata.schema_classes import (
    BaseDataClass,
    ContainerClass,
    MetadataAttributionClass,
    MLMetricClass,
    MLModelDeploymentPropertiesClass,
    MLModelGroupPropertiesClass,
    MLModelPropertiesClass,
    TimeStampClass,
    TrainingDataClass,
    VersionPropertiesClass,
    VersionTagClass,
)
from datahub.metadata.urns import MlModelUrn, VersionSetUrn
from datahub.utilities.time import datetime_to_ts_millis

logger = logging.getLogger(__name__)


class VertexAIModelExtractor:
    """Extracts model, model version, evaluation, and endpoint metadata from Vertex AI."""

    def __init__(
        self,
        config: VertexAIConfig,
        client: Any,  # aiplatform module
        urn_builder: VertexAIUrnBuilder,
        name_formatter: VertexAINameFormatter,
        url_builder: VertexAIExternalURLBuilder,
        project_id: str,
        yield_common_aspects_fn: YieldCommonAspectsProtocol,
        model_usage_tracker: ModelUsageTracker,
        platform: str,
    ):
        self.config = config
        self.client = client
        self.urn_builder = urn_builder
        self.name_formatter = name_formatter
        self.url_builder = url_builder
        self.project_id = project_id
        self._yield_common_aspects = yield_common_aspects_fn
        self.model_usage_tracker = model_usage_tracker
        self.platform = platform

        self.endpoints: Optional[Dict[str, List[Endpoint]]] = None

    def get_model_workunits(self) -> Iterable[MetadataWorkUnit]:
        """Main entry point for model extraction."""
        logger.info("Fetching Models from Vertex AI")
        registered_models = self.client.Model.list(order_by="update_time desc")
        total_versions = 0
        for model_idx, model in enumerate(registered_models, start=1):
            if not self.config.model_name_pattern.allowed(model.display_name or ""):
                continue
            yield from self._gen_ml_group_mcps(model)
            model_versions = model.versioning_registry.list_versions()
            for model_version in model_versions:
                total_versions += 1
                if total_versions % 100 == 0:
                    logger.info(
                        f"Processed {total_versions} model versions from Vertex AI"
                    )
                logger.debug(
                    f"Ingesting model version (name: {model.display_name} id:{model.name} version:{model_version.version_id})"
                )
                yield from self._get_ml_model_mcps(
                    model=model, model_version=model_version
                )

            if (
                self.config.max_models is not None
                and model_idx >= self.config.max_models
            ):
                break

        if total_versions > 0:
            logger.info(
                f"Finished processing {total_versions} model versions from Vertex AI"
            )

    def get_evaluation_workunits(self) -> Iterable[MetadataWorkUnit]:
        """Extract model evaluations."""
        logger.info("Fetching Models for evaluation extraction from Vertex AI")
        registered_models = self.client.Model.list(order_by="update_time desc")
        total_evaluations = 0

        for model in registered_models:
            if not self.config.model_name_pattern.allowed(model.display_name or ""):
                continue

            try:
                evaluations: List[ModelEvaluation] = list(
                    model.list_model_evaluations()
                )

                if self.config.max_evaluations_per_model is not None:
                    evaluations = evaluations[: self.config.max_evaluations_per_model]

                for evaluation in evaluations:
                    total_evaluations += 1
                    if total_evaluations % 100 == 0:
                        logger.info(
                            f"Processed {total_evaluations} model evaluations from Vertex AI"
                        )
                    logger.debug(
                        f"Ingesting evaluation for model {model.display_name}: {evaluation.name}"
                    )
                    yield from self._gen_model_evaluation_mcps(model, evaluation)

            except (PermissionDenied, Unauthenticated) as e:
                logger.warning(
                    f"Failed to fetch evaluations for model {model.display_name} due to permission issue | resource_type=model | resource_name={model.name} | cause={type(e).__name__}: {e}"
                )
            except ResourceExhausted as e:
                logger.warning(
                    f"Failed to fetch evaluations for model {model.display_name} due to quota exceeded | resource_type=model | resource_name={model.name} | cause={type(e).__name__}: {e}"
                )
            except (DeadlineExceeded, ServiceUnavailable) as e:
                logger.warning(
                    f"Failed to fetch evaluations for model {model.display_name} due to timeout or service unavailable | resource_type=model | resource_name={model.name} | cause={type(e).__name__}: {e}"
                )
            except NotFound as e:
                logger.debug(
                    f"Model evaluations not found for model {model.display_name} | resource_type=model | resource_name={model.name} | cause={type(e).__name__}: {e}"
                )
            except GoogleAPICallError as e:
                logger.warning(
                    f"Failed to fetch evaluations for model {model.display_name} | resource_type=model | resource_name={model.name} | cause={type(e).__name__}: {e}"
                )
                continue

        if total_evaluations > 0:
            logger.info(
                f"Finished processing {total_evaluations} model evaluations from Vertex AI"
            )

    def _extract_evaluation_metrics(
        self, evaluation: ModelEvaluation
    ) -> List[MLMetricClass]:
        """Extract metrics from a model evaluation."""
        metrics: List[MLMetricClass] = []
        if not getattr(evaluation, "metrics", None):
            return metrics

        try:
            if isinstance(evaluation.metrics, dict):
                for metric_name, metric_value in evaluation.metrics.items():
                    try:
                        if isinstance(metric_value, Real) and not isinstance(
                            metric_value, bool
                        ):
                            metrics.append(
                                MLMetricClass(name=metric_name, value=str(metric_value))
                            )
                        elif isinstance(metric_value, str):
                            try:
                                float(metric_value)
                                metrics.append(
                                    MLMetricClass(name=metric_name, value=metric_value)
                                )
                            except ValueError:
                                pass
                    except (AttributeError, TypeError) as e:
                        logger.debug(
                            f"Skipping metric {metric_name} due to error: {e}",
                            exc_info=True,
                        )
            else:
                for field_name in dir(evaluation.metrics):
                    if not field_name.startswith("_"):
                        try:
                            field_value = getattr(evaluation.metrics, field_name)
                            if isinstance(field_value, Real) and not isinstance(
                                field_value, bool
                            ):
                                metrics.append(
                                    MLMetricClass(
                                        name=field_name, value=str(field_value)
                                    )
                                )
                        except (AttributeError, TypeError) as e:
                            logger.debug(
                                f"Skipping metric {field_name} due to error: {e}",
                                exc_info=True,
                            )
        except (AttributeError, TypeError) as e:
            logger.warning(f"Failed to extract evaluation metrics: {e}", exc_info=True)

        return metrics

    def _get_evaluation_model_urn(self, model: Model) -> Optional[str]:
        """Get the URN for a model's latest version."""
        try:
            versions: List[VersionInfo] = list(
                model.versioning_registry.list_versions()
            )
            if versions:
                latest_version = max(versions, key=attrgetter("version_id"))
                model_name = self.name_formatter.format_model_name(entity_id=model.name)
                return self.urn_builder.make_ml_model_urn(latest_version, model_name)
        except (PermissionDenied, Unauthenticated) as e:
            logger.debug(
                f"Could not get versioned URN for model {model.name} due to permission issue, using simple URN | resource_type=model | resource_name={model.name} | cause={type(e).__name__}: {e}"
            )
        except (NotFound, AttributeError) as e:
            logger.debug(
                f"Could not get versioned URN for model {model.name} (versioning not available), using simple URN | resource_type=model | resource_name={model.name} | cause={type(e).__name__}: {e}"
            )
        except GoogleAPICallError as e:
            logger.debug(
                f"Could not get versioned URN for model {model.name}, using simple URN | resource_type=model | resource_name={model.name} | cause={type(e).__name__}: {e}"
            )

        model_name = self.name_formatter.format_model_name(entity_id=model.name)
        return builder.make_ml_model_urn(
            platform=self.platform,
            model_name=model_name,
            env=self.config.env,
        )

    def _gen_model_evaluation_mcps(
        self, model: Model, evaluation: ModelEvaluation
    ) -> Iterable[MetadataWorkUnit]:
        """Generate MCPs for a model evaluation."""
        model_urn = self._get_evaluation_model_urn(model)
        if not model_urn:
            logger.warning(
                f"Skipping evaluation {evaluation.name} - could not determine model URN"
            )
            return

        evaluation_urn = builder.make_ml_model_deployment_urn(
            platform=self.platform,
            deployment_name=self.name_formatter.format_evaluation_name(
                entity_id=evaluation.name
            ),
            env=self.config.env,
        )

        created_time = 0
        if getattr(evaluation, "create_time", None):
            created_time = datetime_to_ts_millis(evaluation.create_time)

        yield MetadataChangeProposalWrapper(
            entityUrn=evaluation_urn,
            aspect=MLModelDeploymentPropertiesClass(
                description=f"Evaluation for model {model.display_name}",
                createdAt=created_time,
                customProperties=ModelEvaluationCustomProperties(
                    evaluation_id=evaluation.name,
                    model_name=model.display_name,
                    model_resource_name=model.resource_name,
                ).to_custom_properties(),
            ),
        ).as_workunit()

        yield from self._yield_common_aspects(
            entity_urn=evaluation_urn,
            subtype=VertexAISubTypes.MODEL_EVALUATION,
            resource_category=ResourceCategory.MODELS,
            include_subtypes=False,
        )

    def _get_ml_model_mcps(
        self, model: Model, model_version: VersionInfo
    ) -> Iterable[MetadataWorkUnit]:
        """Generate MCPs for a model version."""
        endpoints = self._search_endpoint(model)
        yield from self._gen_ml_model_mcps(
            ModelMetadata(
                model=model,
                model_version=model_version,
                endpoints=endpoints if endpoints else None,
            )
        )
        if endpoints:
            yield from self._gen_endpoints_mcps(
                ModelMetadata(
                    model=model,
                    model_version=model_version,
                    endpoints=endpoints,
                )
            )

    def _gen_ml_group_mcps(self, model: Model) -> Iterable[MetadataWorkUnit]:
        """Generate container and MLModelGroup mcp for a VertexAI Model."""
        model_group_container_key = self._get_model_group_container(model)

        yield from gen_containers(
            parent_container_key=get_resource_category_container(
                self.project_id,
                self.platform,
                self.config.platform_instance,
                self.config.env,
                ResourceCategory.MODELS,
            ),
            container_key=model_group_container_key,
            name=model.display_name,
            sub_types=[VertexAISubTypes.MODEL_GROUP],
        )

        ml_model_group_urn = self.urn_builder.make_ml_model_group_urn(model)

        yield MetadataChangeProposalWrapper(
            entityUrn=ml_model_group_urn,
            aspect=MLModelGroupPropertiesClass(
                name=model.display_name,
                description=model.description,
                created=(
                    TimeStampClass(
                        time=datetime_to_ts_millis(model.create_time),
                        actor=get_actor_from_labels(getattr(model, "labels", None)),
                    )
                    if model.create_time
                    else None
                ),
                lastModified=(
                    TimeStampClass(
                        time=datetime_to_ts_millis(model.update_time),
                        actor=get_actor_from_labels(getattr(model, "labels", None)),
                    )
                    if model.update_time
                    else None
                ),
                customProperties=None,
                externalUrl=self.url_builder.make_model_url(model.name),
            ),
        ).as_workunit()

        yield from self._yield_common_aspects(
            entity_urn=ml_model_group_urn,
            subtype=VertexAISubTypes.MODEL_GROUP,
            resource_category=ResourceCategory.MODELS,
        )

    def _get_model_group_container(self, model: Model) -> ModelGroupKey:
        """Get container key for model group."""

        return ModelGroupKey(
            project_id=self.project_id,
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
            model_group_name=self.name_formatter.format_model_group_name(model.name),
        )

    def _gen_endpoints_mcps(
        self, model_meta: ModelMetadata
    ) -> Iterable[MetadataWorkUnit]:
        """Generate MCPs for model endpoints."""
        model: Model = model_meta.model
        model_version: VersionInfo = model_meta.model_version

        if model_meta.endpoints:
            for endpoint in model_meta.endpoints:
                endpoint_urn = builder.make_ml_model_deployment_urn(
                    platform=self.platform,
                    deployment_name=self.name_formatter.format_endpoint_name(
                        entity_id=endpoint.name
                    ),
                    env=self.config.env,
                )

                yield MetadataChangeProposalWrapper(
                    entityUrn=endpoint_urn,
                    aspect=MLModelDeploymentPropertiesClass(
                        description=model.description,
                        createdAt=datetime_to_ts_millis(endpoint.create_time),
                        version=VersionTagClass(
                            versionTag=str(model_version.version_id)
                        ),
                        customProperties=EndpointDeploymentCustomProperties(
                            display_name=endpoint.display_name,
                        ).to_custom_properties(),
                    ),
                ).as_workunit()

                yield from self._yield_common_aspects(
                    entity_urn=endpoint_urn,
                    subtype=VertexAISubTypes.ENDPOINT,
                    resource_category=ResourceCategory.ENDPOINTS,
                    include_subtypes=False,
                )

    def _build_endpoint_urns(self, endpoints: Optional[List[Endpoint]]) -> List[str]:
        """Build endpoint deployment URNs from endpoints list."""
        endpoint_urns: List[str] = []
        if endpoints:
            for endpoint in endpoints:
                logger.info(f"found endpoint ({endpoint.display_name}) for model")
                endpoint_urns.append(
                    builder.make_ml_model_deployment_urn(
                        platform=self.platform,
                        deployment_name=self.name_formatter.format_endpoint_name(
                            entity_id=endpoint.display_name
                        ),
                        env=self.config.env,
                    )
                )
        return endpoint_urns

    def _create_ml_model_properties_aspect(
        self,
        model: Model,
        model_version: VersionInfo,
        model_urn: str,
        model_group_urn: str,
        training_job_urn: Optional[str],
        downstream_job_urns: Optional[List[str]],
        endpoint_urns: List[str],
    ) -> MetadataWorkUnit:
        """Create MLModelProperties aspect workunit."""
        return MetadataChangeProposalWrapper(
            entityUrn=model_urn,
            aspect=MLModelPropertiesClass(
                name=f"{model.display_name}_{model_version.version_id}",
                description=model_version.version_description,
                customProperties=MLModelCustomProperties(
                    version_id=str(model_version.version_id),
                    resource_name=model.resource_name,
                ).to_custom_properties(),
                created=(
                    TimeStampClass(
                        time=datetime_to_ts_millis(model_version.version_create_time),
                        actor=get_actor_from_labels(getattr(model, "labels", None)),
                    )
                    if model_version.version_create_time
                    else None
                ),
                lastModified=(
                    TimeStampClass(
                        time=datetime_to_ts_millis(model_version.version_update_time),
                        actor=get_actor_from_labels(getattr(model, "labels", None)),
                    )
                    if model_version.version_update_time
                    else None
                ),
                version=VersionTagClass(versionTag=str(model_version.version_id)),
                groups=[model_group_urn],
                trainingJobs=([training_job_urn] if training_job_urn else None),
                downstreamJobs=(downstream_job_urns if downstream_job_urns else None),
                deployments=endpoint_urns,
                externalUrl=self.url_builder.make_model_version_url(
                    model.name, model.version_id
                ),
                type=MLModelType.ML_MODEL,
            ),
        ).as_workunit()

    def _create_version_properties_aspect(
        self, model: Model, model_version: VersionInfo, model_urn: str
    ) -> MetadataWorkUnit:
        """Create VersionProperties aspect workunit."""
        return MetadataChangeProposalWrapper(
            entityUrn=model_urn,
            aspect=VersionPropertiesClass(
                version=VersionTagClass(
                    versionTag=str(model_version.version_id),
                    metadataAttribution=(
                        MetadataAttributionClass(
                            time=int(
                                model_version.version_create_time.timestamp() * 1000
                            ),
                            actor=get_actor_from_labels(getattr(model, "labels", None))
                            or builder.UNKNOWN_USER,
                        )
                        if model_version.version_create_time
                        else None
                    ),
                ),
                versionSet=self._get_version_set_urn(model).urn(),
                sortId=str(model_version.version_id).zfill(10),
                aliases=None,
            ),
        ).as_workunit()

    def _create_training_data_aspect(
        self, model_urn: str, training_data_urns: List[str]
    ) -> MetadataWorkUnit:
        """Create TrainingData aspect workunit."""
        return MetadataChangeProposalWrapper(
            entityUrn=model_urn,
            aspect=TrainingDataClass(
                trainingData=[
                    BaseDataClass(dataset=ds_urn) for ds_urn in training_data_urns
                ]
            ),
        ).as_workunit()

    def _gen_ml_model_mcps(
        self, ModelMetadata: ModelMetadata
    ) -> Iterable[MetadataWorkUnit]:
        """Generate an MLModel and Endpoint mcp for an VertexAI Model Version."""
        model: Model = ModelMetadata.model
        model_version: VersionInfo = ModelMetadata.model_version
        training_job_urn: Optional[str] = ModelMetadata.training_job_urn
        endpoints: Optional[List[Endpoint]] = ModelMetadata.endpoints

        logging.info(f"generating model mcp for {model.name}")

        endpoint_urns = self._build_endpoint_urns(endpoints)
        model_group_urn = self.urn_builder.make_ml_model_group_urn(model)
        model_name = self.name_formatter.format_model_name(entity_id=model.name)
        model_urn = self.urn_builder.make_ml_model_urn(
            model_version, model_name=model_name
        )
        downstream_job_urns = self.model_usage_tracker.get_downstream_jobs(model_urn)

        yield self._create_ml_model_properties_aspect(
            model,
            model_version,
            model_urn,
            model_group_urn,
            training_job_urn,
            downstream_job_urns,
            endpoint_urns,
        )

        model_group_container_urn = self._get_model_group_container(model).as_urn()
        yield MetadataChangeProposalWrapper(
            entityUrn=model_urn,
            aspect=ContainerClass(container=model_group_container_urn),
        ).as_workunit()

        yield from self._yield_common_aspects(
            entity_urn=model_urn,
            subtype=VertexAISubTypes.MODEL,
            include_container=False,
        )

        yield self._create_version_properties_aspect(model, model_version, model_urn)

        training_data_urns = ModelMetadata.training_data_urns
        if training_data_urns:
            yield self._create_training_data_aspect(model_urn, training_data_urns)

    def _get_version_set_urn(self, model: Model) -> VersionSetUrn:
        """Get version set URN for a model."""
        guid_dict = {"platform": self.platform, "name": model.name}
        version_set_urn = VersionSetUrn(
            id=builder.datahub_guid(guid_dict),
            entity_type=MlModelUrn.ENTITY_TYPE,
        )
        return version_set_urn

    def _search_endpoint(self, model: Model) -> List[Endpoint]:
        """Search for an endpoint associated with the model."""
        if self.endpoints is None:
            logger.info("Fetching Endpoints from Vertex AI")
            endpoint_dict: Dict[str, List[Endpoint]] = {}
            for endpoint in self.client.Endpoint.list():
                for resource in endpoint.list_models():
                    if resource.model not in endpoint_dict:
                        endpoint_dict[resource.model] = []
                    endpoint_dict[resource.model].append(endpoint)
            self.endpoints = endpoint_dict

        return self.endpoints.get(model.resource_name, [])
