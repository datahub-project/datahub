import logging
from contextlib import AbstractContextManager, nullcontext
from numbers import Real
from operator import attrgetter
from typing import Any, Callable, Dict, Iterable, List, Optional, Union

from google.cloud.aiplatform import Endpoint, ModelEvaluation
from google.cloud.aiplatform.models import Model, VersionInfo

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import MLAssetSubTypes
from datahub.ingestion.source.vertexai.vertexai_builder import (
    VertexAIExternalURLBuilder,
    VertexAINameFormatter,
    VertexAIUrnBuilder,
)
from datahub.ingestion.source.vertexai.vertexai_config import VertexAIConfig
from datahub.ingestion.source.vertexai.vertexai_constants import (
    CREATE_TIME_FIELD,
    ORDER_BY_UPDATE_TIME_DESC,
    VERSION_ID_FIELD,
    VERSION_SORT_ID_PADDING,
    MLModelType,
    ResourceCategory,
    ResourceTypes,
)
from datahub.ingestion.source.vertexai.vertexai_models import (
    EndpointDeploymentCustomProperties,
    MLModelCustomProperties,
    ModelEvaluationCustomProperties,
    ModelGroupKey,
    ModelMetadata,
    ModelUsageTracker,
    YieldCommonAspectsProtocol,
)
from datahub.ingestion.source.vertexai.vertexai_state import VertexAIStateHandler
from datahub.ingestion.source.vertexai.vertexai_utils import (
    filter_by_update_time,
    gen_resource_subfolder_container,
    get_actor_from_labels,
    handle_google_api_errors,
    log_checkpoint_time,
    log_progress,
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
from datahub.utilities.ratelimiter import RateLimiter
from datahub.utilities.time import datetime_to_ts_millis

logger = logging.getLogger(__name__)


class VertexAIModelExtractor:
    def __init__(
        self,
        config: VertexAIConfig,
        client: Any,  # aiplatform module
        urn_builder: VertexAIUrnBuilder,
        name_formatter: VertexAINameFormatter,
        url_builder: VertexAIExternalURLBuilder,
        get_project_id_fn: Callable[[], str],
        yield_common_aspects_fn: YieldCommonAspectsProtocol,
        model_usage_tracker: ModelUsageTracker,
        platform: str,
        state_handler: VertexAIStateHandler,
        rate_limiter: Union[RateLimiter, AbstractContextManager[None]] = nullcontext(),
    ):
        self.config = config
        self.client = client
        self.urn_builder = urn_builder
        self.name_formatter = name_formatter
        self.url_builder = url_builder
        self._get_project_id = get_project_id_fn
        self._yield_common_aspects = yield_common_aspects_fn
        self.model_usage_tracker = model_usage_tracker
        self.platform = platform
        self.state_handler = state_handler
        self.rate_limiter = rate_limiter

        self.endpoints: Optional[Dict[str, List[Endpoint]]] = None
        self._models_cache: Optional[List] = None

    def _list_models(self) -> List:
        if self._models_cache is None:
            with self.rate_limiter:
                self._models_cache = self.client.Model.list(
                    order_by=ORDER_BY_UPDATE_TIME_DESC
                )
        return self._models_cache

    def get_model_workunits(self) -> Iterable[MetadataWorkUnit]:
        logger.info("Fetching Models from Vertex AI")
        # Reset so each region gets its own fetch; get_evaluation_workunits reuses
        # the cache while still in the same region.
        self._models_cache = None

        last_checkpoint_millis = self.state_handler.get_last_update_time(
            ResourceTypes.MODEL
        )
        if last_checkpoint_millis:
            log_checkpoint_time(last_checkpoint_millis, "Model")

        registered_models = self._list_models()
        total_versions = 0
        for model_idx, model in enumerate(registered_models, start=1):
            if last_checkpoint_millis:
                model_update_millis = int(model.update_time.timestamp() * 1000)
                if model_update_millis <= last_checkpoint_millis:
                    logger.info(
                        f"Reached checkpoint timestamp for Models, stopping early (processed {model_idx - 1} new models)"
                    )
                    break

                self.state_handler.update_resource_timestamp(
                    ResourceTypes.MODEL, model_update_millis
                )

            if not self.config.model_name_pattern.allowed(model.display_name or ""):
                continue

            yield from self._gen_ml_group_container(model)

            with self.rate_limiter:
                model_versions = list(model.versioning_registry.list_versions())
            for model_version in model_versions:
                total_versions += 1
                log_progress(total_versions, None, "model versions")
                logger.debug(
                    f"Ingesting model version (name: {model.display_name} id:{model.name} version:{model_version.version_id})"
                )
                yield from self._get_ml_model_mcps(
                    model=model, model_version=model_version
                )

            yield from self._gen_ml_group_properties(model)

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
        logger.info("Fetching Models for evaluation extraction from Vertex AI")

        last_checkpoint_millis = self.state_handler.get_last_update_time(
            ResourceTypes.MODEL_EVALUATION
        )
        if last_checkpoint_millis:
            log_checkpoint_time(last_checkpoint_millis, "ModelEvaluation")

        registered_models = self._list_models()
        total_evaluations = 0

        for model in registered_models:
            if not self.config.model_name_pattern.allowed(model.display_name or ""):
                continue

            with handle_google_api_errors(
                "fetch model evaluations", "model", model.name
            ):
                with self.rate_limiter:
                    evaluations: List[ModelEvaluation] = list(
                        model.list_model_evaluations()
                    )

                evaluations.sort(key=attrgetter(CREATE_TIME_FIELD), reverse=True)

                if last_checkpoint_millis:
                    evaluations = filter_by_update_time(
                        evaluations,
                        last_checkpoint_millis,
                        time_field="create_time",
                        resource_type="model evaluations",
                    )

                if self.config.max_evaluations_per_model is not None:
                    evaluations = evaluations[: self.config.max_evaluations_per_model]

                for evaluation in evaluations:
                    if (
                        hasattr(evaluation, CREATE_TIME_FIELD)
                        and evaluation.create_time
                    ):
                        self.state_handler.update_resource_timestamp(
                            ResourceTypes.MODEL_EVALUATION,
                            int(evaluation.create_time.timestamp() * 1000),
                        )

                    total_evaluations += 1
                    log_progress(total_evaluations, None, "model evaluations")
                    logger.debug(
                        f"Ingesting evaluation for model {model.display_name}: {evaluation.name}"
                    )
                    yield from self._gen_model_evaluation_mcps(model, evaluation)

        if total_evaluations > 0:
            logger.info(
                f"Finished processing {total_evaluations} model evaluations from Vertex AI"
            )

    def emit_pending_lineage_updates(self) -> Iterable[MetadataWorkUnit]:
        """
        Emit lineage-only updates for models/model groups that have tracked lineage
        but weren't emitted during normal processing (e.g., in incremental mode).

        This ensures downstream lineage appears even for unchanged models when new
        pipelines/jobs start referencing them.
        """
        pending_urns = self.model_usage_tracker.get_pending_lineage_urns()

        if not pending_urns:
            return

        logger.info(
            f"Emitting lineage updates for {len(pending_urns)} models/model groups that weren't re-processed"
        )

        for urn in pending_urns:
            # Determine if this is a model or model group URN
            is_model_group = ":mlModelGroup:" in urn

            if is_model_group:
                training_jobs = self.model_usage_tracker.get_model_group_training_jobs(
                    urn
                )
                downstream_jobs = self.model_usage_tracker.get_model_group_usage(urn)

                yield MetadataChangeProposalWrapper(
                    entityUrn=urn,
                    aspect=MLModelGroupPropertiesClass(
                        trainingJobs=training_jobs if training_jobs else None,
                        downstreamJobs=downstream_jobs if downstream_jobs else None,
                    ),
                ).as_workunit()
            else:
                training_job = self.model_usage_tracker.get_model_training_job(urn)
                downstream_jobs = self.model_usage_tracker.get_model_usage(urn)

                yield MetadataChangeProposalWrapper(
                    entityUrn=urn,
                    aspect=MLModelPropertiesClass(
                        trainingJobs=[training_job] if training_job else None,
                        downstreamJobs=downstream_jobs if downstream_jobs else None,
                    ),
                ).as_workunit()

            self.model_usage_tracker.mark_emitted(urn)

    def _extract_evaluation_metrics(
        self, evaluation: ModelEvaluation
    ) -> List[MLMetricClass]:
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
        try:
            with handle_google_api_errors(
                "get model versions", "model", model.name, log_level="debug"
            ):
                with self.rate_limiter:
                    versions: List[VersionInfo] = list(
                        model.versioning_registry.list_versions()
                    )
                if versions:
                    latest_version = max(versions, key=attrgetter(VERSION_ID_FIELD))
                    model_name = self.name_formatter.format_model_name(
                        entity_id=model.name
                    )
                    return self.urn_builder.make_ml_model_urn(
                        latest_version, model_name
                    )
        except AttributeError as e:
            logger.debug(
                f"Could not get versioned URN for model {model.name} (versioning not available), using simple URN | resource_type=model | resource_name={model.name} | cause={type(e).__name__}: {e}"
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
            subtype=MLAssetSubTypes.VERTEX_MODEL_EVALUATION,
            resource_category=ResourceCategory.MODELS,
            include_subtypes=False,
        )

    def _get_ml_model_mcps(
        self, model: Model, model_version: VersionInfo
    ) -> Iterable[MetadataWorkUnit]:
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

    def _gen_ml_group_container(self, model: Model) -> Iterable[MetadataWorkUnit]:
        yield from gen_resource_subfolder_container(
            project_id=self._get_project_id(),
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            resource_category=ResourceCategory.MODELS,
            container_key=self._get_model_group_container(model),
            name=model.display_name,
            sub_types=[MLAssetSubTypes.FOLDER],
        )

    def _gen_ml_group_properties(self, model: Model) -> Iterable[MetadataWorkUnit]:
        ml_model_group_urn = self.urn_builder.make_ml_model_group_urn(model)
        model_group_container_urn = self._get_model_group_container(model).as_urn()

        training_jobs = self.model_usage_tracker.get_model_group_training_jobs(
            ml_model_group_urn
        )
        downstream_jobs = self.model_usage_tracker.get_model_group_usage(
            ml_model_group_urn
        )

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
                trainingJobs=training_jobs if training_jobs else None,
                downstreamJobs=downstream_jobs if downstream_jobs else None,
                customProperties=None,
                externalUrl=self.url_builder.make_model_url(model.name),
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=ml_model_group_urn,
            aspect=ContainerClass(container=model_group_container_urn),
        ).as_workunit()

        yield from self._yield_common_aspects(
            entity_urn=ml_model_group_urn,
            subtype=MLAssetSubTypes.VERTEX_MODEL_GROUP,
            include_container=False,
        )

        self.model_usage_tracker.mark_emitted(ml_model_group_urn)

    def _get_model_group_container(self, model: Model) -> ModelGroupKey:
        return ModelGroupKey(
            project_id=self._get_project_id(),
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
            model_group_name=self.name_formatter.format_model_group_name(model.name),
        )

    def _gen_endpoints_mcps(
        self, model_meta: ModelMetadata
    ) -> Iterable[MetadataWorkUnit]:
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
                    subtype=MLAssetSubTypes.VERTEX_ENDPOINT,
                    resource_category=ResourceCategory.ENDPOINTS,
                    include_subtypes=False,
                )

    def _build_endpoint_urns(self, endpoints: Optional[List[Endpoint]]) -> List[str]:
        endpoint_urns: List[str] = []
        if endpoints:
            for endpoint in endpoints:
                logger.info(f"found endpoint ({endpoint.display_name}) for model")
                endpoint_urns.append(
                    builder.make_ml_model_deployment_urn(
                        platform=self.platform,
                        deployment_name=self.name_formatter.format_endpoint_name(
                            entity_id=endpoint.name
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
                    model.name, model_version.version_id
                ),
                type=MLModelType.ML_MODEL,
            ),
        ).as_workunit()

    def _create_version_properties_aspect(
        self, model: Model, model_version: VersionInfo, model_urn: str
    ) -> MetadataWorkUnit:
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
                sortId=str(model_version.version_id).zfill(VERSION_SORT_ID_PADDING),
                aliases=None,
            ),
        ).as_workunit()

    def _create_training_data_aspect(
        self, model_urn: str, training_data_urns: List[str]
    ) -> MetadataWorkUnit:
        return MetadataChangeProposalWrapper(
            entityUrn=model_urn,
            aspect=TrainingDataClass(
                trainingData=[
                    BaseDataClass(dataset=ds_urn) for ds_urn in training_data_urns
                ]
            ),
        ).as_workunit()

    def _gen_ml_model_mcps(
        self, model_metadata: ModelMetadata
    ) -> Iterable[MetadataWorkUnit]:
        model: Model = model_metadata.model
        model_version: VersionInfo = model_metadata.model_version
        training_job_urn: Optional[str] = model_metadata.training_job_urn
        endpoints: Optional[List[Endpoint]] = model_metadata.endpoints

        logging.info(f"generating model mcp for {model.name}")

        endpoint_urns = self._build_endpoint_urns(endpoints)
        model_group_urn = self.urn_builder.make_ml_model_group_urn(model)
        model_name = self.name_formatter.format_model_name(entity_id=model.name)
        model_urn = self.urn_builder.make_ml_model_urn(
            model_version, model_name=model_name
        )

        # Resolve any pending resource usage from pipeline tasks that referenced this model
        if model.resource_name:
            self.model_usage_tracker.resolve_and_track_resource(
                model.resource_name, model_urn, model_group_urn
            )

        # Retrieve training job if it was tracked earlier
        if not training_job_urn:
            training_job_urn = self.model_usage_tracker.get_model_training_job(
                model_urn
            )

        downstream_job_urns = self.model_usage_tracker.get_model_usage(model_urn)

        if training_job_urn:
            self.model_usage_tracker.track_model_training_job(
                model_urn, training_job_urn
            )
            self.model_usage_tracker.track_model_group_training_job(
                model_group_urn, training_job_urn
            )

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
            subtype=MLAssetSubTypes.VERTEX_MODEL,
            include_container=False,
        )

        yield self._create_version_properties_aspect(model, model_version, model_urn)

        training_data_urns = model_metadata.training_data_urns
        if training_data_urns:
            yield self._create_training_data_aspect(model_urn, training_data_urns)

        self.model_usage_tracker.mark_emitted(model_urn)

    def _get_version_set_urn(self, model: Model) -> VersionSetUrn:
        guid_dict = {"platform": self.platform, "name": model.name}
        version_set_urn = VersionSetUrn(
            id=builder.datahub_guid(guid_dict),
            entity_type=MlModelUrn.ENTITY_TYPE,
        )
        return version_set_urn

    def _search_endpoint(self, model: Model) -> List[Endpoint]:
        if self.endpoints is None:
            logger.info("Fetching Endpoints from Vertex AI")
            endpoint_dict: Dict[str, List[Endpoint]] = {}
            with self.rate_limiter:
                all_endpoints = list(self.client.Endpoint.list())
            for endpoint in all_endpoints:
                with self.rate_limiter:
                    deployed = list(endpoint.list_models())
                for resource in deployed:
                    if resource.model not in endpoint_dict:
                        endpoint_dict[resource.model] = []
                    endpoint_dict[resource.model].append(endpoint)
            self.endpoints = endpoint_dict

        return self.endpoints.get(model.resource_name, [])
