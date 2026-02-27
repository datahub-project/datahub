import logging
from contextlib import AbstractContextManager, nullcontext
from typing import Any, Callable, Dict, Iterable, List, Optional, Union

from google.api_core.exceptions import (
    DeadlineExceeded,
    GoogleAPICallError,
    NotFound,
    PermissionDenied,
    ResourceExhausted,
    ServiceUnavailable,
    Unauthenticated,
)
from google.cloud.aiplatform.base import VertexAiResourceNoun
from google.cloud.aiplatform.models import Model, VersionInfo
from google.cloud.aiplatform.training_jobs import _TrainingJob

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import MLAssetSubTypes
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.ingestion.source.vertexai.vertexai_builder import (
    VertexAIExternalURLBuilder,
    VertexAINameFormatter,
    VertexAIURIParser,
    VertexAIUrnBuilder,
)
from datahub.ingestion.source.vertexai.vertexai_config import VertexAIConfig
from datahub.ingestion.source.vertexai.vertexai_constants import (
    ORDER_BY_UPDATE_TIME_DESC,
    DatasetTypes,
    ResourceCategory,
    TrainingJobTypes,
)
from datahub.ingestion.source.vertexai.vertexai_models import (
    AutoMLJobConfig,
    DatasetCustomProperties,
    LineageMetadata,
    MLMetrics,
    ModelMetadata,
    ModelUsageTracker,
    TrainingJobCustomProperties,
    TrainingJobMetadata,
    TrainingJobURNsAndEdges,
    YieldCommonAspectsProtocol,
)
from datahub.ingestion.source.vertexai.vertexai_result_type_utils import (
    get_job_result_status,
    is_status_for_run_event_class,
)
from datahub.ingestion.source.vertexai.vertexai_state import VertexAIStateHandler
from datahub.ingestion.source.vertexai.vertexai_utils import (
    format_api_error_message,
    get_actor_from_labels,
    log_checkpoint_time,
    log_progress,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DataProcessInstanceInputClass,
    DataProcessInstanceOutputClass,
    DataProcessInstancePropertiesClass,
    DataProcessInstanceRunEventClass,
    DataProcessInstanceRunResultClass,
    DataProcessRunStatusClass,
    DatasetPropertiesClass,
    EdgeClass,
    MLHyperParamClass,
    MLMetricClass,
    MLTrainingRunPropertiesClass,
    TimeStampClass,
)
from datahub.utilities.ratelimiter import RateLimiter
from datahub.utilities.time import datetime_to_ts_millis

logger = logging.getLogger(__name__)


class VertexAITrainingExtractor:
    def __init__(
        self,
        config: VertexAIConfig,
        client: Any,  # aiplatform module
        urn_builder: VertexAIUrnBuilder,
        name_formatter: VertexAINameFormatter,
        url_builder: VertexAIExternalURLBuilder,
        uri_parser: VertexAIURIParser,
        get_project_id_fn: Callable[[], str],
        yield_common_aspects_fn: YieldCommonAspectsProtocol,
        gen_ml_model_mcps_fn: Callable[[ModelMetadata], Iterable[MetadataWorkUnit]],
        get_job_lineage_from_ml_metadata_fn: Callable[
            [VertexAiResourceNoun], Optional[LineageMetadata]
        ],
        platform: str,
        report: StaleEntityRemovalSourceReport,
        state_handler: VertexAIStateHandler,
        model_usage_tracker: ModelUsageTracker,
        rate_limiter: Union[RateLimiter, AbstractContextManager[None]] = nullcontext(),
    ):
        self.config = config
        self.client = client
        self.urn_builder = urn_builder
        self.name_formatter = name_formatter
        self.url_builder = url_builder
        self.uri_parser = uri_parser
        self._get_project_id = get_project_id_fn
        self._yield_common_aspects = yield_common_aspects_fn
        self._gen_ml_model_mcps = gen_ml_model_mcps_fn
        self._get_job_lineage_from_ml_metadata = get_job_lineage_from_ml_metadata_fn
        self.platform = platform
        self.report = report
        self.state_handler = state_handler
        self.model_usage_tracker = model_usage_tracker
        self.rate_limiter = rate_limiter

        self.datasets: Optional[Dict[str, VertexAiResourceNoun]] = None

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        """Main entry point for training job extraction."""
        for class_name in TrainingJobTypes.all():
            if not self.config.training_job_type_pattern.allowed(class_name):
                continue
            logger.info(f"Fetching {class_name}s from Vertex AI")

            last_checkpoint_millis = self.state_handler.get_last_update_time(class_name)
            if last_checkpoint_millis:
                log_checkpoint_time(last_checkpoint_millis, class_name)

            with self.rate_limiter:
                jobs_pager = getattr(self.client, class_name).list(
                    order_by=ORDER_BY_UPDATE_TIME_DESC
                )

            max_jobs = self.config.max_training_jobs_per_type
            for job_count, job in enumerate(jobs_pager, start=1):
                if last_checkpoint_millis:
                    job_update_millis = int(job.update_time.timestamp() * 1000)
                    if job_update_millis <= last_checkpoint_millis:
                        logger.info(
                            f"Reached checkpoint timestamp for {class_name}, stopping early (processed {job_count - 1} new jobs)"
                        )
                        break

                    self.state_handler.update_resource_timestamp(
                        class_name, job_update_millis
                    )

                log_progress(job_count, max_jobs, class_name)
                with self.rate_limiter:
                    yield from self._get_training_job_mcps(job)

                if max_jobs is not None and job_count >= max_jobs:
                    logger.info(
                        f"Reached max_training_jobs_per_type limit of {max_jobs} for {class_name}"
                    )
                    break

    def _get_training_job_mcps(
        self, job: VertexAiResourceNoun
    ) -> Iterable[MetadataWorkUnit]:
        job_meta: TrainingJobMetadata = self._get_training_job_metadata(job)
        external_urns = self.uri_parser.extract_external_uris_from_job(job)
        job_meta.external_input_urns = external_urns.input_urns
        job_meta.external_output_urns = external_urns.output_urns
        yield from self._gen_training_job_mcps(job_meta)
        yield from self._get_input_dataset_mcps(job_meta)
        yield from self._gen_output_model_mcps(job_meta)

    def _gen_output_model_mcps(
        self, job_meta: TrainingJobMetadata
    ) -> Iterable[MetadataWorkUnit]:
        """
        Track lineage between training jobs and their output models.
        Note: Model metadata is emitted by the model extractor during Model.list().
        This only tracks the training job relationship for later inclusion in model properties.
        """
        if job_meta.output_model and job_meta.output_model_version:
            job = job_meta.job
            job_urn = builder.make_data_process_instance_urn(
                self.name_formatter.format_job_name(entity_id=job.name)
            )

            model_name = self.name_formatter.format_model_name(
                entity_id=job_meta.output_model.name
            )
            model_urn = self.urn_builder.make_ml_model_urn(
                job_meta.output_model_version, model_name=model_name
            )
            model_group_urn = self.urn_builder.make_ml_model_group_urn(
                job_meta.output_model
            )

            # Track training job relationships (will be emitted when model is processed)
            self.model_usage_tracker.track_model_training_job(model_urn, job_urn)
            self.model_usage_tracker.track_model_group_training_job(
                model_group_urn, job_urn
            )

        return
        yield

    def _get_job_duration_millis(self, job: VertexAiResourceNoun) -> Optional[int]:
        create_time = job.create_time
        duration = None
        if isinstance(job, _TrainingJob) and job.create_time and job.end_time:
            end_time = job.end_time
            duration = datetime_to_ts_millis(end_time) - datetime_to_ts_millis(
                create_time
            )

        return int(duration) if duration else None

    def _build_training_job_urns_and_edges(
        self, job_meta: TrainingJobMetadata
    ) -> TrainingJobURNsAndEdges:
        dataset_urn = (
            builder.make_dataset_urn_with_platform_instance(
                platform=self.platform,
                name=self.name_formatter.format_dataset_name(
                    entity_id=job_meta.input_dataset.name
                ),
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )
            if job_meta.input_dataset
            else None
        )

        model_urn = (
            self.urn_builder.make_ml_model_urn(
                model_version=job_meta.output_model_version,
                model_name=self.name_formatter.format_model_name(
                    entity_id=job_meta.output_model.name
                ),
            )
            if job_meta.output_model and job_meta.output_model_version
            else None
        )

        input_edges = [
            EdgeClass(destinationUrn=u) for u in (job_meta.external_input_urns or [])
        ]
        output_edges = [
            EdgeClass(destinationUrn=u) for u in (job_meta.external_output_urns or [])
        ]

        return TrainingJobURNsAndEdges(
            dataset_urn=dataset_urn,
            model_urn=model_urn,
            input_edges=input_edges,
            output_edges=output_edges,
        )

    def _extract_hyperparams_and_metrics(
        self, job_meta: TrainingJobMetadata
    ) -> MLMetrics:
        hyperparams: List[MLHyperParamClass] = []
        metrics: List[MLMetricClass] = []

        if self.config.extract_execution_metrics and job_meta.lineage:
            hyperparams = job_meta.lineage.hyperparams
            metrics = job_meta.lineage.metrics

            if hyperparams:
                logger.info(
                    f"Extracted {len(hyperparams)} hyperparameters from ML Metadata for job {job_meta.job.display_name}"
                )
            if metrics:
                logger.info(
                    f"Extracted {len(metrics)} metrics from ML Metadata for job {job_meta.job.display_name}"
                )

        return MLMetrics(hyperparams=hyperparams, metrics=metrics)

    def _gen_training_job_mcps(
        self, job_meta: TrainingJobMetadata
    ) -> Iterable[MetadataWorkUnit]:
        job = job_meta.job
        job_id = self.name_formatter.format_job_name(entity_id=job.name)
        job_urn = builder.make_data_process_instance_urn(job_id)

        created_time = datetime_to_ts_millis(job.create_time) if job.create_time else 0
        duration = self._get_job_duration_millis(job)

        urns_and_edges = self._build_training_job_urns_and_edges(job_meta)
        dataset_urn = urns_and_edges.dataset_urn
        model_urn = urns_and_edges.model_urn
        external_input_edges = urns_and_edges.input_edges
        external_output_edges = urns_and_edges.output_edges

        result_type = get_job_result_status(job)
        ml_metrics = self._extract_hyperparams_and_metrics(job_meta)
        hyperparams = ml_metrics.hyperparams
        metrics = ml_metrics.metrics

        yield MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataProcessInstancePropertiesClass(
                name=job.display_name,
                created=AuditStampClass(
                    time=created_time,
                    actor=get_actor_from_labels(getattr(job, "labels", None))
                    or builder.UNKNOWN_USER,
                ),
                externalUrl=self.url_builder.make_job_url(job.name),
                customProperties=TrainingJobCustomProperties(
                    job_type=job.__class__.__name__,
                ).to_custom_properties(),
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=MLTrainingRunPropertiesClass(
                externalUrl=self.url_builder.make_job_url(job.name),
                id=job.name,
                hyperParams=hyperparams if hyperparams else None,
                trainingMetrics=metrics if metrics else None,
            ),
        ).as_workunit()

        yield from self._yield_common_aspects(
            entity_urn=job_urn,
            subtype=MLAssetSubTypes.VERTEX_TRAINING_JOB,
            resource_category=ResourceCategory.TRAINING_JOBS,
        )

        if dataset_urn or external_input_edges:
            yield MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=DataProcessInstanceInputClass(
                    inputs=[],
                    inputEdges=(
                        ([EdgeClass(destinationUrn=dataset_urn)] if dataset_urn else [])
                        + external_input_edges
                    ),
                ),
            ).as_workunit()

        if model_urn or external_output_edges:
            yield MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=DataProcessInstanceOutputClass(
                    outputs=[],
                    outputEdges=(
                        ([EdgeClass(destinationUrn=model_urn)] if model_urn else [])
                        + external_output_edges
                    ),
                ),
            ).as_workunit()

        if is_status_for_run_event_class(result_type) and duration:
            yield MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=DataProcessInstanceRunEventClass(
                    status=DataProcessRunStatusClass.COMPLETE,
                    timestampMillis=created_time,
                    result=DataProcessInstanceRunResultClass(
                        type=result_type,
                        nativeResultType=self.platform,
                    ),
                    durationMillis=duration,
                ),
            ).as_workunit()

    def _ensure_datasets_cached(self) -> None:
        """Fetch and cache all datasets from Vertex AI (one-time operation)."""
        if self.datasets is None:
            logger.info("Fetching Datasets from Vertex AI (one-time cache)")
            self.datasets = {}

            for dtype in DatasetTypes.all():
                dataset_class = getattr(self.client.datasets, dtype)
                with self.rate_limiter:
                    datasets = list(dataset_class.list())
                for ds in datasets:
                    self.datasets[ds.name] = ds

            logger.info(f"Cached {len(self.datasets)} datasets")

    def _search_dataset(self, dataset_id: str) -> Optional[VertexAiResourceNoun]:
        self._ensure_datasets_cached()
        assert self.datasets is not None
        return self.datasets.get(dataset_id)

    def _get_input_dataset_mcps(
        self, job_meta: TrainingJobMetadata
    ) -> Iterable[MetadataWorkUnit]:
        """Cache all datasets from Vertex AI and yield dataset workunits for this job."""
        self._ensure_datasets_cached()
        yield from self._get_dataset_workunits_from_job_metadata(job_meta)

    def _get_dataset_workunits_from_job_metadata(
        self, job_meta: TrainingJobMetadata
    ) -> Iterable[MetadataWorkUnit]:
        ds = job_meta.input_dataset

        if ds:
            dataset_name = self.name_formatter.format_dataset_name(entity_id=ds.name)
            dataset_urn = builder.make_dataset_urn_with_platform_instance(
                platform=self.platform,
                name=dataset_name,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )

            dataset_props = DatasetPropertiesClass(
                name=ds.display_name,
                created=(
                    TimeStampClass(time=datetime_to_ts_millis(ds.create_time))
                    if ds.create_time
                    else None
                ),
                customProperties=DatasetCustomProperties(
                    resource_name=ds.resource_name,
                ).to_custom_properties(),
                qualifiedName=ds.resource_name,
            )

            if hasattr(ds, "description") and ds.description:
                dataset_props.description = ds.description

            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=dataset_props,
            ).as_workunit()

            yield from self._yield_common_aspects(
                entity_urn=dataset_urn,
                subtype=MLAssetSubTypes.VERTEX_DATASET,
                resource_category=ResourceCategory.DATASETS,
            )

    def _search_model_version(
        self, model: Model, model_version_str: str
    ) -> Optional[VersionInfo]:
        with self.rate_limiter:
            versions = list(model.versioning_registry.list_versions())
        for version in versions:
            if version.version_id == model_version_str:
                return version
        return None

    def _is_automl_job(self, job: VertexAiResourceNoun) -> bool:
        return any(
            automl_type in job.__class__.__name__
            for automl_type in ["AutoML", "AutoMl", "Automl"]
        )

    def _get_training_job_metadata(
        self, job: VertexAiResourceNoun
    ) -> TrainingJobMetadata:
        job_meta = TrainingJobMetadata(job=job)

        if (
            self.config.extract_execution_metrics
            or self.config.use_ml_metadata_for_lineage
        ):
            job_meta.lineage = self._get_job_lineage_from_ml_metadata(job)

        if self._is_automl_job(job):
            try:
                job_config = AutoMLJobConfig(**job.to_dict())
            except (TypeError, ValueError) as e:
                logger.warning(
                    f"Failed to parse AutoML job config for {job.display_name}: {e}"
                )
                return job_meta

            if job_config.inputDataConfig and job_config.inputDataConfig.datasetId:
                dataset_id = job_config.inputDataConfig.datasetId
                logger.info(
                    f"Found input dataset (id: {dataset_id}) for training job ({job.display_name})"
                )

                input_ds = self._search_dataset(dataset_id)
                if input_ds:
                    logger.info(
                        f"Found the name of input dataset ({input_ds.display_name}) with dataset id ({dataset_id})"
                    )
                    job_meta.input_dataset = input_ds

            if (
                job_config.modelToUpload
                and job_config.modelToUpload.name
                and job_config.modelToUpload.versionId
            ):
                model_name = job_config.modelToUpload.name
                model_version_str = job_config.modelToUpload.versionId
                try:
                    model = Model(model_name=model_name)
                    model_version = self._search_model_version(model, model_version_str)
                    if model and model_version:
                        logger.info(
                            f"Found output model (name:{model.display_name} id:{model_version_str}) "
                            f"for training job: {job.display_name}"
                        )
                        job_meta.output_model = model
                        job_meta.output_model_version = model_version
                except (PermissionDenied, Unauthenticated) as e:
                    self.report.report_failure(
                        title="Unable to fetch model and model version due to permission issue",
                        message=format_api_error_message(
                            e,
                            f"fetching output model {model_name} version {model_version_str} for training job {job.display_name}",
                            "model",
                            model_name,
                        ),
                        exc=e,
                    )
                except ResourceExhausted as e:
                    self.report.report_failure(
                        title="Unable to fetch model and model version due to quota exceeded",
                        message=format_api_error_message(
                            e,
                            f"fetching output model {model_name} version {model_version_str} for training job {job.display_name}",
                            "model",
                            model_name,
                        ),
                        exc=e,
                    )
                except (DeadlineExceeded, ServiceUnavailable) as e:
                    self.report.report_failure(
                        title="Unable to fetch model and model version due to timeout or service unavailable",
                        message=format_api_error_message(
                            e,
                            f"fetching output model {model_name} version {model_version_str} for training job {job.display_name}",
                            "model",
                            model_name,
                        ),
                        exc=e,
                    )
                except NotFound as e:
                    logger.debug(
                        format_api_error_message(
                            e,
                            f"Output model {model_name} version {model_version_str} for training job {job.display_name}",
                            "model",
                            model_name,
                        )
                    )
                except GoogleAPICallError as e:
                    self.report.report_failure(
                        title="Unable to fetch model and model version",
                        message=format_api_error_message(
                            e,
                            f"fetching output model {model_name} version {model_version_str} for training job {job.display_name}",
                            "model",
                            model_name,
                        ),
                        exc=e,
                    )
        else:
            if job_meta.lineage:
                if job_meta.lineage.input_urns:
                    job_meta.external_input_urns = job_meta.lineage.input_urns
                    logger.info(
                        f"Extracted {len(job_meta.lineage.input_urns)} input URNs from ML Metadata for job {job.display_name}"
                    )

                if job_meta.lineage.output_urns:
                    job_meta.external_output_urns = job_meta.lineage.output_urns
                    logger.info(
                        f"Extracted {len(job_meta.lineage.output_urns)} output URNs from ML Metadata for job {job.display_name}"
                    )
            elif self.config.use_ml_metadata_for_lineage:
                logger.debug(
                    f"No lineage metadata found for CustomJob {job.display_name}. "
                    "Ensure job logs to ML Metadata for lineage tracking."
                )

        return job_meta
