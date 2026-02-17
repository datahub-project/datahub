import logging
from typing import Any, Callable, Dict, Iterable, List, Optional

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
from datahub.ingestion.source.vertexai.vertexai_builder import (
    VertexAIExternalURLBuilder,
    VertexAINameFormatter,
    VertexAIURIParser,
    VertexAIUrnBuilder,
)
from datahub.ingestion.source.vertexai.vertexai_config import VertexAIConfig
from datahub.ingestion.source.vertexai.vertexai_constants import (
    DatasetTypes,
    ResourceCategory,
    TrainingJobTypes,
    VertexAISubTypes,
)
from datahub.ingestion.source.vertexai.vertexai_models import (
    AutoMLJobConfig,
    DatasetCustomProperties,
    MLMetrics,
    ModelMetadata,
    TrainingJobCustomProperties,
    TrainingJobMetadata,
    TrainingJobURNsAndEdges,
)
from datahub.ingestion.source.vertexai.vertexai_result_type_utils import (
    get_job_result_status,
    is_status_for_run_event_class,
)
from datahub.ingestion.source.vertexai.vertexai_utils import get_actor_from_labels
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
from datahub.utilities.time import datetime_to_ts_millis

logger = logging.getLogger(__name__)


class VertexAITrainingExtractor:
    """Extracts training job metadata from Vertex AI."""

    def __init__(
        self,
        config: VertexAIConfig,
        client: Any,
        urn_builder: VertexAIUrnBuilder,
        name_formatter: VertexAINameFormatter,
        url_builder: VertexAIExternalURLBuilder,
        uri_parser: VertexAIURIParser,
        project_id: str,
        yield_common_aspects_fn: Callable,
        gen_ml_model_mcps_fn: Callable,
        get_job_lineage_from_ml_metadata_fn: Callable,
        platform: str,
        report: Any,
    ):
        self.config = config
        self.client = client
        self.urn_builder = urn_builder
        self.name_formatter = name_formatter
        self.url_builder = url_builder
        self.uri_parser = uri_parser
        self.project_id = project_id
        self._yield_common_aspects = yield_common_aspects_fn
        self._gen_ml_model_mcps = gen_ml_model_mcps_fn
        self._get_job_lineage_from_ml_metadata = get_job_lineage_from_ml_metadata_fn
        self.platform = platform
        self.report = report

        self.datasets: Optional[Dict[str, VertexAiResourceNoun]] = None

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        """Main entry point for training job extraction."""
        for class_name in TrainingJobTypes.all():
            if not self.config.training_job_type_pattern.allowed(class_name):
                continue
            logger.info(f"Fetching a list of {class_name}s from VertexAI server")

            jobs: List[VertexAiResourceNoun] = list(
                getattr(self.client, class_name).list(order_by="update_time desc")
            )

            if self.config.max_training_jobs_per_type is not None:
                jobs = jobs[: self.config.max_training_jobs_per_type]

            for i, job in enumerate(jobs, start=1):
                if i % 100 == 0:
                    logger.info(f"Processed {i} {class_name}s from VertexAI server")
                yield from self._get_training_job_mcps(job)

            if jobs:
                logger.info(
                    f"Finished processing {len(jobs)} {class_name}s from VertexAI server"
                )

    def _get_training_job_mcps(
        self, job: VertexAiResourceNoun
    ) -> Iterable[MetadataWorkUnit]:
        """Generate MCPs for a single training job."""
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
        """Generate MCPs for models produced by training jobs."""
        if job_meta.output_model and job_meta.output_model_version:
            job = job_meta.job
            job_urn = builder.make_data_process_instance_urn(
                self.name_formatter.format_job_name(entity_id=job.name)
            )

            training_data_urns: List[str] = []
            if job_meta.input_dataset:
                dataset_urn = builder.make_dataset_urn_with_platform_instance(
                    platform=self.platform,
                    name=self.name_formatter.format_dataset_name(
                        entity_id=job_meta.input_dataset.name
                    ),
                    platform_instance=self.config.platform_instance,
                    env=self.config.env,
                )
                training_data_urns.append(dataset_urn)
            if job_meta.external_input_urns:
                training_data_urns.extend(job_meta.external_input_urns)

            yield from self._gen_ml_model_mcps(
                ModelMetadata(
                    model=job_meta.output_model,
                    model_version=job_meta.output_model_version,
                    training_job_urn=job_urn,
                    training_data_urns=training_data_urns
                    if training_data_urns
                    else None,
                )
            )

    def _get_job_duration_millis(self, job: VertexAiResourceNoun) -> Optional[int]:
        """Calculate job duration in milliseconds."""
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
        """Build dataset URN, model URN, and input/output edges for training job."""
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
        """Extract hyperparameters and metrics from ML Metadata if enabled."""
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
        """Generate MCPs for a training job."""
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
            subtype=VertexAISubTypes.TRAINING_JOB,
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

    def _search_dataset(self, dataset_id: str) -> Optional[VertexAiResourceNoun]:
        """Search for a dataset by ID."""
        if self.datasets is None:
            self.datasets = {}

            for dtype in DatasetTypes.all():
                dataset_class = getattr(self.client.datasets, dtype)
                for ds in dataset_class.list():
                    self.datasets[ds.name] = ds

        return self.datasets.get(dataset_id)

    def _get_input_dataset_mcps(
        self, job_meta: TrainingJobMetadata
    ) -> Iterable[MetadataWorkUnit]:
        """Cache all datasets from Vertex AI."""
        if self.datasets is None:
            self.datasets = {}

            for dtype in DatasetTypes.all():
                dataset_class = getattr(self.client.datasets, dtype)
                for ds in dataset_class.list():
                    self.datasets[ds.name] = ds

        return []

    def _get_dataset_workunits_from_job_metadata(
        self, job_meta: TrainingJobMetadata
    ) -> Iterable[MetadataWorkUnit]:
        """Create DatasetPropertiesClass aspect for Vertex AI dataset."""
        ds = job_meta.input_dataset

        if ds:
            dataset_name = self.name_formatter.format_dataset_name(entity_id=ds.name)
            dataset_urn = builder.make_dataset_urn_with_platform_instance(
                platform=self.platform,
                name=dataset_name,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=DatasetPropertiesClass(
                    name=ds.display_name,
                    created=(
                        TimeStampClass(time=datetime_to_ts_millis(ds.create_time))
                        if ds.create_time
                        else None
                    ),
                    description=ds.display_name,
                    customProperties=DatasetCustomProperties(
                        resource_name=ds.resource_name,
                    ).to_custom_properties(),
                    qualifiedName=ds.resource_name,
                ),
            ).as_workunit()

            yield from self._yield_common_aspects(
                entity_urn=dataset_urn,
                subtype=VertexAISubTypes.DATASET,
                resource_category=ResourceCategory.DATASETS,
            )

    def _search_model_version(
        self, model: Model, model_version_str: str
    ) -> Optional[VersionInfo]:
        """Search for a model version."""
        for version in model.versioning_registry.list_versions():
            if version.version_id == model_version_str:
                return version
        return None

    def _is_automl_job(self, job: VertexAiResourceNoun) -> bool:
        """Check if job is an AutoML job."""
        return any(
            automl_type in job.__class__.__name__
            for automl_type in ["AutoML", "AutoMl", "Automl"]
        )

    def _get_training_job_metadata(
        self, job: VertexAiResourceNoun
    ) -> TrainingJobMetadata:
        """Retrieve metadata for a Vertex AI training job."""
        job_meta = TrainingJobMetadata(job=job)

        # Fetch lineage once if ML Metadata is enabled (for metrics or lineage extraction)
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
                        message=f"Permission denied while fetching output model {model_name} version {model_version_str} for training job {job.display_name} | resource_type=model | resource_name={model_name} | cause={type(e).__name__}: {e}",
                        exc=e,
                    )
                except ResourceExhausted as e:
                    self.report.report_failure(
                        title="Unable to fetch model and model version due to quota exceeded",
                        message=f"API quota exceeded while fetching output model {model_name} version {model_version_str} for training job {job.display_name} | resource_type=model | resource_name={model_name} | cause={type(e).__name__}: {e}",
                        exc=e,
                    )
                except (DeadlineExceeded, ServiceUnavailable) as e:
                    self.report.report_failure(
                        title="Unable to fetch model and model version due to timeout or service unavailable",
                        message=f"Timeout or service unavailable while fetching output model {model_name} version {model_version_str} for training job {job.display_name} | resource_type=model | resource_name={model_name} | cause={type(e).__name__}: {e}",
                        exc=e,
                    )
                except NotFound as e:
                    logger.debug(
                        f"Output model {model_name} version {model_version_str} not found for training job {job.display_name} | resource_type=model | resource_name={model_name} | cause={type(e).__name__}: {e}"
                    )
                except GoogleAPICallError as e:
                    self.report.report_failure(
                        title="Unable to fetch model and model version",
                        message=f"Error while fetching output model {model_name} version {model_version_str} for training job {job.display_name} | resource_type=model | resource_name={model_name} | cause={type(e).__name__}: {e}",
                        exc=e,
                    )
        else:
            # Use cached lineage for non-AutoML jobs
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
