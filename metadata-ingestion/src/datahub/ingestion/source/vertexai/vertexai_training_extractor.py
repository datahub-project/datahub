import logging
import types
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Optional, Union

from google.cloud.aiplatform.base import VertexAiResourceNoun
from google.cloud.aiplatform.models import Model, VersionInfo
from google.cloud.aiplatform.training_jobs import _TrainingJob

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import MLAssetSubTypes
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
    DetailCalls,
    ResourceCategory,
    ResourceKinds,
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
from datahub.ingestion.source.vertexai.vertexai_report import VertexAISourceReport
from datahub.ingestion.source.vertexai.vertexai_result_type_utils import (
    UNKNOWN_STATUS,
    get_job_result_status,
    is_status_for_run_event_class,
)
from datahub.ingestion.source.vertexai.vertexai_scope import (
    CarryForward,
    DetailReader,
    DetailResult,
    ErrorKind,
    Scope,
    StoppedScopes,
    scope_context,
)
from datahub.ingestion.source.vertexai.vertexai_state import VertexAIStateHandler
from datahub.ingestion.source.vertexai.vertexai_utils import (
    get_actor_from_labels,
    log_checkpoint_time,
    log_progress,
    rate_limited_gapic_list,
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
    RunResultTypeClass,
    TimeStampClass,
)
from datahub.utilities.ratelimiter import RateLimiter
from datahub.utilities.time import datetime_to_ts_millis

logger = logging.getLogger(__name__)


@dataclass
class JobLookup:
    result_type: Union[str, RunResultTypeClass]
    duration_millis: Optional[int]


@dataclass
class OutputModel:
    model: Model
    version: VersionInfo


class VertexAITrainingExtractor:
    def __init__(
        self,
        config: VertexAIConfig,
        client: types.ModuleType,
        urn_builder: VertexAIUrnBuilder,
        name_formatter: VertexAINameFormatter,
        url_builder: VertexAIExternalURLBuilder,
        uri_parser: VertexAIURIParser,
        get_project_id_fn: Callable[[], str],
        yield_common_aspects_fn: YieldCommonAspectsProtocol,
        gen_ml_model_mcps_fn: Callable[[ModelMetadata], Iterable[MetadataWorkUnit]],
        get_job_lineage_from_ml_metadata_fn: Callable[
            [VertexAiResourceNoun], Optional[DetailResult[Optional[LineageMetadata]]]
        ],
        get_scope_fn: Callable[[], Scope],
        platform: str,
        report: VertexAISourceReport,
        state_handler: VertexAIStateHandler,
        model_usage_tracker: ModelUsageTracker,
        stopped_scopes: StoppedScopes,
        carry_forward: CarryForward,
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
        self._get_scope = get_scope_fn
        self._stopped = stopped_scopes
        self._carry = carry_forward
        self._read_detail = DetailReader(report, stopped_scopes, get_scope_fn)
        self.platform = platform
        self.report = report
        self.state_handler = state_handler
        self.model_usage_tracker = model_usage_tracker
        self.rate_limiter = rate_limiter

        self.datasets: Optional[Dict[str, VertexAiResourceNoun]] = None
        self._datasets_keep_previous = False

    def reset_regional_caches(self) -> None:
        """Clear the cached datasets, which are region-specific."""
        self.datasets = None
        self._datasets_keep_previous = False

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        """Main entry point for training job extraction."""
        for class_name in TrainingJobTypes.all():
            if not self.config.training_job_type_pattern.allowed(class_name):
                continue
            logger.info(f"Fetching {class_name}s from Vertex AI")

            last_checkpoint_millis = self.state_handler.get_last_update_time(class_name)
            if last_checkpoint_millis:
                log_checkpoint_time(last_checkpoint_millis, class_name)

            jobs: List[VertexAiResourceNoun] = rate_limited_gapic_list(
                getattr(self.client, class_name),
                self.rate_limiter,
                order_by=ORDER_BY_UPDATE_TIME_DESC,
            )

            max_jobs = self.config.max_training_jobs_per_type
            for job_count, job in enumerate(jobs, start=1):
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
                yield from self._get_training_job_mcps(job)

                if max_jobs is not None and job_count >= max_jobs:
                    logger.info(
                        f"Reached max_training_jobs_per_type limit of {max_jobs} for {class_name}"
                    )
                    break

    def _get_training_job_mcps(
        self, job: VertexAiResourceNoun
    ) -> Iterable[MetadataWorkUnit]:
        job_config = (
            self._parse_automl_config(job) if self._is_automl_job(job) else None
        )
        if self._stopped.any_transient(
            self._get_scope(), self._job_call_kinds(job_config)
        ):
            self.report.report_item_kept_after_stop()
            self._keep_previous_job(job, job_config)
            return
        lookup = self._look_up_job(job)
        if lookup.keep_previous:
            self._keep_previous_job(job, job_config)
            return
        job_meta: TrainingJobMetadata = self._get_training_job_metadata(job, job_config)
        if job_meta.keep_previous:
            self._keep_previous_job(job, job_config)
            return
        external_urns = self.uri_parser.extract_external_uris_from_job(job)
        job_meta.external_input_urns = external_urns.input_urns
        job_meta.external_output_urns = external_urns.output_urns
        yield from self._gen_training_job_mcps(job_meta, lookup)
        yield from self._get_input_dataset_mcps(job_meta)
        yield from self._gen_output_model_mcps(job_meta)

    def _job_call_kinds(self, job_config: Optional[AutoMLJobConfig]) -> List[str]:
        kinds = [
            DetailCalls.TRAINING_JOB_LOOKUP,
            DetailCalls.ML_METADATA,
            DetailCalls.ML_METADATA_ARTIFACTS,
        ]
        if job_config:
            if job_config.inputDataConfig and job_config.inputDataConfig.datasetId:
                kinds.append(DetailCalls.TRAINING_DATASETS)
            upload = job_config.modelToUpload
            if upload and upload.name and upload.versionId:
                kinds.append(DetailCalls.AUTOML_OUTPUT_MODEL)
        return kinds

    def _keep_previous_job(
        self, job: VertexAiResourceNoun, job_config: Optional[AutoMLJobConfig]
    ) -> None:
        # The job itself is a process instance, which stale removal never deletes.
        dataset_id = (
            job_config.inputDataConfig.datasetId
            if job_config and job_config.inputDataConfig
            else None
        )
        if dataset_id:
            self._carry.keep_urn(self.urn_builder.make_dataset_urn(dataset_id))
        self._track_listed_output_model(job, job_config, dataset_id)

    def _track_listed_output_model(
        self,
        job: VertexAiResourceNoun,
        job_config: Optional[AutoMLJobConfig],
        dataset_id: Optional[str],
    ) -> None:
        """A kept job still names its output model in its own listing, so the
        model step writes that model with its trained-by link. A model the step
        does not write (deleted, or a version gone) gets no lineage-only write."""
        upload = job_config.modelToUpload if job_config else None
        if not (upload and upload.name and upload.versionId):
            return
        # The SDK resolves "<id>@<version or alias>" to the model "<id>".
        model_id = upload.name.rsplit("/", 1)[-1].split("@", 1)[0]
        model_urn = self.urn_builder.make_ml_model_version_urn(
            self.name_formatter.format_model_name(entity_id=model_id),
            upload.versionId,
        )
        self.model_usage_tracker.track_model_training_job(
            model_urn,
            builder.make_data_process_instance_urn(
                self.name_formatter.format_job_name(entity_id=job.name)
            ),
        )
        # As a clean read: training data only when the dataset list holds it.
        dataset = self._search_dataset(dataset_id) if dataset_id else None
        if dataset is not None:
            self.model_usage_tracker.track_model_training_data(
                model_urn, self.urn_builder.make_dataset_urn(dataset.name)
            )
        self.model_usage_tracker.skip_pending_lineage([model_urn])

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

    def _job_scope(self, job: VertexAiResourceNoun) -> Scope:
        scope = self._get_scope()
        return Scope(project=scope.project, region=scope.region, item=job.name)

    def _look_up_job(self, job: VertexAiResourceNoun) -> DetailResult[JobLookup]:
        # The SDK refreshes the job from Google when its state or end time is read.
        return self._read_detail(
            DetailCalls.TRAINING_JOB_LOOKUP,
            lambda: JobLookup(
                result_type=get_job_result_status(job),
                duration_millis=self._get_job_duration_millis(job),
            ),
            item=job.name,
        )

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
            self.urn_builder.make_dataset_urn(job_meta.input_dataset.name)
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
        self,
        job_meta: TrainingJobMetadata,
        lookup: DetailResult[JobLookup],
    ) -> Iterable[MetadataWorkUnit]:
        job = job_meta.job
        job_id = self.name_formatter.format_job_name(entity_id=job.name)
        job_urn = builder.make_data_process_instance_urn(job_id)

        result_type = lookup.value.result_type if lookup.value else UNKNOWN_STATUS
        duration = lookup.value.duration_millis if lookup.value else None

        created_time = datetime_to_ts_millis(job.create_time) if job.create_time else 0

        urns_and_edges = self._build_training_job_urns_and_edges(job_meta)
        dataset_urn = urns_and_edges.dataset_urn
        model_urn = urns_and_edges.model_urn
        external_input_edges = urns_and_edges.input_edges
        external_output_edges = urns_and_edges.output_edges

        if dataset_urn and model_urn:
            self.model_usage_tracker.track_model_training_data(model_urn, dataset_urn)

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

    def _list_datasets(self) -> Dict[str, VertexAiResourceNoun]:
        datasets: Dict[str, VertexAiResourceNoun] = {}
        for dtype in DatasetTypes.all():
            dataset_class = getattr(self.client.datasets, dtype)
            for ds in rate_limited_gapic_list(dataset_class, self.rate_limiter):
                datasets[ds.name] = ds
        return datasets

    def _ensure_datasets_cached(self) -> None:
        """Fetch and cache all datasets from Vertex AI, once per region."""
        if self.datasets is None:
            logger.info("Fetching Datasets from Vertex AI (once per region)")
            listed = self._read_detail.region_list(
                DetailCalls.TRAINING_DATASETS,
                self._list_datasets,
                ResourceKinds.DATASET,
                self._carry,
            )
            self.datasets = listed.value or {}
            self._datasets_keep_previous = listed.keep_previous
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
            dataset_urn = self.urn_builder.make_dataset_urn(ds.name)

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
            versions = model.versioning_registry.list_versions()
        for version in versions:
            if version.version_id == model_version_str:
                return version
        return None

    def _is_automl_job(self, job: VertexAiResourceNoun) -> bool:
        return any(
            automl_type in job.__class__.__name__
            for automl_type in ["AutoML", "AutoMl", "Automl"]
        )

    def _parse_automl_config(
        self, job: VertexAiResourceNoun
    ) -> Optional[AutoMLJobConfig]:
        try:
            return AutoMLJobConfig(**job.to_dict())
        except (TypeError, ValueError) as e:
            self.report.warning(
                title="AutoML job config could not be parsed",
                message="The job config could not be parsed, so its input dataset and output model links are not read. The job is written without them, or kept as it was when it cannot be written without them.",
                context=scope_context(self._job_scope(job), e),
                exc=e,
            )
            scope = self._get_scope()
            self._stopped.lost(
                Scope(scope.project, scope.region, kind=DetailCalls.AUTOML_JOB_CONFIG),
                ErrorKind.ITEM_LASTING,
                e,
            )
            return None

    def _look_up_output_model(
        self, job: VertexAiResourceNoun, model_name: str, model_version_str: str
    ) -> DetailResult[Optional[OutputModel]]:
        def read() -> Optional[OutputModel]:
            model = Model(model_name=model_name)
            model_version = self._search_model_version(model, model_version_str)
            if model and model_version:
                return OutputModel(model=model, version=model_version)
            return None

        return self._read_detail(DetailCalls.AUTOML_OUTPUT_MODEL, read, item=job.name)

    def _get_training_job_metadata(
        self, job: VertexAiResourceNoun, job_config: Optional[AutoMLJobConfig]
    ) -> TrainingJobMetadata:
        job_meta = TrainingJobMetadata(job=job)

        if (
            self.config.extract_execution_metrics
            or self.config.use_ml_metadata_for_lineage
        ):
            lineage_result = self._get_job_lineage_from_ml_metadata(job)
            if lineage_result:
                if lineage_result.keep_previous:
                    job_meta.keep_previous = True
                    return job_meta
                job_meta.lineage = lineage_result.value

        if self._is_automl_job(job):
            if job_config is None:
                return job_meta

            if job_config.inputDataConfig and job_config.inputDataConfig.datasetId:
                dataset_id = job_config.inputDataConfig.datasetId
                logger.info(
                    f"Found input dataset (id: {dataset_id}) for training job ({job.display_name})"
                )

                input_ds = self._search_dataset(dataset_id)
                if self._datasets_keep_previous:
                    job_meta.keep_previous = True
                    return job_meta
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
                output_result = self._look_up_output_model(
                    job,
                    job_config.modelToUpload.name,
                    job_config.modelToUpload.versionId,
                )
                if output_result.keep_previous:
                    job_meta.keep_previous = True
                    return job_meta
                if output_result.value:
                    logger.info(
                        f"Found output model (name:{output_result.value.model.display_name} id:{job_config.modelToUpload.versionId}) "
                        f"for training job: {job.display_name}"
                    )
                    job_meta.output_model = output_result.value.model
                    job_meta.output_model_version = output_result.value.version
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
