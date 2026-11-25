import logging
import types
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass, replace
from typing import Callable, Dict, Iterable, List, Mapping, Optional, Union

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
    JOB_PIPELINE_TASK_RUN,
    ORDER_BY_UPDATE_TIME_DESC,
    PIPELINE_BACKING_JOB_NAME,
    PIPELINE_BILLING_LABEL,
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
    get_custom_job_result_type,
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
    DataProcessInstanceRelationshipsClass,
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
    # When the run event is stamped; None means the job gets no run event.
    event_millis: Optional[int]
    duration_millis: Optional[int]


@dataclass(frozen=True)
class _HeldJob:
    job: VertexAiResourceNoun
    billing_id: str
    task_id: str


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
        # Listed CustomJobs that Vertex ran for a pipeline task, waiting to be
        # joined to their task run.
        self._held_jobs: List[_HeldJob] = []

    def reset_regional_caches(self) -> None:
        """Clear the cached datasets, which are region-specific."""
        self.datasets = None

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

            holds_backing_jobs = (
                self.config.links_pipeline_backed_jobs
                and class_name == TrainingJobTypes.CUSTOM_JOB
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

                log_progress(job_count, max_jobs, class_name)
                held_job = self._as_held_job(job) if holds_backing_jobs else None
                if held_job is not None:
                    # Its checkpoint time is recorded only if the join writes it.
                    self._held_jobs.append(held_job)
                else:
                    if last_checkpoint_millis:
                        self.state_handler.update_resource_timestamp(
                            class_name, job_update_millis
                        )
                    yield from self._get_training_job_mcps(job)

                if max_jobs is not None and job_count >= max_jobs:
                    logger.info(
                        f"Reached max_training_jobs_per_type limit of {max_jobs} for {class_name}"
                    )
                    break

    @staticmethod
    def _as_held_job(job: VertexAiResourceNoun) -> Optional[_HeldJob]:
        """The job, if a pipeline task ran it: it carries the billing label, and its
        display name is the backing-job name with that same billing id."""
        billing_id = (getattr(job, "labels", None) or {}).get(PIPELINE_BILLING_LABEL)
        name = PIPELINE_BACKING_JOB_NAME.match(job.display_name or "")
        if name is None or name.group(1) != billing_id:
            return None
        return _HeldJob(job=job, billing_id=name.group(1), task_id=name.group(2))

    def gen_backing_job_records(
        self,
        parents: Mapping[str, Mapping[str, str]],
        *,
        run_not_read: List[str],
        task_not_found: List[str],
    ) -> Iterable[MetadataWorkUnit]:
        """Write each held job under the task run that ran it, then forget the held jobs.

        `parents` maps a pipeline run's billing id to its task ids and their task run URNs.
        The display names of held jobs whose run is not in `parents` are added to
        `run_not_read`, and of those whose task is not in their run to `task_not_found`.
        """
        held, self._held_jobs = self._held_jobs, []
        # As in the listing loop, a checkpoint time is recorded only after a prior one.
        has_checkpoint = bool(
            self.state_handler.get_last_update_time(TrainingJobTypes.CUSTOM_JOB)
        )
        for held_job in held:
            tasks = parents.get(held_job.billing_id)
            if tasks is None:
                self.report.report_backing_job_skipped_run_not_read()
                run_not_read.append(held_job.job.gca_resource.display_name)
                continue
            task_run_urn = tasks.get(held_job.task_id)
            if task_run_urn is None:
                self.report.report_backing_job_skipped_task_not_found()
                task_not_found.append(held_job.job.gca_resource.display_name)
                continue
            job = held_job.job
            written = False
            for workunit in self._get_training_job_mcps(job, task_run_urn=task_run_urn):
                written = True
                yield workunit
            # A job kept as it was (a stop, an unreadable detail) writes nothing:
            # it is not linked, and its checkpoint time must not move.
            if not written:
                continue
            self.report.report_backing_job_linked()
            if has_checkpoint:
                # The SDK's update_time property calls GetCustomJob; the listing holds it.
                self.state_handler.update_resource_timestamp(
                    TrainingJobTypes.CUSTOM_JOB,
                    int(job.gca_resource.update_time.timestamp() * 1000),
                )

    def _get_training_job_mcps(
        self, job: VertexAiResourceNoun, task_run_urn: Optional[str] = None
    ) -> Iterable[MetadataWorkUnit]:
        """Write a training job; with `task_run_urn`, a job record under that task run."""
        job_config = (
            self._parse_automl_config(job) if self._is_automl_job(job) else None
        )
        call_kinds = self._job_call_kinds(job_config)
        if task_run_urn is not None:
            # A job record reads its state from the listing: no lookup call to stop.
            call_kinds.remove(DetailCalls.TRAINING_JOB_LOOKUP)
        if self._stopped.any_transient(self._get_scope(), call_kinds):
            self.report.report_item_kept_after_stop()
            self._keep_previous_job(job, job_config)
            return
        lookup = (
            self._look_up_job(job)
            if task_run_urn is None
            else self._read_listed_job(job)
        )
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
        yield from self._gen_training_job_mcps(job_meta, lookup, task_run_urn)
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
        job_urn = builder.make_data_process_instance_urn(
            self.name_formatter.format_job_name(entity_id=job.name)
        )
        self.model_usage_tracker.track_model_training_job(model_urn, job_urn)
        self.model_usage_tracker.track_kept_job_of_version(model_urn, job_urn)
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

    def _look_up_job(self, job: VertexAiResourceNoun) -> DetailResult[JobLookup]:
        # The SDK refreshes the job from Google when its state or end time is read.
        def read() -> JobLookup:
            result_type = get_job_result_status(job)
            duration = self._get_job_duration_millis(job)
            return JobLookup(
                result_type=result_type,
                # A duration means the job has a create time.
                event_millis=(
                    datetime_to_ts_millis(job.create_time)
                    if duration is not None
                    else None
                ),
                duration_millis=duration,
            )

        return self._read_detail(DetailCalls.TRAINING_JOB_LOOKUP, read, item=job.name)

    @staticmethod
    def _read_listed_job(job: VertexAiResourceNoun) -> DetailResult[JobLookup]:
        # The listed resource holds the state and times; the SDK's `state` would
        # call GetCustomJob for each job.
        resource = job.gca_resource
        event_millis: Optional[int] = None
        duration_millis: Optional[int] = None
        if resource.end_time and resource.start_time and resource.create_time:
            event_millis = datetime_to_ts_millis(resource.create_time)
            duration_millis = datetime_to_ts_millis(resource.end_time) - event_millis
        elif resource.end_time:
            # An attempt that ended without starting: its end, with no duration.
            event_millis = datetime_to_ts_millis(resource.end_time)
        return DetailResult(
            value=JobLookup(
                result_type=get_custom_job_result_type(resource.state),
                event_millis=event_millis,
                duration_millis=duration_millis,
            ),
            keep_previous=False,
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
        task_run_urn: Optional[str] = None,
    ) -> Iterable[MetadataWorkUnit]:
        job = job_meta.job
        job_id = self.name_formatter.format_job_name(entity_id=job.name)
        job_urn = builder.make_data_process_instance_urn(job_id)

        result_type = lookup.value.result_type if lookup.value else UNKNOWN_STATUS
        event_millis = lookup.value.event_millis if lookup.value else None
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

        custom_properties = TrainingJobCustomProperties(
            job_type=job.__class__.__name__,
        ).to_custom_properties()
        if task_run_urn is not None:
            custom_properties[JOB_PIPELINE_TASK_RUN] = task_run_urn

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
                customProperties=custom_properties,
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
            include_container=task_run_urn is None,
        )

        if task_run_urn is not None:
            yield MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=DataProcessInstanceRelationshipsClass(
                    upstreamInstances=[], parentInstance=task_run_urn
                ),
            ).as_workunit()

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

        if is_status_for_run_event_class(result_type) and event_millis is not None:
            yield MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=DataProcessInstanceRunEventClass(
                    status=DataProcessRunStatusClass.COMPLETE,
                    timestampMillis=event_millis,
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
            scope = self._get_scope()
            self.report.warning(
                title="AutoML job config could not be parsed",
                message="The job config could not be parsed, so its input dataset and output model links are not read. The job is written without them, or kept as it was when it cannot be written without them.",
                context=scope_context(replace(scope, item=job.name), e),
                exc=e,
            )
            self._stopped.lost(
                replace(scope, kind=DetailCalls.AUTOML_JOB_CONFIG),
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
                if self._stopped.any_transient(
                    self._get_scope(), [DetailCalls.TRAINING_DATASETS]
                ):
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
