import logging
from contextlib import AbstractContextManager, nullcontext
from datetime import timedelta
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Set,
    Union,
)

from google.cloud.aiplatform import PipelineJob
from google.cloud.aiplatform_v1.types import (
    PipelineJob as PipelineJobType,
    PipelineTaskDetail,
)

import datahub.emitter.mce_builder as builder
from datahub.emitter.mce_builder import UNKNOWN_USER
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
    KUBEFLOW_TIMESTAMP_SUFFIX_PATTERN,
    ORDER_BY_UPDATE_TIME_DESC,
    DateTimeFormat,
    DurationUnit,
    LabelFormat,
    ResourceCategory,
    ResourceTypes,
)
from datahub.ingestion.source.vertexai.vertexai_models import (
    ModelUsageTracker,
    PipelineContainerKey,
    PipelineMetadata,
    PipelineProperties,
    PipelineTaskArtifacts,
    PipelineTaskMetadata,
    PipelineTaskProperties,
    YieldCommonAspectsProtocol,
)
from datahub.ingestion.source.vertexai.vertexai_result_type_utils import (
    get_pipeline_task_result_status,
    is_status_for_run_event_class,
)
from datahub.ingestion.source.vertexai.vertexai_state import VertexAIStateHandler
from datahub.ingestion.source.vertexai.vertexai_utils import (
    gen_resource_subfolder_container,
    get_actor_from_labels,
    log_checkpoint_time,
    log_progress,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ContainerClass,
    DataJobInputOutputClass,
    DataProcessInstanceInputClass,
    DataProcessInstanceOutputClass,
    DataProcessInstancePropertiesClass,
    DataProcessInstanceRelationshipsClass,
    DataProcessInstanceRunEventClass,
    DataProcessInstanceRunResultClass,
    DataProcessRunStatusClass,
    EdgeClass,
    RunResultTypeClass,
    StatusClass,
)
from datahub.metadata.urns import CorpUserUrn, DataFlowUrn, DataJobUrn
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.specific.datajob import DataJobPatchBuilder
from datahub.utilities.ratelimiter import RateLimiter
from datahub.utilities.time import datetime_to_ts_millis
from datahub.utilities.urns.dataset_urn import DatasetUrn
from datahub.utilities.urns.urn import guess_entity_type

logger = logging.getLogger(__name__)


class VertexAIPipelineExtractor:
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
        self.uri_parser = uri_parser
        self._get_project_id = get_project_id_fn
        self._yield_common_aspects = yield_common_aspects_fn
        self.model_usage_tracker = model_usage_tracker
        self.platform = platform
        self.state_handler = state_handler
        self.rate_limiter = rate_limiter
        self._emitted_pipeline_urns: Set[str] = set()
        self._emitted_task_urns: Set[str] = set()

    def _get_stable_pipeline_id(self, pipeline: PipelineJob) -> str:
        """
        Prefer pipeline_spec.pipelineInfo.name (set at compile time, unaffected by
        display_name edits) over display_name. Falls back to display_name with the
        Kubeflow timestamp suffix stripped for non-Kubeflow pipelines (e.g. AutoML).
        """
        resource = pipeline.gca_resource
        if isinstance(resource, PipelineJobType):
            pipeline_info_name = resource.pipeline_spec.get("pipelineInfo", {}).get(
                "name"
            )
            if pipeline_info_name:
                return pipeline_info_name

        name = pipeline.display_name or pipeline.name
        return KUBEFLOW_TIMESTAMP_SUFFIX_PATTERN.sub("", name)

    def _get_pipeline_container(self, pipeline_name: str) -> PipelineContainerKey:
        return PipelineContainerKey(
            project_id=self._get_project_id(),
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
            pipeline_name=pipeline_name,
        )

    def _gen_pipeline_container(
        self, pipeline_name: str, container_key: PipelineContainerKey
    ) -> Iterable[MetadataWorkUnit]:
        yield from gen_resource_subfolder_container(
            project_id=self._get_project_id(),
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            resource_category=ResourceCategory.PIPELINES,
            container_key=container_key,
            name=pipeline_name,
            sub_types=[MLAssetSubTypes.FOLDER],
        )

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        logger.info("Fetching PipelineJobs from Vertex AI")

        last_checkpoint_millis = self.state_handler.get_last_update_time(
            ResourceTypes.PIPELINE_JOB
        )
        if last_checkpoint_millis:
            log_checkpoint_time(last_checkpoint_millis, "PipelineJob")

        with self.rate_limiter:
            pipeline_jobs_pager = self.client.PipelineJob.list(
                order_by=ORDER_BY_UPDATE_TIME_DESC
            )

        for job_count, pipeline in enumerate(pipeline_jobs_pager, start=1):
            if last_checkpoint_millis:
                job_update_millis = int(pipeline.update_time.timestamp() * 1000)
                if job_update_millis <= last_checkpoint_millis:
                    logger.info(
                        f"Reached checkpoint timestamp for PipelineJobs, stopping early (processed {job_count - 1} new jobs)"
                    )
                    break

                self.state_handler.update_resource_timestamp(
                    ResourceTypes.PIPELINE_JOB, job_update_millis
                )

            log_progress(job_count, None, f"{ResourceTypes.PIPELINE_JOB}s")

            logger.debug(f"Fetching pipeline ({pipeline.name})")
            with self.rate_limiter:
                pipeline_meta = self._get_pipeline_metadata(pipeline)

            yield from self._get_pipeline_workunits(pipeline, pipeline_meta)

    def _get_pipeline_workunits(
        self, pipeline_job: PipelineJob, pipeline_meta: PipelineMetadata
    ) -> Iterable[MetadataWorkUnit]:
        """
        Emit stable DataFlow/DataJob entities only once,
        create DataProcessInstances for each run, and emit lineage.
        """
        dataflow_urn = pipeline_meta.urn
        pipeline_urn_str = str(dataflow_urn.urn())
        stable_pipeline_id = self._get_stable_pipeline_id(pipeline_job)
        pipeline_container_key = self._get_pipeline_container(stable_pipeline_id)

        if pipeline_urn_str not in self._emitted_pipeline_urns:
            logger.debug(
                f"Emitting stable DataFlow for pipeline: {pipeline_job.display_name or pipeline_job.name}"
            )
            yield from self._gen_pipeline_container(
                stable_pipeline_id, pipeline_container_key
            )
            yield from self._get_pipeline_mcps(pipeline_meta, pipeline_container_key)
            self._emitted_pipeline_urns.add(pipeline_urn_str)
        else:
            logger.debug(
                f"Skipping DataFlow (already emitted): {pipeline_job.display_name or pipeline_job.name}"
            )

        yield from self._gen_pipeline_run_instance(pipeline_job, pipeline_meta)

        for task in pipeline_meta.tasks:
            task_urn_str = str(task.urn.urn())

            if task_urn_str not in self._emitted_task_urns:
                logger.debug(f"Emitting stable DataJob for task: {task.name}")
                yield from self._gen_pipeline_task_datajob(
                    task, dataflow_urn, pipeline_container_key
                )
                self._emitted_task_urns.add(task_urn_str)
            else:
                logger.debug(f"Skipping DataJob (already emitted): {task.name}")

            yield from self._gen_pipeline_task_run_instance(task, pipeline_meta)
            yield from self._gen_pipeline_task_lineage(task)

    def _gen_pipeline_run_instance(
        self, pipeline_job: PipelineJob, pipeline_meta: PipelineMetadata
    ) -> Iterable[MetadataWorkUnit]:
        run_id = self.name_formatter.format_pipeline_run_id(pipeline_job.name)
        dpi_urn = builder.make_data_process_instance_urn(run_id)

        yield MetadataChangeProposalWrapper(
            entityUrn=dpi_urn,
            aspect=DataProcessInstancePropertiesClass(
                name=pipeline_job.display_name or pipeline_job.name,
                created=AuditStampClass(
                    time=(
                        datetime_to_ts_millis(pipeline_job.create_time)
                        if pipeline_job.create_time
                        else 0
                    ),
                    actor=get_actor_from_labels(pipeline_meta.labels) or UNKNOWN_USER,
                ),
                externalUrl=self.url_builder.make_pipeline_url(pipeline_job.name),
                customProperties={
                    "resource_name": pipeline_job.resource_name,
                    "region": pipeline_meta.region or "",
                },
            ),
        ).as_workunit()

        yield from self._yield_common_aspects(
            entity_urn=dpi_urn,
            subtype=MLAssetSubTypes.VERTEX_PIPELINE_TASK_RUN,
            include_container=False,
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=dpi_urn,
            aspect=DataProcessInstanceRelationshipsClass(
                upstreamInstances=[],
                parentTemplate=str(pipeline_meta.urn.urn()),
            ),
        ).as_workunit()

    def _gen_pipeline_task_datajob(
        self,
        task: PipelineTaskMetadata,
        dataflow_urn: DataFlowUrn,
        container_key: PipelineContainerKey,
    ) -> Iterable[MetadataWorkUnit]:
        datajob = DataJob(
            name=task.name,
            display_name=task.name,
            flow_urn=str(dataflow_urn),
            platform_instance=self.config.platform_instance,
        )

        datajob._set_container(container_key)

        yield from self._yield_common_aspects(
            entity_urn=datajob.urn.urn(),
            subtype=MLAssetSubTypes.VERTEX_PIPELINE_TASK,
            include_container=False,
        )

        yield from datajob.as_workunits()

        yield MetadataChangeProposalWrapper(
            entityUrn=datajob.urn.urn(),
            aspect=StatusClass(removed=False),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=datajob.urn.urn(),
            aspect=ContainerClass(container=container_key.as_urn()),
        ).as_workunit()

    def _gen_pipeline_task_run_instance(
        self, task: PipelineTaskMetadata, pipeline: PipelineMetadata
    ) -> Iterable[MetadataWorkUnit]:
        task_run_id = f"{pipeline.name}_{task.name}"
        dpi_urn = builder.make_data_process_instance_urn(
            self.name_formatter.format_pipeline_task_run_id(entity_id=task_run_id)
        )
        result_status: Union[str, RunResultTypeClass] = get_pipeline_task_result_status(
            task.state
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=dpi_urn,
            aspect=DataProcessInstancePropertiesClass(
                name=task.name,
                created=AuditStampClass(
                    time=(
                        int(task.create_time.timestamp() * 1000)
                        if task.create_time
                        else 0
                    ),
                    actor=get_actor_from_labels(pipeline.labels)
                    or builder.UNKNOWN_USER,
                ),
                externalUrl=self.url_builder.make_pipeline_url(pipeline.name),
                customProperties={},
            ),
        ).as_workunit()

        yield from self._yield_common_aspects(
            entity_urn=dpi_urn,
            subtype=MLAssetSubTypes.VERTEX_PIPELINE_TASK_RUN,
            include_container=False,
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=dpi_urn,
            aspect=DataProcessInstanceRelationshipsClass(
                upstreamInstances=[], parentTemplate=str(task.urn.urn())
            ),
        ).as_workunit()

        if is_status_for_run_event_class(result_status) and task.duration:
            yield MetadataChangeProposalWrapper(
                entityUrn=dpi_urn,
                aspect=DataProcessInstanceRunEventClass(
                    status=DataProcessRunStatusClass.COMPLETE,
                    timestampMillis=(
                        int(task.create_time.timestamp() * 1000)
                        if task.create_time
                        else 0
                    ),
                    result=DataProcessInstanceRunResultClass(
                        type=result_status,
                        nativeResultType=self.platform,
                    ),
                    durationMillis=task.duration,
                ),
            ).as_workunit()

        input_edges: List[EdgeClass] = []
        if task.input_dataset_urns:
            input_edges.extend(
                [EdgeClass(destinationUrn=urn) for urn in task.input_dataset_urns]
            )

        if input_edges:
            yield MetadataChangeProposalWrapper(
                entityUrn=dpi_urn,
                aspect=DataProcessInstanceInputClass(
                    inputs=[],
                    inputEdges=input_edges,
                ),
            ).as_workunit()

        output_edges: List[EdgeClass] = []
        if task.output_dataset_urns:
            output_edges.extend(
                [EdgeClass(destinationUrn=urn) for urn in task.output_dataset_urns]
            )

        # Note: Model outputs are tracked for downstream lineage but not emitted as edges here,
        # since we need to resolve resource names to URNs when processing the actual models

        if output_edges:
            yield MetadataChangeProposalWrapper(
                entityUrn=dpi_urn,
                aspect=DataProcessInstanceOutputClass(
                    outputs=[],
                    outputEdges=output_edges,
                ),
            ).as_workunit()

    def _gen_pipeline_task_lineage(
        self, task: PipelineTaskMetadata
    ) -> Iterable[MetadataWorkUnit]:
        """Emit lineage for a task - either as patches (incremental) or full aspects (non-incremental)."""
        task_urn_str = str(task.urn.urn())

        input_datasets = (
            [
                urn
                for urn in task.input_dataset_urns
                if guess_entity_type(urn) == DatasetUrn.ENTITY_TYPE
            ]
            if task.input_dataset_urns
            else []
        )
        output_datasets = (
            [
                urn
                for urn in task.output_dataset_urns
                if guess_entity_type(urn) == DatasetUrn.ENTITY_TYPE
            ]
            if task.output_dataset_urns
            else []
        )

        upstream_jobs = task.upstreams if task.upstreams else []

        if self.config.incremental_lineage:
            patch_builder = DataJobPatchBuilder(task_urn_str)

            for input_urn in input_datasets:
                patch_builder.add_input_dataset(input_urn)
            for output_urn in output_datasets:
                patch_builder.add_output_dataset(output_urn)
            for upstream_urn in upstream_jobs:
                patch_builder.add_input_datajob(upstream_urn)

            for patch_mcp in patch_builder.build():
                yield MetadataWorkUnit(
                    id=f"{task_urn_str}-{patch_mcp.aspectName}", mcp_raw=patch_mcp
                )
        else:
            yield MetadataChangeProposalWrapper(
                entityUrn=task_urn_str,
                aspect=DataJobInputOutputClass(
                    inputDatasets=input_datasets,
                    outputDatasets=output_datasets,
                    inputDatajobs=[urn.urn() for urn in upstream_jobs]
                    if upstream_jobs
                    else [],
                ),
            ).as_workunit()

    def _extract_pipeline_task_inputs(
        self, task_detail: PipelineTaskDetail, task_name: str, task_urn: str
    ) -> PipelineTaskArtifacts:
        if not task_detail.inputs:
            return PipelineTaskArtifacts()

        input_dataset_urns: List[str] = []
        try:
            for input_entry in task_detail.inputs.values():
                if input_entry.artifacts:
                    for artifact in input_entry.artifacts:
                        if artifact.uri:
                            if self.uri_parser._is_model_uri(artifact.uri):
                                # Track by resource name, to be resolved later when we process the model
                                self.model_usage_tracker.track_resource_usage(
                                    artifact.uri, task_urn
                                )
                            else:
                                input_dataset_urns.extend(
                                    self.uri_parser.dataset_urns_from_artifact_uri(
                                        artifact.uri
                                    )
                                )
            return PipelineTaskArtifacts(
                input_dataset_urns=input_dataset_urns if input_dataset_urns else None,
            )
        except (AttributeError, TypeError, KeyError) as e:
            logger.debug(
                f"Failed to extract input artifacts for task {task_name}: {e}",
                exc_info=True,
            )
            return PipelineTaskArtifacts()

    def _extract_pipeline_task_outputs(
        self, task_detail: PipelineTaskDetail, task_name: str, task_urn: str
    ) -> PipelineTaskArtifacts:
        if not task_detail.outputs:
            return PipelineTaskArtifacts()

        output_dataset_urns: List[str] = []
        output_model_resource_names: List[str] = []
        try:
            for output_entry in task_detail.outputs.values():
                if output_entry.artifacts:
                    for artifact in output_entry.artifacts:
                        if artifact.uri:
                            # Check if this is a model resource
                            if self.uri_parser._is_model_uri(artifact.uri):
                                # Store resource name for later resolution
                                output_model_resource_names.append(artifact.uri)
                                # Also track for downstream lineage (will be resolved when model is processed)
                                self.model_usage_tracker.track_resource_usage(
                                    artifact.uri, task_urn
                                )
                            else:
                                output_dataset_urns.extend(
                                    self.uri_parser.dataset_urns_from_artifact_uri(
                                        artifact.uri
                                    )
                                )
            return PipelineTaskArtifacts(
                output_dataset_urns=output_dataset_urns
                if output_dataset_urns
                else None,
                output_model_resource_names=output_model_resource_names
                if output_model_resource_names
                else None,
            )
        except (AttributeError, TypeError, KeyError) as e:
            logger.debug(
                f"Failed to extract output artifacts for task {task_name}: {e}",
                exc_info=True,
            )
            return PipelineTaskArtifacts()

    def _get_pipeline_tasks_metadata(
        self, pipeline: PipelineJob, pipeline_urn: DataFlowUrn
    ) -> List[PipelineTaskMetadata]:
        tasks: List[PipelineTaskMetadata] = list()
        task_map: Dict[str, PipelineTaskDetail] = dict()
        for task in pipeline.task_details:
            task_map[task.task_name] = task

        resource = pipeline.gca_resource
        if isinstance(resource, PipelineJobType):
            for task_name in resource.pipeline_spec["root"]["dag"]["tasks"]:
                logger.debug(
                    f"fetching pipeline task ({task_name}) in pipeline ({pipeline.name})"
                )
                task_urn = DataJobUrn.create_from_ids(
                    data_flow_urn=pipeline_urn.urn(),
                    job_id=task_name,
                )
                task_meta = PipelineTaskMetadata(name=task_name, urn=task_urn)

                dpi_urn = builder.make_data_process_instance_urn(
                    self.name_formatter.format_pipeline_task_run_id(entity_id=task_name)
                )
                if (
                    "dependentTasks"
                    in resource.pipeline_spec["root"]["dag"]["tasks"][task_name]
                ):
                    upstream_tasks = resource.pipeline_spec["root"]["dag"]["tasks"][
                        task_name
                    ]["dependentTasks"]
                    upstream_urls = [
                        DataJobUrn.create_from_ids(
                            data_flow_urn=pipeline_urn.urn(),
                            job_id=upstream_task,
                        )
                        for upstream_task in upstream_tasks
                    ]
                    task_meta.upstreams = upstream_urls

                task_detail = task_map.get(task_name)
                if task_detail:
                    task_meta.id = task_detail.task_id
                    task_meta.state = task_detail.state
                    task_meta.start_time = task_detail.start_time
                    task_meta.create_time = task_detail.create_time
                    if task_detail.end_time and task_meta.start_time:
                        task_meta.end_time = task_detail.end_time
                        if task_meta.start_time and task_meta.end_time:
                            task_meta.duration = int(
                                (
                                    task_meta.end_time.timestamp()
                                    - task_meta.start_time.timestamp()
                                )
                                * 1000
                            )

                    inputs = self._extract_pipeline_task_inputs(
                        task_detail, task_name, dpi_urn
                    )
                    task_meta.input_dataset_urns = inputs.input_dataset_urns

                    outputs = self._extract_pipeline_task_outputs(
                        task_detail, task_name, dpi_urn
                    )
                    task_meta.output_dataset_urns = outputs.output_dataset_urns
                    task_meta.output_model_resource_names = (
                        outputs.output_model_resource_names
                    )

                tasks.append(task_meta)
        return tasks

    def _get_pipeline_metadata(self, pipeline: PipelineJob) -> PipelineMetadata:
        stable_id = self._get_stable_pipeline_id(pipeline)

        dataflow_urn = DataFlowUrn.create_from_ids(
            orchestrator=self.platform,
            env=self.config.env,
            flow_id=stable_id,
            platform_instance=self.config.platform_instance,
        )
        tasks = self._get_pipeline_tasks_metadata(
            pipeline=pipeline, pipeline_urn=dataflow_urn
        )

        pipeline_meta = PipelineMetadata(
            name=pipeline.name,
            display_name=stable_id,
            resource_name=pipeline.resource_name,
            urn=dataflow_urn,
            tasks=tasks,
        )
        pipeline_meta.resource_name = pipeline.resource_name
        pipeline_meta.labels = pipeline.labels
        pipeline_meta.create_time = pipeline.create_time
        pipeline_meta.region = pipeline.location
        if pipeline.update_time:
            pipeline_meta.update_time = pipeline.update_time
            pipeline_meta.duration = timedelta(
                milliseconds=datetime_to_ts_millis(pipeline.update_time)
                - datetime_to_ts_millis(pipeline.create_time)
            )
        return pipeline_meta

    def _format_pipeline_duration(self, td: timedelta) -> str:
        days = td.days
        hours, remainder = divmod(td.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        milliseconds = td.microseconds // 1000

        parts: List[str] = []
        if days:
            parts.append(f"{days}{DurationUnit.DAYS}")
        if hours:
            parts.append(f"{hours}{DurationUnit.HOURS}")
        if minutes:
            parts.append(f"{minutes}{DurationUnit.MINUTES}")
        if seconds:
            parts.append(f"{seconds}{DurationUnit.SECONDS}")
        if milliseconds:
            parts.append(f"{milliseconds}{DurationUnit.MILLISECONDS}")
        return " ".join(parts) if parts else f"0{DurationUnit.SECONDS}"

    def _get_pipeline_task_properties(
        self, task: PipelineTaskMetadata
    ) -> PipelineTaskProperties:
        return PipelineTaskProperties(
            created_time=(
                task.create_time.strftime(DateTimeFormat.TIMESTAMP)
                if task.create_time
                else ""
            )
        )

    def _get_pipeline_properties(
        self, pipeline: PipelineMetadata
    ) -> PipelineProperties:
        return PipelineProperties(
            resource_name=pipeline.resource_name if pipeline.resource_name else "",
            create_time=(
                pipeline.create_time.isoformat() if pipeline.create_time else ""
            ),
            update_time=(
                pipeline.update_time.isoformat() if pipeline.update_time else ""
            ),
            duration=(
                self._format_pipeline_duration(pipeline.duration)
                if pipeline.duration
                else ""
            ),
            location=(pipeline.region if pipeline.region else ""),
            labels=LabelFormat.ITEM_SEPARATOR.join(
                [
                    f"{k}{LabelFormat.KEY_VALUE_SEPARATOR}{v}"
                    for k, v in pipeline.labels.items()
                ]
            )
            if pipeline.labels
            else "",
        )

    def _get_pipeline_mcps(
        self, pipeline: PipelineMetadata, container_key: PipelineContainerKey
    ) -> Iterable[MetadataWorkUnit]:
        owner = get_actor_from_labels(pipeline.labels)

        dataflow = DataFlow(
            platform=self.platform,
            name=pipeline.urn.flow_id,
            display_name=pipeline.display_name,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
            custom_properties=self._get_pipeline_properties(
                pipeline
            ).to_custom_properties(),
            owners=[CorpUserUrn.from_string(owner)] if owner else None,
            external_url=self.url_builder.make_pipeline_url(
                pipeline_name=pipeline.name
            ),
            parent_container=container_key,  # Pass ContainerKey itself, not URN string
        )

        yield from dataflow.as_workunits()

        yield MetadataChangeProposalWrapper(
            entityUrn=dataflow.urn.urn(),
            aspect=StatusClass(removed=False),
        ).as_workunit()

        yield from self._yield_common_aspects(
            entity_urn=dataflow.urn.urn(),
            subtype=MLAssetSubTypes.VERTEX_PIPELINE,
            include_platform=False,
            include_container=False,  # DataFlow already sets container via parent_container parameter
        )
