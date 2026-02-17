import logging
from datetime import timedelta
from typing import Any, Dict, Iterable, List, Optional, Set, Union, cast

from google.cloud.aiplatform import PipelineJob
from google.cloud.aiplatform_v1.types import (
    PipelineJob as PipelineJobType,
    PipelineTaskDetail,
)

import datahub.emitter.mce_builder as builder
from datahub.emitter.mce_builder import UNKNOWN_USER
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
    DateTimeFormat,
    DurationUnit,
    LabelFormat,
    ResourceCategory,
    VertexAISubTypes,
)
from datahub.ingestion.source.vertexai.vertexai_models import (
    PipelineMetadata,
    PipelineProperties,
    PipelineTaskArtifacts,
    PipelineTaskMetadata,
    PipelineTaskProperties,
)
from datahub.ingestion.source.vertexai.vertexai_result_type_utils import (
    get_pipeline_task_result_status,
    is_status_for_run_event_class,
)
from datahub.ingestion.source.vertexai.vertexai_utils import get_actor_from_labels
from datahub.metadata.com.linkedin.pegasus2avro.dataprocess import (
    DataProcessInstanceRelationships,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DataProcessInstanceInputClass,
    DataProcessInstanceOutputClass,
    DataProcessInstancePropertiesClass,
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
from datahub.utilities.urns.dataset_urn import DatasetUrn

logger = logging.getLogger(__name__)


def log_progress(current: int, total: Optional[int], resource_type: str) -> None:
    """Log progress for resource processing."""
    if total:
        logger.info(f"Processing {resource_type} {current}/{total}")
    else:
        logger.info(f"Processing {resource_type} {current}")


def datetime_to_ts_millis(dt: Any) -> int:
    """Convert datetime to milliseconds timestamp."""
    return int(dt.timestamp() * 1000)


class VertexAIPipelineExtractor:
    """Extracts pipeline metadata from Vertex AI."""

    def __init__(
        self,
        config: VertexAIConfig,
        client: Any,
        urn_builder: VertexAIUrnBuilder,
        name_formatter: VertexAINameFormatter,
        url_builder: VertexAIExternalURLBuilder,
        uri_parser: VertexAIURIParser,
        project_id: str,
        yield_common_aspects_fn: Any,
        model_usage_tracker: Any,
        platform: str,
    ):
        self.config = config
        self.client = client
        self.urn_builder = urn_builder
        self.name_formatter = name_formatter
        self.url_builder = url_builder
        self.uri_parser = uri_parser
        self.project_id = project_id
        self._yield_common_aspects = yield_common_aspects_fn
        self.model_usage_tracker = model_usage_tracker
        self.platform = platform
        self._emitted_pipeline_urns: Set[str] = set()
        self._emitted_task_urns: Set[str] = set()

    def _get_stable_pipeline_id(self, pipeline: PipelineJob) -> str:
        """
        Extract stable pipeline identifier.

        Prefers display_name (user-defined, consistent across runs) over name (run-specific).
        """
        if pipeline.display_name:
            return pipeline.display_name

        return pipeline.name

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        """Main entry point for pipeline extraction."""
        pipeline_jobs: List[PipelineJob] = list(
            self.client.PipelineJob.list(order_by="update_time desc")
        )

        total = len(pipeline_jobs)
        for i, pipeline in enumerate(pipeline_jobs, start=1):
            log_progress(i, total, "pipelines")
            logger.debug(f"Fetching pipeline ({pipeline.name})")
            pipeline_meta = self._get_pipeline_metadata(pipeline)

            if self.config.incremental_lineage:
                yield from self._get_pipeline_mcps_incremental(pipeline, pipeline_meta)
            else:
                yield from self._get_pipeline_mcps(pipeline_meta)
                yield from self._gen_pipeline_task_mcps(pipeline_meta)

    def _get_pipeline_mcps_incremental(
        self, pipeline_job: PipelineJob, pipeline_meta: PipelineMetadata
    ) -> Iterable[MetadataWorkUnit]:
        """
        Incremental lineage mode: emits stable DataFlow/DataJob entities only once,
        creates DataProcessInstances for each run, and uses patch builders to aggregate lineage.
        """
        dataflow_urn = pipeline_meta.urn
        pipeline_urn_str = str(dataflow_urn.urn())

        if pipeline_urn_str not in self._emitted_pipeline_urns:
            logger.debug(
                f"Emitting stable DataFlow for pipeline: {pipeline_job.display_name or pipeline_job.name}"
            )
            yield from self._get_pipeline_mcps(pipeline_meta)
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
                yield from self._gen_pipeline_task_datajob(task, dataflow_urn)
                self._emitted_task_urns.add(task_urn_str)
            else:
                logger.debug(f"Skipping DataJob (already emitted): {task.name}")

            yield from self._gen_pipeline_task_run_instance(task, pipeline_meta)
            yield from self._gen_pipeline_task_lineage_patches(task)

    def _gen_pipeline_run_instance(
        self, pipeline_job: PipelineJob, pipeline_meta: PipelineMetadata
    ) -> Iterable[MetadataWorkUnit]:
        """Create DataProcessInstance for a pipeline run (execution)."""
        run_id = self.name_formatter.format_pipeline_run_id(pipeline_job.name)
        dpi_urn = builder.make_data_process_instance_urn(run_id)

        yield MetadataChangeProposalWrapper(
            entityUrn=dpi_urn,
            aspect=DataProcessInstancePropertiesClass(
                name=f"Run: {pipeline_job.display_name or pipeline_job.name}",
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
            subtype=VertexAISubTypes.PIPELINE_TASK_RUN,
            include_container=False,
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=dpi_urn,
            aspect=DataProcessInstanceRelationships(
                upstreamInstances=[],
                parentTemplate=str(pipeline_meta.urn.urn()),
            ),
        ).as_workunit()

    def _gen_pipeline_task_datajob(
        self, task: PipelineTaskMetadata, dataflow_urn: DataFlowUrn
    ) -> Iterable[MetadataWorkUnit]:
        """Emit stable DataJob entity for a pipeline task."""
        datajob = DataJob(
            name=self.name_formatter.format_pipeline_task_id(task.name),
            display_name=task.name,
            flow_urn=str(dataflow_urn),
            platform_instance=self.config.platform_instance,
        )

        yield from self._yield_common_aspects(
            entity_urn=datajob.urn.urn(),
            subtype=VertexAISubTypes.PIPELINE_TASK,
            include_container=False,
        )

        yield from datajob.as_workunits()

        # Explicitly emit Status aspect (SDK doesn't emit it automatically)
        yield MetadataChangeProposalWrapper(
            entityUrn=datajob.urn.urn(),
            aspect=StatusClass(removed=False),
        ).as_workunit()

        # Add upstream DataJob dependencies (task-to-task lineage)
        if task.upstreams:
            patch_builder = DataJobPatchBuilder(datajob.urn.urn())
            for upstream_urn in task.upstreams:
                patch_builder.add_input_datajob(upstream_urn)
            for mcp in patch_builder.build():
                yield MetadataWorkUnit(id=f"{datajob.urn.urn()}-patch", mcp_raw=mcp)

    def _gen_pipeline_task_run_instance(
        self, task: PipelineTaskMetadata, pipeline: PipelineMetadata
    ) -> Iterable[MetadataWorkUnit]:
        """Create DataProcessInstance for a task run (execution)."""
        # Use pipeline name (unique per execution) + task name for unique run URN
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
            subtype=VertexAISubTypes.PIPELINE_TASK_RUN,
            include_container=False,
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=dpi_urn,
            aspect=DataProcessInstanceRelationships(
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

        if task.output_model_urns:
            output_edges.extend(
                [EdgeClass(destinationUrn=urn) for urn in task.output_model_urns]
            )

        if output_edges:
            yield MetadataChangeProposalWrapper(
                entityUrn=dpi_urn,
                aspect=DataProcessInstanceOutputClass(
                    outputs=[],
                    outputEdges=output_edges,
                ),
            ).as_workunit()

    def _gen_pipeline_task_lineage_patches(
        self, task: PipelineTaskMetadata
    ) -> Iterable[MetadataWorkUnit]:
        """Use DataJobPatchBuilder to aggregate lineage from task runs to the stable DataJob."""
        task_urn_str = str(task.urn.urn())
        patch_builder = DataJobPatchBuilder(task_urn_str)

        # DataJobPatchBuilder only supports dataset URNs, not model URNs
        if task.input_dataset_urns:
            for input_urn in task.input_dataset_urns:
                if ":dataset:" in input_urn:
                    patch_builder.add_input_dataset(input_urn)

        if task.output_dataset_urns:
            for output_urn in task.output_dataset_urns:
                if ":dataset:" in output_urn:
                    patch_builder.add_output_dataset(output_urn)

        for patch_mcp in patch_builder.build():
            yield MetadataWorkUnit(
                id=f"{task_urn_str}-{patch_mcp.aspectName}", mcp_raw=patch_mcp
            )

    def _extract_pipeline_task_inputs(
        self, task_detail: PipelineTaskDetail, task_name: str, task_urn: str
    ) -> Optional[List[str]]:
        """Extract input dataset and model URNs from pipeline task artifacts."""
        if not task_detail.inputs:
            return None

        input_urns: List[str] = []
        try:
            for input_entry in task_detail.inputs.values():
                if input_entry.artifacts:
                    for artifact in input_entry.artifacts:
                        if artifact.uri:
                            model_urn = self.uri_parser.model_urn_from_artifact_uri(
                                artifact.uri
                            )
                            if model_urn:
                                input_urns.append(model_urn)
                                self.model_usage_tracker.track_usage(
                                    model_urn, task_urn
                                )
                            else:
                                input_urns.extend(
                                    self.uri_parser.dataset_urns_from_artifact_uri(
                                        artifact.uri
                                    )
                                )
            return input_urns if input_urns else None
        except (AttributeError, TypeError, KeyError) as e:
            logger.debug(
                f"Failed to extract input artifacts for task {task_name}: {e}",
                exc_info=True,
            )
            return None

    def _extract_pipeline_task_outputs(
        self, task_detail: PipelineTaskDetail, task_name: str
    ) -> PipelineTaskArtifacts:
        """Extract output dataset and model URNs from pipeline task artifacts."""
        if not task_detail.outputs:
            return PipelineTaskArtifacts()

        output_dataset_urns: List[str] = []
        output_model_urns: List[str] = []
        try:
            for output_entry in task_detail.outputs.values():
                if output_entry.artifacts:
                    for artifact in output_entry.artifacts:
                        if artifact.uri:
                            model_urn = self.uri_parser.model_urn_from_artifact_uri(
                                artifact.uri
                            )
                            if model_urn:
                                output_model_urns.append(model_urn)
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
                output_model_urns=output_model_urns if output_model_urns else None,
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
                    job_id=self.name_formatter.format_pipeline_task_id(task_name),
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
                            job_id=self.name_formatter.format_pipeline_task_id(
                                upstream_task
                            ),
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

                    task_meta.input_dataset_urns = self._extract_pipeline_task_inputs(
                        task_detail, task_name, dpi_urn
                    )
                    outputs = self._extract_pipeline_task_outputs(
                        task_detail, task_name
                    )
                    task_meta.output_dataset_urns = outputs.output_dataset_urns
                    task_meta.output_model_urns = outputs.output_model_urns

                tasks.append(task_meta)
        return tasks

    def _get_pipeline_metadata(self, pipeline: PipelineJob) -> PipelineMetadata:
        """Extract pipeline metadata from PipelineJob."""
        if self.config.incremental_lineage:
            stable_id = self._get_stable_pipeline_id(pipeline)
            flow_id = self.name_formatter.format_pipeline_id(stable_id)
        else:
            flow_id = self.name_formatter.format_pipeline_id(pipeline.name)

        dataflow_urn = DataFlowUrn.create_from_ids(
            orchestrator=self.platform,
            env=self.config.env,
            flow_id=flow_id,
            platform_instance=self.config.platform_instance,
        )
        tasks = self._get_pipeline_tasks_metadata(
            pipeline=pipeline, pipeline_urn=dataflow_urn
        )

        pipeline_meta = PipelineMetadata(
            name=pipeline.name,
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

    def _gen_pipeline_task_run_mcps(
        self, task: PipelineTaskMetadata, datajob: DataJob, pipeline: PipelineMetadata
    ) -> Iterable[MetadataWorkUnit]:
        """Legacy method for non-incremental mode."""
        # Use pipeline name (unique per execution) + task name for unique run URN
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
            subtype=VertexAISubTypes.PIPELINE_TASK_RUN,
            include_container=False,
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=dpi_urn,
            aspect=DataProcessInstanceRelationships(
                upstreamInstances=[], parentTemplate=datajob.urn.urn()
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
        if task.output_model_urns:
            output_edges.extend(
                [EdgeClass(destinationUrn=urn) for urn in task.output_model_urns]
            )

        if output_edges:
            yield MetadataChangeProposalWrapper(
                entityUrn=dpi_urn,
                aspect=DataProcessInstanceOutputClass(
                    outputs=[],
                    outputEdges=output_edges,
                ),
            ).as_workunit()

    def _gen_pipeline_task_mcps(
        self, pipeline: PipelineMetadata
    ) -> Iterable[MetadataWorkUnit]:
        """Legacy method for non-incremental mode."""
        dataflow_urn = pipeline.urn

        for task in pipeline.tasks:
            inlets = (
                [
                    DatasetUrn.from_string(urn)
                    for urn in task.input_dataset_urns
                    if ":dataset:" in urn
                ]
                if task.input_dataset_urns
                else []
            )
            outlets = (
                [
                    DatasetUrn.from_string(urn)
                    for urn in task.output_dataset_urns
                    if ":dataset:" in urn
                ]
                if task.output_dataset_urns
                else []
            )

            owner = get_actor_from_labels(pipeline.labels)
            datajob = DataJob(
                name=self.name_formatter.format_pipeline_task_id(task.name),
                display_name=task.name,
                flow_urn=str(dataflow_urn),
                custom_properties={},
                owners=[CorpUserUrn(owner)] if owner else None,
                inlets=cast(
                    Optional[List[Union[str, DatasetUrn]]], inlets if inlets else None
                ),
                outlets=cast(
                    Optional[List[Union[str, DatasetUrn]]], outlets if outlets else None
                ),
                external_url=self.url_builder.make_pipeline_url(pipeline.name),
                platform_instance=self.config.platform_instance,
            )

            yield from self._yield_common_aspects(
                entity_urn=datajob.urn.urn(),
                subtype=VertexAISubTypes.PIPELINE_TASK,
                include_platform=False,
                include_container=False,
            )
            yield from datajob.as_workunits()

            # Explicitly emit Status aspect (SDK doesn't emit it automatically)
            yield MetadataChangeProposalWrapper(
                entityUrn=datajob.urn.urn(),
                aspect=StatusClass(removed=False),
            ).as_workunit()

            # Add upstream DataJob dependencies (task-to-task lineage)
            if task.upstreams:
                patch_builder = DataJobPatchBuilder(datajob.urn.urn())
                for upstream_urn in task.upstreams:
                    patch_builder.add_input_datajob(upstream_urn)
                for mcp in patch_builder.build():
                    yield MetadataWorkUnit(id=f"{datajob.urn.urn()}-patch", mcp_raw=mcp)

            yield from self._gen_pipeline_task_run_mcps(task, datajob, pipeline)

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
        self, pipeline: PipelineMetadata
    ) -> Iterable[MetadataWorkUnit]:
        owner = get_actor_from_labels(pipeline.labels)
        # Use the URN that was already calculated in pipeline.urn (which respects incremental_lineage)
        dataflow = DataFlow(
            platform=self.platform,
            name=pipeline.urn.flow_id,
            display_name=pipeline.name,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
            custom_properties=self._get_pipeline_properties(
                pipeline
            ).to_custom_properties(),
            owners=[CorpUserUrn(owner)] if owner else None,
            external_url=self.url_builder.make_pipeline_url(
                pipeline_name=pipeline.name
            ),
        )

        yield from dataflow.as_workunits()

        # Explicitly emit Status aspect (SDK doesn't emit it automatically)
        yield MetadataChangeProposalWrapper(
            entityUrn=dataflow.urn.urn(),
            aspect=StatusClass(removed=False),
        ).as_workunit()

        yield from self._yield_common_aspects(
            entity_urn=dataflow.urn.urn(),
            subtype=VertexAISubTypes.PIPELINE,
            include_platform=False,
            resource_category=ResourceCategory.PIPELINES,
        )
