import itertools
import logging
import types
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from functools import partial
from typing import (
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
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
    EXTRA_ROOT_DETAILS,
    EXTRA_ROOT_DETAILS_TITLE,
    KUBEFLOW_TIMESTAMP_SUFFIX_PATTERN,
    LINEAGE_FLUSH_PENDING_LINKS,
    LINEAGE_MAX_LINKS_PER_PATCH,
    ORDER_BY_UPDATE_TIME_DESC,
    UNMATCHED_DETAILS,
    UNMATCHED_DETAILS_TITLE,
    UNMATCHED_SAMPLE_SIZE,
    DetailCalls,
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
    PipelineTaskRunMetadata,
    YieldCommonAspectsProtocol,
)
from datahub.ingestion.source.vertexai.vertexai_pipeline_tree import (
    PipelineStep,
    RunTree,
    build_run_tree,
    build_step_tree,
)
from datahub.ingestion.source.vertexai.vertexai_report import VertexAISourceReport
from datahub.ingestion.source.vertexai.vertexai_result_type_utils import (
    RunOutcome,
    is_cache_hit,
    pipeline_run_outcome,
    task_run_outcome,
)
from datahub.ingestion.source.vertexai.vertexai_scope import (
    CarryForward,
    DetailReader,
    DetailResult,
    Scope,
    StoppedScopes,
)
from datahub.ingestion.source.vertexai.vertexai_state import VertexAIStateHandler
from datahub.ingestion.source.vertexai.vertexai_utils import (
    gen_resource_subfolder_container,
    get_actor_from_labels,
    log_checkpoint_time,
    log_progress,
    rate_limited_gapic_list,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    DataProcessInstanceInputClass,
    DataProcessInstanceOutputClass,
    DataProcessInstancePropertiesClass,
    DataProcessInstanceRelationshipsClass,
    DataProcessInstanceRunEventClass,
    DataProcessInstanceRunResultClass,
    DataProcessRunStatusClass,
    EdgeClass,
    StatusClass,
)
from datahub.metadata.urns import CorpUserUrn, DataFlowUrn, DataJobUrn
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.specific.datajob import DataJobPatchBuilder
from datahub.utilities.ratelimiter import RateLimiter
from datahub.utilities.time import datetime_to_ts_millis

logger = logging.getLogger(__name__)


def _first_set_millis(*times: Optional[datetime]) -> Optional[int]:
    """The first time that is set and not zero, or None when none is."""
    for candidate in times:
        millis = datetime_to_ts_millis(candidate) if candidate else 0
        if millis:
            return millis
    return None


@dataclass
class _TaskLinks:
    input_datasets: Dict[str, None] = field(default_factory=dict)
    output_datasets: Dict[str, None] = field(default_factory=dict)
    upstream_jobs: Dict[str, None] = field(default_factory=dict)


class _PendingTaskLineage:
    """Task lineage links not yet sent, per task URN, in first-seen order.

    Each call of get_workunits makes its own, so phase-1 threads share none.
    """

    def __init__(self) -> None:
        self.tasks: Dict[str, _TaskLinks] = {}
        self.count = 0

    def add(self, task: PipelineTaskMetadata) -> None:
        links = self.tasks.setdefault(str(task.urn.urn()), _TaskLinks())
        self._add(links.input_datasets, task.input_dataset_urns or [])
        self._add(links.output_datasets, task.output_dataset_urns or [])
        self._add(links.upstream_jobs, (str(urn) for urn in task.upstreams or []))

    def _add(self, pending: Dict[str, None], urns: Iterable[str]) -> None:
        for urn in urns:
            if urn not in pending:
                pending[urn] = None
                self.count += 1

    def clear(self) -> None:
        self.tasks = {}
        self.count = 0


class VertexAIPipelineExtractor:
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
        model_usage_tracker: ModelUsageTracker,
        platform: str,
        state_handler: VertexAIStateHandler,
        report: VertexAISourceReport,
        get_scope_fn: Callable[[], Scope],
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
        self.model_usage_tracker = model_usage_tracker
        self.platform = platform
        self.state_handler = state_handler
        self.report = report
        self._get_scope = get_scope_fn
        self._stopped = stopped_scopes
        self._carry = carry_forward
        self._read_detail = DetailReader(report, stopped_scopes, get_scope_fn)
        self.rate_limiter = rate_limiter
        self._emitted_pipeline_urns: Set[str] = set()
        self._emitted_task_urns: Set[str] = set()
        self._lineage_patch_numbers = itertools.count()

    @staticmethod
    def _pipeline_spec(pipeline: PipelineJob) -> Mapping[str, object]:
        resource = pipeline.gca_resource
        if isinstance(resource, PipelineJobType):
            # An absent Struct reads as None.
            return resource.pipeline_spec or {}
        return {}

    def _get_stable_pipeline_id(self, pipeline: PipelineJob) -> str:
        """
        Prefer pipeline_spec.pipelineInfo.name (set at compile time, unaffected by
        display_name edits) over display_name. Falls back to display_name with the
        Kubeflow timestamp suffix stripped for non-Kubeflow pipelines (e.g. AutoML).
        """
        pipeline_info = self._pipeline_spec(pipeline).get("pipelineInfo")
        if isinstance(pipeline_info, Mapping):
            name = pipeline_info.get("name")
            if isinstance(name, str) and name:
                return name

        name = pipeline.display_name or pipeline.name
        return KUBEFLOW_TIMESTAMP_SUFFIX_PATTERN.sub("", name)

    def _flow_urn(self, pipeline: PipelineJob) -> DataFlowUrn:
        return DataFlowUrn.create_from_ids(
            orchestrator=self.platform,
            env=self.config.env,
            flow_id=self._get_stable_pipeline_id(pipeline),
            platform_instance=self.config.platform_instance,
        )

    def _read_pipeline_task_details(
        self, pipeline: PipelineJob
    ) -> DetailResult[List[PipelineTaskDetail]]:
        # The list response lacks job_detail; task_details fetches the job.
        def read() -> List[PipelineTaskDetail]:
            return list(pipeline.task_details)

        return self._read_detail(DetailCalls.PIPELINE_LOOKUP, read, item=pipeline.name)

    def _keep_pipeline(self, pipeline: PipelineJob) -> None:
        try:
            flow_urn = str(self._flow_urn(pipeline))
            container_urn = self._get_pipeline_container(
                self._get_stable_pipeline_id(pipeline)
            ).as_urn()
        except Exception as e:
            # Without a stable id the job's flow and folder have no URN, so every
            # earlier pipeline is kept, as for a lost pipelines read.
            logger.warning(
                f"Cannot compute the stable id of pipeline {pipeline.name}",
                exc_info=True,
            )
            lost = replace(
                self._get_scope(), kind=DetailCalls.PIPELINE_BUILD, item=pipeline.name
            )
            self._carry.keep_all_flows(lost, cause=e)
            self._carry.keep_all_containers(lost, cause=e, experiments=False)
            return
        self._carry.keep_flow(flow_urn)
        self._carry.keep_urn(container_urn)

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

        pipeline_jobs: List[PipelineJob] = rate_limited_gapic_list(
            self.client.PipelineJob,
            self.rate_limiter,
            order_by=ORDER_BY_UPDATE_TIME_DESC,
        )

        pending = _PendingTaskLineage()
        try:
            for job_count, pipeline in enumerate(pipeline_jobs, start=1):
                if last_checkpoint_millis:
                    job_update_millis = int(
                        pipeline.gca_resource.update_time.timestamp() * 1000
                    )
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
                details = self._read_pipeline_task_details(pipeline)
                pipeline_meta = (
                    None
                    if details.keep_previous
                    else self._read_detail(
                        DetailCalls.PIPELINE_BUILD,
                        partial(
                            self._get_pipeline_metadata, pipeline, details.value or []
                        ),
                        item=pipeline.name,
                    ).value
                )
                if pipeline_meta is None:
                    self._keep_pipeline(pipeline)
                    continue
                yield from self._get_pipeline_workunits(
                    pipeline,
                    pipeline_meta,
                    pending,
                    lookup_lost=details.value is None,
                )
                if pending.count >= LINEAGE_FLUSH_PENDING_LINKS:
                    yield from self._gen_task_lineage(pending)
        except Exception:
            # Runs read before the failure keep their lineage.
            yield from self._gen_task_lineage(pending)
            raise
        yield from self._gen_task_lineage(pending)

    def _get_pipeline_workunits(
        self,
        pipeline_job: PipelineJob,
        pipeline_meta: PipelineMetadata,
        pending: _PendingTaskLineage,
        *,
        lookup_lost: bool,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Emit stable DataFlow/DataJob entities only once,
        create DataProcessInstances for each run, and hold its lineage in `pending`.
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

        # The SDK's start_time and end_time properties fetch the job again.
        resource = pipeline_job.gca_resource
        run_created = _first_set_millis(
            pipeline_job.create_time, resource.start_time, resource.end_time
        )
        if run_created is None:
            self.report.report_run_skipped_without_time()
        else:
            yield from self._gen_pipeline_run_instance(
                pipeline_job, pipeline_meta, run_created
            )

        tasks_by_path = {task.path: task for task in pipeline_meta.tasks}
        for task in pipeline_meta.tasks:
            task_urn_str = str(task.urn.urn())

            if task_urn_str not in self._emitted_task_urns:
                logger.debug(f"Emitting stable DataJob for task: {task.path}")
                yield from self._gen_pipeline_task_datajob(
                    task, dataflow_urn, pipeline_container_key, tasks_by_path
                )
                self._emitted_task_urns.add(task_urn_str)
                if task.parent_path is not None:
                    self.report.report_pipeline_nested_step_written()
            else:
                logger.debug(f"Skipping DataJob (already emitted): {task.path}")

        for task_run in pipeline_meta.task_runs:
            yield from self._gen_pipeline_task_run_instance(
                task_run,
                pipeline_meta,
                tasks_by_path,
                has_pipeline_run_record=run_created is not None,
            )

        # A run whose details were lost must not write the links the spec implies.
        if not lookup_lost:
            for task in pipeline_meta.tasks:
                pending.add(task)

    def _gen_pipeline_run_instance(
        self,
        pipeline_job: PipelineJob,
        pipeline_meta: PipelineMetadata,
        created_millis: int,
    ) -> Iterable[MetadataWorkUnit]:
        dpi_urn = self._pipeline_run_urn(pipeline_job.name)

        resource = pipeline_job.gca_resource

        yield MetadataChangeProposalWrapper(
            entityUrn=dpi_urn,
            aspect=DataProcessInstancePropertiesClass(
                name=pipeline_job.display_name or pipeline_job.name,
                created=AuditStampClass(
                    time=created_millis,
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
            subtype=MLAssetSubTypes.VERTEX_PIPELINE_RUN,
            include_container=False,
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=dpi_urn,
            aspect=DataProcessInstanceRelationshipsClass(
                upstreamInstances=[],
                parentTemplate=str(pipeline_meta.urn.urn()),
            ),
        ).as_workunit()

        yield from self._gen_run_events(
            dpi_urn,
            start_time=resource.start_time,
            end_time=resource.end_time,
            outcome=pipeline_run_outcome(resource.state),
        )

    @staticmethod
    def _gen_run_events(
        dpi_urn: str,
        *,
        start_time: Optional[datetime],
        end_time: Optional[datetime],
        outcome: Optional[RunOutcome],
        attempt: Optional[int] = None,
    ) -> Iterable[MetadataWorkUnit]:
        start_millis = _first_set_millis(start_time)
        if start_millis is not None:
            yield MetadataChangeProposalWrapper(
                entityUrn=dpi_urn,
                aspect=DataProcessInstanceRunEventClass(
                    status=DataProcessRunStatusClass.STARTED,
                    timestampMillis=start_millis,
                ),
            ).as_workunit()
        end_millis = _first_set_millis(end_time)
        # An end time without an outcome is a state we cannot map, so no result.
        if outcome is None or end_millis is None:
            return
        yield MetadataChangeProposalWrapper(
            entityUrn=dpi_urn,
            aspect=DataProcessInstanceRunEventClass(
                status=DataProcessRunStatusClass.COMPLETE,
                timestampMillis=end_millis,
                result=DataProcessInstanceRunResultClass(
                    type=outcome.result, nativeResultType=outcome.native
                ),
                durationMillis=(
                    end_millis - start_millis if start_millis is not None else None
                ),
                attempt=attempt,
            ),
        ).as_workunit()

    def _gen_pipeline_task_datajob(
        self,
        task: PipelineTaskMetadata,
        dataflow_urn: DataFlowUrn,
        container_key: PipelineContainerKey,
        tasks_by_path: Mapping[str, PipelineTaskMetadata],
    ) -> Iterable[MetadataWorkUnit]:
        datajob = DataJob(
            name=task.path,
            display_name=task.shown_name,
            flow_urn=str(dataflow_urn),
            platform_instance=self.config.platform_instance,
        )

        datajob._set_container(container_key)
        self._append_container_step_browse_path(datajob, task, tasks_by_path)

        # The SDK DataJob writes the container and platform instance.
        yield from self._yield_common_aspects(
            entity_urn=datajob.urn.urn(),
            subtype=MLAssetSubTypes.VERTEX_PIPELINE_TASK,
            include_container=False,
            include_platform=False,
        )

        yield from datajob.as_workunits()

        yield MetadataChangeProposalWrapper(
            entityUrn=datajob.urn.urn(),
            aspect=StatusClass(removed=False),
        ).as_workunit()

    @staticmethod
    def _step_browse_entries(
        task: PipelineTaskMetadata, tasks_by_path: Mapping[str, PipelineTaskMetadata]
    ) -> List[BrowsePathEntryClass]:
        """Ancestor container steps of `task`, outermost first."""
        ancestors: List[PipelineTaskMetadata] = []
        parent_path = task.parent_path
        while parent_path is not None:
            parent = tasks_by_path[parent_path]
            ancestors.append(parent)
            parent_path = parent.parent_path
        return [
            BrowsePathEntryClass(id=str(a.urn.urn()), urn=str(a.urn.urn()))
            for a in reversed(ancestors)
        ]

    def _append_container_step_browse_path(
        self,
        datajob: DataJob,
        task: PipelineTaskMetadata,
        tasks_by_path: Mapping[str, PipelineTaskMetadata],
    ) -> None:
        entries = self._step_browse_entries(task, tasks_by_path)
        if not entries:
            return
        browse_paths = datajob._get_aspect(BrowsePathsV2Class)
        assert browse_paths is not None
        # Must run after _set_container, which replaces the whole path.
        datajob._set_aspect(BrowsePathsV2Class(path=[*browse_paths.path, *entries]))

    def _pipeline_run_urn(self, pipeline_name: str) -> str:
        return builder.make_data_process_instance_urn(
            self.name_formatter.format_pipeline_run_id(pipeline_name)
        )

    def _task_run_urn(self, pipeline_name: str, run_path: str) -> str:
        return builder.make_data_process_instance_urn(
            self.name_formatter.format_pipeline_task_run_id(
                entity_id=f"{pipeline_name}_{run_path}"
            )
        )

    def _gen_pipeline_task_run_instance(
        self,
        task_run: PipelineTaskRunMetadata,
        pipeline: PipelineMetadata,
        tasks_by_path: Mapping[str, PipelineTaskMetadata],
        has_pipeline_run_record: bool,
    ) -> Iterable[MetadataWorkUnit]:
        run = task_run.run
        detail = run.detail
        create_millis = _first_set_millis(
            detail.create_time, detail.start_time, detail.end_time
        )
        # Unreachable while is_started requires start_time.
        if create_millis is None:
            self.report.report_run_skipped_without_time()
            return
        dpi_urn = self._task_run_urn(pipeline.name, run.run_path)
        parent_instance: Optional[str]
        if run.parent_run_path is not None:
            parent_instance = self._task_run_urn(pipeline.name, run.parent_run_path)
        elif has_pipeline_run_record:
            parent_instance = self._pipeline_run_urn(pipeline.name)
        else:
            parent_instance = None

        yield MetadataChangeProposalWrapper(
            entityUrn=dpi_urn,
            aspect=DataProcessInstancePropertiesClass(
                name=run.step.shown_name,
                created=AuditStampClass(
                    time=create_millis,
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
                upstreamInstances=[
                    self._task_run_urn(pipeline.name, upstream_run_path)
                    for upstream_run_path in run.upstream_run_paths
                ],
                parentTemplate=str(tasks_by_path[run.step.path].urn.urn()),
                parentInstance=parent_instance,
            ),
        ).as_workunit()

        yield from self._gen_run_events(
            dpi_urn,
            start_time=detail.start_time,
            end_time=detail.end_time,
            outcome=task_run_outcome(detail),
            # Vertex copies the earlier run's failed jobs into a cached detail.
            attempt=(
                1
                if is_cache_hit(detail)
                else len(detail.executor_detail.container_detail.failed_main_jobs) + 1
            ),
        )

        input_edges: List[EdgeClass] = [
            EdgeClass(destinationUrn=urn)
            for urn in task_run.artifacts.input_dataset_urns
        ]
        if input_edges:
            yield MetadataChangeProposalWrapper(
                entityUrn=dpi_urn,
                aspect=DataProcessInstanceInputClass(
                    inputs=[],
                    inputEdges=input_edges,
                ),
            ).as_workunit()

        # Model outputs are tracked for downstream lineage but not emitted as edges here,
        # since we need to resolve resource names to URNs when processing the actual models
        output_edges: List[EdgeClass] = [
            EdgeClass(destinationUrn=urn)
            for urn in task_run.artifacts.output_dataset_urns
        ]
        if output_edges:
            yield MetadataChangeProposalWrapper(
                entityUrn=dpi_urn,
                aspect=DataProcessInstanceOutputClass(
                    outputs=[],
                    outputEdges=output_edges,
                ),
            ).as_workunit()

    def _gen_task_lineage(
        self, pending: _PendingTaskLineage
    ) -> Iterable[MetadataWorkUnit]:
        """Send the pending links as add patches, whatever `incremental_lineage` says.

        A task keeps the links of all its runs, and a link stays after a pipeline
        stops using a table. A task with no links gets no patch.
        """
        patches_sent = 0
        links_sent = 0
        for task_urn, links in pending.tasks.items():
            adds: List[Tuple[Callable[[DataJobPatchBuilder, str], object], str]] = [
                *(
                    (DataJobPatchBuilder.add_input_dataset, urn)
                    for urn in links.input_datasets
                ),
                *(
                    (DataJobPatchBuilder.add_output_dataset, urn)
                    for urn in links.output_datasets
                ),
                *(
                    (DataJobPatchBuilder.add_input_datajob, urn)
                    for urn in links.upstream_jobs
                ),
            ]
            links_sent += len(adds)
            for start in range(0, len(adds), LINEAGE_MAX_LINKS_PER_PATCH):
                patch_builder = DataJobPatchBuilder(task_urn)
                for add, urn in adds[start : start + LINEAGE_MAX_LINKS_PER_PATCH]:
                    add(patch_builder, urn)
                for patch_mcp in patch_builder.build():
                    patches_sent += 1
                    yield MetadataWorkUnit(
                        id=f"{task_urn}-{patch_mcp.aspectName}-{next(self._lineage_patch_numbers)}",
                        mcp_raw=patch_mcp,
                    )
        pending.clear()
        self.report.report_pipeline_lineage_sent(patches=patches_sent, links=links_sent)

    def _extract_pipeline_task_artifacts(
        self,
        task_detail: PipelineTaskDetail,
        task_urn: str,
    ) -> PipelineTaskArtifacts:
        inputs = self._extract_pipeline_task_inputs(task_detail, task_urn)
        outputs = self._extract_pipeline_task_outputs(task_detail, task_urn)
        return PipelineTaskArtifacts(
            input_dataset_urns=inputs.input_dataset_urns,
            output_dataset_urns=outputs.output_dataset_urns,
            output_model_resource_names=outputs.output_model_resource_names,
        )

    def _extract_pipeline_task_inputs(
        self, task_detail: PipelineTaskDetail, task_urn: str
    ) -> PipelineTaskArtifacts:
        if not task_detail.inputs:
            return PipelineTaskArtifacts()

        input_dataset_urns: List[str] = []
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
            input_dataset_urns=input_dataset_urns,
        )

    def _extract_pipeline_task_outputs(
        self, task_detail: PipelineTaskDetail, task_urn: str
    ) -> PipelineTaskArtifacts:
        if not task_detail.outputs:
            return PipelineTaskArtifacts()

        output_dataset_urns: List[str] = []
        output_model_resource_names: List[str] = []
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
            output_dataset_urns=output_dataset_urns,
            output_model_resource_names=output_model_resource_names,
        )

    def _read_task_runs(
        self, pipeline: PipelineJob, pipeline_urn: DataFlowUrn, tree: RunTree
    ) -> List[PipelineTaskRunMetadata]:
        task_runs: List[PipelineTaskRunMetadata] = []
        for run in tree.runs:
            logger.debug(
                f"reading task run ({run.run_path}) in pipeline ({pipeline.name})"
            )
            # Keeps the URN shape the artifact tracker has always used.
            tracker_urn = builder.make_data_process_instance_urn(
                self.name_formatter.format_pipeline_task_run_id(entity_id=run.step.path)
            )
            read_artifacts = self._read_detail(
                DetailCalls.PIPELINE_TASK_ARTIFACTS,
                partial(self._extract_pipeline_task_artifacts, run.detail, tracker_urn),
                item=f"{pipeline_urn.flow_id}/{pipeline.name}/{run.run_path}",
            )
            task_runs.append(
                PipelineTaskRunMetadata(
                    run=run, artifacts=read_artifacts.value or PipelineTaskArtifacts()
                )
            )
        return task_runs

    def _report_details_without_run(
        self,
        pipeline: PipelineJob,
        *,
        title: str,
        message: str,
        count: Callable[[int], None],
        label: str,
        details: List[str],
    ) -> None:
        """Count `details` and write one warning naming a sample of them."""
        if not details:
            return
        count(len(details))
        self.report.warning(
            title=title,
            message=message,
            context=(
                f"pipeline={pipeline.name}, {label}={len(details)}, "
                f"sample={details[:UNMATCHED_SAMPLE_SIZE]}"
            ),
        )

    def _get_pipeline_tasks_metadata(
        self,
        pipeline_urn: DataFlowUrn,
        steps: Mapping[str, PipelineStep],
        task_runs: List[PipelineTaskRunMetadata],
    ) -> List[PipelineTaskMetadata]:
        runs_by_step: Dict[str, List[PipelineTaskRunMetadata]] = {}
        for task_run in task_runs:
            runs_by_step.setdefault(task_run.run.step.path, []).append(task_run)

        tasks: List[PipelineTaskMetadata] = list()
        for step in steps.values():
            step_runs = runs_by_step.get(step.path, [])
            tasks.append(
                PipelineTaskMetadata(
                    path=step.path,
                    shown_name=step.shown_name,
                    parent_path=step.parent_path,
                    urn=DataJobUrn.create_from_ids(
                        data_flow_urn=pipeline_urn.urn(), job_id=step.path
                    ),
                    upstreams=[
                        DataJobUrn.create_from_ids(
                            data_flow_urn=pipeline_urn.urn(), job_id=upstream_path
                        )
                        for upstream_path in step.upstream_paths
                    ],
                    input_dataset_urns=list(
                        dict.fromkeys(
                            urn
                            for step_run in step_runs
                            for urn in step_run.artifacts.input_dataset_urns
                        )
                    ),
                    output_dataset_urns=list(
                        dict.fromkeys(
                            urn
                            for step_run in step_runs
                            for urn in step_run.artifacts.output_dataset_urns
                        )
                    ),
                )
            )
        return tasks

    def _get_pipeline_metadata(
        self, pipeline: PipelineJob, task_details: List[PipelineTaskDetail]
    ) -> PipelineMetadata:
        stable_id = self._get_stable_pipeline_id(pipeline)
        dataflow_urn = self._flow_urn(pipeline)
        steps = build_step_tree(self._pipeline_spec(pipeline))
        tree = build_run_tree(steps, task_details, run_name=pipeline.name)
        self.report.report_pipeline_loop_passes(tree.loop_passes)
        self.report.report_pipeline_task_runs_skipped_not_started(tree.not_started)
        self._report_details_without_run(
            pipeline,
            title=UNMATCHED_DETAILS_TITLE,
            message=UNMATCHED_DETAILS,
            count=self.report.report_pipeline_task_details_without_step,
            label="unmatched",
            details=tree.unmatched,
        )
        self._report_details_without_run(
            pipeline,
            title=EXTRA_ROOT_DETAILS_TITLE,
            message=EXTRA_ROOT_DETAILS,
            count=self.report.report_pipeline_task_details_extra_roots,
            label="extra",
            details=tree.extra_roots,
        )
        task_runs = self._read_task_runs(pipeline, dataflow_urn, tree)
        tasks = self._get_pipeline_tasks_metadata(dataflow_urn, steps, task_runs)

        pipeline_meta = PipelineMetadata(
            name=pipeline.name,
            display_name=stable_id,
            resource_name=pipeline.resource_name,
            urn=dataflow_urn,
            tasks=tasks,
            task_runs=task_runs,
        )
        pipeline_meta.resource_name = pipeline.resource_name
        pipeline_meta.labels = pipeline.labels
        pipeline_meta.create_time = pipeline.create_time
        pipeline_meta.region = pipeline.location
        # Read from the listed resource: the SDK's update_time property fetches
        # the job again.
        update_time = pipeline.gca_resource.update_time
        if update_time:
            pipeline_meta.update_time = update_time
            if pipeline.create_time:
                pipeline_meta.duration = timedelta(
                    milliseconds=datetime_to_ts_millis(update_time)
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

        # display_name holds the stable pipeline id that the task URNs use.
        dataflow = DataFlow(
            platform=self.platform,
            name=pipeline.display_name,
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
