import logging
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import partial
from typing import (
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    TypeVar,
    Union,
)

from google.api_core.exceptions import NotFound
from google.cloud.aiplatform import (
    CustomJob,
    ExperimentRun,
    initializer as vertex_initializer,
)
from google.cloud.aiplatform.metadata import (
    constants as metadata_constants,
    utils as metadata_utils,
)
from google.cloud.aiplatform.metadata.artifact import Artifact
from google.cloud.aiplatform.metadata.context import Context as MetadataContext
from google.cloud.aiplatform.metadata.execution import (
    Execution as MetadataExecution,
)
from google.cloud.aiplatform.metadata.experiment_resources import Experiment

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ExperimentKey
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
    DetailCalls,
    ResourceCategory,
    ResourceReads,
    ResourceTypes,
)
from datahub.ingestion.source.vertexai.vertexai_models import (
    ExperimentMetadata,
    ExperimentRunMetadata,
    ModelUsageTracker,
    RunTimestamps,
    YieldCommonAspectsProtocol,
)
from datahub.ingestion.source.vertexai.vertexai_report import VertexAISourceReport
from datahub.ingestion.source.vertexai.vertexai_result_type_utils import (
    get_execution_result_status,
    is_status_for_run_event_class,
)
from datahub.ingestion.source.vertexai.vertexai_scope import (
    DetailReader,
    Scope,
    StoppedScopes,
    read_list_item,
    scope_context,
)
from datahub.ingestion.source.vertexai.vertexai_state import VertexAIStateHandler
from datahub.ingestion.source.vertexai.vertexai_utils import (
    filter_by_update_time,
    gen_resource_subfolder_container,
    get_actor_from_labels,
    log_checkpoint_time,
    log_progress,
    rate_limited_gapic_list,
    sort_by_update_time,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ContainerClass,
    DataProcessInstanceInputClass,
    DataProcessInstanceOutputClass,
    DataProcessInstancePropertiesClass,
    DataProcessInstanceRelationshipsClass,
    DataProcessInstanceRunEventClass,
    DataProcessInstanceRunResultClass,
    DataProcessRunStatusClass,
    EdgeClass,
    MLHyperParamClass,
    MLMetricClass,
    MLTrainingRunPropertiesClass,
    RunResultTypeClass,
)
from datahub.utilities.ratelimiter import RateLimiter
from datahub.utilities.time import datetime_to_ts_millis

logger = logging.getLogger(__name__)

T = TypeVar("T")

_RUN_CALL_KINDS = (
    DetailCalls.EXPERIMENT_RUN_EXECUTIONS,
    DetailCalls.EXPERIMENT_RUN_LOGGED_JOBS,
    DetailCalls.EXPERIMENT_RUN_DETAIL,
)


@dataclass
class _RunState:
    state: str
    params: List[MLHyperParamClass]
    metrics: List[MLMetricClass]


@dataclass
class _RunReads:
    executions: List[MetadataExecution]
    logged_jobs: List[CustomJob]
    state: _RunState


@dataclass
class _ExecutionArtifacts:
    inputs: List[Artifact]
    outputs: List[Artifact]


class VertexAIExperimentExtractor:
    def __init__(
        self,
        config: VertexAIConfig,
        urn_builder: VertexAIUrnBuilder,
        name_formatter: VertexAINameFormatter,
        url_builder: VertexAIExternalURLBuilder,
        uri_parser: VertexAIURIParser,
        get_project_id_fn: Callable[[], str],
        yield_common_aspects_fn: YieldCommonAspectsProtocol,
        model_usage_tracker: ModelUsageTracker,
        platform: str,
        report: VertexAISourceReport,
        state_handler: VertexAIStateHandler,
        get_scope_fn: Callable[[], Scope],
        stopped_scopes: StoppedScopes,
        rate_limiter: Union[RateLimiter, AbstractContextManager[None]] = nullcontext(),
    ):
        self.config = config
        self.urn_builder = urn_builder
        self.name_formatter = name_formatter
        self.url_builder = url_builder
        self.uri_parser = uri_parser
        self._get_project_id = get_project_id_fn
        self._yield_common_aspects = yield_common_aspects_fn
        self.model_usage_tracker = model_usage_tracker
        self.platform = platform
        self.report = report
        self.state_handler = state_handler
        self.rate_limiter = rate_limiter
        self._get_scope = get_scope_fn
        self._stopped = stopped_scopes
        self._read_detail = DetailReader(report, stopped_scopes, get_scope_fn)

        self.experiments: Optional[List[ExperimentMetadata]] = None

    def _metadata_store_parent(self, experiment: Optional[Experiment] = None) -> str:
        if experiment is not None:
            ctx = experiment._metadata_context
            return f"projects/{ctx.project}/locations/{ctx.location}/metadataStores/default"
        return (
            vertex_initializer.global_config.common_location_path()
            + "/metadataStores/default"
        )

    def _list_experiments_rate_limited(self) -> List[Experiment]:
        filter_str = metadata_utils._make_filter_string(
            schema_title=metadata_constants.SYSTEM_EXPERIMENT
        )
        try:
            contexts = rate_limited_gapic_list(
                MetadataContext,
                self.rate_limiter,
                parent=self._metadata_store_parent(),
                filter_str=filter_str,
            )
        except NotFound:
            # Vertex AI creates the default Metadata Store lazily on first use of
            # ML Metadata, so a 404 here means the project has no experiments, and
            # earlier experiments are really gone.
            self.report.info(
                title="Vertex AI Metadata Store not found",
                message=(
                    "Skipping experiment extraction because the project has no "
                    "default Vertex AI ML Metadata Store. This is expected for "
                    "projects that have never logged an Experiment or run a "
                    "Vertex AI Pipeline."
                ),
                context=scope_context(self._get_scope()),
            )
            return []
        experiments = []
        for ctx in contexts:
            if (
                metadata_constants.TENSORBOARD_CUSTOM_JOB_EXPERIMENT_FIELD
                not in ctx.metadata
            ):
                exp = Experiment.__new__(Experiment)
                exp._metadata_context = ctx
                experiments.append(exp)
        return experiments

    def _list_experiment_runs_rate_limited(
        self, experiment: Experiment
    ) -> List[ExperimentRun]:
        parent = self._metadata_store_parent(experiment)
        run_context_filter = metadata_utils._make_filter_string(
            schema_title=metadata_constants.SYSTEM_EXPERIMENT_RUN,
            parent_contexts=[experiment.resource_name],
        )
        run_exec_filter = metadata_utils._make_filter_string(
            schema_title=metadata_constants.SYSTEM_RUN,
            in_context=[experiment.resource_name],
        )
        nodes: List[Union[MetadataContext, MetadataExecution]] = [
            *rate_limited_gapic_list(
                MetadataContext,
                self.rate_limiter,
                parent=parent,
                filter_str=run_context_filter,
            ),
            *rate_limited_gapic_list(
                MetadataExecution,
                self.rate_limiter,
                parent=parent,
                filter_str=run_exec_filter,
            ),
        ]
        runs: List[ExperimentRun] = []
        for node in nodes:
            run = ExperimentRun.__new__(ExperimentRun)
            run._initialize_experiment_run(node, experiment)
            runs.append(run)
        return runs

    def get_experiment_workunits(self) -> Iterable[MetadataWorkUnit]:
        logger.info("Fetching Experiments from Vertex AI")

        last_checkpoint_millis = self.state_handler.get_last_update_time(
            ResourceTypes.EXPERIMENT
        )
        if last_checkpoint_millis:
            log_checkpoint_time(last_checkpoint_millis, "Experiment")

        experiments = self._list_experiments_rate_limited()
        filtered = [
            e
            for e in experiments
            if self.config.experiment_name_pattern.allowed(e.name)
        ]

        experiment_metadata = [
            ExperimentMetadata(
                experiment=e,
                name=e.name,
                update_time=getattr(e, "update_time", None),
            )
            for e in filtered
        ]
        sort_by_update_time(experiment_metadata)

        if last_checkpoint_millis:
            experiment_metadata = filter_by_update_time(
                experiment_metadata,
                last_checkpoint_millis,
                resource_type="experiments",
            )

        if self.config.max_experiments is not None:
            experiment_metadata = experiment_metadata[: self.config.max_experiments]
        self.experiments = experiment_metadata

        logger.info(f"Processing {len(self.experiments)} experiments")
        total = len(self.experiments)
        for i, experiment_meta in enumerate(self.experiments, start=1):
            if experiment_meta.update_time:
                self.state_handler.update_resource_timestamp(
                    ResourceTypes.EXPERIMENT,
                    int(experiment_meta.update_time.timestamp() * 1000),
                )

            log_progress(i, total, "experiments")
            yield from self._gen_experiment_workunits(experiment_meta)

    def get_experiment_run_workunits(self) -> Iterable[MetadataWorkUnit]:
        if self.experiments is None:
            logger.info("Fetching Experiments from Vertex AI")
            raw_experiments = self._list_experiments_rate_limited()
            filtered_experiments = [
                e
                for e in raw_experiments
                if self.config.experiment_name_pattern.allowed(e.name)
            ]
            experiment_metadata = [
                ExperimentMetadata(
                    experiment=e,
                    name=e.name,
                    update_time=getattr(e, "update_time", None),
                )
                for e in filtered_experiments
            ]
            experiment_metadata.sort(
                key=lambda e: (
                    e.update_time or datetime.min.replace(tzinfo=timezone.utc)
                ),
                reverse=True,
            )
            self.experiments = experiment_metadata
            logger.info(f"Retrieved {len(self.experiments)} experiments")

        last_checkpoint_millis = self.state_handler.get_last_update_time(
            ResourceTypes.EXPERIMENT_RUN
        )
        if last_checkpoint_millis:
            log_checkpoint_time(last_checkpoint_millis, "ExperimentRun")

        for experiment_meta in self.experiments:
            logger.info(
                f"Fetching ExperimentRuns for experiment {experiment_meta.name}"
            )
            scope = self._get_scope()
            runs = read_list_item(
                Scope(
                    project=scope.project,
                    region=scope.region,
                    item=experiment_meta.name,
                ),
                partial(
                    self._list_experiment_runs_rate_limited, experiment_meta.experiment
                ),
                report=self.report,
                stopped=self._stopped,
                call_kind=ResourceReads.EXPERIMENTS,
            )
            if runs is None:
                continue

            run_metadata = [
                ExperimentRunMetadata(
                    run=r,
                    name=r.name,
                    update_time=getattr(r, "update_time", None),
                    experiment_name=experiment_meta.name,
                )
                for r in runs
            ]
            sort_by_update_time(run_metadata)

            if last_checkpoint_millis:
                run_metadata = filter_by_update_time(
                    run_metadata,
                    last_checkpoint_millis,
                    resource_type="experiment runs",
                )

            if self.config.max_runs_per_experiment is not None:
                run_metadata = run_metadata[: self.config.max_runs_per_experiment]

            for i, run_meta in enumerate(run_metadata, start=1):
                if run_meta.update_time:
                    self.state_handler.update_resource_timestamp(
                        ResourceTypes.EXPERIMENT_RUN,
                        int(run_meta.update_time.timestamp() * 1000),
                    )
                log_progress(i, None, f"runs for experiment {experiment_meta.name}")
                yield from self._gen_experiment_run_mcps(experiment_meta, run_meta)

    def _gen_experiment_workunits(
        self, experiment_meta: ExperimentMetadata
    ) -> Iterable[MetadataWorkUnit]:
        experiment = experiment_meta.experiment
        yield from gen_resource_subfolder_container(
            project_id=self._get_project_id(),
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            resource_category=ResourceCategory.EXPERIMENTS,
            container_key=ExperimentKey(
                platform=self.platform,
                instance=self.config.platform_instance,
                env=self.config.env,
                id=self.name_formatter.format_experiment_id(experiment.name),
            ),
            name=experiment.name,
            sub_types=[MLAssetSubTypes.VERTEX_EXPERIMENT],
            extra_properties={
                "name": experiment.name,
                "resourceName": experiment.resource_name,
                "dashboardURL": experiment.dashboard_url
                if experiment.dashboard_url
                else "",
            },
            external_url=self.url_builder.make_experiment_url(experiment.name),
        )

    def _read_run_state(self, run: ExperimentRun) -> _RunState:
        return _RunState(
            state=run.get_state(),
            params=[
                MLHyperParamClass(name=k, value=str(v))
                for k, v in run.get_params().items()
            ],
            metrics=[
                MLMetricClass(name=k, value=str(v))
                for k, v in run.get_metrics().items()
            ],
        )

    def _read_logged_custom_jobs(self, run: ExperimentRun) -> List[CustomJob]:
        # The SDK only sets this key for runs that logged a custom job, and
        # fails on every other run.
        node = getattr(run, "_metadata_node", None)
        metadata = getattr(node, "metadata", None)
        if (
            isinstance(metadata, Mapping)
            and metadata_constants._CUSTOM_JOB_KEY not in metadata
        ):
            return []
        with self.rate_limiter:
            return list(run.get_logged_custom_jobs())

    def _logged_training_job_urns(self, custom_jobs: List[CustomJob]) -> List[str]:
        """Return DataProcessInstance URNs for any CustomJobs logged to this run."""
        return [
            builder.make_data_process_instance_urn(
                self.name_formatter.format_job_name(entity_id=job.name)
            )
            for job in custom_jobs
        ]

    def _get_run_timestamps(self, executions: List[MetadataExecution]) -> RunTimestamps:
        if len(executions) == 1:
            create_time = executions[0].create_time
            update_time = executions[0].update_time
            if create_time and update_time:
                duration = (
                    update_time.timestamp() * 1000 - create_time.timestamp() * 1000
                )
                return RunTimestamps(
                    created_time_ms=int(create_time.timestamp() * 1000),
                    duration_ms=int(duration),
                )
        return RunTimestamps()

    def _get_run_result_status(self, status: str) -> Union[str, RunResultTypeClass]:
        """Convert experiment run status to DataHub RunResultType."""
        if status == "COMPLETE":
            return RunResultTypeClass.SUCCESS
        elif status == "FAILED":
            return RunResultTypeClass.FAILURE
        elif status == "RUNNING":
            return "RUNNING"
        else:
            return "UNKNOWN"

    def _make_custom_properties_for_run(
        self,
        experiment: Experiment,
        run: ExperimentRun,
        executions: List[MetadataExecution],
    ) -> Dict[str, str]:
        properties: Dict[str, str] = dict()
        properties["externalUrl"] = self.url_builder.make_experiment_run_url(
            experiment, run
        )
        for exec in executions:
            exec_name = exec.name
            properties[f"created time ({exec_name})"] = str(exec.create_time)
            properties[f"update time ({exec_name})"] = str(exec.update_time)
        return properties

    def _make_custom_properties_for_execution(
        self, artifacts: _ExecutionArtifacts
    ) -> Dict[str, str]:
        properties: Dict[str, str] = dict()
        for input in artifacts.inputs:
            input_name = getattr(input, "name", "")
            properties[f"input artifact ({input_name})"] = getattr(input, "uri", "")
        for output in artifacts.outputs:
            output_name = getattr(output, "name", "")
            properties[f"output artifact ({output_name})"] = getattr(output, "uri", "")

        return properties

    def _extract_edges_from_artifacts(
        self, artifacts: List[Artifact], execution_urn: str, is_input: bool
    ) -> List[EdgeClass]:
        """Extract dataset and model URNs from artifacts and convert to edges."""
        edges: List[EdgeClass] = []
        for artifact in artifacts:
            for ds_urn in self.uri_parser.dataset_urns_from_artifact_uri(artifact.uri):
                edges.append(EdgeClass(destinationUrn=ds_urn))

            model_urn = self.uri_parser.model_urn_from_artifact_uri(artifact.uri)
            if model_urn:
                edges.append(EdgeClass(destinationUrn=model_urn))
                if is_input:
                    self.model_usage_tracker.track_model_usage(model_urn, execution_urn)

        return edges

    def _gen_run_execution(
        self, execution: MetadataExecution, run: ExperimentRun, exp: Experiment
    ) -> Iterable[MetadataWorkUnit]:
        create_time = execution.create_time
        update_time = execution.update_time
        duration: Optional[int] = None
        if create_time and update_time:
            duration = datetime_to_ts_millis(update_time) - datetime_to_ts_millis(
                create_time
            )
        result_status: Union[str, RunResultTypeClass] = get_execution_result_status(
            execution.state
        )
        execution_urn = builder.make_data_process_instance_urn(
            self.name_formatter.format_run_execution_name(execution.name)
        )

        read_artifacts = self._read_detail(
            DetailCalls.EXPERIMENT_RUN_ARTIFACTS,
            lambda: _ExecutionArtifacts(
                inputs=list(execution.get_input_artifacts()),
                outputs=list(execution.get_output_artifacts()),
            ),
            item=f"{exp.name}/{run.name}/{execution.name}",
        )
        if read_artifacts.keep_previous:
            return
        artifacts = read_artifacts.value or _ExecutionArtifacts(inputs=[], outputs=[])

        input_edges = self._extract_edges_from_artifacts(
            artifacts.inputs, execution_urn, is_input=True
        )
        output_edges = self._extract_edges_from_artifacts(
            artifacts.outputs, execution_urn, is_input=False
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=execution_urn,
            aspect=DataProcessInstancePropertiesClass(
                name=execution.name,
                created=AuditStampClass(
                    time=datetime_to_ts_millis(create_time) if create_time else 0,
                    actor=get_actor_from_labels(getattr(execution, "labels", None))
                    or builder.UNKNOWN_USER,
                ),
                externalUrl=self.url_builder.make_artifact_url(experiment=exp, run=run),
                customProperties=self._make_custom_properties_for_execution(artifacts),
            ),
        ).as_workunit()

        yield from self._yield_common_aspects(
            entity_urn=execution_urn,
            subtype=MLAssetSubTypes.VERTEX_EXECUTION,
            include_container=False,
        )

        if is_status_for_run_event_class(result_status) and duration:
            yield MetadataChangeProposalWrapper(
                entityUrn=execution_urn,
                aspect=DataProcessInstanceRunEventClass(
                    status=DataProcessRunStatusClass.COMPLETE,
                    timestampMillis=datetime_to_ts_millis(create_time)
                    if create_time
                    else 0,
                    result=DataProcessInstanceRunResultClass(
                        type=result_status,
                        nativeResultType=self.platform,
                    ),
                    durationMillis=int(duration),
                ),
            ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=execution_urn,
            aspect=DataProcessInstanceRelationshipsClass(
                upstreamInstances=[self.urn_builder.make_experiment_run_urn(exp, run)],
                parentInstance=self.urn_builder.make_experiment_run_urn(exp, run),
            ),
        ).as_workunit()

        if input_edges:
            yield MetadataChangeProposalWrapper(
                entityUrn=execution_urn,
                aspect=DataProcessInstanceInputClass(
                    inputs=[],
                    inputEdges=input_edges,
                ),
            ).as_workunit()

        if output_edges:
            yield MetadataChangeProposalWrapper(
                entityUrn=execution_urn,
                aspect=DataProcessInstanceOutputClass(
                    outputs=[],
                    outputEdges=output_edges,
                ),
            ).as_workunit()

    def _read_run(
        self, experiment: Experiment, run: ExperimentRun
    ) -> Optional[_RunReads]:
        """The run's details, or None when the stored run is kept. Only a
        temporary failure keeps it, and then its remaining reads are not made."""
        item = f"{experiment.name}/{run.name}"
        if self._stopped.any_transient(self._get_scope(), _RUN_CALL_KINDS):
            self.report.report_item_kept_after_stop()
            return None
        executions = self._read_detail(
            DetailCalls.EXPERIMENT_RUN_EXECUTIONS,
            lambda: list(run.get_executions()),
            item=item,
        )
        if executions.keep_previous:
            return None
        jobs = self._read_detail(
            DetailCalls.EXPERIMENT_RUN_LOGGED_JOBS,
            lambda: self._read_logged_custom_jobs(run),
            item=item,
        )
        if jobs.keep_previous:
            return None
        state = self._read_detail(
            DetailCalls.EXPERIMENT_RUN_DETAIL,
            lambda: self._read_run_state(run),
            item=item,
        )
        if state.keep_previous:
            return None
        return _RunReads(
            executions=executions.value or [],
            logged_jobs=jobs.value or [],
            state=state.value or _RunState(state="", params=[], metrics=[]),
        )

    def _gen_experiment_run_mcps(
        self, experiment_meta: ExperimentMetadata, run_meta: ExperimentRunMetadata
    ) -> Iterable[MetadataWorkUnit]:
        experiment = experiment_meta.experiment
        run = run_meta.run
        experiment_key = ExperimentKey(
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
            id=self.name_formatter.format_experiment_id(experiment.name),
        )
        run_urn = self.urn_builder.make_experiment_run_urn(experiment, run)
        reads = self._read_run(experiment, run)
        if reads is None:
            return
        executions = reads.executions
        run_state = reads.state
        timestamps = self._get_run_timestamps(executions)
        run_result_type = self._get_run_result_status(run_state.state)

        for execution in executions:
            yield from self._gen_run_execution(
                execution=execution, exp=experiment, run=run
            )

        training_job_urns = self._logged_training_job_urns(reads.logged_jobs)
        if training_job_urns:
            yield MetadataChangeProposalWrapper(
                entityUrn=run_urn,
                aspect=DataProcessInstanceRelationshipsClass(
                    upstreamInstances=training_job_urns,
                    parentInstance=experiment_key.as_urn(),
                ),
            ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=run_urn,
            aspect=DataProcessInstancePropertiesClass(
                name=run.name,
                created=AuditStampClass(
                    time=timestamps.created_time_ms
                    if timestamps.created_time_ms
                    else 0,
                    actor=get_actor_from_labels(getattr(run, "labels", None))
                    or builder.UNKNOWN_USER,
                ),
                externalUrl=self.url_builder.make_experiment_run_url(experiment, run),
                customProperties=self._make_custom_properties_for_run(
                    experiment, run, executions
                ),
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=run_urn,
            aspect=ContainerClass(container=experiment_key.as_urn()),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=run_urn,
            aspect=MLTrainingRunPropertiesClass(
                hyperParams=run_state.params,
                trainingMetrics=run_state.metrics,
                externalUrl=self.url_builder.make_experiment_run_url(experiment, run),
                id=f"{experiment.name}-{run.name}",
            ),
        ).as_workunit()

        yield from self._yield_common_aspects(
            entity_urn=run_urn,
            subtype=MLAssetSubTypes.VERTEX_EXPERIMENT_RUN,
            include_container=False,
        )

        if is_status_for_run_event_class(run_result_type):
            yield MetadataChangeProposalWrapper(
                entityUrn=run_urn,
                aspect=DataProcessInstanceRunEventClass(
                    status=DataProcessRunStatusClass.COMPLETE,
                    timestampMillis=timestamps.created_time_ms
                    if timestamps.created_time_ms
                    else 0,
                    result=DataProcessInstanceRunResultClass(
                        type=run_result_type,
                        nativeResultType=self.platform,
                    ),
                    durationMillis=timestamps.duration_ms
                    if timestamps.duration_ms
                    else None,
                ),
            ).as_workunit()
