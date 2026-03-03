import logging
from contextlib import AbstractContextManager, nullcontext
from datetime import datetime, timezone
from typing import Callable, Dict, Iterable, List, Optional, Union

from google.cloud import aiplatform
from google.cloud.aiplatform import ExperimentRun
from google.cloud.aiplatform.metadata.experiment_resources import Experiment
from google.cloud.aiplatform_v1.types import Artifact, Execution

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
    ResourceCategory,
    ResourceTypes,
)
from datahub.ingestion.source.vertexai.vertexai_models import (
    ExperimentMetadata,
    ExperimentRunMetadata,
    ModelUsageTracker,
    RunTimestamps,
    YieldCommonAspectsProtocol,
)
from datahub.ingestion.source.vertexai.vertexai_result_type_utils import (
    get_execution_result_status,
    is_status_for_run_event_class,
)
from datahub.ingestion.source.vertexai.vertexai_state import VertexAIStateHandler
from datahub.ingestion.source.vertexai.vertexai_utils import (
    filter_by_update_time,
    gen_resource_subfolder_container,
    get_actor_from_labels,
    log_checkpoint_time,
    log_progress,
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
        state_handler: VertexAIStateHandler,
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
        self.state_handler = state_handler
        self.rate_limiter = rate_limiter

        self.experiments: Optional[List[ExperimentMetadata]] = None

    def get_experiment_workunits(self) -> Iterable[MetadataWorkUnit]:
        logger.info("Fetching Experiments from Vertex AI")

        last_checkpoint_millis = self.state_handler.get_last_update_time(
            ResourceTypes.EXPERIMENT
        )
        if last_checkpoint_millis:
            log_checkpoint_time(last_checkpoint_millis, "Experiment")

        with self.rate_limiter:
            experiments = aiplatform.Experiment.list()
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
            with self.rate_limiter:
                raw_experiments = aiplatform.Experiment.list()
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
                key=lambda e: e.update_time
                or datetime.min.replace(tzinfo=timezone.utc),
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
            with self.rate_limiter:
                runs = list(
                    aiplatform.ExperimentRun.list(experiment=experiment_meta.name)
                )

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

    def _get_experiment_run_params(self, run: ExperimentRun) -> List[MLHyperParamClass]:
        return [
            MLHyperParamClass(name=k, value=str(v)) for k, v in run.get_params().items()
        ]

    def _get_experiment_run_metrics(self, run: ExperimentRun) -> List[MLMetricClass]:
        return [
            MLMetricClass(name=k, value=str(v)) for k, v in run.get_metrics().items()
        ]

    def _get_run_timestamps(self, run: ExperimentRun) -> RunTimestamps:
        executions = run.get_executions()
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
        self, experiment: Experiment, run: ExperimentRun
    ) -> dict:
        properties: Dict[str, str] = dict()
        properties["externalUrl"] = self.url_builder.make_experiment_run_url(
            experiment, run
        )
        for exec in run.get_executions():
            exec_name = exec.name
            properties[f"created time ({exec_name})"] = str(exec.create_time)
            properties[f"update time ({exec_name})"] = str(exec.update_time)
        return properties

    def _make_custom_properties_for_execution(self, execution: Execution) -> dict:
        properties: Dict[str, Optional[str]] = dict()
        for input in execution.get_input_artifacts():
            input_name = getattr(input, "name", "")
            properties[f"input artifact ({input_name})"] = getattr(input, "uri", "")
        for output in execution.get_output_artifacts():
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
        self, execution: Execution, run: ExperimentRun, exp: Experiment
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

        input_edges = self._extract_edges_from_artifacts(
            execution.get_input_artifacts(), execution_urn, is_input=True
        )
        output_edges = self._extract_edges_from_artifacts(
            execution.get_output_artifacts(), execution_urn, is_input=False
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
                customProperties=self._make_custom_properties_for_execution(execution),
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
        timestamps = self._get_run_timestamps(run)
        run_result_type = self._get_run_result_status(run.get_state())

        for execution in run.get_executions():
            yield from self._gen_run_execution(
                execution=execution, exp=experiment, run=run
            )

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
                customProperties=self._make_custom_properties_for_run(experiment, run),
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=run_urn,
            aspect=ContainerClass(container=experiment_key.as_urn()),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=run_urn,
            aspect=MLTrainingRunPropertiesClass(
                hyperParams=self._get_experiment_run_params(run),
                trainingMetrics=self._get_experiment_run_metrics(run),
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
