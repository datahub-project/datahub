import logging
from operator import attrgetter
from typing import Any, Dict, Iterable, List, Optional, Union

from google.cloud import aiplatform
from google.cloud.aiplatform import ExperimentRun
from google.cloud.aiplatform.metadata.experiment_resources import Experiment
from google.cloud.aiplatform_v1.types import Execution

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ExperimentKey, gen_containers
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.vertexai.vertexai_builder import (
    VertexAIExternalURLBuilder,
    VertexAINameFormatter,
    VertexAIURIParser,
    VertexAIUrnBuilder,
)
from datahub.ingestion.source.vertexai.vertexai_config import VertexAIConfig
from datahub.ingestion.source.vertexai.vertexai_constants import VertexAISubTypes
from datahub.ingestion.source.vertexai.vertexai_models import RunTimestamps
from datahub.ingestion.source.vertexai.vertexai_result_type_utils import (
    get_execution_result_status,
    is_status_for_run_event_class,
)
from datahub.ingestion.source.vertexai.vertexai_utils import (
    get_actor_from_labels,
    get_project_container,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataprocess import (
    DataProcessInstanceRelationships,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ContainerClass,
    DataProcessInstanceInputClass,
    DataProcessInstanceOutputClass,
    DataProcessInstancePropertiesClass,
    DataProcessInstanceRunEventClass,
    DataProcessInstanceRunResultClass,
    DataProcessRunStatusClass,
    EdgeClass,
    MLHyperParamClass,
    MLMetricClass,
    MLTrainingRunPropertiesClass,
    RunResultTypeClass,
)
from datahub.utilities.time import datetime_to_ts_millis

logger = logging.getLogger(__name__)


def log_progress(current: int, total: Optional[int], resource_type: str) -> None:
    """Log progress for resource processing."""
    if total:
        logger.info(f"Processing {resource_type} {current}/{total}")
    else:
        logger.info(f"Processing {resource_type} {current}")


class VertexAIExperimentExtractor:
    """Extracts experiment and experiment run metadata from Vertex AI."""

    def __init__(
        self,
        config: VertexAIConfig,
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
        self.urn_builder = urn_builder
        self.name_formatter = name_formatter
        self.url_builder = url_builder
        self.uri_parser = uri_parser
        self.project_id = project_id
        self._yield_common_aspects = yield_common_aspects_fn
        self.model_usage_tracker = model_usage_tracker
        self.platform = platform

        self.experiments: Optional[List[Experiment]] = None

    def get_experiment_workunits(self) -> Iterable[MetadataWorkUnit]:
        """Extract experiments and generate workunits."""
        exps = aiplatform.Experiment.list()
        filtered = [
            e for e in exps if self.config.experiment_name_pattern.allowed(e.name)
        ]
        filtered.sort(key=attrgetter("update_time"), reverse=True)
        if self.config.max_experiments is not None:
            filtered = filtered[: self.config.max_experiments]
        self.experiments = filtered

        logger.info("Fetching experiments from VertexAI server")
        total = len(self.experiments)
        for i, experiment in enumerate(self.experiments, start=1):
            log_progress(i, total, "experiments")
            yield from self._gen_experiment_workunits(experiment)

    def get_experiment_run_workunits(self) -> Iterable[MetadataWorkUnit]:
        """Extract experiment runs and generate workunits."""
        if self.experiments is None:
            exps = [
                e
                for e in aiplatform.Experiment.list()
                if self.config.experiment_name_pattern.allowed(e.name)
            ]
            exps.sort(key=attrgetter("update_time"), reverse=True)
            self.experiments = exps

        for experiment in self.experiments:
            logger.info(f"Fetching experiment runs for experiment {experiment.name}")
            experiment_runs: List[ExperimentRun] = list(
                aiplatform.ExperimentRun.list(experiment=experiment.name)
            )
            experiment_runs.sort(key=attrgetter("update_time"), reverse=True)
            if self.config.max_runs_per_experiment is not None:
                experiment_runs = experiment_runs[: self.config.max_runs_per_experiment]
            for i, run in enumerate(experiment_runs, start=1):
                log_progress(i, None, f"runs for experiment {experiment.name}")
                yield from self._gen_experiment_run_mcps(experiment, run)

    def _gen_experiment_workunits(
        self, experiment: Experiment
    ) -> Iterable[MetadataWorkUnit]:
        """Generate workunits for a single experiment."""
        yield from gen_containers(
            parent_container_key=get_project_container(
                self.project_id,
                self.platform,
                self.config.platform_instance,
                self.config.env,
            ),
            container_key=ExperimentKey(
                platform=self.platform,
                instance=self.config.platform_instance,
                env=self.config.env,
                id=self.name_formatter.format_experiment_id(experiment.name),
            ),
            name=experiment.name,
            sub_types=[VertexAISubTypes.EXPERIMENT],
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
        """Extract hyperparameters from experiment run."""
        return [
            MLHyperParamClass(name=k, value=str(v)) for k, v in run.get_params().items()
        ]

    def _get_experiment_run_metrics(self, run: ExperimentRun) -> List[MLMetricClass]:
        """Extract metrics from experiment run."""
        return [
            MLMetricClass(name=k, value=str(v)) for k, v in run.get_metrics().items()
        ]

    def _get_run_timestamps(self, run: ExperimentRun) -> RunTimestamps:
        """Extract timestamps from experiment run."""
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
        """Build custom properties for experiment run."""
        properties: Dict[str, str] = dict()
        properties["externalUrl"] = self.url_builder.make_experiment_run_url(
            experiment, run
        )
        for exec in run.get_executions():
            exec_name = exec.name
            properties[f"created time ({exec_name})"] = str(exec.create_time)
            properties[f"update time ({exec_name}) "] = str(exec.update_time)
        return properties

    def _make_custom_properties_for_execution(self, execution: Execution) -> dict:
        """Build custom properties for execution."""
        properties: Dict[str, Optional[str]] = dict()
        for input in execution.get_input_artifacts():
            input_name = getattr(input, "name", "")
            properties[f"input artifact ({input_name})"] = getattr(input, "uri", "")
        for output in execution.get_output_artifacts():
            output_name = getattr(output, "name", "")
            properties[f"output artifact ({output_name})"] = getattr(output, "uri", "")

        return properties

    def _extract_edges_from_artifacts(
        self, artifacts: List[Any], execution_urn: str, is_input: bool
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
                    self.model_usage_tracker.track_usage(model_urn, execution_urn)

        return edges

    def _gen_run_execution(
        self, execution: Execution, run: ExperimentRun, exp: Experiment
    ) -> Iterable[MetadataWorkUnit]:
        """Generate workunits for a single execution within an experiment run."""
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
            subtype=VertexAISubTypes.EXECUTION,
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
            aspect=DataProcessInstanceRelationships(
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
        self, experiment: Experiment, run: ExperimentRun
    ) -> Iterable[MetadataWorkUnit]:
        """Generate metadata workunits for an experiment run."""
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
            subtype=VertexAISubTypes.EXPERIMENT_RUN,
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
