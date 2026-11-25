import contextlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Type, Union
from unittest.mock import MagicMock, Mock, PropertyMock, patch

import google.cloud.aiplatform as aiplatform
from google.cloud.aiplatform import (
    AutoMLTabularTrainingJob,
    CustomJob,
    Experiment,
    ExperimentRun,
    Model,
    ModelEvaluation,
    PipelineJob,
)
from google.cloud.aiplatform.base import VertexAiResourceNoun
from google.cloud.aiplatform.metadata.execution import Execution
from google.cloud.aiplatform.models import Endpoint, VersionInfo
from google.cloud.aiplatform_v1 import PipelineTaskDetail
from google.cloud.aiplatform_v1.types import (
    Artifact,
    Event,
    PipelineJob as PipelineJobType,
    PipelineState,
)

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.vertexai.vertexai_report import VertexAISourceReport

PIPELINE_TASKS_FIXTURE_DIR = Path(__file__).parent / "pipeline_tasks"
_EXTRACTOR_MODULES = [
    "datahub.ingestion.source.vertexai.vertexai_model_extractor",
    "datahub.ingestion.source.vertexai.vertexai_pipeline_extractor",
    "datahub.ingestion.source.vertexai.vertexai_training_extractor",
]

MOCK_CREATE_TIME = datetime(2022, 3, 21, 16, 0, 0, tzinfo=timezone.utc)
MOCK_UPDATE_TIME = datetime(2022, 3, 21, 16, 1, 40, tzinfo=timezone.utc)
MOCK_TASK_START_TIME = datetime(2022, 3, 21, 16, 0, 0, tzinfo=timezone.utc)
MOCK_TASK_END_TIME = datetime(2022, 3, 21, 16, 1, 40, tzinfo=timezone.utc)


def gen_mock_model(
    num: int = 1,
    labels: Optional[Dict[str, str]] = None,
    list_model_evaluations_return: Optional[List] = None,
) -> Model:
    mock_model = MagicMock(spec=Model)
    mock_model.name = f"mock_prediction_model_{num}"
    mock_model.create_time = MOCK_CREATE_TIME
    mock_model.update_time = MOCK_UPDATE_TIME
    mock_model.version_id = f"{num}"
    mock_model.display_name = f"mock_prediction_model_{num}_display_name"
    mock_model.description = f"mock_prediction_model_{num}_description"
    mock_model.resource_name = (
        f"projects/123/locations/us-central1/models/{num + 1}{num + 2}{num + 3}"
    )
    # labels is a property descriptor on the real class, so set it on the type
    if labels is not None:
        type(mock_model).labels = PropertyMock(return_value=labels)
    else:
        mock_model.labels = {}

    if list_model_evaluations_return is not None:
        mock_model.list_model_evaluations = Mock(
            return_value=list(list_model_evaluations_return)
        )

    # Mock version proto (fields accessed by _list_versions via GAPIC path)
    mock_version_proto = Mock()
    mock_version_proto.version_id = f"{num}"
    mock_version_proto.version_description = "test version"
    mock_version_proto.version_create_time = MOCK_CREATE_TIME
    mock_version_proto.version_update_time = MOCK_UPDATE_TIME
    mock_version_proto.display_name = f"mock_prediction_model_{num}_display_name"
    mock_version_proto.version_aliases = []
    mock_version_proto.name = f"{mock_model.resource_name}@{num}"

    mock_versioning_registry = Mock()
    mock_versioning_registry.model_resource_name = mock_model.resource_name
    mock_versioning_registry.client.list_model_versions.return_value = [
        mock_version_proto
    ]
    # list_versions() is still called directly in training_extractor._search_model_version
    mock_version = Mock()
    mock_version.version_id = mock_version_proto.version_id
    mock_versioning_registry.list_versions.return_value = [mock_version]

    object.__setattr__(mock_model, "versioning_registry", mock_versioning_registry)

    return mock_model


def gen_mock_models(num: int = 2) -> List[Model]:
    return [gen_mock_model(i) for i in range(1, num + 1)]


def gen_mock_training_custom_job(labels: Optional[Dict[str, str]] = None) -> CustomJob:
    mock_training_job = MagicMock(spec=CustomJob)
    mock_training_job.name = "mock_training_job"
    mock_training_job.create_time = MOCK_CREATE_TIME
    mock_training_job.update_time = MOCK_UPDATE_TIME
    mock_training_job.display_name = "mock_training_job_display_name"
    mock_training_job.description = "mock_training_job_description"
    # labels is a property descriptor on the real class, so set it on the type
    if labels is not None:
        type(mock_training_job).labels = PropertyMock(return_value=labels)
    mock_training_job.labels = {}

    # modelToUpload.name must match gen_mock_model(1).resource_name to wire up training → model lineage
    mock_training_job.to_dict.return_value = {
        "jobSpec": {
            "output": {"artifactOutputUri": "gs://bucket/training-output/model"}
        },
        "modelToUpload": {
            "name": "projects/123/locations/us-central1/models/234",
            "versionId": "1",
        },
    }

    return mock_training_job


def gen_mock_training_automl_job() -> AutoMLTabularTrainingJob:
    mock_automl_job = MagicMock(spec=AutoMLTabularTrainingJob)
    mock_automl_job.name = "mock_auto_automl_tabular_job"
    mock_automl_job.create_time = MOCK_CREATE_TIME
    mock_automl_job.update_time = MOCK_UPDATE_TIME
    mock_automl_job.display_name = "mock_auto_automl_tabular_job_display_name"
    mock_automl_job.description = "mock_auto_automl_tabular_job_description"
    mock_automl_job.labels = {}

    # datasetId must match gen_mock_dataset().name; modelToUpload.name must match gen_mock_model(1).resource_name
    mock_automl_job.to_dict.return_value = {
        "inputDataConfig": {"datasetId": "2562882439508656128"},
        "modelToUpload": {
            "name": "projects/123/locations/us-central1/models/234",
            "versionId": "1",
        },
    }

    return mock_automl_job


def gen_mock_model_version(mock_model: Model) -> VersionInfo:
    version = "1"
    return VersionInfo(
        version_id=version,
        version_description="test",
        version_create_time=MOCK_CREATE_TIME,
        version_update_time=MOCK_UPDATE_TIME,
        model_display_name=mock_model.name,
        model_resource_name=mock_model.resource_name,
    )


def gen_mock_dataset() -> VertexAiResourceNoun:
    mock_dataset = MagicMock(spec=VertexAiResourceNoun)
    mock_dataset.name = "2562882439508656128"
    mock_dataset.create_time = MOCK_CREATE_TIME
    mock_dataset.update_time = MOCK_UPDATE_TIME
    mock_dataset.display_name = "mock_dataset_display_name"
    mock_dataset.description = "mock_dataset_description"
    mock_dataset.resource_name = "projects/123/locations/us-central1/datasets/456"
    return mock_dataset


def gen_mock_endpoint() -> Endpoint:
    mock_endpoint = MagicMock(spec=Endpoint)
    mock_endpoint.description = "test_endpoint_description"
    mock_endpoint.display_name = "test_endpoint_display_name"
    mock_endpoint.name = "test-endpoint"
    mock_endpoint.create_time = MOCK_CREATE_TIME
    return mock_endpoint


def gen_mock_experiment(num: int = 1) -> Experiment:
    mock_experiment = MagicMock(spec=Experiment)
    mock_experiment.name = f"mock_experiment_{num}"
    mock_experiment.create_time = MOCK_CREATE_TIME
    mock_experiment.update_time = MOCK_UPDATE_TIME
    mock_experiment.display_name = f"mock_experiment_{num}_display_name"
    mock_experiment.description = f"mock_experiment_{num}_description"
    mock_experiment.resource_name = (
        f"projects/123/locations/us-central1/experiments/{num}"
    )
    mock_experiment.dashboard_url = "https://console.cloud.google.com/vertex-ai/locations/us-central1/experiments/123"
    return mock_experiment


def gen_mock_experiment_run() -> ExperimentRun:
    mock_experiment_run = MagicMock(spec=ExperimentRun)
    mock_experiment_run.name = "mock_experiment_run"
    mock_experiment_run.create_time = MOCK_CREATE_TIME
    mock_experiment_run.update_time = MOCK_UPDATE_TIME
    mock_experiment_run.display_name = "mock_experiment_run_display_name"
    mock_experiment_run.description = "mock_experiment_run_description"
    mock_experiment_run._is_legacy_experiment_run.return_value = False

    # name must match gen_mock_training_custom_job().name to resolve the ExperimentRun → TrainingJob URN
    mock_logged_job = MagicMock()
    mock_logged_job.name = "mock_training_job"
    mock_experiment_run.get_logged_custom_jobs.return_value = [mock_logged_job]

    return mock_experiment_run


def gen_mock_task_detail(
    task_name: str,
    task_id: int,
    parent_task_id: int = 2,
    state: int = PipelineTaskDetail.State.SUCCEEDED,
    started: bool = True,
    end_time: Optional[datetime] = MOCK_TASK_END_TIME,
    input_uris: Sequence[str] = (),
    output_uris: Sequence[str] = (),
) -> PipelineTaskDetail:
    """A task detail as the API lists it. Defaults make it a started child of `gen_mock_root_task_detail()`."""

    def artifacts(uris: Sequence[str]) -> Dict[str, PipelineTaskDetail.ArtifactList]:
        return {
            f"artifact_{index}": PipelineTaskDetail.ArtifactList(
                artifacts=[Artifact(uri=uri)]
            )
            for index, uri in enumerate(uris)
        }

    detail = PipelineTaskDetail(
        task_id=task_id,
        parent_task_id=parent_task_id,
        task_name=task_name,
        state=state,
        create_time=MOCK_CREATE_TIME,
        inputs=artifacts(input_uris),
        outputs=artifacts(output_uris),
    )
    if started:
        detail.start_time = MOCK_TASK_START_TIME
    if end_time is not None:
        detail.end_time = end_time
    return detail


def gen_mock_root_task_detail() -> PipelineTaskDetail:
    """The detail of the pipeline run itself, which every real run lists."""
    return gen_mock_task_detail(
        "mock_pipeline_job", task_id=2, parent_task_id=0, end_time=None
    )


def gen_mock_pipeline_resource(
    update_time: datetime,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    state: int = PipelineState.PIPELINE_STATE_UNSPECIFIED,
) -> MagicMock:
    """The listed resource of a hand-built pipeline run, with a state and times."""
    gca_resource = MagicMock(spec=PipelineJobType)
    gca_resource.update_time = update_time
    gca_resource.start_time = start_time
    gca_resource.end_time = end_time
    gca_resource.state = state
    return gca_resource


def get_mock_pipeline_job() -> PipelineJob:
    mock_pipeline_job = MagicMock(spec=PipelineJob)
    mock_pipeline_job.name = "mock_pipeline_job"
    mock_pipeline_job.display_name = "Mock Pipeline"
    mock_pipeline_job.resource_name = (
        "projects/123/locations/us-central1/pipelineJobs/456"
    )
    mock_pipeline_job.labels = {"key1": "value1"}
    mock_pipeline_job.create_time = MOCK_CREATE_TIME
    mock_pipeline_job.update_time = MOCK_UPDATE_TIME
    mock_pipeline_job.location = "us-west2"
    gca_resource = gen_mock_pipeline_resource(
        MOCK_UPDATE_TIME,
        start_time=MOCK_CREATE_TIME,
        end_time=MOCK_UPDATE_TIME,
        state=PipelineState.PIPELINE_STATE_SUCCEEDED,
    )
    mock_pipeline_job.gca_resource = gca_resource
    # Input URI matches gen_mock_model(1).resource_name: exercises downstream model lineage from pipeline tasks;
    # output URI matches gen_mock_model(2).resource_name
    task_detail = gen_mock_task_detail(
        "reverse",
        task_id=1,
        input_uris=["projects/123/locations/us-central1/models/234"],
        output_uris=["projects/123/locations/us-central1/models/345"],
    )

    mock_pipeline_job.task_details = [task_detail, gen_mock_root_task_detail()]
    gca_resource.pipeline_spec = {
        "root": {
            "dag": {
                "tasks": {
                    "concat": {
                        "componentRef": {"name": "comp-concat"},
                        "taskInfo": {"name": "concat"},
                    },
                    "reverse": {
                        "componentRef": {"name": "comp-reverse"},
                        "inputs": {
                            "parameters": {
                                "a": {
                                    "taskOutputParameter": {
                                        "producerTask": "concat",
                                        "outputParameterKey": "Output",
                                    }
                                }
                            }
                        },
                        "taskInfo": {"name": "reverse"},
                        "dependentTasks": ["concat"],
                    },
                }
            }
        }
    }

    return mock_pipeline_job


def gen_pipeline_job_from_fixture(data: Dict[str, Any]) -> PipelineJob:
    gca_resource = PipelineJobType.from_json(
        json.dumps(data), ignore_unknown_fields=True
    )
    job = MagicMock(spec=PipelineJob)
    job.name = data["name"].split("/")[-1]
    job.resource_name = data["name"]
    job.display_name = data.get("displayName", "")
    job.labels = dict(gca_resource.labels)
    job.create_time = gca_resource.create_time
    job.update_time = gca_resource.update_time
    job.location = data["name"].split("/")[3]
    job.gca_resource = gca_resource
    job.task_details = list(gca_resource.job_detail.task_details)
    return job


def run_pipeline_fixtures(
    fixtures: Sequence[Union[str, Dict[str, Any]]],
    sink_path: str,
    **config: Any,
) -> VertexAISourceReport:
    """Run the Vertex AI source offline over fixtures, writing MCPs to `sink_path`.

    A fixture is the name of a file in `pipeline_tasks/` or an in-memory pipeline run
    (the same JSON as a file). Any `config` entry overrides the source config.
    """

    def load(name: str) -> Any:
        return json.loads((PIPELINE_TASKS_FIXTURE_DIR / f"{name}.json").read_text())

    def pipeline_run(fixture: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
        return load(fixture) if isinstance(fixture, str) else fixture

    class_data: Dict[Type, List[Any]] = {
        aiplatform.PipelineJob: [
            gen_pipeline_job_from_fixture(pipeline_run(fixture)) for fixture in fixtures
        ],
    }
    source_config: Dict[str, Any] = {
        "project_id": "my-project",
        "region": "us-central1",
        "include_models": False,
        "include_experiments": False,
        "include_evaluations": False,
        "include_training_jobs": False,
        "include_pipelines": True,
        "use_ml_metadata_for_lineage": False,
        "extract_execution_metrics": False,
        **config,
    }

    with contextlib.ExitStack() as exit_stack:
        exit_stack.enter_context(
            patch("google.auth.default", return_value=(None, None))
        )
        exit_stack.enter_context(patch("google.cloud.aiplatform.init"))
        for module in _EXTRACTOR_MODULES:
            exit_stack.enter_context(
                patch(
                    f"{module}.rate_limited_gapic_list",
                    side_effect=lambda cls, _rl, **_kw: class_data.get(cls, []),
                )
            )
        pipeline = Pipeline.create(
            {
                "run_id": "vertexai-pipeline-tasks-test",
                "source": {"type": "vertexai", "config": source_config},
                "sink": {"type": "file", "config": {"filename": sink_path}},
            }
        )
        pipeline.run()
        pipeline.raise_from_status()
        report = pipeline.source.get_report()
        assert isinstance(report, VertexAISourceReport)
        return report


def gen_mock_model_evaluation(
    num: int = 1, display_name: Optional[str] = None
) -> ModelEvaluation:
    mock_evaluation = MagicMock(spec=ModelEvaluation)
    mock_evaluation.name = f"projects/123/locations/us/models/456/evaluations/{num}"
    if display_name is not None:
        type(mock_evaluation).display_name = PropertyMock(return_value=display_name)
    else:
        mock_evaluation.display_name = f"Evaluation {num}"
    mock_evaluation.create_time = MOCK_CREATE_TIME

    mock_evaluation.to_dict.return_value = {
        "name": mock_evaluation.name,
        "displayName": mock_evaluation.display_name,
        "metrics": {
            "auPrc": 0.95,
            "auRoc": 0.94,
            "logLoss": 0.15,
        },
        "metricSchemaUri": "gs://bucket/schema.json",
    }

    return mock_evaluation


def gen_mock_execution(name: str = "test-execution") -> Execution:
    """Generate a mock ML Metadata Execution object."""
    mock_execution = MagicMock(spec=Execution)
    mock_execution.name = (
        f"projects/123/locations/us/metadataStores/default/executions/{name}"
    )
    mock_execution.display_name = name
    mock_execution.metadata = {}

    return mock_execution


def gen_mock_artifact(uri: str, artifact_type: str = "input") -> Artifact:
    """Generate a mock ML Metadata Artifact object."""
    mock_artifact = Artifact(
        name=f"{artifact_type}-artifact",
        uri=uri,
        display_name=f"{artifact_type.title()} Artifact",
    )
    mock_artifact.schema_title = "system.Dataset"

    return mock_artifact


def gen_mock_event(artifact_name: str, event_type: str = "INPUT") -> Event:
    """Generate a mock ML Metadata Event object."""
    mock_event = Event(artifact=artifact_name)
    mock_event.type_.name = event_type

    return mock_event
