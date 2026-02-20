from datetime import datetime, timezone
from typing import Dict, List, Optional
from unittest.mock import MagicMock, Mock, PropertyMock

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
)

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
    # Configure labels property with PropertyMock
    if labels is not None:
        type(mock_model).labels = PropertyMock(return_value=labels)
    else:
        mock_model.labels = {}

    # Configure list_model_evaluations if provided
    if list_model_evaluations_return is not None:
        mock_model.list_model_evaluations = Mock(
            return_value=iter(list_model_evaluations_return)
        )

    mock_version = Mock()
    mock_version.version_id = f"{num}"
    mock_version.version_description = "test version"
    mock_version.version_create_time = MOCK_CREATE_TIME
    mock_version.version_update_time = MOCK_UPDATE_TIME

    # Replace the versioning_registry attribute entirely with a mock
    # that has list_versions configured
    mock_versioning_registry = Mock()
    mock_versioning_registry.list_versions.return_value = [mock_version]

    # Use object.__setattr__ to bypass any property descriptors
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
    # Configure labels property with PropertyMock if provided
    if labels is not None:
        type(mock_training_job).labels = PropertyMock(return_value=labels)
    mock_training_job.labels = {}

    # Link training job to output model (for training job lineage)
    # Use the same model name as gen_mock_model(1) produces
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

    # Link training job to output model (for training job lineage)
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
    return mock_experiment_run


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
    gca_resource = MagicMock(spec=PipelineJobType)
    mock_pipeline_job.gca_resource = gca_resource
    task_detail = MagicMock(spec=PipelineTaskDetail)
    task_detail.task_name = "reverse"
    task_detail.task_id = 1
    task_detail.state = PipelineTaskDetail.State.SUCCEEDED
    task_detail.start_time = MOCK_TASK_START_TIME
    task_detail.create_time = MOCK_CREATE_TIME
    task_detail.end_time = MOCK_TASK_END_TIME

    # Add model artifact in task inputs to test downstream lineage
    # Use same project/location as gen_mock_model(1) produces: projects/123/locations/us-central1/models/234
    mock_model_artifact = MagicMock()
    mock_model_artifact.uri = "projects/123/locations/us-central1/models/234"
    mock_input_entry = MagicMock()
    mock_input_entry.artifacts = [mock_model_artifact]
    task_detail.inputs = {"model": mock_input_entry}

    # Add model artifact in outputs for completeness
    # Use model 2: projects/123/locations/us-central1/models/345
    mock_output_model_artifact = MagicMock()
    mock_output_model_artifact.uri = "projects/123/locations/us-central1/models/345"
    mock_output_entry = MagicMock()
    mock_output_entry.artifacts = [mock_output_model_artifact]
    task_detail.outputs = {"output_model": mock_output_entry}

    mock_pipeline_job.task_details = [task_detail]
    gca_resource.pipeline_spec = {
        "root": {
            "dag": {
                "tasks": {
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
                    }
                }
            }
        }
    }

    return mock_pipeline_job


def gen_mock_model_evaluation(
    num: int = 1, display_name: Optional[str] = None
) -> ModelEvaluation:
    """Generate a mock ModelEvaluation object."""
    mock_evaluation = MagicMock(spec=ModelEvaluation)
    mock_evaluation.name = f"projects/123/locations/us/models/456/evaluations/{num}"
    # Configure display_name property with PropertyMock if provided
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
