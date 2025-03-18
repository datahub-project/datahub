from datetime import datetime
from typing import List
from unittest.mock import MagicMock

from google.cloud.aiplatform import AutoMLTabularTrainingJob, CustomJob, Model
from google.cloud.aiplatform.base import VertexAiResourceNoun
from google.cloud.aiplatform.models import Endpoint, VersionInfo
from google.protobuf import timestamp_pb2


def gen_mock_model(num: int = 1) -> Model:
    mock_model = MagicMock(spec=Model)
    mock_model.name = f"mock_prediction_model_{num}"

    mock_model.create_time = timestamp_pb2.Timestamp().GetCurrentTime()
    mock_model.update_time = timestamp_pb2.Timestamp().GetCurrentTime()
    mock_model.version_id = f"{num}"
    mock_model.display_name = f"mock_prediction_model_{num}_display_name"
    mock_model.description = f"mock_prediction_model_{num}_description"
    mock_model.resource_name = (
        f"projects/123/locations/us-central1/models/{num + 1}{num + 2}{num + 3}"
    )
    return mock_model


def gen_mock_models(num: int = 2) -> List[Model]:
    return [gen_mock_model(i) for i in range(1, num + 1)]


def gen_mock_training_custom_job() -> CustomJob:
    mock_training_job = MagicMock(spec=CustomJob)
    mock_training_job.name = "mock_training_job"
    mock_training_job.create_time = timestamp_pb2.Timestamp().GetCurrentTime()
    mock_training_job.update_time = timestamp_pb2.Timestamp().GetCurrentTime()
    mock_training_job.display_name = "mock_training_job_display_name"
    mock_training_job.description = "mock_training_job_description"

    return mock_training_job


def gen_mock_training_automl_job() -> AutoMLTabularTrainingJob:
    mock_automl_job = MagicMock(spec=AutoMLTabularTrainingJob)
    mock_automl_job.name = "mock_auto_automl_tabular_job"
    mock_automl_job.create_time = timestamp_pb2.Timestamp().GetCurrentTime()
    mock_automl_job.update_time = timestamp_pb2.Timestamp().GetCurrentTime()
    mock_automl_job.display_name = "mock_auto_automl_tabular_job_display_name"
    mock_automl_job.description = "mock_auto_automl_tabular_job_display_name"
    mock_automl_job.to_dict.return_value = {
        "inputDataConfig": {"datasetId": "2562882439508656128"}
    }
    return mock_automl_job


def gen_mock_model_version(mock_model: Model) -> VersionInfo:
    version = "1"
    return VersionInfo(
        version_id=version,
        version_description="test",
        version_create_time=timestamp_pb2.Timestamp().GetCurrentTime(),
        version_update_time=timestamp_pb2.Timestamp().GetCurrentTime(),
        model_display_name=mock_model.name,
        model_resource_name=mock_model.resource_name,
    )


def gen_mock_dataset() -> VertexAiResourceNoun:
    mock_dataset = MagicMock(spec=VertexAiResourceNoun)
    mock_dataset.name = "2562882439508656128"
    mock_dataset.create_time = timestamp_pb2.Timestamp().GetCurrentTime()
    mock_dataset.update_time = timestamp_pb2.Timestamp().GetCurrentTime()
    mock_dataset.display_name = "mock_dataset_display_name"
    mock_dataset.description = "mock_dataset_description"
    mock_dataset.resource_name = "projects/123/locations/us-central1/datasets/456"
    return mock_dataset


def gen_mock_endpoint() -> Endpoint:
    mock_endpoint = MagicMock(spec=Endpoint)
    mock_endpoint.description = "test endpoint"
    mock_endpoint.create_time = datetime.now()
    mock_endpoint.display_name = "test endpoint display name"
    return mock_endpoint
