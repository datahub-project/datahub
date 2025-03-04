import contextlib
from pathlib import Path
from typing import Any, Dict, List, TypeVar
from unittest.mock import MagicMock, patch

import pytest
from google.cloud.aiplatform import AutoMLTabularTrainingJob, CustomJob, Model
from google.protobuf import timestamp_pb2
from pytest import Config

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers

T = TypeVar("T")

PROJECT_ID = "test-project-id"
REGION = "us-west2"


@pytest.fixture
def sink_file_path(tmp_path: Path) -> str:
    return str(tmp_path / "vertexai_source_mcps.json")


def get_pipeline_config(sink_file_path: str) -> Dict[str, Any]:
    source_type = "vertexai"
    return {
        "run_id": "vertexai-source-test",
        "source": {
            "type": source_type,
            "config": {
                "project_id": PROJECT_ID,
                "region": REGION,
            },
        },
        "sink": {
            "type": "file",
            "config": {
                "filename": sink_file_path,
            },
        },
    }


def gen_mock_models() -> List[Model]:
    mock_model_1 = MagicMock(spec=Model)
    mock_model_1.name = "mock_prediction_model_1"
    mock_model_1.create_time = timestamp_pb2.Timestamp().GetCurrentTime()
    mock_model_1.update_time = timestamp_pb2.Timestamp().GetCurrentTime()
    mock_model_1.version_id = "1"
    mock_model_1.display_name = "mock_prediction_model_1_display_name"
    mock_model_1.description = "mock_prediction_model_1_description"

    mock_model_2 = MagicMock(spec=Model)
    mock_model_2.name = "mock_prediction_model_2"

    mock_model_2.create_time = timestamp_pb2.Timestamp().GetCurrentTime()
    mock_model_2.update_time = timestamp_pb2.Timestamp().GetCurrentTime()
    mock_model_2.version_id = "1"
    mock_model_2.display_name = "mock_prediction_model_2_display_name"
    mock_model_2.description = "mock_prediction_model_1_description"

    return [mock_model_1, mock_model_2]


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
    return mock_automl_job


def test_vertexai_source_ingestion(pytestconfig: Config, sink_file_path: str) -> None:
    with contextlib.ExitStack() as exit_stack:
        for func_to_mock in [
            "google.cloud.aiplatform.init",
            "google.cloud.aiplatform.Model.list",
            "google.cloud.aiplatform.datasets.TextDataset.list",
            "google.cloud.aiplatform.datasets.TabularDataset.list",
            "google.cloud.aiplatform.datasets.ImageDataset.list",
            "google.cloud.aiplatform.datasets.TimeSeriesDataset.list",
            "google.cloud.aiplatform.datasets.VideoDataset.list",
            "google.cloud.aiplatform.CustomJob.list",
            "google.cloud.aiplatform.CustomTrainingJob.list",
            "google.cloud.aiplatform.CustomContainerTrainingJob.list",
            "google.cloud.aiplatform.CustomPythonPackageTrainingJob.list",
            "google.cloud.aiplatform.AutoMLTabularTrainingJob.list",
            "google.cloud.aiplatform.AutoMLTextTrainingJob.list",
            "google.cloud.aiplatform.AutoMLImageTrainingJob.list",
            "google.cloud.aiplatform.AutoMLVideoTrainingJob.list",
            "google.cloud.aiplatform.AutoMLForecastingTrainingJob.list",
        ]:
            mock = exit_stack.enter_context(patch(func_to_mock))
            if func_to_mock == "google.cloud.aiplatform.Model.list":
                mock.return_value = gen_mock_models()
            elif func_to_mock == "google.cloud.aiplatform.CustomJob.list":
                mock.return_value = [
                    gen_mock_training_custom_job(),
                    gen_mock_training_automl_job(),
                ]
            elif (
                func_to_mock == "google.cloud.aiplatform.AutoMLTabularTrainingJob.list"
            ):
                mock.return_value = [gen_mock_training_automl_job()]
            else:
                mock.return_value = []

        golden_file_path = (
            pytestconfig.rootpath
            / "tests/integration/vertexai/vertexai_mcps_golden.json"
        )

        print(f"mcps file path: {str(sink_file_path)}")
        print(f"golden file path: {str(golden_file_path)}")

        pipeline = Pipeline.create(get_pipeline_config(sink_file_path))
        pipeline.run()
        pipeline.pretty_print_summary()
        pipeline.raise_from_status()

        mce_helpers.check_golden_file(
            pytestconfig=pytestconfig,
            output_path=sink_file_path,
            golden_path=golden_file_path,
        )
