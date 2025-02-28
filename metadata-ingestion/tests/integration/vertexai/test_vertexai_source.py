from pathlib import Path
from typing import Any, Dict, List, TypeVar
from unittest.mock import MagicMock, patch

import pytest
from _pytest.config import Config
from google.cloud.aiplatform import Model
from google.cloud.aiplatform.base import VertexAiResourceNoun
from google.protobuf import timestamp_pb2

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers

T = TypeVar("T")


@pytest.fixture
def project_id() -> str:
    return "test-project-id"


@pytest.fixture
def region() -> str:
    return "us-west2"


@pytest.fixture
def sink_file_path(tmp_path: Path) -> str:
    return str(tmp_path / "vertexai_source_mcps.json")


@pytest.fixture
def pipeline_config(
    project_id: str, region: str, sink_file_path: str
) -> Dict[str, Any]:
    source_type = "vertexai"
    return {
        "run_id": "vertexai-source-test",
        "source": {
            "type": source_type,
            "config": {
                "project_id": project_id,
                "region": region,
            },
        },
        "sink": {
            "type": "file",
            "config": {
                "filename": sink_file_path,
            },
        },
    }


@pytest.fixture
def mock_models() -> List[Model]:
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


@pytest.fixture
def mock_training_jobs() -> List[VertexAiResourceNoun]:
    mock_training_job = MagicMock(spec=VertexAiResourceNoun)
    mock_training_job.name = "mock_training_job"
    mock_training_job.create_time = timestamp_pb2.Timestamp().GetCurrentTime()
    mock_training_job.update_time = timestamp_pb2.Timestamp().GetCurrentTime()
    mock_training_job.display_name = "mock_training_job_display_name"
    mock_training_job.description = "mock_training_job_description"
    return [mock_training_job]


@patch("google.cloud.aiplatform.init")
@patch("google.cloud.aiplatform.Model.list")
@patch("google.cloud.aiplatform.datasets.TextDataset.list")
@patch("google.cloud.aiplatform.datasets.TabularDataset.list")
@patch("google.cloud.aiplatform.datasets.ImageDataset.list")
@patch("google.cloud.aiplatform.datasets.TimeSeriesDataset.list")
@patch("google.cloud.aiplatform.datasets.VideoDataset.list")
@patch("google.cloud.aiplatform.CustomJob.list")
@patch("google.cloud.aiplatform.CustomTrainingJob.list")
@patch("google.cloud.aiplatform.CustomContainerTrainingJob.list")
@patch("google.cloud.aiplatform.CustomPythonPackageTrainingJob.list")
@patch("google.cloud.aiplatform.AutoMLTabularTrainingJob.list")
@patch("google.cloud.aiplatform.AutoMLTextTrainingJob.list")
@patch("google.cloud.aiplatform.AutoMLImageTrainingJob.list")
@patch("google.cloud.aiplatform.AutoMLVideoTrainingJob.list")
def test_vertexai_source_ingestion(
    mock_automl_video_job_list: List[VertexAiResourceNoun],
    mock_automl_image_list: List[VertexAiResourceNoun],
    mock_automl_text_job_list: List[VertexAiResourceNoun],
    mock_automl_tabular_job_list: List[VertexAiResourceNoun],
    mock_custom_python_job_list: List[VertexAiResourceNoun],
    mock_custom_container_job_list: List[VertexAiResourceNoun],
    mock_custom_training_job_list: List[VertexAiResourceNoun],
    mock_custom_job_list: List[VertexAiResourceNoun],
    mock_video_ds_list: List[VertexAiResourceNoun],
    mock_time_series_ds_list: List[VertexAiResourceNoun],
    mock_image_ds_list: List[VertexAiResourceNoun],
    mock_tabular_ds_list: List[VertexAiResourceNoun],
    mock_text_ds_list: List[VertexAiResourceNoun],
    mock_model_list: List[Model],
    mock_init: MagicMock,
    pytestconfig: Config,
    sink_file_path: str,
    pipeline_config: Dict[str, Any],
    mock_models: List[Model],
    mock_training_jobs: List[VertexAiResourceNoun],
) -> None:
    assert hasattr(mock_model_list, "return_value")
    mock_model_list.return_value = mock_models
    assert hasattr(mock_text_ds_list, "return_value")
    mock_text_ds_list.return_value = []
    assert hasattr(mock_tabular_ds_list, "return_value")
    mock_tabular_ds_list.return_value = []
    assert hasattr(mock_image_ds_list, "return_value")
    mock_image_ds_list.return_value = []
    assert hasattr(mock_time_series_ds_list, "return_value")
    mock_time_series_ds_list.return_value = []
    assert hasattr(mock_video_ds_list, "return_value")
    mock_video_ds_list.return_value = []
    assert hasattr(mock_custom_job_list, "return_value")
    mock_custom_job_list.return_value = mock_training_jobs
    assert hasattr(mock_custom_training_job_list, "return_value")
    mock_custom_training_job_list.return_value = []
    assert hasattr(mock_custom_container_job_list, "return_value")
    mock_custom_container_job_list.return_value = []
    assert hasattr(mock_custom_python_job_list, "return_value")
    mock_custom_python_job_list.return_value = []
    assert hasattr(mock_automl_tabular_job_list, "return_value")
    mock_automl_tabular_job_list.return_value = []
    assert hasattr(mock_automl_text_job_list, "return_value")
    mock_automl_text_job_list.return_value = []
    assert hasattr(mock_automl_image_list, "return_value")
    mock_automl_image_list.return_value = []
    assert hasattr(mock_automl_video_job_list, "return_value")
    mock_automl_video_job_list.return_value = []

    golden_file_path = (
        pytestconfig.rootpath / "tests/integration/vertexai/vertexai_mcps_golden.json"
    )

    print(f"mcps file path: {str(sink_file_path)}")
    print(f"golden file path: {str(golden_file_path)}")

    pipeline = Pipeline.create(pipeline_config)
    pipeline.run()
    pipeline.pretty_print_summary()
    pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig,
        output_path=sink_file_path,
        golden_path=golden_file_path,
    )
