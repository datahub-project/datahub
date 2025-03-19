import contextlib
from pathlib import Path
from typing import Any, Dict, TypeVar
from unittest.mock import patch

from pytest import Config

from datahub.ingestion.run.pipeline import Pipeline
from tests.integration.vertexai.mock_vertexai import (
    gen_mock_dataset,
    gen_mock_model,
    gen_mock_models,
    gen_mock_training_automl_job,
    gen_mock_training_custom_job,
)
from tests.test_helpers import mce_helpers

T = TypeVar("T")

PROJECT_ID = "test-project-id"
REGION = "us-west2"


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


def test_vertexai_source_ingestion(pytestconfig: Config, tmp_path: Path) -> None:
    with contextlib.ExitStack() as exit_stack:
        # Mock the Vertex API with empty list
        for func_to_mock in [
            "google.cloud.aiplatform.init",
            "google.cloud.aiplatform.datasets.TextDataset.list",
            "google.cloud.aiplatform.datasets.ImageDataset.list",
            "google.cloud.aiplatform.datasets.TimeSeriesDataset.list",
            "google.cloud.aiplatform.datasets.VideoDataset.list",
            "google.cloud.aiplatform.CustomTrainingJob.list",
            "google.cloud.aiplatform.CustomContainerTrainingJob.list",
            "google.cloud.aiplatform.CustomPythonPackageTrainingJob.list",
            "google.cloud.aiplatform.AutoMLTextTrainingJob.list",
            "google.cloud.aiplatform.AutoMLImageTrainingJob.list",
            "google.cloud.aiplatform.AutoMLVideoTrainingJob.list",
            "google.cloud.aiplatform.AutoMLForecastingTrainingJob.list",
            "google.cloud.aiplatform.TabularDataset.list",
        ]:
            mock = exit_stack.enter_context(patch(func_to_mock))
            mock.return_value = []

        # Mock the Vertex AI with Mock data list
        mock_models = exit_stack.enter_context(
            patch("google.cloud.aiplatform.Model.list")
        )
        mock_models.return_value = gen_mock_models()

        mock_custom_job = exit_stack.enter_context(
            patch("google.cloud.aiplatform.CustomJob.list")
        )
        mock_custom_job.return_value = [gen_mock_training_custom_job()]

        mock_automl_job = exit_stack.enter_context(
            patch("google.cloud.aiplatform.AutoMLTabularTrainingJob.list")
        )
        mock_automl_job.return_value = [gen_mock_training_automl_job()]

        mock_ds = exit_stack.enter_context(
            patch("google.cloud.aiplatform.datasets.TabularDataset.list")
        )
        mock_ds.return_value = [gen_mock_dataset()]

        mock_model = exit_stack.enter_context(
            patch("google.cloud.aiplatform.models.Model")
        )
        mock_model.return_value = gen_mock_model()

        golden_file_path = (
            pytestconfig.rootpath
            / "tests/integration/vertexai/vertexai_mcps_golden.json"
        )

        sink_file_path = str(tmp_path / "vertexai_source_mcps.json")
        print(f"Output mcps file path: {str(sink_file_path)}")
        print(f"Golden file path: {str(golden_file_path)}")

        pipeline = Pipeline.create(get_pipeline_config(sink_file_path))
        pipeline.run()
        pipeline.pretty_print_summary()
        pipeline.raise_from_status()

        mce_helpers.check_golden_file(
            pytestconfig=pytestconfig,
            output_path=sink_file_path,
            golden_path=golden_file_path,
        )
