import contextlib
from pathlib import Path
from typing import Any, Dict, TypeVar
from unittest.mock import patch

from pytest import Config

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.vertexai.vertexai import VertexAISource
from datahub.testing import mce_helpers
from tests.integration.vertexai.mock_vertexai import (
    gen_mock_dataset,
    gen_mock_experiment,
    gen_mock_experiment_run,
    gen_mock_model,
    gen_mock_models,
    gen_mock_training_automl_job,
    gen_mock_training_custom_job,
    get_mock_pipeline_job,
)

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
        # Mock Google auth to prevent auto-discovery of credentials
        exit_stack.enter_context(
            patch("google.auth.default", return_value=(None, None))
        )

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

        # Mock Model constructor to return gen_mock_model(1) when called with model_name
        # Patch both the module location and where it's imported in the training extractor
        mock_model_class = exit_stack.enter_context(
            patch("google.cloud.aiplatform.models.Model")
        )
        mock_model_class.return_value = gen_mock_model(1)

        # Also patch where it's imported in the training extractor
        mock_model_training = exit_stack.enter_context(
            patch("datahub.ingestion.source.vertexai.vertexai_training_extractor.Model")
        )
        mock_model_training.return_value = gen_mock_model(1)

        mock_exp = exit_stack.enter_context(
            patch("google.cloud.aiplatform.Experiment.list")
        )
        mock_exp.return_value = [gen_mock_experiment()]

        mock_exp_run = exit_stack.enter_context(
            patch("google.cloud.aiplatform.ExperimentRun.list")
        )
        mock_exp_run.return_value = [gen_mock_experiment_run()]

        mock = exit_stack.enter_context(
            patch("google.cloud.aiplatform.PipelineJob.list")
        )
        mock.return_value = [get_mock_pipeline_job()]

        mock_endpoints = exit_stack.enter_context(
            patch("google.cloud.aiplatform.Endpoint.list")
        )
        mock_endpoints.return_value = []

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


def test_vertexai_platform_instance_config(
    pytestconfig: Config, tmp_path: Path
) -> None:
    """Test that platform_instance configuration is properly propagated"""
    config_dict = {
        "run_id": "vertexai-platform-instance-test",
        "source": {
            "type": "vertexai",
            "config": {
                "project_id": PROJECT_ID,
                "region": REGION,
                "platform_instance": "prod-vertexai-us",
            },
        },
        "sink": {
            "type": "file",
            "config": {
                "filename": str(tmp_path / "vertexai_platform_instance.json"),
            },
        },
    }

    with contextlib.ExitStack() as exit_stack:
        # Mock Google auth to prevent auto-discovery of credentials
        exit_stack.enter_context(
            patch("google.auth.default", return_value=(None, None))
        )

        for func_to_mock in [
            "google.cloud.aiplatform.init",
            "google.cloud.aiplatform.Model.list",
            "google.cloud.aiplatform.CustomJob.list",
            "google.cloud.aiplatform.CustomTrainingJob.list",
            "google.cloud.aiplatform.CustomContainerTrainingJob.list",
            "google.cloud.aiplatform.CustomPythonPackageTrainingJob.list",
            "google.cloud.aiplatform.AutoMLTabularTrainingJob.list",
            "google.cloud.aiplatform.AutoMLImageTrainingJob.list",
            "google.cloud.aiplatform.AutoMLTextTrainingJob.list",
            "google.cloud.aiplatform.AutoMLVideoTrainingJob.list",
            "google.cloud.aiplatform.AutoMLForecastingTrainingJob.list",
            "google.cloud.aiplatform.TabularDataset.list",
            "google.cloud.aiplatform.ImageDataset.list",
            "google.cloud.aiplatform.TextDataset.list",
            "google.cloud.aiplatform.VideoDataset.list",
            "google.cloud.aiplatform.TimeSeriesDataset.list",
            "google.cloud.aiplatform.Experiment.list",
            "google.cloud.aiplatform.ExperimentRun.list",
            "google.cloud.aiplatform.PipelineJob.list",
        ]:
            mock = exit_stack.enter_context(patch(func_to_mock))
            mock.return_value = []

        pipeline = Pipeline.create(config_dict)
        pipeline.run()
        pipeline.raise_from_status()

        source = pipeline.source
        assert isinstance(source, VertexAISource)
        assert source.config.platform_instance == "prod-vertexai-us"
        assert source.uri_parser.platform_instance == "prod-vertexai-us"
        assert source.urn_builder.platform_instance == "prod-vertexai-us"
