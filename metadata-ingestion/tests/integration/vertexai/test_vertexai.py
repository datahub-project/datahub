import contextlib
from pathlib import Path
from typing import Any, Callable, Dict, Type, TypeVar
from unittest.mock import patch

import google.cloud.aiplatform as aiplatform
from google.cloud.aiplatform import datasets as aiplatform_datasets
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

_EXTRACTOR_MODULES = [
    "datahub.ingestion.source.vertexai.vertexai_model_extractor",
    "datahub.ingestion.source.vertexai.vertexai_pipeline_extractor",
    "datahub.ingestion.source.vertexai.vertexai_training_extractor",
]


def _patch_gapic_list(
    exit_stack: contextlib.ExitStack,
    side_effect: Callable,
) -> None:
    for module in _EXTRACTOR_MODULES:
        exit_stack.enter_context(
            patch(f"{module}.rate_limited_gapic_list", side_effect=side_effect)
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
                "use_ml_metadata_for_lineage": False,
                "extract_execution_metrics": False,
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
    class_data: Dict[Type, Any] = {
        aiplatform.Model: gen_mock_models(),
        aiplatform.PipelineJob: [get_mock_pipeline_job()],
        aiplatform.CustomJob: [gen_mock_training_custom_job()],
        aiplatform.AutoMLTabularTrainingJob: [gen_mock_training_automl_job()],
        aiplatform_datasets.TabularDataset: [gen_mock_dataset()],
    }

    def mock_gapic_list(cls: Type, _rl: Any, **_kw: Any) -> Any:
        return class_data.get(cls, [])

    with contextlib.ExitStack() as exit_stack:
        exit_stack.enter_context(
            patch("google.auth.default", return_value=(None, None))
        )
        exit_stack.enter_context(patch("google.cloud.aiplatform.init"))

        _patch_gapic_list(exit_stack, mock_gapic_list)

        exit_stack.enter_context(
            patch(
                "google.cloud.aiplatform.Experiment.list",
                return_value=[gen_mock_experiment()],
            )
        )
        exit_stack.enter_context(
            patch(
                "google.cloud.aiplatform.ExperimentRun.list",
                return_value=[gen_mock_experiment_run()],
            )
        )

        exit_stack.enter_context(
            patch(
                "google.cloud.aiplatform.models.Model", return_value=gen_mock_model(1)
            )
        )
        exit_stack.enter_context(
            patch(
                "datahub.ingestion.source.vertexai.vertexai_training_extractor.Model",
                return_value=gen_mock_model(1),
            )
        )

        golden_file_path = (
            pytestconfig.rootpath
            / "tests/integration/vertexai/vertexai_mcps_golden.json"
        )
        sink_file_path = str(tmp_path / "vertexai_source_mcps.json")

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
                "use_ml_metadata_for_lineage": False,
                "extract_execution_metrics": False,
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
        exit_stack.enter_context(
            patch("google.auth.default", return_value=(None, None))
        )
        exit_stack.enter_context(patch("google.cloud.aiplatform.init"))

        _patch_gapic_list(exit_stack, lambda *_a, **_kw: [])

        exit_stack.enter_context(
            patch("google.cloud.aiplatform.Experiment.list", return_value=[])
        )
        exit_stack.enter_context(
            patch("google.cloud.aiplatform.ExperimentRun.list", return_value=[])
        )

        pipeline = Pipeline.create(config_dict)
        pipeline.run()
        pipeline.raise_from_status()

        source = pipeline.source
        assert isinstance(source, VertexAISource)
        assert source.config.platform_instance == "prod-vertexai-us"
        assert source.uri_parser.platform_instance == "prod-vertexai-us"
        assert source.urn_builder.platform_instance == "prod-vertexai-us"
