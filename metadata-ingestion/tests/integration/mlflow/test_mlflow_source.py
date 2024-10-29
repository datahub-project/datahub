from pathlib import Path
from typing import Any, Dict, TypeVar

import pytest
from mlflow import MlflowClient

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers

T = TypeVar("T")


@pytest.fixture
def tracking_uri(tmp_path: Path) -> str:
    return str(tmp_path / "mlruns")


@pytest.fixture
def sink_file_path(tmp_path: Path) -> str:
    return str(tmp_path / "mlflow_source_mcps.json")


@pytest.fixture
def pipeline_config(tracking_uri: str, sink_file_path: str) -> Dict[str, Any]:
    source_type = "mlflow"
    return {
        "run_id": "mlflow-source-test",
        "source": {
            "type": source_type,
            "config": {
                "tracking_uri": tracking_uri,
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
def generate_mlflow_data(tracking_uri: str) -> None:
    client = MlflowClient(tracking_uri=tracking_uri)
    experiment_name = "test-experiment"
    run_name = "test-run"
    model_name = "test-model"
    test_experiment_id = client.create_experiment(experiment_name)
    test_run = client.create_run(
        experiment_id=test_experiment_id,
        run_name=run_name,
    )
    client.log_param(
        run_id=test_run.info.run_id,
        key="p",
        value=1,
    )
    client.log_metric(
        run_id=test_run.info.run_id,
        key="m",
        value=0.85,
    )
    client.create_registered_model(
        name=model_name,
        tags=dict(
            model_id=1,
            model_env="test",
        ),
        description="This a test registered model",
    )
    client.create_model_version(
        name=model_name,
        source="dummy_dir/dummy_file",
        run_id=test_run.info.run_id,
        tags=dict(model_version_id=1),
    )
    client.transition_model_version_stage(
        name=model_name,
        version="1",
        stage="Archived",
    )


def test_ingestion(
    pytestconfig,
    mock_time,
    sink_file_path,
    pipeline_config,
    generate_mlflow_data,
):
    print(f"MCPs file path: {sink_file_path}")
    golden_file_path = (
        pytestconfig.rootpath / "tests/integration/mlflow/mlflow_mcps_golden.json"
    )

    pipeline = Pipeline.create(pipeline_config)
    pipeline.run()
    pipeline.pretty_print_summary()
    pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig,
        output_path=sink_file_path,
        golden_path=golden_file_path,
    )
