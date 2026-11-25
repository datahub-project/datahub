from pathlib import Path

from pytest import Config

from datahub.testing import mce_helpers
from tests.integration.vertexai.mock_vertexai import run_pipeline_fixtures

ALL_FIXTURES = [
    "nested_run",
    "loop_run",
    "running_run",
    "failure_run",
    "cancel_run",
    "refused_run",
    "cache_first_run",
    "cache_hit_run",
    "custom_jobs",
]


def test_vertexai_pipeline_tasks_world(pytestconfig: Config, tmp_path: Path) -> None:
    sink_file_path = str(tmp_path / "vertexai_pipeline_tasks_mcps.json")
    run_pipeline_fixtures(ALL_FIXTURES, sink_file_path)

    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig,
        output_path=sink_file_path,
        golden_path=pytestconfig.rootpath
        / "tests/integration/vertexai/vertexai_pipeline_tasks_golden.json",
    )
