import json
from pathlib import Path
from typing import Any, Dict
from unittest.mock import patch

import pytest
import time_machine

from datahub.configuration.config_loader import load_config_file
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.matillion_dpc.matillion_api import MatillionAPIClient
from datahub.testing import mce_helpers

FROZEN_TIME = "2024-01-01 00:00:00"


def load_fixture(test_resources_dir: Path, fixture_name: str) -> Dict[str, Any]:
    """Load a JSON fixture from the setup directory."""
    fixture_path = test_resources_dir / "setup" / fixture_name
    with open(fixture_path) as f:
        return json.load(f)


@time_machine.travel(FROZEN_TIME)
def test_matillion_source_basic(pytestconfig: pytest.Config, tmp_path: Any) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/integration/matillion_dpc"
    output_path = tmp_path / "matillion_mces.json"

    # Load fixtures
    mock_projects_response = load_fixture(test_resources_dir, "projects_basic.json")
    mock_environments_response = load_fixture(
        test_resources_dir, "environments_basic.json"
    )
    mock_pipelines_response = load_fixture(test_resources_dir, "pipelines_basic.json")
    mock_empty_response = load_fixture(test_resources_dir, "empty_results.json")

    def mock_make_request(
        self: Any, method: str, endpoint: str, **kwargs: Any
    ) -> Dict[str, Any]:
        if endpoint == "v1/projects":
            return mock_projects_response
        elif "/environments" in endpoint:
            return mock_environments_response
        elif "published-pipelines" in endpoint:
            return mock_pipelines_response
        elif "executions" in endpoint:
            return {"results": []}
        return mock_empty_response

    with patch.object(MatillionAPIClient, "_make_request", mock_make_request):
        pipeline_config = load_config_file(
            test_resources_dir / "matillion_basic_to_file.yml"
        )
        pipeline = Pipeline.create(
            {
                "run_id": "matillion-test",
                **pipeline_config,
                "sink": {
                    "type": "file",
                    "config": {"filename": str(output_path)},
                },
            }
        )

        pipeline.run()
        pipeline.raise_from_status()

    assert output_path.exists()

    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir / "matillion_mces_golden.json",
    )


@time_machine.travel(FROZEN_TIME)
def test_matillion_source_with_lineage(
    pytestconfig: pytest.Config, tmp_path: Any
) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/integration/matillion_dpc"
    output_path = tmp_path / "matillion_lineage_mces.json"

    # Load fixtures
    mock_projects_response = load_fixture(test_resources_dir, "projects_lineage.json")
    mock_environments_response = load_fixture(
        test_resources_dir, "environments_basic.json"
    )
    mock_pipelines_response = load_fixture(test_resources_dir, "pipelines_lineage.json")
    mock_empty_response = load_fixture(test_resources_dir, "empty_results.json")

    def mock_make_request(
        self: Any, method: str, endpoint: str, **kwargs: Any
    ) -> Dict[str, Any]:
        if endpoint == "v1/projects":
            return mock_projects_response
        elif "/environments" in endpoint:
            return mock_environments_response
        elif "published-pipelines" in endpoint:
            return mock_pipelines_response
        elif "executions" in endpoint:
            return {"results": []}
        return mock_empty_response

    with patch.object(MatillionAPIClient, "_make_request", mock_make_request):
        pipeline_config = load_config_file(
            test_resources_dir / "matillion_lineage_to_file.yml"
        )
        pipeline = Pipeline.create(
            {
                "run_id": "matillion-lineage-test",
                **pipeline_config,
                "sink": {
                    "type": "file",
                    "config": {"filename": str(output_path)},
                },
            }
        )

        pipeline.run()
        pipeline.raise_from_status()

    assert output_path.exists()

    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir / "matillion_lineage_mces_golden.json",
    )


@time_machine.travel(FROZEN_TIME)
def test_matillion_source_with_streaming_pipelines(
    pytestconfig: pytest.Config, tmp_path: Any
) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/integration/matillion_dpc"
    output_path = tmp_path / "matillion_streaming_mces.json"

    # Load fixtures
    mock_projects_response = load_fixture(test_resources_dir, "projects_streaming.json")
    mock_environments_response = load_fixture(
        test_resources_dir, "environments_basic.json"
    )
    mock_streaming_pipelines_response = load_fixture(
        test_resources_dir, "streaming_pipelines.json"
    )
    mock_empty_response = load_fixture(test_resources_dir, "empty_results.json")

    def mock_make_request(
        self: Any, method: str, endpoint: str, **kwargs: Any
    ) -> Dict[str, Any]:
        if "projects" in endpoint:
            return mock_projects_response
        elif "environments" in endpoint:
            return mock_environments_response
        elif "streaming-pipelines" in endpoint:
            return mock_streaming_pipelines_response
        elif "pipelines" in endpoint:
            return mock_empty_response
        elif "executions" in endpoint:
            return {"results": []}
        return mock_empty_response

    with patch.object(MatillionAPIClient, "_make_request", mock_make_request):
        pipeline_config = load_config_file(
            test_resources_dir / "matillion_streaming_to_file.yml"
        )
        pipeline = Pipeline.create(
            {
                "run_id": "matillion-streaming-test",
                **pipeline_config,
                "sink": {
                    "type": "file",
                    "config": {"filename": str(output_path)},
                },
            }
        )

        pipeline.run()
        pipeline.raise_from_status()

    assert output_path.exists()

    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir / "matillion_streaming_mces_golden.json",
    )


@time_machine.travel(FROZEN_TIME)
def test_matillion_source_comprehensive(
    pytestconfig: pytest.Config, tmp_path: Any
) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/integration/matillion_dpc"
    output_path = tmp_path / "matillion_comprehensive_mces.json"

    # Load fixtures
    mock_projects_response = load_fixture(
        test_resources_dir, "projects_comprehensive.json"
    )
    mock_environments_response = load_fixture(
        test_resources_dir, "environments_basic.json"
    )
    mock_pipelines_response = load_fixture(
        test_resources_dir, "pipelines_comprehensive.json"
    )
    mock_lineage_response = load_fixture(test_resources_dir, "lineage_graph.json")
    mock_streaming_pipelines_response = load_fixture(
        test_resources_dir, "streaming_pipelines_comprehensive.json"
    )
    mock_executions_response = load_fixture(test_resources_dir, "executions.json")
    mock_empty_response = load_fixture(test_resources_dir, "empty_results.json")

    def mock_make_request(
        self: Any, method: str, endpoint: str, **kwargs: Any
    ) -> Dict[str, Any]:
        if "projects" in endpoint:
            return mock_projects_response
        elif "environments" in endpoint:
            return mock_environments_response
        elif "streaming-pipelines" in endpoint:
            return mock_streaming_pipelines_response
        elif "pipelines" in endpoint and "/lineage" in endpoint:
            return mock_lineage_response
        elif "pipelines" in endpoint and "/executions" in endpoint:
            return mock_executions_response
        elif "pipelines" in endpoint:
            return mock_pipelines_response
        return mock_empty_response

    with patch.object(MatillionAPIClient, "_make_request", mock_make_request):
        pipeline_config = load_config_file(
            test_resources_dir / "matillion_comprehensive_to_file.yml"
        )
        pipeline = Pipeline.create(
            {
                "run_id": "matillion-comprehensive-test",
                **pipeline_config,
                "sink": {
                    "type": "file",
                    "config": {"filename": str(output_path)},
                },
            }
        )

        pipeline.run()
        pipeline.raise_from_status()

    assert output_path.exists()

    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir / "matillion_comprehensive_mces_golden.json",
    )


@time_machine.travel(FROZEN_TIME)
def test_matillion_postgres_to_snowflake_with_sql_parsing(
    pytestconfig: pytest.Config, tmp_path: Any
) -> None:
    """
    Comprehensive test modeling realistic Postgres → Snowflake transformations
    with OpenLineage events, SQL parsing, and column-level lineage.
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/matillion_dpc"
    output_path = tmp_path / "matillion_postgres_snowflake_mces.json"

    # Load fixtures
    mock_projects_response = load_fixture(
        test_resources_dir, "projects_sql_parsing.json"
    )
    mock_environments_response = load_fixture(
        test_resources_dir, "environments_sql_parsing.json"
    )
    mock_pipelines_response = load_fixture(
        test_resources_dir, "pipelines_sql_parsing.json"
    )
    mock_lineage_events_response = load_fixture(
        test_resources_dir, "lineage_events_sql_parsing.json"
    )
    mock_empty_response = load_fixture(test_resources_dir, "empty_results.json")

    def mock_make_request(
        self: Any, method: str, endpoint: str, **kwargs: Any
    ) -> Dict[str, Any]:
        if endpoint == "v1/projects":
            return mock_projects_response
        elif "/environments" in endpoint:
            return mock_environments_response
        elif "published-pipelines" in endpoint:
            return mock_pipelines_response
        elif "lineage/events" in endpoint:
            return mock_lineage_events_response
        elif "executions" in endpoint:
            return {"results": []}
        return mock_empty_response

    with patch.object(MatillionAPIClient, "_make_request", mock_make_request):
        pipeline_config = load_config_file(
            test_resources_dir / "matillion_postgres_snowflake_to_file.yml"
        )
        pipeline = Pipeline.create(
            {
                "run_id": "matillion-postgres-snowflake-test",
                **pipeline_config,
                "sink": {
                    "type": "file",
                    "config": {"filename": str(output_path)},
                },
            }
        )

        pipeline.run()
        pipeline.raise_from_status()

    assert output_path.exists()

    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir
        / "matillion_postgres_snowflake_mces_golden.json",
    )


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
