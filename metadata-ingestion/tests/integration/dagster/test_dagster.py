"""Golden-file tests for the pull-based Dagster source.

The Dagster GraphQL transport is mocked with recorded `/graphql` responses so the
tests are deterministic and run without a live Dagster instance. They exercise the
full parse -> emit -> workunit-processor path (including auto-generated Status and
browse-path aspects) and compare the output to golden MCP files.

The source is run under INGESTION attribution (as the real pipeline does) so that
source-owned descriptions land in datasetProperties.

For a live end-to-end check against a real Dagster webserver, see
`setup/definitions.py` and `setup/Dockerfile`: build the image, run
`dagster dev -f definitions.py -h 0.0.0.0 -p 3000`, and point a recipe at
`http://localhost:3000`.
"""

import json
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.sink.file import write_metadata_file
from datahub.ingestion.source.dagster.config import DagsterSourceConfig
from datahub.ingestion.source.dagster.dagster import DagsterSource
from datahub.sdk._attribution import KnownAttribution, change_default_attribution
from datahub.testing import mce_helpers

pytestmark = pytest.mark.integration


@pytest.fixture
def test_resources_dir(pytestconfig: Any) -> Path:
    return pytestconfig.rootpath / "tests/integration/dagster"


def _recorded_execute(test_resources_dir: Path) -> Any:
    """Route the two query types to their recorded responses (assets are paginated)."""
    repositories = json.loads(
        (test_resources_dir / "dagster_graphql_repositories.json").read_text()
    )
    assets = json.loads(
        (test_resources_dir / "dagster_graphql_assets.json").read_text()
    )

    def execute(query: str, variables: Any = None) -> Dict[str, Any]:
        if "repositoriesOrError" in query:
            return repositories
        return assets

    return execute


def _run(source: DagsterSource, execute: Any) -> List[Any]:
    with (
        change_default_attribution(KnownAttribution.INGESTION),
        patch.object(source.client, "_execute", side_effect=execute),
    ):
        return [wu.metadata for wu in source.get_workunits()]


def test_dagster_ingest(
    test_resources_dir: Path, tmp_path: Path, pytestconfig: Any
) -> None:
    source = DagsterSource(
        config=DagsterSourceConfig.parse_obj(
            {"host": "http://localhost:3000", "extraction_mode": "full"}
        ),
        ctx=PipelineContext(run_id="dagster-test"),
    )
    mce_objects = _run(source, _recorded_execute(test_resources_dir))

    output_path = tmp_path / "dagster_mces.json"
    write_metadata_file(output_path, mce_objects)
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir / "dagster_mces_golden.json",
    )

    assert source.report.assets_scanned == 2
    # Jobs/ops are off by default — the golden contains only assets + lineage.
    assert source.report.jobs_scanned == 0


def test_dagster_ingest_with_jobs(
    test_resources_dir: Path, tmp_path: Path, pytestconfig: Any
) -> None:
    source = DagsterSource(
        config=DagsterSourceConfig.parse_obj(
            {
                "host": "http://localhost:3000",
                "extraction_mode": "full",
                "include_jobs": True,
            }
        ),
        ctx=PipelineContext(run_id="dagster-test"),
    )
    mce_objects = _run(source, _recorded_execute(test_resources_dir))

    output_path = tmp_path / "dagster_mces_with_jobs.json"
    write_metadata_file(output_path, mce_objects)
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir / "dagster_mces_with_jobs_golden.json",
    )

    assert source.report.jobs_scanned == 1
