"""Golden-file test for the pull-based Dagster source.

The Dagster GraphQL transport is mocked with a recorded `/graphql` response so the
test is deterministic and runs without a live Dagster instance. It exercises the
full parse -> emit -> workunit-processor path (including auto-generated Status and
browse-path aspects) and compares the output to a golden MCP file.

For a live end-to-end check against a real Dagster webserver, see
`setup/definitions.py` and `setup/Dockerfile`: build the image, run
`dagster dev -f definitions.py -h 0.0.0.0 -p 3000`, and point a recipe at
`http://localhost:3000`.
"""

import json
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.sink.file import write_metadata_file
from datahub.ingestion.source.dagster.config import DagsterSourceConfig
from datahub.ingestion.source.dagster.dagster import DagsterSource
from datahub.testing import mce_helpers

pytestmark = pytest.mark.integration


@pytest.fixture
def test_resources_dir(pytestconfig: Any) -> Path:
    return pytestconfig.rootpath / "tests/integration/dagster"


def test_dagster_ingest(
    test_resources_dir: Path, tmp_path: Path, pytestconfig: Any
) -> None:
    recorded = json.loads(
        (test_resources_dir / "dagster_graphql_response.json").read_text()
    )

    source = DagsterSource(
        config=DagsterSourceConfig.parse_obj({"host": "http://localhost:3000"}),
        ctx=PipelineContext(run_id="dagster-test"),
    )

    with patch.object(source.client, "_execute", return_value=recorded):
        mce_objects = [wu.metadata for wu in source.get_workunits()]

    output_path = tmp_path / "dagster_mces.json"
    write_metadata_file(output_path, mce_objects)

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir / "dagster_mces_golden.json",
    )

    assert source.report.assets_scanned == 2
    assert source.report.jobs_scanned == 1
