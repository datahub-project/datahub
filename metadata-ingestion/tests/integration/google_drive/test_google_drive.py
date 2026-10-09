import json
from pathlib import Path
from unittest import mock

import pytest
import time_machine

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers
from tests.test_helpers.google_drive_fakes import (
    ROOT_A,
    SHARED_B,
    build_fixture_service,
)

FROZEN_TIME = "2026-03-01 12:00:00"
GOLDEN_FILE = "google_drive_mces_golden.json"


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_google_drive_ingest(pytestconfig: pytest.Config, tmp_path: Path) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/integration/google_drive"
    output_path = tmp_path / "google_drive_mces.json"

    with mock.patch(
        "datahub.ingestion.source.google_drive.google_drive_client.build_drive_service",
        return_value=build_fixture_service(),
    ):
        pipeline = Pipeline.create(
            {
                "run_id": "google-drive-test",
                "source": {
                    "type": "google-drive",
                    "config": {
                        "root_folder_ids": [ROOT_A, SHARED_B],
                        "folder_pattern": {"deny": [".*/Archive$"]},
                        "document_pattern": {"deny": ["^Copy of "]},
                        "platform_instance": "test-workspace",
                    },
                },
                "sink": {"type": "file", "config": {"filename": str(output_path)}},
            }
        )
        pipeline.run()
        pipeline.raise_from_status(raise_warnings=False)

    with open(output_path) as f:
        emitted = json.load(f)
    document_urns = {
        mcp["entityUrn"] for mcp in emitted if mcp.get("aspectName") == "documentInfo"
    }
    # 5 folders (2 roots + Eng, Specs, Team) + 6 Google Docs.
    assert len(document_urns) == 11

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir / GOLDEN_FILE,
    )
