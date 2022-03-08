import json
import pathlib
import shutil
from typing import List
from unittest import mock

import great_expectations as ge
import pytest
from freezegun import freeze_time

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from tests.test_helpers import mce_helpers
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2021-12-28 12:00:00"


class MockDatahubEmitter:
    def __init__(self, gms_server: str):
        self.mcps: List[MetadataChangeProposalWrapper] = []

    def emit_mcp(self, mcp: MetadataChangeProposalWrapper) -> None:
        self.mcps.append(mcp)

    def write_to_file(self, filename):
        fpath = pathlib.Path(filename)
        file = fpath.open("w")
        file.write("[\n")

        for i, mcp in enumerate(self.mcps):
            if i != 0:
                file.write(",\n")
            json.dump(mcp.to_obj(), file, indent=4)

        file.write("\n]")
        file.close()


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_ge_ingest(docker_compose_runner, pytestconfig, tmp_path, mock_time, **kwargs):

    test_resources_dir = pytestconfig.rootpath / "tests/integration/great-expectations"

    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "great-expectations"
    ) as docker_services, mock.patch(
        "datahub.emitter.rest_emitter.DatahubRestEmitter.emit_mcp"
    ) as mock_emit_mcp:
        wait_for_port(docker_services, "ge_postgres", 5432)

        emitter = MockDatahubEmitter("")
        mock_emit_mcp.side_effect = emitter.emit_mcp

        shutil.copytree(
            test_resources_dir / "setup/great_expectations",
            tmp_path / "great_expectations",
        )
        context = ge.DataContext.create(tmp_path)
        context.run_checkpoint(checkpoint_name="test_checkpoint")

        emitter.write_to_file(tmp_path / "ge_mcps.json")

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "ge_mcps.json",
            golden_path=test_resources_dir / "ge_mcps_golden.json",
            ignore_paths=[],
        )
