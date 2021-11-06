import pytest
from click.testing import CliRunner
from freezegun import freeze_time

from datahub.entrypoints import datahub
from tests.test_helpers import fs_helpers, mce_helpers
from tests.test_helpers.click_helpers import assert_result_ok
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2020-04-14 07:00:00"


@pytest.mark.integration
def test_data_lake_inggest(pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/data_lake"
    
    # Run the metadata ingestion pipeline.
    runner = CliRunner()
    with fs_helpers.isolated_filesystem(tmp_path):
        config_file = (test_resources_dir / "data_lake_to_file.yml").resolve()
        result = runner.invoke(datahub, ["ingest", "-c", f"{config_file}"])
        assert_result_ok(result)

        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path="data_lake_mces.json",
            golden_path=test_resources_dir / "data_lake_mces_golden.json",
        )
