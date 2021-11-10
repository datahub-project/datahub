import pytest
from click.testing import CliRunner
from freezegun import freeze_time

from datahub.entrypoints import datahub
from tests.test_helpers import fs_helpers, mce_helpers
from tests.test_helpers.click_helpers import assert_result_ok

FROZEN_TIME = "2020-04-14 07:00:00"


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_openapi_ingest(pytestconfig, tmp_path):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/openapi"

    # Run the metadata ingestion pipeline.
    runner = CliRunner()
    with fs_helpers.isolated_filesystem(tmp_path):
        config_file = (test_resources_dir / "openapi_to_file.yml").resolve()
        result = runner.invoke(datahub, ["ingest", "-c", f"{config_file}"])
        assert_result_ok(result)

        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path="/tmp/openapi_mces.json",
            golden_path=test_resources_dir / "openapi_mces_golden.json",
        )
