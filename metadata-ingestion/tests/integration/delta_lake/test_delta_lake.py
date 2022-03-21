import pytest
from freezegun import freeze_time

from tests.test_helpers import mce_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd

FROZEN_TIME = "2020-04-14 07:00:00"


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_deltalake_ingest(pytestconfig, tmp_path):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/delta_lake"

    # Run the metadata ingestion pipeline.
    config_file = (test_resources_dir / "delta_lake_to_file.yml").resolve()
    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path="/tmp/delta_lake_mces.json",
        golden_path=test_resources_dir / "delta_lake_mces_golden.json",
    )
