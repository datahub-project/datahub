import sys

import pytest

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers

FROZEN_TIME = "2020-04-14 07:00:00"


@pytest.mark.integration
@pytest.mark.skipif(sys.version_info < (3, 7), reason="iceberg requires Python 3.7+")
@pytest.mark.skip(reason="Skip until unit tests are passing, which will mean the avro mapping bug has been fixed")
def test_iceberg_ingest(pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/iceberg/"

    # Run the metadata ingestion pipeline.
    pipeline = Pipeline.create(
        {
            "run_id": "iceberg-test",
            "source": {
                "type": "iceberg",
                "config": {
                    "localfs": str(test_resources_dir / "test_data"),
                    "user_ownership_property": "owner",
                    "group_ownership_property": "owner",
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/iceberg_mces.json",
                },
            },
        }
    )
    pipeline.run()
    pipeline.raise_from_status()

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "iceberg_mces.json",
        golden_path=test_resources_dir / "iceberg_mces_golden.json",
    )
