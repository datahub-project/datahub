import sys

import pytest
from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers

FROZEN_TIME = "2020-04-14 07:00:00"


# The test is skipped for python 3.11 due to conflicting dependencies in installDev
# setup that requires pydantic < 2 for majority plugins. Note that the test works with
# python 3.11 if run with standalone virtual env setup with feast plugin alone using
# `pip install acryl-datahub[feast]` since it allows pydantic > 2
@pytest.mark.skipif(sys.version_info > (3, 11), reason="Skipped on Python 3.11+")
@freeze_time(FROZEN_TIME)
def test_feast_repository_ingest(pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/feast"
    output_path = tmp_path / "feast_repository_mces.json"

    pipeline = Pipeline.create(
        {
            "run_id": "feast-repository-test",
            "source": {
                "type": "feast",
                "config": {
                    "path": str(test_resources_dir / "feature_store"),
                    "environment": "PROD",
                    "enable_tag_extraction": True,
                    "enable_owner_extraction": True,
                    "owner_mappings": [
                        {
                            "feast_owner_name": "MOCK_OWNER",
                            "datahub_owner_urn": "urn:li:corpGroup:MOCK_OWNER",
                            "datahub_ownership_type": "BUSINESS_OWNER",
                        }
                    ],
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(output_path),
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir / "feast_repository_mces_golden.json",
    )
