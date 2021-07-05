import logging
import sys

import pytest

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers

logging.getLogger("lkml").setLevel(logging.INFO)


@pytest.mark.skipif(sys.version_info < (3, 7), reason="lkml requires Python 3.7+")
def test_lookml_ingest(pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/lookml"

    pipeline = Pipeline.create(
        {
            "run_id": "lookml-test",
            "source": {
                "type": "lookml",
                "config": {
                    "base_folder": str(test_resources_dir),
                    "connection_to_platform_map": {"my_connection": "conn"},
                    "parse_table_names_from_sql": True,
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/lookml_mces.json",
                },
            },
        }
    )
    pipeline.run()
    pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "lookml_mces.json",
        golden_path=test_resources_dir / "expected_output.json",
    )
