from unittest.mock import patch

from freezegun import freeze_time

from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from tests.test_helpers import mce_helpers
from tests.test_helpers.state_helpers import run_and_get_pipeline

FROZEN_TIME = "2024-08-19 07:00:00"


@freeze_time(FROZEN_TIME)
@patch("google.cloud.bigquery.Client")
def test_queries_ingestion(client, pytestconfig, monkeypatch):

    test_resources_dir = pytestconfig.rootpath / "tests/integration/bigquery_v2"
    mcp_golden_path = f"{test_resources_dir}/bigquery_queries_mcps_golden.json"
    mcp_output_path = "bigquery_queries_mcps.json"

    pipeline_config_dict: dict = {
        "source": {
            "type": "bigquery-queries",
            "config": {
                "project_ids": ["gcp-staging", "gcp-staging-2"],
                "local_temp_path": test_resources_dir,
            },
        },
        "sink": {"type": "file", "config": {"filename": mcp_output_path}},
    }

    # This is hacky to pick all queries instead of any 10.
    # Should be easy to remove once top_n_queries is supported in queries config
    monkeypatch.setattr(BaseUsageConfig.__fields__["top_n_queries"], "default", 20)

    pipeline = run_and_get_pipeline(pipeline_config_dict)
    pipeline.pretty_print_summary()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=mcp_output_path,
        golden_path=mcp_golden_path,
    )
