import os
from unittest.mock import patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.snowflake.snowflake_queries import SnowflakeQueriesSource


@patch("snowflake.connector.connect")
def test_source_close_cleans_tmp(snowflake_connect, tmp_path):
    with patch("tempfile.tempdir", str(tmp_path)):
        source = SnowflakeQueriesSource.create(
            {
                "connection": {
                    "account_id": "ABC12345.ap-south-1.aws",
                    "username": "TST_USR",
                    "password": "TST_PWD",
                }
            },
            PipelineContext("run-id"),
        )
        assert len(os.listdir(tmp_path)) > 0
        # This closes QueriesExtractor which in turn closes SqlParsingAggregator
        source.close()
        assert len(os.listdir(tmp_path)) == 0
