import functools
import logging
import os
from datetime import datetime, timezone
from unittest import mock

import humanfriendly
import psutil

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_v2 import SnowflakeV2Source
from datahub.utilities.perf_timer import PerfTimer
from tests.integration.snowflake.common import default_query_results
from tests.performance.helpers import workunit_sink


def run_test():
    with mock.patch("snowflake.connector.connect") as mock_connect:
        sf_connection = mock.MagicMock()
        sf_cursor = mock.MagicMock()
        mock_connect.return_value = sf_connection
        sf_connection.cursor.return_value = sf_cursor

        sf_cursor.execute.side_effect = functools.partial(
            default_query_results,
            num_tables=30000,
            num_views=10000,
            num_cols=30,
            num_ops=30,
            num_usages=500,
        )

        config = SnowflakeV2Config(
            account_id="ABC12345.ap-south-1.aws",
            username="TST_USR",
            password="TST_PWD",
            include_technical_schema=False,
            include_table_lineage=True,
            include_usage_stats=True,
            include_operational_stats=True,
            start_time=datetime(2022, 6, 6, 0, 0, 0, 0).replace(tzinfo=timezone.utc),
            end_time=datetime(2022, 6, 7, 7, 17, 0, 0).replace(tzinfo=timezone.utc),
            format_sql_queries=True,
        )
        ctx = PipelineContext(run_id="test")
        source = SnowflakeV2Source(ctx, config)

        pre_mem_usage = psutil.Process(os.getpid()).memory_info().rss
        logging.info(f"Test data size: {humanfriendly.format_size(pre_mem_usage)}")

        with PerfTimer() as timer:
            workunits = source.get_workunits()
            num_workunits, peak_memory_usage = workunit_sink(workunits)
            logging.info(f"Workunits Generated: {num_workunits}")
            logging.info(f"Seconds Elapsed: {timer.elapsed_seconds(digits=2)} seconds")
            logging.info(source.get_report().as_string())

        logging.info(
            f"Peak Memory Used: {humanfriendly.format_size(peak_memory_usage - pre_mem_usage)}"
        )
        logging.info(source.report.aspects)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_test()
