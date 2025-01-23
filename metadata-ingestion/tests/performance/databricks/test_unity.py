import logging
import os
from unittest.mock import patch

import humanfriendly
import psutil

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from datahub.ingestion.source.unity.source import UnityCatalogSource
from datahub.utilities.perf_timer import PerfTimer
from tests.performance.data_generation import (
    NormalDistribution,
    generate_data,
    generate_queries,
)
from tests.performance.databricks.unity_proxy_mock import UnityCatalogApiProxyMock
from tests.performance.helpers import workunit_sink


def run_test():
    seed_metadata = generate_data(
        num_containers=[1, 100, 5000],
        num_tables=50000,
        num_views=10000,
        columns_per_table=NormalDistribution(100, 50),
        parents_per_view=NormalDistribution(5, 5),
        view_definition_length=NormalDistribution(1000, 300),
    )
    queries = generate_queries(
        seed_metadata,
        num_selects=100000,
        num_operations=100000,
        num_unique_queries=10000,
        num_users=1000,
    )
    proxy_mock = UnityCatalogApiProxyMock(
        seed_metadata, queries=queries, num_service_principals=10000
    )
    print("Data generated")

    config = UnityCatalogSourceConfig(
        token="",
        workspace_url="http://localhost:1234",
        include_usage_statistics=True,
        include_hive_metastore=False,
    )
    ctx = PipelineContext(run_id="test")
    with patch(
        "datahub.ingestion.source.unity.source.UnityCatalogApiProxy",
        lambda *args, **kwargs: proxy_mock,
    ):
        source: UnityCatalogSource = UnityCatalogSource(ctx, config)

    pre_mem_usage = psutil.Process(os.getpid()).memory_info().rss
    print(f"Test data size: {humanfriendly.format_size(pre_mem_usage)}")

    with PerfTimer() as timer:
        workunits = source.get_workunits()
        num_workunits, peak_memory_usage = workunit_sink(workunits)
        print(f"Workunits Generated: {num_workunits}")
        print(f"Seconds Elapsed: {timer.elapsed_seconds(digits=2)} seconds")

    print(
        f"Peak Memory Used: {humanfriendly.format_size(peak_memory_usage - pre_mem_usage)}"
    )
    print(source.report.as_string())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_test()
