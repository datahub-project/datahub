import logging
import os
import random
from datetime import timedelta

import humanfriendly
import psutil
import pytest

from datahub.ingestion.source.bigquery_v2.bigquery_config import (
    BigQueryUsageConfig,
    BigQueryV2Config,
)
from datahub.ingestion.source.bigquery_v2.bigquery_report import (
    BigQueryV2Report,
    logger as report_logger,
)
from datahub.ingestion.source.bigquery_v2.usage import BigQueryUsageExtractor
from datahub.utilities.perf_timer import PerfTimer
from tests.performance.bigquery import generate_events, ref_from_table
from tests.performance.data_generation import generate_data, generate_queries

pytestmark = pytest.mark.performance


@pytest.fixture(autouse=True)
def report_log_level_info(caplog):
    with caplog.at_level(logging.INFO, logger=report_logger.name):
        yield


def test_bigquery_usage(report_log_level_info):
    report = BigQueryV2Report()
    report.set_project_state("All", "Seed Data Generation")
    seed_metadata = generate_data(
        num_containers=100,
        num_tables=2500,
        num_views=100,
        time_range=timedelta(days=1),
    )
    all_tables = seed_metadata.tables + seed_metadata.views

    config = BigQueryV2Config(
        start_time=seed_metadata.start_time,
        end_time=seed_metadata.end_time,
        usage=BigQueryUsageConfig(include_top_n_queries=True, top_n_queries=10),
    )
    usage_extractor = BigQueryUsageExtractor(config, report)
    report.set_project_state("All", "Event Generation")

    num_projects = 5
    projects = [f"project-{i}" for i in range(num_projects)]
    table_to_project = {table.name: random.choice(projects) for table in all_tables}
    table_refs = {str(ref_from_table(table, table_to_project)) for table in all_tables}

    queries = generate_queries(
        seed_metadata,
        num_selects=30000,
        num_operations=20000,
        num_users=10,
    )
    events = generate_events(queries, projects, table_to_project, config=config)
    events = list(events)
    print(f"Events generated: {len(events)}")

    report.set_project_state("All", "Event Ingestion")
    with PerfTimer() as timer:
        workunits = usage_extractor._run(events, table_refs)
        num_workunits = sum(1 for _ in workunits)
        report.set_project_state("All", "Done")
        print(f"Workunits Generated: {num_workunits}")
        print(f"Seconds Elapsed: {timer.elapsed_seconds():.2f} seconds")

    print(
        f"Memory Used: {humanfriendly.format_size(psutil.Process(os.getpid()).memory_info().rss)}"
    )
