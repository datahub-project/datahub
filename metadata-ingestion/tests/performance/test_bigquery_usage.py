import logging
import os
import random
import sys
from datetime import timedelta
from typing import Generator

import humanfriendly
import psutil
import pytest
from performance.bigquery import generate_events, ref_from_table
from performance.data_generation import generate_data, generate_queries

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


def set_log_level(logger: logging.Logger, level: int) -> Generator[None, None, None]:
    old_log_level = logger.level
    try:
        logger.setLevel(level)
        yield
    finally:
        logger.setLevel(old_log_level)


@pytest.fixture(autouse=True)
def default_log_level_error():
    root_logger = logging.getLogger()
    stream_handler = logging.StreamHandler(sys.stdout)
    try:
        root_logger.addHandler(stream_handler)
        yield from set_log_level(root_logger, logging.ERROR)
    finally:
        root_logger.removeHandler(stream_handler)


@pytest.fixture
def report_log_level_info():
    yield from set_log_level(report_logger, logging.INFO)


@pytest.mark.performance
def test_bigquery_usage(report_log_level_info):
    report = BigQueryV2Report()
    report.set_project_state("All", "Seed Data Generation")
    seed_metadata = generate_data(
        num_containers=100,
        num_tables=250,
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
        num_selects=10000,
        num_operations=20000,
        num_users=10,
    )
    events = generate_events(queries, projects, table_to_project, config=config)
    events = list(events)
    print(f"Events generated: {len(events)}")

    report.set_project_state("All", "Event Ingestion")
    with PerfTimer() as timer:
        assert usage_extractor, table_refs  # TODO: Replace with call to usage extractor
        num_workunits = sum(1 for _ in [])
        report.set_project_state("All", "Done")
        print(f"Workunits Generated: {num_workunits}")
        print(f"Seconds Elapsed: {timer.elapsed_seconds():.2f} seconds")

    print(
        f"Memory Used: {humanfriendly.format_size(psutil.Process(os.getpid()).memory_info().rss)}"
    )
