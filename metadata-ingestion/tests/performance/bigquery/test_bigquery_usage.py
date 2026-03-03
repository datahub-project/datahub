import logging
import os
import random
from datetime import timedelta

import humanfriendly
import psutil

from datahub.ingestion.source.bigquery_v2.bigquery_config import (
    BigQueryUsageConfig,
    BigQueryV2Config,
)
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.common import BigQueryIdentifierBuilder
from datahub.ingestion.source.bigquery_v2.usage import BigQueryUsageExtractor
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.utilities.perf_timer import PerfTimer
from tests.performance.bigquery.bigquery_events import generate_events, ref_from_table
from tests.performance.data_generation import (
    NormalDistribution,
    generate_data,
    generate_queries,
)
from tests.performance.helpers import workunit_sink


def run_test():
    report = BigQueryV2Report()
    with report.new_stage("All: Seed Data Generation"):
        seed_metadata = generate_data(
            num_containers=2000,
            num_tables=20000,
            num_views=2000,
            time_range=timedelta(days=7),
        )
        all_tables = seed_metadata.all_tables

    config = BigQueryV2Config(
        start_time=seed_metadata.start_time,
        end_time=seed_metadata.end_time,
        usage=BigQueryUsageConfig(
            include_top_n_queries=True,
            top_n_queries=10,
            apply_view_usage_to_tables=True,
        ),
        file_backed_cache_size=1000,
    )
    usage_extractor = BigQueryUsageExtractor(
        config,
        report,
        schema_resolver=SchemaResolver(platform="bigquery"),
        identifiers=BigQueryIdentifierBuilder(config, report),
    )
    with report.new_stage("All: Event Generation"):
        num_projects = 100
        projects = [f"project-{i}" for i in range(num_projects)]
        table_to_project = {table.name: random.choice(projects) for table in all_tables}
        table_refs = {
            str(ref_from_table(table, table_to_project)) for table in all_tables
        }

        queries = list(
            generate_queries(
                seed_metadata,
                num_selects=240_000,
                num_operations=800_000,
                num_unique_queries=50_000,
                num_users=2000,
                query_length=NormalDistribution(2000, 500),
            )
        )
        queries.sort(key=lambda q: q.timestamp)
        events = list(
            generate_events(queries, projects, table_to_project, config=config)
        )
        print(f"Events generated: {len(events)}")
        pre_mem_usage = psutil.Process(os.getpid()).memory_info().rss
        print(f"Test data size: {humanfriendly.format_size(pre_mem_usage)}")

        with report.new_stage("All: Event Ingestion"):
            with PerfTimer() as timer:
                workunits = usage_extractor._get_workunits_internal(events, table_refs)
                num_workunits, peak_memory_usage = workunit_sink(workunits)
                with report.new_stage("All: Done"):
                    print(f"Workunits Generated: {num_workunits}")
                    print(f"Seconds Elapsed: {timer.elapsed_seconds(digits=2)} seconds")

            print(
                f"Peak Memory Used: {humanfriendly.format_size(peak_memory_usage - pre_mem_usage)}"
            )
            print(f"Disk Used: {report.processing_perf.usage_state_size}")
            print(f"Hash collisions: {report.num_usage_query_hash_collisions}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_test()
