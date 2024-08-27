from datetime import datetime, timezone

from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigqueryColumn,
    BigqueryTable,
    PartitionInfo,
)
from datahub.ingestion.source.bigquery_v2.profiler import BigqueryProfiler


def test_not_generate_partition_profiler_query_if_not_partitioned_sharded_table():
    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())
    test_table = BigqueryTable(
        name="test_table",
        comment="test_comment",
        rows_count=1,
        size_in_bytes=1,
        last_altered=datetime.now(timezone.utc),
        created=datetime.now(timezone.utc),
    )
    query = profiler.generate_partition_profiler_query(
        project="test_project",
        schema="test_dataset",
        table=test_table,
        partition_datetime=None,
    )

    assert query == (None, None)


def test_generate_day_partitioned_partition_profiler_query():
    column = BigqueryColumn(
        name="date",
        field_path="date",
        ordinal_position=1,
        data_type="TIMESTAMP",
        is_partition_column=True,
        cluster_column_position=None,
        comment=None,
        is_nullable=False,
    )
    partition_info = PartitionInfo(type="DAY", field="date", column=column)
    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())
    test_table = BigqueryTable(
        name="test_table",
        comment="test_comment",
        rows_count=1,
        size_in_bytes=1,
        last_altered=datetime.now(timezone.utc),
        created=datetime.now(timezone.utc),
        partition_info=partition_info,
        max_partition_id="20200101",
    )
    query = profiler.generate_partition_profiler_query(
        project="test_project",
        schema="test_dataset",
        table=test_table,
    )
    expected_query = """
SELECT
    *
FROM
    `test_project.test_dataset.test_table`
WHERE
    `date` BETWEEN TIMESTAMP('2020-01-01 00:00:00') AND TIMESTAMP('2020-01-02 00:00:00')
""".strip()

    assert "20200101" == query[0]
    assert query[1]
    assert expected_query == query[1].strip()


# If partition time is passed in we force to use that time instead of the max partition id
def test_generate_day_partitioned_partition_profiler_query_with_set_partition_time():
    column = BigqueryColumn(
        name="date",
        field_path="date",
        ordinal_position=1,
        data_type="TIMESTAMP",
        is_partition_column=True,
        cluster_column_position=None,
        comment=None,
        is_nullable=False,
    )
    partition_info = PartitionInfo(type="DAY", field="date", column=column)
    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())
    test_table = BigqueryTable(
        name="test_table",
        comment="test_comment",
        rows_count=1,
        size_in_bytes=1,
        last_altered=datetime.now(timezone.utc),
        created=datetime.now(timezone.utc),
        partition_info=partition_info,
        max_partition_id="20200101",
    )
    query = profiler.generate_partition_profiler_query(
        project="test_project",
        schema="test_dataset",
        table=test_table,
    )
    expected_query = """
SELECT
    *
FROM
    `test_project.test_dataset.test_table`
WHERE
    `date` BETWEEN TIMESTAMP('2020-01-01 00:00:00') AND TIMESTAMP('2020-01-02 00:00:00')
""".strip()

    assert "20200101" == query[0]
    assert query[1]
    assert expected_query == query[1].strip()


def test_generate_hour_partitioned_partition_profiler_query():
    column = BigqueryColumn(
        name="partition_column",
        field_path="partition_column",
        ordinal_position=1,
        data_type="TIMESTAMP",
        is_partition_column=True,
        cluster_column_position=None,
        comment=None,
        is_nullable=False,
    )
    partition_info = PartitionInfo(type="DAY", field="date", column=column)
    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())
    test_table = BigqueryTable(
        name="test_table",
        comment="test_comment",
        rows_count=1,
        size_in_bytes=1,
        last_altered=datetime.now(timezone.utc),
        created=datetime.now(timezone.utc),
        partition_info=partition_info,
        max_partition_id="2020010103",
    )
    query = profiler.generate_partition_profiler_query(
        project="test_project",
        schema="test_dataset",
        table=test_table,
        partition_datetime=None,
    )
    expected_query = """
SELECT
    *
FROM
    `test_project.test_dataset.test_table`
WHERE
    `partition_column` BETWEEN TIMESTAMP('2020-01-01 03:00:00') AND TIMESTAMP('2020-01-01 04:00:00')
""".strip()

    assert "2020010103" == query[0]
    assert query[1]
    assert expected_query == query[1].strip()


# Ingestion partitioned tables do not have partition column in the schema as it uses a psudo column _PARTITIONTIME to partition
def test_generate_ingestion_partitioned_partition_profiler_query():
    partition_info = PartitionInfo(type="DAY", field="date")
    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())
    test_table = BigqueryTable(
        name="test_table",
        comment="test_comment",
        rows_count=1,
        size_in_bytes=1,
        last_altered=datetime.now(timezone.utc),
        created=datetime.now(timezone.utc),
        partition_info=partition_info,
        max_partition_id="20200101",
    )
    query = profiler.generate_partition_profiler_query(
        project="test_project",
        schema="test_dataset",
        table=test_table,
    )
    expected_query = """
SELECT
    *
FROM
    `test_project.test_dataset.test_table`
WHERE
    `_PARTITIONTIME` BETWEEN TIMESTAMP('2020-01-01 00:00:00') AND TIMESTAMP('2020-01-02 00:00:00')
""".strip()

    assert "20200101" == query[0]
    assert query[1]
    assert expected_query == query[1].strip()


def test_generate_sharded_table_profiler_query():
    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())
    test_table = BigqueryTable(
        name="my_sharded_table",
        max_shard_id="20200101",
        comment="test_comment",
        rows_count=1,
        size_in_bytes=1,
        last_altered=datetime.now(timezone.utc),
        created=datetime.now(timezone.utc),
    )
    query = profiler.generate_partition_profiler_query(
        project="test_project",
        schema="test_dataset",
        table=test_table,
    )

    assert "20200101" == query[0]
    assert query[1] is None
