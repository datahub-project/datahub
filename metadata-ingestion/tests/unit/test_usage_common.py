import time
from datetime import datetime

import pytest
from freezegun import freeze_time
from pydantic import ValidationError

import datahub.ingestion.source.usage.usage_common
from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.time_window_config import BucketDuration, get_time_bucket
from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.usage.usage_common import (
    DEFAULT_QUERIES_CHARACTER_LIMIT,
    BaseUsageConfig,
    GenericAggregatedDataset,
    convert_usage_aggregation_class,
)
from datahub.metadata.schema_classes import (
    CalendarIntervalClass,
    DatasetFieldUsageCountsClass,
    DatasetUsageStatisticsClass,
    DatasetUserUsageCountsClass,
    FieldUsageCountsClass,
    TimeWindowSizeClass,
    UsageAggregationClass,
    UsageAggregationMetricsClass,
    UserUsageCountsClass,
    WindowDurationClass,
)
from datahub.testing.doctest import assert_doctest

_TestTableRef = str

_TestAggregatedDataset = GenericAggregatedDataset[_TestTableRef]
USAGE_ASPECT_NAME = DatasetUsageStatisticsClass.get_aspect_name()


def _simple_urn_builder(resource):
    return make_dataset_urn_with_platform_instance(
        "snowflake",
        resource.lower(),
        "snowflake-dev",
        "DEV",
    )


def test_add_one_query_without_columns():
    test_email = "test_email@test.com"
    test_query = "select * from test"
    event_time = datetime(2020, 1, 1)
    floored_ts = get_time_bucket(event_time, BucketDuration.DAY)
    resource = "test_db.test_schema.test_table"

    ta = _TestAggregatedDataset(bucket_start_time=floored_ts, resource=resource)
    ta.add_read_entry(
        test_email,
        test_query,
        [],
    )

    assert ta.queryCount == 1
    assert ta.queryFreq[test_query] == 1
    assert ta.userFreq[test_email] == 1
    assert len(ta.columnFreq) == 0


def test_add_one_query_with_ignored_user():
    test_email = "test_email@test.com"
    test_query = "select * from test"
    event_time = datetime(2020, 1, 1)
    floored_ts = get_time_bucket(event_time, BucketDuration.DAY)
    resource = "test_db.test_schema.test_table"
    user_email_pattern = AllowDenyPattern(deny=list(["test_email@test.com"]))

    ta = _TestAggregatedDataset(
        bucket_start_time=floored_ts,
        resource=resource,
    )
    ta.add_read_entry(
        test_email,
        test_query,
        [],
        user_email_pattern=user_email_pattern,
    )

    assert ta.queryCount == 0
    assert ta.queryFreq[test_query] == 0
    assert ta.userFreq[test_email] == 0
    assert len(ta.columnFreq) == 0


def test_multiple_query_with_ignored_user():
    test_email = "test_email@test.com"
    test_email2 = "test_email2@test.com"
    test_query = "select * from test"
    test_query2 = "select * from test2"
    event_time = datetime(2020, 1, 1)
    floored_ts = get_time_bucket(event_time, BucketDuration.DAY)
    resource = "test_db.test_schema.test_table"
    user_email_pattern = AllowDenyPattern(deny=list(["test_email@test.com"]))

    ta = _TestAggregatedDataset(
        bucket_start_time=floored_ts,
        resource=resource,
    )
    ta.add_read_entry(
        test_email,
        test_query,
        [],
        user_email_pattern=user_email_pattern,
    )
    ta.add_read_entry(
        test_email,
        test_query,
        [],
        user_email_pattern=user_email_pattern,
    )
    ta.add_read_entry(
        test_email2,
        test_query2,
        [],
        user_email_pattern=user_email_pattern,
    )

    assert ta.queryCount == 1
    assert ta.queryFreq[test_query] == 0
    assert ta.userFreq[test_email] == 0
    assert ta.queryFreq[test_query2] == 1
    assert ta.userFreq[test_email2] == 1
    assert len(ta.columnFreq) == 0


def test_multiple_query_without_columns():
    test_email = "test_email@test.com"
    test_email2 = "test_email2@test.com"
    test_query = "select * from test"
    test_query2 = "select * from test2"
    event_time = datetime(2020, 1, 1)
    floored_ts = get_time_bucket(event_time, BucketDuration.DAY)
    resource = "test_db.test_schema.test_table"

    ta = _TestAggregatedDataset(bucket_start_time=floored_ts, resource=resource)
    ta.add_read_entry(
        test_email,
        test_query,
        [],
    )
    ta.add_read_entry(
        test_email,
        test_query,
        [],
    )
    ta.add_read_entry(
        test_email2,
        test_query2,
        [],
    )

    assert ta.queryCount == 3
    assert ta.queryFreq[test_query] == 2
    assert ta.userFreq[test_email] == 2
    assert ta.queryFreq[test_query2] == 1
    assert ta.userFreq[test_email2] == 1
    assert len(ta.columnFreq) == 0


def test_make_usage_workunit():
    test_email = "test_email@test.com"
    test_query = "select * from test"
    event_time = datetime(2020, 1, 1)
    floored_ts = get_time_bucket(event_time, BucketDuration.DAY)
    resource = "test_db.test_schema.test_table"

    ta = _TestAggregatedDataset(bucket_start_time=floored_ts, resource=resource)
    ta.add_read_entry(
        test_email,
        test_query,
        [],
    )
    wu: MetadataWorkUnit = ta.make_usage_workunit(
        bucket_duration=BucketDuration.DAY,
        resource_urn_builder=_simple_urn_builder,
        top_n_queries=10,
        format_sql_queries=False,
        include_top_n_queries=True,
        queries_character_limit=DEFAULT_QUERIES_CHARACTER_LIMIT,
    )

    ts_timestamp = int(floored_ts.timestamp() * 1000)
    assert (
        wu.id == f"{_simple_urn_builder(resource)}-{USAGE_ASPECT_NAME}-{ts_timestamp}"
    )
    assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
    assert isinstance(wu.metadata.aspect, DatasetUsageStatisticsClass)
    du: DatasetUsageStatisticsClass = wu.metadata.aspect
    assert du.totalSqlQueries == 1
    assert du.topSqlQueries
    assert du.topSqlQueries.pop() == test_query


def test_query_formatting():
    test_email = "test_email@test.com"
    test_query = "select * from foo where id in (select id from bar);"
    formatted_test_query: str = "SELECT *\n  FROM foo\n WHERE id IN (\n        SELECT id\n          FROM bar\n       );"
    event_time = datetime(2020, 1, 1)

    floored_ts = get_time_bucket(event_time, BucketDuration.DAY)

    resource = "test_db.test_schema.test_table"

    ta = _TestAggregatedDataset(bucket_start_time=floored_ts, resource=resource)
    ta.add_read_entry(
        test_email,
        test_query,
        [],
    )
    wu: MetadataWorkUnit = ta.make_usage_workunit(
        bucket_duration=BucketDuration.DAY,
        resource_urn_builder=_simple_urn_builder,
        top_n_queries=10,
        format_sql_queries=True,
        include_top_n_queries=True,
        queries_character_limit=DEFAULT_QUERIES_CHARACTER_LIMIT,
    )
    ts_timestamp = int(floored_ts.timestamp() * 1000)
    assert (
        wu.id == f"{_simple_urn_builder(resource)}-{USAGE_ASPECT_NAME}-{ts_timestamp}"
    )
    assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
    assert isinstance(wu.metadata.aspect, DatasetUsageStatisticsClass)
    du: DatasetUsageStatisticsClass = wu.metadata.aspect
    assert du.totalSqlQueries == 1
    assert du.topSqlQueries
    assert du.topSqlQueries.pop() == formatted_test_query


def test_query_trimming():
    test_email: str = "test_email@test.com"
    test_query: str = "select * from test where a > 10 and b > 20 order by a asc"
    top_n_queries: int = 10
    queries_character_limit: int = 200
    event_time = datetime(2020, 1, 1)
    floored_ts = get_time_bucket(event_time, BucketDuration.DAY)
    resource = "test_db.test_schema.test_table"

    ta = _TestAggregatedDataset(bucket_start_time=floored_ts, resource=resource)
    ta.add_read_entry(
        test_email,
        test_query,
        [],
    )
    wu: MetadataWorkUnit = ta.make_usage_workunit(
        bucket_duration=BucketDuration.DAY,
        resource_urn_builder=_simple_urn_builder,
        top_n_queries=top_n_queries,
        format_sql_queries=False,
        include_top_n_queries=True,
        queries_character_limit=queries_character_limit,
    )

    ts_timestamp = int(floored_ts.timestamp() * 1000)
    assert (
        wu.id == f"{_simple_urn_builder(resource)}-{USAGE_ASPECT_NAME}-{ts_timestamp}"
    )
    assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
    assert isinstance(wu.metadata.aspect, DatasetUsageStatisticsClass)
    du: DatasetUsageStatisticsClass = wu.metadata.aspect
    assert du.totalSqlQueries == 1
    assert du.topSqlQueries
    assert du.topSqlQueries.pop() == "select * from te ..."


def test_top_n_queries_validator_fails():
    with pytest.raises(ValidationError) as excinfo:
        BaseUsageConfig(top_n_queries=2, queries_character_limit=20)
    assert "top_n_queries is set to 2 but it can be maximum 1" in str(excinfo.value)


def test_make_usage_workunit_include_top_n_queries():
    test_email = "test_email@test.com"
    test_query = "select * from test"
    event_time = datetime(2020, 1, 1)
    floored_ts = get_time_bucket(event_time, BucketDuration.DAY)
    resource = "test_db.test_schema.test_table"

    ta = _TestAggregatedDataset(bucket_start_time=floored_ts, resource=resource)
    ta.add_read_entry(
        test_email,
        test_query,
        [],
    )
    wu: MetadataWorkUnit = ta.make_usage_workunit(
        bucket_duration=BucketDuration.DAY,
        resource_urn_builder=_simple_urn_builder,
        top_n_queries=10,
        format_sql_queries=False,
        include_top_n_queries=False,
        queries_character_limit=DEFAULT_QUERIES_CHARACTER_LIMIT,
    )

    ts_timestamp = int(floored_ts.timestamp() * 1000)
    assert (
        wu.id == f"{_simple_urn_builder(resource)}-{USAGE_ASPECT_NAME}-{ts_timestamp}"
    )
    assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
    assert isinstance(wu.metadata.aspect, DatasetUsageStatisticsClass)
    du: DatasetUsageStatisticsClass = wu.metadata.aspect
    assert du.totalSqlQueries == 1
    assert du.topSqlQueries is None


@freeze_time("2023-01-01 00:00:00")
def test_convert_usage_aggregation_class():
    urn = make_dataset_urn_with_platform_instance(
        "platform", "test_db.test_schema.test_table", None
    )
    usage_aggregation = UsageAggregationClass(
        bucket=int(time.time() * 1000),
        duration=WindowDurationClass.DAY,
        resource=urn,
        metrics=UsageAggregationMetricsClass(
            uniqueUserCount=5,
            users=[
                UserUsageCountsClass(count=3, user="abc", userEmail="abc@acryl.io"),
                UserUsageCountsClass(count=2),
                UserUsageCountsClass(count=1, user="def"),
            ],
            totalSqlQueries=10,
            topSqlQueries=["SELECT * FROM my_table", "SELECT col from a.b.c"],
            fields=[FieldUsageCountsClass("col", 7), FieldUsageCountsClass("col2", 0)],
        ),
    )
    assert convert_usage_aggregation_class(
        usage_aggregation
    ) == MetadataChangeProposalWrapper(
        entityUrn=urn,
        aspect=DatasetUsageStatisticsClass(
            timestampMillis=int(time.time() * 1000),
            eventGranularity=TimeWindowSizeClass(unit=CalendarIntervalClass.DAY),
            uniqueUserCount=5,
            totalSqlQueries=10,
            topSqlQueries=["SELECT * FROM my_table", "SELECT col from a.b.c"],
            userCounts=[
                DatasetUserUsageCountsClass(
                    user="abc", count=3, userEmail="abc@acryl.io"
                ),
                DatasetUserUsageCountsClass(user="def", count=1),
            ],
            fieldCounts=[
                DatasetFieldUsageCountsClass(fieldPath="col", count=7),
                DatasetFieldUsageCountsClass(fieldPath="col2", count=0),
            ],
        ),
    )

    empty_urn = make_dataset_urn_with_platform_instance(
        "platform",
        "test_db.test_schema.empty_table",
        None,
    )
    empty_usage_aggregation = UsageAggregationClass(
        bucket=int(time.time() * 1000) - 1000 * 60 * 60 * 24,
        duration=WindowDurationClass.MONTH,
        resource=empty_urn,
        metrics=UsageAggregationMetricsClass(),
    )
    assert convert_usage_aggregation_class(
        empty_usage_aggregation
    ) == MetadataChangeProposalWrapper(
        entityUrn=empty_urn,
        aspect=DatasetUsageStatisticsClass(
            timestampMillis=int(time.time() * 1000) - 1000 * 60 * 60 * 24,
            eventGranularity=TimeWindowSizeClass(unit=CalendarIntervalClass.MONTH),
        ),
    )


def test_extract_user_email():
    assert_doctest(datahub.ingestion.source.usage.usage_common)
