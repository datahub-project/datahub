from datetime import datetime
from unittest import mock

import pytest
from pydantic import ValidationError

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.time_window_config import BucketDuration, get_time_bucket
from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.usage.usage_common import (
    BaseUsageConfig,
    GenericAggregatedDataset,
)
from datahub.metadata.schema_classes import DatasetUsageStatisticsClass

_TestTableRef = str

_TestAggregatedDataset = GenericAggregatedDataset[_TestTableRef]


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

    ta = _TestAggregatedDataset(
        bucket_start_time=floored_ts,
        resource=resource,
        user_email_pattern=AllowDenyPattern(deny=list(["test_email@test.com"])),
    )
    ta.add_read_entry(
        test_email,
        test_query,
        [],
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

    ta = _TestAggregatedDataset(
        bucket_start_time=floored_ts,
        resource=resource,
        user_email_pattern=AllowDenyPattern(deny=list(["test_email@test.com"])),
    )
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
        urn_builder=_simple_urn_builder,
        top_n_queries=10,
        format_sql_queries=False,
        include_top_n_queries=True,
    )

    assert wu.id == "2020-01-01T00:00:00-test_db.test_schema.test_table"
    assert isinstance(wu.get_metadata()["metadata"], MetadataChangeProposalWrapper)
    du: DatasetUsageStatisticsClass = wu.get_metadata()["metadata"].aspect
    assert du.totalSqlQueries == 1
    assert du.topSqlQueries
    assert du.topSqlQueries.pop() == test_query


def test_query_formatting():
    test_email = "test_email@test.com"
    test_query = "select * from foo where id in (select id from bar);"
    formatted_test_query: str = "SELECT *\n  FROM foo\n WHERE id in (\n        SELECT id\n          FROM bar\n       );"
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
        urn_builder=_simple_urn_builder,
        top_n_queries=10,
        format_sql_queries=True,
        include_top_n_queries=True,
    )
    assert wu.id == "2020-01-01T00:00:00-test_db.test_schema.test_table"
    assert isinstance(wu.get_metadata()["metadata"], MetadataChangeProposalWrapper)
    du: DatasetUsageStatisticsClass = wu.get_metadata()["metadata"].aspect
    assert du.totalSqlQueries == 1
    assert du.topSqlQueries
    assert du.topSqlQueries.pop() == formatted_test_query


def test_query_trimming():
    test_email: str = "test_email@test.com"
    test_query: str = "select * from test where a > 10 and b > 20 order by a asc"
    top_n_queries: int = 10
    total_budget_for_query_list: int = 200
    event_time = datetime(2020, 1, 1)
    floored_ts = get_time_bucket(event_time, BucketDuration.DAY)
    resource = "test_db.test_schema.test_table"

    ta = _TestAggregatedDataset(bucket_start_time=floored_ts, resource=resource)
    ta.total_budget_for_query_list = total_budget_for_query_list
    ta.add_read_entry(
        test_email,
        test_query,
        [],
    )
    wu: MetadataWorkUnit = ta.make_usage_workunit(
        bucket_duration=BucketDuration.DAY,
        urn_builder=_simple_urn_builder,
        top_n_queries=top_n_queries,
        format_sql_queries=False,
        include_top_n_queries=True,
    )

    assert wu.id == "2020-01-01T00:00:00-test_db.test_schema.test_table"
    assert isinstance(wu.get_metadata()["metadata"], MetadataChangeProposalWrapper)
    du: DatasetUsageStatisticsClass = wu.get_metadata()["metadata"].aspect
    assert du.totalSqlQueries == 1
    assert du.topSqlQueries
    assert du.topSqlQueries.pop() == "select * f ..."


def test_top_n_queries_validator_fails():
    with pytest.raises(ValidationError) as excinfo:
        with mock.patch(
            "datahub.ingestion.source.usage.usage_common.GenericAggregatedDataset.total_budget_for_query_list",
            20,
        ):
            BaseUsageConfig(top_n_queries=2)
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
        urn_builder=_simple_urn_builder,
        top_n_queries=10,
        format_sql_queries=False,
        include_top_n_queries=False,
    )

    assert wu.id == "2020-01-01T00:00:00-test_db.test_schema.test_table"
    assert isinstance(wu.get_metadata()["metadata"], MetadataChangeProposalWrapper)
    du: DatasetUsageStatisticsClass = wu.get_metadata()["metadata"].aspect
    assert du.totalSqlQueries == 1
    assert du.topSqlQueries is None
