from dataclasses import dataclass
from datetime import datetime

import pytest
from pydantic import ValidationError

import datahub.ingestion.source.usage.usage_common
from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.env_vars import (
    DEFAULT_USAGE_AGGREGATOR_CACHE_SIZE,
    get_usage_aggregator_cache_size,
)
from datahub.configuration.time_window_config import BucketDuration, get_time_bucket
from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.usage.usage_common import (
    DEFAULT_QUERIES_CHARACTER_LIMIT,
    BaseUsageConfig,
    GenericAggregatedDataset,
    UsageAggregator,
)
from datahub.metadata.schema_classes import (
    DatasetUsageStatisticsClass,
)
from datahub.testing.doctest import assert_doctest
from datahub.utilities.file_backed_collections import ConnectionWrapper

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


def test_extract_user_email():
    assert_doctest(datahub.ingestion.source.usage.usage_common)


def test_usage_aggregator_passes_user_email_pattern():
    test_email = "test_email@test.com"
    test_query = "select * from test"
    event_time = datetime(2020, 1, 1)
    resource = "test_db.test_schema.test_table"
    user_email_pattern = AllowDenyPattern(deny=["test_email@test.com"])

    config = BaseUsageConfig(user_email_pattern=user_email_pattern)
    aggregator: UsageAggregator[str] = UsageAggregator(config)

    aggregator.aggregate_event(
        resource=resource,
        start_time=event_time,
        query=test_query,
        user=test_email,
        fields=[],
    )

    # The denied user is filtered out, so the resource's aggregate records no
    # queries or users (matching the pre-FileBackedDict behavior, which kept an
    # empty aggregate for the resource).
    workunits = list(
        aggregator.generate_workunits(resource_urn_builder=_simple_urn_builder)
    )
    assert len(workunits) == 1
    aspect = workunits[0].metadata.aspect  # type: ignore[union-attr]
    assert isinstance(aspect, DatasetUsageStatisticsClass)
    assert aspect.totalSqlQueries == 0
    assert aspect.userCounts == []
    aggregator.close()


def test_usage_aggregator_persists_across_eviction():
    # cache_max_size=1 forces every other key out of the in-memory cache,
    # proving dirty values are flushed to SQLite and re-merged correctly.
    config = BaseUsageConfig()
    aggregator: UsageAggregator[str] = UsageAggregator(config, cache_max_size=1)

    event_time = datetime(2020, 1, 1)
    resource_a = "db.schema.table_a"
    resource_b = "db.schema.table_b"

    # Interleave events across two resources so each access evicts the other.
    aggregator.aggregate_event(
        resource=resource_a,
        start_time=event_time,
        query="select 1",
        user="u1@test.com",
        fields=[],
    )
    aggregator.aggregate_event(
        resource=resource_b,
        start_time=event_time,
        query="select 2",
        user="u2@test.com",
        fields=[],
    )
    aggregator.aggregate_event(
        resource=resource_a,
        start_time=event_time,
        query="select 1",
        user="u1@test.com",
        fields=[],
    )

    workunits = list(
        aggregator.generate_workunits(resource_urn_builder=_simple_urn_builder)
    )
    counts_by_urn = {
        wu.get_urn(): wu.metadata.aspect.totalSqlQueries  # type: ignore[union-attr]
        for wu in workunits
    }
    # resource_a saw 2 reads of "select 1"; resource_b saw 1 read of "select 2".
    assert counts_by_urn == {
        _simple_urn_builder(resource_a): 2,
        _simple_urn_builder(resource_b): 1,
    }
    aggregator.close()


@dataclass(frozen=True)
class _FrozenResource:
    db: str
    table: str

    def __str__(self) -> str:
        return f"{self.db}.{self.table}"


def test_usage_aggregator_with_non_str_resource_survives_eviction():
    # A frozen-dataclass resource (like Unity's TableReference) must pickle round-trip
    # and key correctly through SQLite under forced eviction.
    config = BaseUsageConfig()
    aggregator: UsageAggregator[_FrozenResource] = UsageAggregator(
        config, cache_max_size=1
    )
    res_a = _FrozenResource("db", "table_a")
    res_b = _FrozenResource("db", "table_b")
    event_time = datetime(2020, 1, 1)

    aggregator.aggregate_event(
        resource=res_a,
        start_time=event_time,
        query="select 1",
        user="u@test.com",
        fields=[],
    )
    aggregator.aggregate_event(
        resource=res_b,
        start_time=event_time,
        query="select 2",
        user="u@test.com",
        fields=[],
    )
    aggregator.aggregate_event(
        resource=res_a,
        start_time=event_time,
        query="select 1",
        user="u@test.com",
        fields=[],
    )

    def _urn_builder(r: _FrozenResource) -> str:
        return f"urn:li:dataset:(urn:li:dataPlatform:test,{r},PROD)"

    actual = {
        wu.get_urn(): wu.metadata.aspect.totalSqlQueries  # type: ignore[union-attr]
        for wu in aggregator.generate_workunits(resource_urn_builder=_urn_builder)
    }
    assert actual == {_urn_builder(res_a): 2, _urn_builder(res_b): 1}
    aggregator.close()


def test_usage_aggregator_close_is_idempotent():
    aggregator: UsageAggregator[str] = UsageAggregator(BaseUsageConfig())
    aggregator.aggregate_event(
        resource="db.s.t",
        start_time=datetime(2020, 1, 1),
        query="select 1",
        user="u@test.com",
        fields=[],
    )
    aggregator.close()
    aggregator.close()  # must not raise


def test_usage_aggregator_shared_connection_not_closed_on_aggregator_close():
    conn = ConnectionWrapper()
    try:
        aggregator: UsageAggregator[str] = UsageAggregator(
            BaseUsageConfig(), shared_connection=conn
        )
        aggregator.aggregate_event(
            resource="db.s.t",
            start_time=datetime(2020, 1, 1),
            query="select 1",
            user="u@test.com",
            fields=[],
        )
        workunits = list(
            aggregator.generate_workunits(resource_urn_builder=_simple_urn_builder)
        )
        assert len(workunits) == 1
        aggregator.close()
        # Shared connection must remain usable after the aggregator closes.
        conn.execute("SELECT 1").fetchone()
    finally:
        conn.close()


@dataclass(frozen=True)
class _LossyStrResource:
    table: str
    # Part of __eq__/__hash__ but intentionally omitted from __str__, mirroring
    # TableReference's last_updated field.
    version: int

    def __str__(self) -> str:
        return self.table


def test_usage_aggregator_merges_resources_with_equal_str():
    # Pins the intended key contract: the (bucket, str(resource)) pair is the aggregation
    # identity, so two resources that are != but share str() are merged into one aspect.
    # This mirrors usage-path TableReference (str() omits last_updated). A change to the
    # key contract that re-split these would flip the count from 2 to 1.
    aggregator: UsageAggregator[_LossyStrResource] = UsageAggregator(BaseUsageConfig())
    event_time = datetime(2020, 1, 1)
    res_v1 = _LossyStrResource("db.t", 1)
    res_v2 = _LossyStrResource("db.t", 2)
    assert res_v1 != res_v2
    assert str(res_v1) == str(res_v2)

    for res in (res_v1, res_v2):
        aggregator.aggregate_event(
            resource=res,
            start_time=event_time,
            query="select 1",
            user="u@test.com",
            fields=[],
        )

    def _urn_builder(r: _LossyStrResource) -> str:
        return f"urn:li:dataset:(urn:li:dataPlatform:test,{r},PROD)"

    workunits = list(aggregator.generate_workunits(resource_urn_builder=_urn_builder))
    assert len(workunits) == 1
    assert workunits[0].metadata.aspect.totalSqlQueries == 2  # type: ignore[union-attr]
    aggregator.close()


def test_usage_aggregator_rejects_non_positive_cache_size():
    # The ctor guard prevents a downstream crash in FileBackedDict.for_mutation, which
    # would otherwise fail on every event rather than at construction.
    with pytest.raises(ValueError):
        UsageAggregator(BaseUsageConfig(), cache_max_size=0)


def test_get_usage_aggregator_cache_size(monkeypatch):
    env = "DATAHUB_USAGE_AGGREGATOR_CACHE_SIZE"

    monkeypatch.delenv(env, raising=False)
    assert get_usage_aggregator_cache_size() == DEFAULT_USAGE_AGGREGATOR_CACHE_SIZE

    monkeypatch.setenv(env, "500")
    assert get_usage_aggregator_cache_size() == 500

    # Non-integer and non-positive overrides fall back to the default rather than crash.
    for bad in ("notanint", "0", "-5"):
        monkeypatch.setenv(env, bad)
        assert get_usage_aggregator_cache_size() == DEFAULT_USAGE_AGGREGATOR_CACHE_SIZE
