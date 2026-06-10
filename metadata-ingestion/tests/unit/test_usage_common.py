from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime

import pytest
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
    UsageAggregator,
    _Dim,
    _UsageKeys,
)
from datahub.metadata.schema_classes import (
    DatasetUsageStatisticsClass,
)
from datahub.testing.doctest import assert_doctest
from datahub.utilities.file_backed_collections import (
    ConnectionWrapper,
    InMemoryGroupedCounter,
)

_TestTableRef = str

_TestAggregatedDataset = GenericAggregatedDataset[_TestTableRef]
USAGE_ASPECT_NAME = DatasetUsageStatisticsClass.get_aspect_name()


@dataclass(frozen=True)
class _MergeRes:
    table: str
    version: int  # in __eq__ but omitted from __str__

    def __str__(self) -> str:
        return self.table


@dataclass(frozen=True)
class _RoundtripRes:
    db: str
    table: str

    def __str__(self) -> str:
        return f"{self.db}.{self.table}"


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


def test_usage_keys_group_roundtrip():
    # resource_key containing ":" must round-trip (partition splits on the first ":").
    gk = _UsageKeys.group(1700000000000, "urn:li:dataset:(x,y,PROD)")
    assert _UsageKeys.split_group(gk) == (1700000000000, "urn:li:dataset:(x,y,PROD)")


def test_usage_keys_item_roundtrip():
    # value containing ":" must round-trip; dim prefix is ":"-free.
    ik = _UsageKeys.item(_Dim.QUERY, "SELECT a::int FROM t WHERE x = 'a:b'")
    assert _UsageKeys.split_item(ik) == (
        _Dim.QUERY,
        "SELECT a::int FROM t WHERE x = 'a:b'",
    )


def test_extract_user_email():
    assert_doctest(datahub.ingestion.source.usage.usage_common)


def test_usage_aggregator_passes_user_email_pattern():
    config = BaseUsageConfig(
        user_email_pattern=AllowDenyPattern(deny=["test_email@test.com"])
    )
    agg: UsageAggregator[str] = UsageAggregator(config)
    agg.aggregate_event(
        resource="test_db.test_schema.test_table",
        start_time=datetime(2020, 1, 1),
        query="select * from test",
        user="test_email@test.com",
        fields=[],
    )
    wus = list(agg.generate_workunits(_urn))
    assert len(wus) == 1
    assert wus[0].metadata.aspect.totalSqlQueries == 0  # type: ignore[union-attr]
    assert wus[0].metadata.aspect.userCounts == []  # type: ignore[union-attr]
    agg.close()


def _urn(resource: object) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:test,{resource},PROD)"


def _normalize(wu: MetadataWorkUnit) -> tuple:
    assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
    a = wu.metadata.aspect
    assert isinstance(a, DatasetUsageStatisticsClass)
    return (
        wu.get_urn(),
        a.timestampMillis,
        a.totalSqlQueries,
        a.uniqueUserCount,
        sorted((u.user, u.count) for u in (a.userCounts or [])),
        sorted((f.fieldPath, f.count) for f in (a.fieldCounts or [])),
        frozenset(a.topSqlQueries or []),
    )


def _reference_workunits(events: list, config: BaseUsageConfig) -> list:
    agg: dict = defaultdict(dict)
    for resource, start_time, query, user, fields, count in events:
        ts = get_time_bucket(start_time, config.bucket_duration)
        agg[ts].setdefault(
            resource,
            GenericAggregatedDataset(bucket_start_time=ts, resource=resource),
        ).add_read_entry(
            user,
            query,
            fields,
            user_email_pattern=config.user_email_pattern,
            count=count,
        )
    out = []
    for bucket in agg.values():
        for a in bucket.values():
            out.append(
                a.make_usage_workunit(
                    bucket_duration=config.bucket_duration,
                    resource_urn_builder=_urn,
                    top_n_queries=config.top_n_queries,
                    format_sql_queries=config.format_sql_queries,
                    include_top_n_queries=config.include_top_n_queries,
                    queries_character_limit=config.queries_character_limit,
                )
            )
    return out


def _feed(agg: UsageAggregator, events: list) -> None:
    for resource, start_time, query, user, fields, count in events:
        agg.aggregate_event(
            resource=resource,
            start_time=start_time,
            query=query,
            user=user,
            fields=fields,
            count=count,
        )


def _varied_events() -> list:
    day1 = datetime(2020, 1, 1)
    day2 = datetime(2020, 1, 2)
    return [
        ("db.t1", day1, "select * from t1", "a@x.com", [], 1),
        ("db.t1", day1, "select * from t1", "a@x.com", [], 2),
        ("db.t1", day1, "select count(*) from t1", "b@x.com", ["c1", "c2"], 1),
        ("db.t2", day1, "select * from t2", "b@x.com", ["c1"], 1),
        ("db.t1", day2, "select * from t1", "a@x.com", [], 1),
        ("db.t3", day1, None, None, [], 1),
    ]


def test_usage_aggregator_parity_with_reference():
    config = BaseUsageConfig()
    events = _varied_events()
    agg: UsageAggregator[str] = UsageAggregator(config)
    _feed(agg, events)
    actual = sorted(_normalize(wu) for wu in agg.generate_workunits(_urn))
    agg.close()
    expected = sorted(_normalize(wu) for wu in _reference_workunits(events, config))
    assert actual == expected


@pytest.mark.parametrize("use_in_memory", [False, True])
def test_usage_aggregator_parity_across_backends(use_in_memory):
    config = BaseUsageConfig()
    events = _varied_events()
    counter = InMemoryGroupedCounter() if use_in_memory else None
    agg: UsageAggregator[str] = UsageAggregator(config, counter=counter)
    _feed(agg, events)
    actual = sorted(_normalize(wu) for wu in agg.generate_workunits(_urn))
    agg.close()
    expected = sorted(_normalize(wu) for wu in _reference_workunits(events, config))
    assert actual == expected


@pytest.mark.parametrize("batch_size", [1, 3, 50000])
def test_usage_aggregator_parity_across_flush_sizes(batch_size: int) -> None:
    # A small batch_size forces many flushes, exercising cross-flush count
    # accumulation (ON CONFLICT upsert) and the rowid tie-break — the core of the
    # incremental-counting design — which a single-flush run would not cover.
    config = BaseUsageConfig()
    events = _varied_events()
    agg: UsageAggregator[str] = UsageAggregator(config, batch_size=batch_size)
    _feed(agg, events)
    actual = sorted(_normalize(wu) for wu in agg.generate_workunits(_urn))
    agg.close()
    expected = sorted(_normalize(wu) for wu in _reference_workunits(events, config))
    assert actual == expected


def test_usage_aggregator_parity_with_denied_user():
    config = BaseUsageConfig(user_email_pattern=AllowDenyPattern(deny=["denied@x.com"]))
    day1 = datetime(2020, 1, 1)
    events: list = [
        ("db.t1", day1, "select 1", "denied@x.com", [], 1),
        ("db.t1", day1, "select 1", "ok@x.com", [], 1),
    ]
    agg: UsageAggregator[str] = UsageAggregator(config)
    _feed(agg, events)
    actual = sorted(_normalize(wu) for wu in agg.generate_workunits(_urn))
    agg.close()
    expected = sorted(_normalize(wu) for wu in _reference_workunits(events, config))
    assert actual == expected


def test_usage_aggregator_top_n_tie_break_matches_reference():
    # Tied query counts at the top-N cutoff: the SQL path must break ties the same way
    # as Counter.most_common (insertion order) so output is deterministic and matches.
    config = BaseUsageConfig(top_n_queries=2)
    day1 = datetime(2020, 1, 1)
    # Three distinct queries, each seen once (all tied at count 1); top_n=2.
    events: list = [
        ("db.t", day1, "q_first", "a@x.com", [], 1),
        ("db.t", day1, "q_second", "a@x.com", [], 1),
        ("db.t", day1, "q_third", "a@x.com", [], 1),
    ]
    agg: UsageAggregator[str] = UsageAggregator(config)
    _feed(agg, events)
    actual = list(agg.generate_workunits(_urn))
    agg.close()
    expected = _reference_workunits(events, config)
    assert len(actual) == 1 and len(expected) == 1
    assert (
        actual[0].metadata.aspect.topSqlQueries  # type: ignore[union-attr]
        == expected[0].metadata.aspect.topSqlQueries  # type: ignore[union-attr]
    )


def test_usage_aggregator_merges_resources_with_equal_str():
    config = BaseUsageConfig()
    agg: UsageAggregator[_MergeRes] = UsageAggregator(config)
    day1 = datetime(2020, 1, 1)
    for res in (_MergeRes("db.t", 1), _MergeRes("db.t", 2)):
        agg.aggregate_event(
            resource=res, start_time=day1, query="select 1", user="a@x.com", fields=[]
        )
    wus = list(agg.generate_workunits(resource_urn_builder=lambda r: _urn(r)))
    assert len(wus) == 1
    assert wus[0].metadata.aspect.totalSqlQueries == 2  # type: ignore[union-attr]
    agg.close()


def test_usage_aggregator_non_str_resource_roundtrip():
    config = BaseUsageConfig()
    agg: UsageAggregator[_RoundtripRes] = UsageAggregator(config)
    day1 = datetime(2020, 1, 1)
    agg.aggregate_event(
        resource=_RoundtripRes("db", "t1"),
        start_time=day1,
        query="select 1",
        user="a@x.com",
        fields=[],
    )
    wus = list(
        agg.generate_workunits(
            resource_urn_builder=lambda r: f"urn:li:dataset:(urn:li:dataPlatform:test,{r.db}.{r.table},PROD)"
        )
    )
    assert len(wus) == 1
    assert "db.t1" in wus[0].get_urn()
    agg.close()


def test_usage_aggregator_close_is_idempotent():
    agg: UsageAggregator[str] = UsageAggregator(BaseUsageConfig())
    agg.aggregate_event(
        resource="db.t",
        start_time=datetime(2020, 1, 1),
        query="select 1",
        user="a@x.com",
        fields=[],
    )
    agg.close()
    agg.close()


def test_usage_aggregator_shared_connection_not_closed_on_close():
    conn = ConnectionWrapper()
    try:
        agg: UsageAggregator[str] = UsageAggregator(
            BaseUsageConfig(), shared_connection=conn
        )
        agg.aggregate_event(
            resource="db.t",
            start_time=datetime(2020, 1, 1),
            query="select 1",
            user="a@x.com",
            fields=[],
        )
        wus = list(agg.generate_workunits(_urn))
        assert len(wus) == 1
        agg.close()
        conn.execute("SELECT 1").fetchone()
    finally:
        conn.close()


def test_usage_aggregator_parity_with_colons_in_keys():
    config = BaseUsageConfig()
    day1 = datetime(2020, 1, 1)
    events = [
        (
            "db:weird.tbl",
            day1,
            "SELECT a::int FROM t WHERE x = 'a:b'",
            "u:1@x.com",
            ["col:1"],
            1,
        ),
        (
            "db:weird.tbl",
            day1,
            "SELECT a::int FROM t WHERE x = 'a:b'",
            "u:1@x.com",
            ["col:1"],
            1,
        ),
    ]
    agg: UsageAggregator[str] = UsageAggregator(config)
    _feed(agg, events)
    actual = sorted(_normalize(wu) for wu in agg.generate_workunits(_urn))
    agg.close()
    expected = sorted(_normalize(wu) for wu in _reference_workunits(events, config))
    assert actual == expected
