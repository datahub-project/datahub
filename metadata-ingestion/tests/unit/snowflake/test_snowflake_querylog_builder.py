import datetime

import pytest
import sqlglot
from sqlglot.dialects.snowflake import Snowflake

from datahub.configuration.time_window_config import BucketDuration
from datahub.ingestion.source.snowflake.snowflake_config import QueryDedupStrategyType
from datahub.ingestion.source.snowflake.snowflake_queries import QueryLogQueryBuilder


def test_non_implemented_strategy():
    with pytest.raises(NotImplementedError):
        QueryLogQueryBuilder(
            start_time=datetime.datetime(year=2021, month=1, day=1),
            end_time=datetime.datetime(year=2021, month=1, day=1),
            bucket_duration=BucketDuration.HOUR,
            deny_usernames=None,
            dedup_strategy="DUMMY",  # type: ignore[arg-type]
        ).build_enriched_query_log_query()


def test_fetch_query_for_all_strategies():
    for strategy in QueryDedupStrategyType:
        query = QueryLogQueryBuilder(
            start_time=datetime.datetime(year=2021, month=1, day=1),
            end_time=datetime.datetime(year=2021, month=1, day=1),
            bucket_duration=BucketDuration.HOUR,
            deny_usernames=None,
            dedup_strategy=strategy,
        ).build_enriched_query_log_query()
        # SQL parsing should succeed
        sqlglot.parse(query, dialect=Snowflake)
