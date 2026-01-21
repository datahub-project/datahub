import functools
import logging
import os
import pathlib
import random
import time
import types
from datetime import datetime, timedelta, timezone
from typing import List, NoReturn, Optional, Tuple
from unittest.mock import MagicMock, patch

import pytest
from freezegun import freeze_time

from datahub.configuration.datetimes import parse_user_datetime
from datahub.configuration.time_window_config import BucketDuration, get_time_bucket
from datahub.ingestion.sink.file import write_metadata_file
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.metadata.urns import CorpUserUrn, DatasetUrn
from datahub.sql_parsing.sql_parsing_aggregator import (
    KnownQueryLineageInfo,
    ObservedQuery,
    PreparsedQuery,
    QueryLogSetting,
    SqlParsingAggregator,
    TableRename,
    TableSwap,
)
from datahub.sql_parsing.sql_parsing_common import QueryType
from datahub.sql_parsing.sqlglot_lineage import (
    ColumnLineageInfo,
    ColumnRef,
    DownstreamColumnRef,
    SqlParsingResult,
)
from datahub.testing import mce_helpers
from datahub.utilities.cooperative_timeout import CooperativeTimeoutError
from tests.test_helpers.click_helpers import run_datahub_cmd

RESOURCE_DIR = pathlib.Path(__file__).parent / "aggregator_goldens"
FROZEN_TIME = "2024-02-06T01:23:45Z"

check_goldens_stream = functools.partial(
    mce_helpers.check_goldens_stream, ignore_order=False
)


def _ts(ts: int) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def make_basic_aggregator(store: bool = False) -> SqlParsingAggregator:
    aggregator = SqlParsingAggregator(
        platform="redshift",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
        query_log=QueryLogSetting.STORE_ALL if store else QueryLogSetting.DISABLED,
    )

    aggregator.add_observed_query(
        ObservedQuery(
            query="create table foo as select a, b from bar",
            default_db="dev",
            default_schema="public",
        )
    )

    return aggregator


@freeze_time(FROZEN_TIME)
def test_basic_lineage(pytestconfig: pytest.Config, tmp_path: pathlib.Path) -> None:
    aggregator = make_basic_aggregator()
    mcps = list(aggregator.gen_metadata())

    check_goldens_stream(
        outputs=mcps,
        golden_path=RESOURCE_DIR / "test_basic_lineage.json",
    )


@freeze_time(FROZEN_TIME)
def test_aggregator_dump(pytestconfig: pytest.Config, tmp_path: pathlib.Path) -> None:
    # Validates the query log storage + extraction functionality.
    aggregator = make_basic_aggregator(store=True)
    aggregator.close()

    query_log_db = aggregator.report.query_log_path
    assert query_log_db is not None

    run_datahub_cmd(["check", "extract-sql-agg-log", query_log_db])

    output_json_dir = pathlib.Path(query_log_db).with_suffix("")
    assert (
        len(list(output_json_dir.glob("*.json"))) > 5
    )  # 5 is arbitrary, but should have at least a couple tables
    query_log_json = output_json_dir / "stored_queries.json"
    mce_helpers.check_golden_file(
        pytestconfig, query_log_json, RESOURCE_DIR / "test_basic_lineage_query_log.json"
    )


@freeze_time(FROZEN_TIME)
def test_overlapping_inserts() -> None:
    aggregator = SqlParsingAggregator(
        platform="redshift",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
    )

    aggregator.add_observed_query(
        ObservedQuery(
            query="insert into downstream (a, b) select a, b from upstream1",
            default_db="dev",
            default_schema="public",
            timestamp=_ts(20),
        )
    )
    aggregator.add_observed_query(
        ObservedQuery(
            query="insert into downstream (a, c) select a, c from upstream2",
            default_db="dev",
            default_schema="public",
            timestamp=_ts(25),
        )
    )

    mcps = list(aggregator.gen_metadata())

    check_goldens_stream(
        outputs=mcps,
        golden_path=RESOURCE_DIR / "test_overlapping_inserts.json",
    )


@freeze_time(FROZEN_TIME)
def test_temp_table() -> None:
    aggregator = SqlParsingAggregator(
        platform="redshift",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
    )

    aggregator._schema_resolver.add_raw_schema_info(
        DatasetUrn("redshift", "dev.public.bar").urn(),
        {"a": "int", "b": "int", "c": "int"},
    )

    aggregator.add_observed_query(
        ObservedQuery(
            query="create table foo as select a, 2*b as b from bar",
            default_db="dev",
            default_schema="public",
            session_id="session1",
        )
    )
    aggregator.add_observed_query(
        ObservedQuery(
            query="create temp table foo as select a, b+c as c from bar",
            default_db="dev",
            default_schema="public",
            session_id="session2",
        )
    )
    aggregator.add_observed_query(
        ObservedQuery(
            query="create table foo_session2 as select * from foo",
            default_db="dev",
            default_schema="public",
            session_id="session2",
        )
    )
    aggregator.add_observed_query(
        ObservedQuery(
            query="create table foo_session3 as select * from foo",
            default_db="dev",
            default_schema="public",
            session_id="session3",
        )
    )

    # foo_session2 should come from bar (via temp table foo), have columns a and c, and depend on bar.{a,b,c}
    # foo_session3 should come from foo, have columns a and b, and depend on bar.b

    mcps = list(aggregator.gen_metadata())

    check_goldens_stream(
        outputs=mcps,
        golden_path=RESOURCE_DIR / "test_temp_table.json",
    )


@freeze_time(FROZEN_TIME)
def test_multistep_temp_table() -> None:
    aggregator = SqlParsingAggregator(
        platform="redshift",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
    )

    aggregator.add_observed_query(
        ObservedQuery(
            query="create table #temp1 as select a, 2*b as b from upstream1",
            default_db="dev",
            default_schema="public",
            session_id="session1",
        )
    )
    aggregator.add_observed_query(
        ObservedQuery(
            query="create table #temp2 as select b, c from upstream2",
            default_db="dev",
            default_schema="public",
            session_id="session1",
        )
    )
    aggregator.add_observed_query(
        ObservedQuery(
            query="create temp table staging_foo as select up1.a, up1.b, up2.c from #temp1 up1 left join #temp2 up2 on up1.b = up2.b where up1.b > 0",
            default_db="dev",
            default_schema="public",
            session_id="session1",
        )
    )
    aggregator.add_observed_query(
        ObservedQuery(
            query="insert into table prod_foo\nselect * from staging_foo",
            default_db="dev",
            default_schema="public",
            session_id="session1",
        )
    )

    mcps = list(aggregator.gen_metadata())

    # Extra check to make sure that the report is populated correctly.
    report = aggregator.report
    assert len(report.queries_with_temp_upstreams) == 1
    assert (
        len(
            report.queries_with_temp_upstreams[
                "composite_48c238412066895ccad5d27f9425ce969b2c0633203627eb476d0c9e5357825a"
            ]
        )
        == 4
    )
    check_goldens_stream(
        outputs=mcps,
        golden_path=RESOURCE_DIR / "test_multistep_temp_table.json",
    )


@freeze_time(FROZEN_TIME)
def test_overlapping_inserts_from_temp_tables() -> None:
    aggregator = SqlParsingAggregator(
        platform="redshift",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
    )
    report = aggregator.report

    # The "all_returns" table is populated from "#stage_in_person_returns" and "#stage_online_returns".
    # #stage_in_person_returns is populated from "in_person_returns" and "customer".
    # #stage_online_returns is populated from "online_returns", "customer", and "online_survey".

    aggregator.add_observed_query(
        ObservedQuery(
            query="create table #stage_in_person_returns as select ipr.customer_id, customer.customer_email, ipr.return_date "
            "from in_person_returns ipr "
            "left join customer on in_person_returns.customer_id = customer.customer_id",
            default_db="dev",
            default_schema="public",
            session_id="1234",
        )
    )

    aggregator.add_observed_query(
        ObservedQuery(
            query="create table #stage_online_returns as select online_ret.customer_id, customer.customer_email, online_ret.return_date, online_survey.return_reason "
            "from online_returns online_ret "
            "left join customer on online_ret.customer_id = customer.customer_id "
            "left join online_survey on online_ret.customer_id = online_survey.customer_id and online_ret.return_id = online_survey.event_id",
            default_db="dev",
            default_schema="public",
            session_id="2323",
        )
    )

    aggregator.add_observed_query(
        ObservedQuery(
            query="insert into all_returns (customer_id, customer_email, return_date) select customer_id, customer_email, return_date from #stage_in_person_returns",
            default_db="dev",
            default_schema="public",
            session_id="1234",
        )
    )

    aggregator.add_observed_query(
        ObservedQuery(
            query="insert into all_returns (customer_id, customer_email, return_date, return_reason) select customer_id, customer_email, return_date, return_reason from #stage_online_returns",
            default_db="dev",
            default_schema="public",
            session_id="2323",
        )
    )

    # We only have one create temp table, but the same insert command from multiple sessions.
    # This should get ignored.
    assert len(report.queries_with_non_authoritative_session) == 0
    aggregator.add_observed_query(
        ObservedQuery(
            query="insert into all_returns (customer_id, customer_email, return_date, return_reason) select customer_id, customer_email, return_date, return_reason from #stage_online_returns",
            default_db="dev",
            default_schema="public",
            session_id="5435",
        )
    )
    assert len(report.queries_with_non_authoritative_session) == 1

    mcps = list(aggregator.gen_metadata())
    check_goldens_stream(
        outputs=mcps,
        golden_path=RESOURCE_DIR / "test_overlapping_inserts_from_temp_tables.json",
    )


@freeze_time(FROZEN_TIME)
def test_aggregate_operations() -> None:
    aggregator = SqlParsingAggregator(
        platform="redshift",
        generate_lineage=False,
        generate_queries=True,
        generate_usage_statistics=False,
        generate_operations=True,
    )

    aggregator.add_observed_query(
        ObservedQuery(
            query="create table foo as select a, b from bar",
            default_db="dev",
            default_schema="public",
            timestamp=_ts(20),
            user=CorpUserUrn("user1"),
        )
    )
    aggregator.add_observed_query(
        ObservedQuery(
            query="create table foo as select a, b from bar",
            default_db="dev",
            default_schema="public",
            timestamp=_ts(25),
            user=CorpUserUrn("user2"),
        )
    )
    aggregator.add_observed_query(
        ObservedQuery(
            query="create table foo as select a, b+1 as b from bar",
            default_db="dev",
            default_schema="public",
            timestamp=_ts(26),
            user=CorpUserUrn("user3"),
        )
    )

    # The first query will basically be ignored, as it's a duplicate of the second one.

    mcps = list(aggregator.gen_metadata())

    check_goldens_stream(
        outputs=mcps,
        golden_path=RESOURCE_DIR / "test_aggregate_operations.json",
    )


@freeze_time(FROZEN_TIME)
def test_view_lineage() -> None:
    aggregator = SqlParsingAggregator(
        platform="redshift",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
        query_log=QueryLogSetting.STORE_ALL,
    )

    aggregator.add_view_definition(
        view_urn=DatasetUrn("redshift", "dev.public.foo"),
        view_definition="create view foo as select a, b from bar",
        default_db="dev",
        default_schema="public",
    )

    aggregator._schema_resolver.add_raw_schema_info(
        urn=DatasetUrn("redshift", "dev.public.foo").urn(),
        schema_info={"a": "int", "b": "int"},
    )
    aggregator._schema_resolver.add_raw_schema_info(
        urn=DatasetUrn("redshift", "dev.public.bar").urn(),
        schema_info={"a": "int", "b": "int"},
    )

    # Because we have schema information, despite it being registered after the view definition,
    # the confidence score should be high.

    mcps = list(aggregator.gen_metadata())

    check_goldens_stream(
        outputs=mcps,
        golden_path=RESOURCE_DIR / "test_view_lineage.json",
    )


@freeze_time(FROZEN_TIME)
def test_known_lineage_mapping() -> None:
    aggregator = SqlParsingAggregator(
        platform="redshift",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
    )

    aggregator.add_known_lineage_mapping(
        upstream_urn=DatasetUrn("redshift", "dev.public.bar").urn(),
        downstream_urn=DatasetUrn("redshift", "dev.public.foo").urn(),
    )
    aggregator.add_known_lineage_mapping(
        upstream_urn=DatasetUrn("s3", "bucket1/key1").urn(),
        downstream_urn=DatasetUrn("redshift", "dev.public.bar").urn(),
    )
    aggregator.add_known_lineage_mapping(
        upstream_urn=DatasetUrn("redshift", "dev.public.foo").urn(),
        downstream_urn=DatasetUrn("s3", "bucket2/key2").urn(),
    )

    mcps = list(aggregator.gen_metadata())

    check_goldens_stream(
        outputs=mcps,
        golden_path=RESOURCE_DIR / "test_known_lineage_mapping.json",
    )


@freeze_time(FROZEN_TIME)
def test_column_lineage_deduplication() -> None:
    aggregator = SqlParsingAggregator(
        platform="redshift",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
    )

    aggregator.add_observed_query(
        ObservedQuery(
            query="/* query 1 */ insert into foo (a, b, c) select a, b, c from bar",
            default_db="dev",
            default_schema="public",
        )
    )
    aggregator.add_observed_query(
        ObservedQuery(
            query="/* query 2 */ insert into foo (a, b) select a, b from bar",
            default_db="dev",
            default_schema="public",
        )
    )

    mcps = list(aggregator.gen_metadata())

    # In this case, the lineage for a and b is attributed to query 2, and
    # the lineage for c is attributed to query 1. Note that query 1 does
    # not get any credit for a and b, as they are already covered by query 2,
    # which came later and hence has higher precedence.

    check_goldens_stream(
        outputs=mcps,
        golden_path=RESOURCE_DIR / "test_column_lineage_deduplication.json",
    )


@freeze_time(FROZEN_TIME)
def test_add_known_query_lineage() -> None:
    aggregator = SqlParsingAggregator(
        platform="redshift",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=True,
    )

    downstream_urn = DatasetUrn("redshift", "dev.public.foo").urn()
    upstream_urn = DatasetUrn("redshift", "dev.public.bar").urn()

    known_query_lineage = KnownQueryLineageInfo(
        query_text="insert into foo (a, b, c) select a, b, c from bar",
        downstream=downstream_urn,
        upstreams=[upstream_urn],
        column_lineage=[
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(table=downstream_urn, column="a"),
                upstreams=[ColumnRef(table=upstream_urn, column="a")],
            ),
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(table=downstream_urn, column="b"),
                upstreams=[ColumnRef(table=upstream_urn, column="b")],
            ),
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(table=downstream_urn, column="c"),
                upstreams=[ColumnRef(table=upstream_urn, column="c")],
            ),
        ],
        timestamp=_ts(20),
        query_type=QueryType.INSERT,
    )

    aggregator.add_known_query_lineage(known_query_lineage)

    mcps = list(aggregator.gen_metadata())

    check_goldens_stream(
        outputs=mcps,
        golden_path=RESOURCE_DIR / "test_add_known_query_lineage.json",
    )


@freeze_time(FROZEN_TIME)
def test_table_rename() -> None:
    aggregator = SqlParsingAggregator(
        platform="redshift",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
    )

    aggregator._schema_resolver.add_raw_schema_info(
        DatasetUrn("redshift", "dev.public.foo").urn(),
        {"a": "int", "b": "int", "c": "int"},
    )

    # Register that foo_staging is renamed to foo.
    aggregator.add_table_rename(
        TableRename(
            original_urn=DatasetUrn("redshift", "dev.public.foo_staging").urn(),
            new_urn=DatasetUrn("redshift", "dev.public.foo").urn(),
        )
    )

    # Add an unrelated query.
    aggregator.add_observed_query(
        ObservedQuery(
            query="create table bar as select a, b from baz",
            default_db="dev",
            default_schema="public",
        )
    )

    # Add the query that created the staging table.
    aggregator.add_observed_query(
        ObservedQuery(
            query="create table foo_staging as select a, b from foo_dep",
            default_db="dev",
            default_schema="public",
        )
    )

    # Add the query that created the downstream from foo_staging table.
    aggregator.add_observed_query(
        ObservedQuery(
            query="create table foo_downstream as select a, b from foo_staging",
            default_db="dev",
            default_schema="public",
        )
    )

    mcps = list(aggregator.gen_metadata())

    check_goldens_stream(
        outputs=mcps,
        golden_path=RESOURCE_DIR / "test_table_rename.json",
    )


@freeze_time(FROZEN_TIME)
def test_table_rename_with_temp() -> None:
    aggregator = SqlParsingAggregator(
        platform="redshift",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
        is_temp_table=lambda x: "staging" in x.lower(),
    )

    aggregator._schema_resolver.add_raw_schema_info(
        DatasetUrn("redshift", "dev.public.foo").urn(),
        {"a": "int", "b": "int", "c": "int"},
    )

    # Register that foo_staging is renamed to foo.
    aggregator.add_table_rename(
        TableRename(
            original_urn=DatasetUrn("redshift", "dev.public.foo_staging").urn(),
            new_urn=DatasetUrn("redshift", "dev.public.foo").urn(),
            query="alter table dev.public.foo_staging rename to dev.public.foo",
        )
    )

    # Add an unrelated query.
    aggregator.add_observed_query(
        ObservedQuery(
            query="create table bar as select a, b from baz",
            default_db="dev",
            default_schema="public",
        )
    )

    # Add the query that created the staging table.
    aggregator.add_observed_query(
        ObservedQuery(
            query="create table foo_staging as select a, b from foo_dep",
            default_db="dev",
            default_schema="public",
        )
    )

    # Add the query that created the downstream from foo_staging table.
    aggregator.add_observed_query(
        ObservedQuery(
            query="create table foo_downstream as select a, b from foo_staging",
            default_db="dev",
            default_schema="public",
        )
    )

    mcps = list(aggregator.gen_metadata())

    check_goldens_stream(
        outputs=mcps,
        golden_path=RESOURCE_DIR / "test_table_rename_with_temp.json",
    )


@freeze_time(FROZEN_TIME)
def test_table_swap() -> None:
    aggregator = SqlParsingAggregator(
        platform="snowflake",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
    )

    aggregator._schema_resolver.add_raw_schema_info(
        DatasetUrn("snowflake", "dev.public.person_info").urn(),
        {"a": "int", "b": "int", "c": "int"},
    )

    # Add an unrelated query.
    aggregator.add_observed_query(
        ObservedQuery(
            query="create table bar as select a, b from baz",
            default_db="dev",
            default_schema="public",
        )
    )

    # Add the query that created the swap table initially.
    aggregator.add_preparsed_query(
        PreparsedQuery(
            query_id=None,
            query_text="CREATE TABLE person_info_swap CLONE person_info;",
            upstreams=[DatasetUrn("snowflake", "dev.public.person_info").urn()],
            downstream=DatasetUrn("snowflake", "dev.public.person_info_swap").urn(),
        )
    )

    # Add the query that created the incremental table.
    aggregator.add_preparsed_query(
        PreparsedQuery(
            query_id=None,
            query_text="CREATE TABLE person_info_incremental AS SELECT * from person_info_dep;",
            upstreams=[
                DatasetUrn("snowflake", "dev.public.person_info_dep").urn(),
            ],
            downstream=DatasetUrn(
                "snowflake", "dev.public.person_info_incremental"
            ).urn(),
        )
    )

    # Add the query that updated the swap table.
    aggregator.add_preparsed_query(
        PreparsedQuery(
            query_id=None,
            query_text="INSERT INTO person_info_swap SELECT * from person_info_incremental;",
            upstreams=[
                DatasetUrn("snowflake", "dev.public.person_info_incremental").urn(),
            ],
            downstream=DatasetUrn("snowflake", "dev.public.person_info_swap").urn(),
        )
    )

    aggregator.add_table_swap(
        TableSwap(
            urn1=DatasetUrn("snowflake", "dev.public.person_info").urn(),
            urn2=DatasetUrn("snowflake", "dev.public.person_info_swap").urn(),
        )
    )

    # Add the query that is created from swap table.
    aggregator.add_preparsed_query(
        PreparsedQuery(
            query_id=None,
            query_text="create table person_info_backup as select * from person_info_swap",
            upstreams=[
                DatasetUrn("snowflake", "dev.public.person_info_swap").urn(),
            ],
            downstream=DatasetUrn("snowflake", "dev.public.person_info_backup").urn(),
        )
    )

    mcps = list(aggregator.gen_metadata())

    check_goldens_stream(
        outputs=mcps,
        golden_path=RESOURCE_DIR / "test_table_swap.json",
    )


@freeze_time(FROZEN_TIME)
def test_table_swap_with_temp() -> None:
    aggregator = SqlParsingAggregator(
        platform="snowflake",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
        is_temp_table=lambda x: "swap" in x.lower() or "incremental" in x.lower(),
    )

    aggregator._schema_resolver.add_raw_schema_info(
        DatasetUrn("snowflake", "dev.public.person_info").urn(),
        {"a": "int", "b": "int", "c": "int"},
    )

    # Add an unrelated query.
    aggregator.add_observed_query(
        ObservedQuery(
            query="create table bar as select a, b from baz",
            default_db="dev",
            default_schema="public",
        )
    )

    # Add the query that created the swap table initially.
    aggregator.add_preparsed_query(
        PreparsedQuery(
            query_id=None,
            query_text="CREATE TABLE person_info_swap CLONE person_info;",
            upstreams=[DatasetUrn("snowflake", "dev.public.person_info").urn()],
            downstream=DatasetUrn("snowflake", "dev.public.person_info_swap").urn(),
            session_id="xxx",
            timestamp=_ts(10),
            column_lineage=[
                ColumnLineageInfo(
                    downstream=DownstreamColumnRef(
                        table=DatasetUrn(
                            "snowflake", "dev.public.person_info_swap"
                        ).urn(),
                        column="a",
                    ),
                    upstreams=[
                        ColumnRef(
                            table=DatasetUrn(
                                "snowflake", "dev.public.person_info"
                            ).urn(),
                            column="a",
                        )
                    ],
                )
            ],
        )
    )

    # Add the query that created the incremental table.
    aggregator.add_preparsed_query(
        PreparsedQuery(
            query_id=None,
            query_text="CREATE TABLE person_info_incremental AS SELECT * from person_info_dep;",
            upstreams=[
                DatasetUrn("snowflake", "dev.public.person_info_dep").urn(),
            ],
            downstream=DatasetUrn(
                "snowflake", "dev.public.person_info_incremental"
            ).urn(),
            session_id="xxx",
            timestamp=_ts(20),
            column_lineage=[
                ColumnLineageInfo(
                    downstream=DownstreamColumnRef(
                        table=DatasetUrn(
                            "snowflake", "dev.public.person_info_incremental"
                        ).urn(),
                        column="a",
                    ),
                    upstreams=[
                        ColumnRef(
                            table=DatasetUrn(
                                "snowflake", "dev.public.person_info_dep"
                            ).urn(),
                            column="a",
                        )
                    ],
                )
            ],
        )
    )

    # Add the query that updated the swap table.
    aggregator.add_preparsed_query(
        PreparsedQuery(
            query_id=None,
            query_text="INSERT INTO person_info_swap SELECT * from person_info_incremental;",
            upstreams=[
                DatasetUrn("snowflake", "dev.public.person_info_incremental").urn(),
            ],
            downstream=DatasetUrn("snowflake", "dev.public.person_info_swap").urn(),
            session_id="xxx",
            timestamp=_ts(30),
            column_lineage=[
                ColumnLineageInfo(
                    downstream=DownstreamColumnRef(
                        table=DatasetUrn(
                            "snowflake", "dev.public.person_info_swap"
                        ).urn(),
                        column="a",
                    ),
                    upstreams=[
                        ColumnRef(
                            table=DatasetUrn(
                                "snowflake", "dev.public.person_info_incremental"
                            ).urn(),
                            column="a",
                        )
                    ],
                )
            ],
        )
    )

    aggregator.add_table_swap(
        TableSwap(
            urn1=DatasetUrn("snowflake", "dev.public.person_info").urn(),
            urn2=DatasetUrn("snowflake", "dev.public.person_info_swap").urn(),
            session_id="xxx",
            timestamp=_ts(40),
        )
    )

    # Add the query that is created from swap table.
    aggregator.add_preparsed_query(
        PreparsedQuery(
            query_id=None,
            query_text="create table person_info_backup as select * from person_info_swap",
            upstreams=[
                DatasetUrn("snowflake", "dev.public.person_info_swap").urn(),
            ],
            downstream=DatasetUrn("snowflake", "dev.public.person_info_backup").urn(),
            session_id="xxx",
            timestamp=_ts(50),
            column_lineage=[
                ColumnLineageInfo(
                    downstream=DownstreamColumnRef(
                        table=DatasetUrn(
                            "snowflake", "dev.public.person_info_backup"
                        ).urn(),
                        column="a",
                    ),
                    upstreams=[
                        ColumnRef(
                            table=DatasetUrn(
                                "snowflake", "dev.public.person_info_swap"
                            ).urn(),
                            column="a",
                        )
                    ],
                )
            ],
        )
    )

    mcps = list(aggregator.gen_metadata())

    check_goldens_stream(
        outputs=mcps,
        golden_path=RESOURCE_DIR / "test_table_swap_with_temp.json",
    )


@freeze_time(FROZEN_TIME)
def test_create_table_query_mcps() -> None:
    aggregator = SqlParsingAggregator(
        platform="bigquery",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=True,
    )

    aggregator.add_observed_query(
        ObservedQuery(
            query="create or replace table `dataset.foo` (date_utc timestamp, revenue int);",
            default_db="dev",
            default_schema="public",
            timestamp=datetime.now(),
        )
    )

    mcps = list(aggregator.gen_metadata())

    check_goldens_stream(
        outputs=mcps,
        golden_path=RESOURCE_DIR / "test_create_table_query_mcps.json",
    )


@freeze_time(FROZEN_TIME)
def test_table_lineage_via_temp_table_disordered_add() -> None:
    aggregator = SqlParsingAggregator(
        platform="redshift",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
    )

    aggregator.add_observed_query(
        ObservedQuery(
            query="create table derived_from_foo as select * from foo",
            default_db="dev",
            default_schema="public",
        )
    )
    aggregator.add_observed_query(
        ObservedQuery(
            query="create temp table foo as select a, b+c as c from bar",
            default_db="dev",
            default_schema="public",
        )
    )

    mcps = list(aggregator.gen_metadata())

    check_goldens_stream(
        outputs=mcps,
        golden_path=RESOURCE_DIR
        / "test_table_lineage_via_temp_table_disordered_add.json",
    )


@freeze_time(FROZEN_TIME)
def test_basic_usage() -> None:
    frozen_timestamp = parse_user_datetime(FROZEN_TIME)
    aggregator = SqlParsingAggregator(
        platform="redshift",
        generate_lineage=False,
        generate_usage_statistics=True,
        generate_operations=False,
        usage_config=BaseUsageConfig(
            start_time=get_time_bucket(frozen_timestamp, BucketDuration.DAY),
            end_time=frozen_timestamp,
        ),
    )

    aggregator._schema_resolver.add_raw_schema_info(
        DatasetUrn("redshift", "dev.public.foo").urn(),
        {"a": "int", "b": "int", "c": "int"},
    )

    aggregator.add_observed_query(
        ObservedQuery(
            query="select * from foo",
            default_db="dev",
            default_schema="public",
            usage_multiplier=5,
            timestamp=frozen_timestamp,
            user=CorpUserUrn("user1"),
        )
    )
    aggregator.add_observed_query(
        ObservedQuery(
            query="create table bar as select b+c as c from foo",
            default_db="dev",
            default_schema="public",
            timestamp=frozen_timestamp,
            user=CorpUserUrn("user2"),
        )
    )

    mcps = list(aggregator.gen_metadata())

    check_goldens_stream(
        outputs=mcps,
        golden_path=RESOURCE_DIR / "test_basic_usage.json",
    )


def test_table_swap_id() -> None:
    assert (
        TableSwap(
            urn1=DatasetUrn("snowflake", "dev.public.foo").urn(),
            urn2=DatasetUrn("snowflake", "dev.public.foo_staging").urn(),
        ).id()
        == TableSwap(
            urn1=DatasetUrn("snowflake", "dev.public.foo_staging").urn(),
            urn2=DatasetUrn("snowflake", "dev.public.foo").urn(),
        ).id()
    )


def test_sql_aggreator_close_cleans_tmp(tmp_path):
    frozen_timestamp = parse_user_datetime(FROZEN_TIME)
    with patch("tempfile.tempdir", str(tmp_path)):
        aggregator = SqlParsingAggregator(
            platform="redshift",
            generate_lineage=False,
            generate_usage_statistics=True,
            generate_operations=False,
            usage_config=BaseUsageConfig(
                start_time=get_time_bucket(frozen_timestamp, BucketDuration.DAY),
                end_time=frozen_timestamp,
            ),
            generate_queries=True,
            generate_query_usage_statistics=True,
        )
        assert len(os.listdir(tmp_path)) > 0
        aggregator.close()
        assert len(os.listdir(tmp_path)) == 0


@freeze_time(FROZEN_TIME)
def test_override_dialect_passed_to_sqlglot_lineage() -> None:
    """Test that override_dialect is correctly passed to sqlglot_lineage"""
    aggregator = SqlParsingAggregator(
        platform="redshift",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
    )
    base_query = ObservedQuery(
        query="create table foo as select a, b from bar",
        default_db="dev",
        default_schema="public",
    )

    with patch(
        "datahub.sql_parsing.sql_parsing_aggregator.sqlglot_lineage"
    ) as mock_sqlglot_lineage:
        mock_sqlglot_lineage.return_value = MagicMock()

        # Test with override_dialect set

        base_query.override_dialect = "snowflake"
        aggregator.add_observed_query(base_query)

        mock_sqlglot_lineage.assert_called_once()
        call_args = mock_sqlglot_lineage.call_args
        assert call_args.kwargs["override_dialect"] == "snowflake"

        # Reset mock
        mock_sqlglot_lineage.reset_mock()

        # Test without override_dialect (should be None)

        base_query.override_dialect = None
        aggregator.add_observed_query(base_query)

        mock_sqlglot_lineage.assert_called_once()
        call_args = mock_sqlglot_lineage.call_args
        assert call_args.kwargs["override_dialect"] is None


@freeze_time(FROZEN_TIME)
def test_diamond_problem(pytestconfig: pytest.Config, tmp_path: pathlib.Path) -> None:
    aggregator = SqlParsingAggregator(
        platform="snowflake",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
        is_temp_table=lambda x: x.lower()
        in [
            "dummy_test.diamond_problem.t1",
            "dummy_test.diamond_problem.t2",
            "dummy_test.diamond_problem.t3",
            "dummy_test.diamond_problem.t4",
        ],
    )

    aggregator._schema_resolver.add_raw_schema_info(
        DatasetUrn("snowflake", "dummy_test.diamond_problem.diamond_source1").urn(),
        {"col_a": "int", "col_b": "int", "col_c": "int"},
    )

    aggregator._schema_resolver.add_raw_schema_info(
        DatasetUrn(
            "snowflake",
            "dummy_test.diamond_problem.diamond_destination",
        ).urn(),
        {"col_a": "int", "col_b": "int", "col_c": "int"},
    )

    # Diamond query pattern: source1 -> t1 -> {t2, t3} -> t4 -> destination
    queries = [
        "CREATE TEMPORARY TABLE t1 as select * from diamond_source1;",
        "CREATE TEMPORARY TABLE t2 as select * from t1;",
        "CREATE TEMPORARY TABLE t3 as select * from t1;",
        "CREATE TEMPORARY TABLE t4 as select t2.col_a, t3.col_b, t2.col_c from t2 join t3 on t2.col_a = t3.col_a;",
        "CREATE TABLE diamond_destination as select * from t4;",
    ]

    base_timestamp = datetime(2025, 7, 1, 13, 52, 18, 741000, tzinfo=timezone.utc)

    for i, query in enumerate(queries):
        aggregator.add(
            ObservedQuery(
                query=query,
                default_db="dummy_test",
                default_schema="diamond_problem",
                session_id="14774700499701726",
                timestamp=base_timestamp + timedelta(seconds=i),
            )
        )

    mcpws = [mcp for mcp in aggregator.gen_metadata()]
    lineage_mcpws = [mcpw for mcpw in mcpws if mcpw.aspectName == "upstreamLineage"]
    out_path = tmp_path / "mcpw.json"
    write_metadata_file(out_path, lineage_mcpws)

    mce_helpers.check_golden_file(
        pytestconfig,
        out_path,
        pytestconfig.rootpath
        / "tests/unit/sql_parsing/aggregator_goldens/test_diamond_problem_golden.json",
    )


@freeze_time(FROZEN_TIME)
def test_empty_column_in_snowflake_lineage(
    pytestconfig: pytest.Config, tmp_path: pathlib.Path
) -> None:
    """Test that column lineage with empty string column names doesn't cause errors.

    Note: Uses KnownQueryLineageInfo instead of ObservedQuery since empty column names from
    external systems would require mocking _run_sql_parser().
    """
    aggregator = SqlParsingAggregator(
        platform="snowflake",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
    )

    downstream_urn = DatasetUrn("snowflake", "dev.public.target_table").urn()
    upstream_urn = DatasetUrn("snowflake", "dev.public.source_table").urn()

    known_query_lineage = KnownQueryLineageInfo(
        query_text="insert into target_table (col_a, col_b, col_c) select col_a, col_b, col_c from source_table",
        downstream=downstream_urn,
        upstreams=[upstream_urn],
        column_lineage=[
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(table=downstream_urn, column="col_a"),
                upstreams=[ColumnRef(table=upstream_urn, column="col_a")],
            ),
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(table=downstream_urn, column="col_b"),
                upstreams=[
                    ColumnRef(table=upstream_urn, column="col_b"),
                    ColumnRef(table=upstream_urn, column=""),
                ],
            ),
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(table=downstream_urn, column="col_c"),
                upstreams=[
                    ColumnRef(table=upstream_urn, column=""),
                ],
            ),
        ],
        timestamp=_ts(20),
        query_type=QueryType.INSERT,
    )

    aggregator.add_known_query_lineage(known_query_lineage)

    mcpws = [mcp for mcp in aggregator.gen_metadata()]
    lineage_mcpws = [mcpw for mcpw in mcpws if mcpw.aspectName == "upstreamLineage"]
    out_path = tmp_path / "mcpw.json"
    write_metadata_file(out_path, lineage_mcpws)

    mce_helpers.check_golden_file(
        pytestconfig,
        out_path,
        RESOURCE_DIR / "test_empty_column_in_snowflake_lineage_golden.json",
    )


@freeze_time(FROZEN_TIME)
def test_empty_downstream_column_in_snowflake_lineage(
    pytestconfig: pytest.Config, tmp_path: pathlib.Path
) -> None:
    """Test that column lineage with empty downstream column names doesn't cause errors.

    Note: Uses KnownQueryLineageInfo instead of ObservedQuery since empty column names from
    external systems would require mocking _run_sql_parser().
    """
    aggregator = SqlParsingAggregator(
        platform="snowflake",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
    )

    downstream_urn = DatasetUrn("snowflake", "dev.public.target_table").urn()
    upstream_urn = DatasetUrn("snowflake", "dev.public.source_table").urn()

    known_query_lineage = KnownQueryLineageInfo(
        query_text='create table target_table as select $1 as "", $2 as "   " from source_table',
        downstream=downstream_urn,
        upstreams=[upstream_urn],
        column_lineage=[
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(table=downstream_urn, column=""),
                upstreams=[ColumnRef(table=upstream_urn, column="col_a")],
            ),
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(table=downstream_urn, column="   "),
                upstreams=[ColumnRef(table=upstream_urn, column="col_b")],
            ),
        ],
        timestamp=_ts(20),
        query_type=QueryType.CREATE_TABLE_AS_SELECT,
    )

    aggregator.add_known_query_lineage(known_query_lineage)

    mcpws = [mcp for mcp in aggregator.gen_metadata()]
    lineage_mcpws = [mcpw for mcpw in mcpws if mcpw.aspectName == "upstreamLineage"]
    out_path = tmp_path / "mcpw.json"
    write_metadata_file(out_path, lineage_mcpws)

    mce_helpers.check_golden_file(
        pytestconfig,
        out_path,
        RESOURCE_DIR / "test_empty_downstream_column_in_snowflake_lineage_golden.json",
    )


@freeze_time(FROZEN_TIME)
def test_partial_empty_downstream_column_in_snowflake_lineage(
    pytestconfig: pytest.Config, tmp_path: pathlib.Path
) -> None:
    """Test that column lineage with mix of empty and valid downstream columns works correctly.

    Note: Uses KnownQueryLineageInfo instead of ObservedQuery since empty column names from
    external systems would require mocking _run_sql_parser().
    """
    aggregator = SqlParsingAggregator(
        platform="snowflake",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
    )

    downstream_urn = DatasetUrn("snowflake", "dev.public.empty_downstream").urn()
    upstream_urn = DatasetUrn("snowflake", "dev.public.empty_upstream").urn()

    known_query_lineage = KnownQueryLineageInfo(
        query_text='create table empty_downstream as select $1 as "", $2 as "TITLE_DOWNSTREAM" from empty_upstream',
        downstream=downstream_urn,
        upstreams=[upstream_urn],
        column_lineage=[
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(table=downstream_urn, column=""),
                upstreams=[ColumnRef(table=upstream_urn, column="name")],
            ),
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(
                    table=downstream_urn, column="TITLE_DOWNSTREAM"
                ),
                upstreams=[ColumnRef(table=upstream_urn, column="title")],
            ),
        ],
        timestamp=_ts(20),
        query_type=QueryType.CREATE_TABLE_AS_SELECT,
    )

    aggregator.add_known_query_lineage(known_query_lineage)

    mcpws = [mcp for mcp in aggregator.gen_metadata()]
    lineage_mcpws = [mcpw for mcpw in mcpws if mcpw.aspectName == "upstreamLineage"]
    out_path = tmp_path / "mcpw.json"
    write_metadata_file(out_path, lineage_mcpws)

    mce_helpers.check_golden_file(
        pytestconfig,
        out_path,
        RESOURCE_DIR
        / "test_partial_empty_downstream_column_in_snowflake_lineage_golden.json",
    )


@freeze_time(FROZEN_TIME)
def test_empty_column_in_query_subjects(
    pytestconfig: pytest.Config, tmp_path: pathlib.Path
) -> None:
    """Test that QuerySubjects with empty column names doesn't create invalid URNs.

    This simulates a scenario where Snowflake's access_history may contain empty
    column names, which should not result in invalid schemaField URNs.
    """
    aggregator = SqlParsingAggregator(
        platform="snowflake",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
        generate_queries=True,
        generate_query_subject_fields=True,
    )

    downstream_urn = DatasetUrn(
        "snowflake", "production.dca_core.snowplow_user_engagement_mart"
    ).urn()
    upstream_urn = DatasetUrn(
        "snowflake", "production.dca_core.snowplow_user_engagement_mart__dbt_tmp"
    ).urn()

    # Simulate a query where Snowflake's access_history contains empty column names.
    preparsed_query = PreparsedQuery(
        query_id="test-delete-query",
        query_text=(
            "delete from PRODUCTION.DCA_CORE.snowplow_user_engagement_mart "
            "as DBT_INTERNAL_DEST where (unique_key_input) in ("
            "select distinct unique_key_input from "
            "PRODUCTION.DCA_CORE.snowplow_user_engagement_mart__dbt_tmp "
            "as DBT_INTERNAL_SOURCE)"
        ),
        upstreams=[upstream_urn],
        downstream=downstream_urn,
        column_lineage=[
            # This simulates a case where an empty column name might be present in the audit log.
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(table=downstream_urn, column=""),
                upstreams=[ColumnRef(table=upstream_urn, column="unique_key_input")],
            ),
        ],
        column_usage={
            upstream_urn: {"unique_key_input", ""},  # Empty column from Snowflake
        },
        query_type=QueryType.DELETE,
        timestamp=_ts(20),
    )

    aggregator.add_preparsed_query(preparsed_query)

    mcpws = [mcp for mcp in aggregator.gen_metadata()]
    query_mcpws = [
        mcpw
        for mcpw in mcpws
        if mcpw.entityUrn and mcpw.entityUrn.startswith("urn:li:query:")
    ]

    out_path = tmp_path / "mcpw.json"
    write_metadata_file(out_path, query_mcpws)

    mce_helpers.check_golden_file(
        pytestconfig,
        out_path,
        RESOURCE_DIR / "test_empty_column_in_query_subjects_golden.json",
    )


@freeze_time(FROZEN_TIME)
def test_empty_column_in_query_subjects_only_column_usage(
    pytestconfig: pytest.Config, tmp_path: pathlib.Path
) -> None:
    """Test that QuerySubjects with empty columns ONLY in column_usage doesn't create invalid URNs.

    This simulates the exact customer scenario where:
    - Snowflake returns empty columns in direct_objects_accessed (column_usage)
    - But NO empty columns in objects_modified (column_lineage is empty or valid)

    This is the scenario that would send invalid URNs to GMS rather than crash in Python,
    matching the customer's error: "Provided urn urn:li:schemaField:(...,) is invalid"
    """
    aggregator = SqlParsingAggregator(
        platform="snowflake",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
        generate_queries=True,
        generate_query_subject_fields=True,
        generate_query_usage_statistics=True,
        usage_config=BaseUsageConfig(
            bucket_duration=BucketDuration.DAY,
            start_time=parse_user_datetime("2024-02-06T00:00:00Z"),
            end_time=parse_user_datetime("2024-02-07T00:00:00Z"),
        ),
    )

    # Simulate table name from user: production.dsd_digital_private.gsheets_legacy_views
    upstream_urn = DatasetUrn(
        "snowflake", "production.dsd_digital_private.gsheets_legacy_views"
    ).urn()

    # Simulate a SELECT query (no downstream) where the audit log contains empty column names.
    preparsed_query = PreparsedQuery(
        query_id="test-select-gsheets-view",
        query_text="SELECT * FROM production.dsd_digital_private.gsheets_legacy_views WHERE id = 123",
        upstreams=[upstream_urn],
        downstream=None,  # SELECT query has no downstream
        column_lineage=[],  # No column lineage because no downstream
        column_usage={
            # Simulate a case where an empty column name is present in the audit log.
            upstream_urn: {"id", "name", ""},  # Empty column from Snowflake!
        },
        query_type=QueryType.SELECT,
        timestamp=_ts(20),
    )

    aggregator.add_preparsed_query(preparsed_query)

    mcpws = [mcp for mcp in aggregator.gen_metadata()]
    query_mcpws = [
        mcpw
        for mcpw in mcpws
        if mcpw.entityUrn and mcpw.entityUrn.startswith("urn:li:query:")
    ]

    out_path = tmp_path / "mcpw.json"
    write_metadata_file(out_path, query_mcpws)

    mce_helpers.check_golden_file(
        pytestconfig,
        out_path,
        RESOURCE_DIR
        / "test_empty_column_in_query_subjects_only_column_usage_golden.json",
    )


@freeze_time(FROZEN_TIME)
def test_lineage_consistency_fix_tables_added_from_column_lineage() -> None:
    """Test that tables present in column lineage but missing from table lineage are automatically added.

    This tests the consistency fix where:
    - Column lineage references upstream tables
    - Table lineage is missing some of those upstream tables
    - The fix should automatically add the missing tables to table lineage
    - Metrics should be tracked for monitoring
    """
    aggregator = SqlParsingAggregator(
        platform="snowflake",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
    )

    downstream_urn = DatasetUrn("snowflake", "dev.public.target_table").urn()
    upstream_urn_1 = DatasetUrn("snowflake", "dev.public.source_table_1").urn()
    upstream_urn_2 = DatasetUrn("snowflake", "dev.public.source_table_2").urn()
    upstream_urn_3 = DatasetUrn("snowflake", "dev.public.source_table_3").urn()

    # Simulate inconsistent lineage: column lineage has 3 tables, but table lineage only has 2
    known_query_lineage = KnownQueryLineageInfo(
        query_text="insert into target_table (a, b, c) select t1.a, t2.b, t3.c from source_table_1 t1, source_table_2 t2, source_table_3 t3",
        downstream=downstream_urn,
        upstreams=[
            upstream_urn_1,
            upstream_urn_2,
            # upstream_urn_3 is MISSING from table lineage (simulating Snowflake metadata issue)
        ],
        column_lineage=[
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(table=downstream_urn, column="a"),
                upstreams=[ColumnRef(table=upstream_urn_1, column="a")],
            ),
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(table=downstream_urn, column="b"),
                upstreams=[ColumnRef(table=upstream_urn_2, column="b")],
            ),
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(table=downstream_urn, column="c"),
                upstreams=[
                    ColumnRef(table=upstream_urn_3, column="c")
                ],  # This table is missing from upstreams
            ),
        ],
        timestamp=_ts(20),
        query_type=QueryType.INSERT,
    )

    aggregator.add_known_query_lineage(known_query_lineage)

    mcps = list(aggregator.gen_metadata())

    # Verify the fix worked: all 3 tables should appear in upstreamLineage
    lineage_mcps = [mcp for mcp in mcps if mcp.aspectName == "upstreamLineage"]
    assert len(lineage_mcps) == 1

    lineage_aspect = lineage_mcps[0].aspect
    assert lineage_aspect is not None
    assert hasattr(lineage_aspect, "upstreams")
    upstream_urns = {u.dataset for u in lineage_aspect.upstreams}

    # All 3 tables should be present after the fix
    assert upstream_urn_1 in upstream_urns
    assert upstream_urn_2 in upstream_urns
    assert upstream_urn_3 in upstream_urns  # This was added by the fix

    # Verify metrics were tracked
    assert aggregator.report.num_tables_added_from_column_lineage == 1
    assert aggregator.report.num_queries_with_lineage_inconsistencies_fixed == 1


@freeze_time(FROZEN_TIME)
def test_lineage_consistency_no_fix_needed() -> None:
    """Test that no fix is applied when lineage is already consistent.

    This tests that:
    - When table lineage already contains all tables from column lineage
    - No tables are added
    - Metrics remain at zero
    """
    aggregator = SqlParsingAggregator(
        platform="snowflake",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
    )

    downstream_urn = DatasetUrn("snowflake", "dev.public.target_table").urn()
    upstream_urn_1 = DatasetUrn("snowflake", "dev.public.source_table_1").urn()
    upstream_urn_2 = DatasetUrn("snowflake", "dev.public.source_table_2").urn()

    # Both table and column lineage are consistent
    known_query_lineage = KnownQueryLineageInfo(
        query_text="insert into target_table (a, b) select t1.a, t2.b from source_table_1 t1, source_table_2 t2",
        downstream=downstream_urn,
        upstreams=[
            upstream_urn_1,
            upstream_urn_2,  # Both tables present
        ],
        column_lineage=[
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(table=downstream_urn, column="a"),
                upstreams=[ColumnRef(table=upstream_urn_1, column="a")],
            ),
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(table=downstream_urn, column="b"),
                upstreams=[ColumnRef(table=upstream_urn_2, column="b")],
            ),
        ],
        timestamp=_ts(20),
        query_type=QueryType.INSERT,
    )

    aggregator.add_known_query_lineage(known_query_lineage)

    mcps = list(aggregator.gen_metadata())

    # Verify no fix was needed
    lineage_mcps = [mcp for mcp in mcps if mcp.aspectName == "upstreamLineage"]
    assert len(lineage_mcps) == 1

    lineage_aspect = lineage_mcps[0].aspect
    assert lineage_aspect is not None
    assert hasattr(lineage_aspect, "upstreams")
    upstream_urns = {u.dataset for u in lineage_aspect.upstreams}

    assert len(upstream_urns) == 2
    assert upstream_urn_1 in upstream_urns
    assert upstream_urn_2 in upstream_urns

    # Verify metrics show no inconsistencies
    assert aggregator.report.num_tables_added_from_column_lineage == 0
    assert aggregator.report.num_queries_with_lineage_inconsistencies_fixed == 0


@freeze_time(FROZEN_TIME)
def test_lineage_consistency_multiple_missing_tables() -> None:
    """Test that multiple missing tables are all added correctly.

    This tests the fix when:
    - Multiple tables are missing from table lineage
    - All of them are present in column lineage
    - All should be added and metrics should reflect the count
    """
    aggregator = SqlParsingAggregator(
        platform="snowflake",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
    )

    downstream_urn = DatasetUrn("snowflake", "dev.public.target_table").urn()
    upstream_urn_1 = DatasetUrn("snowflake", "dev.public.source_table_1").urn()
    upstream_urn_2 = DatasetUrn("snowflake", "dev.public.source_table_2").urn()
    upstream_urn_3 = DatasetUrn("snowflake", "dev.public.source_table_3").urn()
    upstream_urn_4 = DatasetUrn("snowflake", "dev.public.source_table_4").urn()

    # Only 1 table in table lineage, but 4 in column lineage
    known_query_lineage = KnownQueryLineageInfo(
        query_text="insert into target_table (a, b, c, d) select t1.a, t2.b, t3.c, t4.d from source_table_1 t1, source_table_2 t2, source_table_3 t3, source_table_4 t4",
        downstream=downstream_urn,
        upstreams=[
            upstream_urn_1,
            # upstream_urn_2, upstream_urn_3, upstream_urn_4 are all MISSING
        ],
        column_lineage=[
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(table=downstream_urn, column="a"),
                upstreams=[ColumnRef(table=upstream_urn_1, column="a")],
            ),
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(table=downstream_urn, column="b"),
                upstreams=[ColumnRef(table=upstream_urn_2, column="b")],
            ),
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(table=downstream_urn, column="c"),
                upstreams=[ColumnRef(table=upstream_urn_3, column="c")],
            ),
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(table=downstream_urn, column="d"),
                upstreams=[ColumnRef(table=upstream_urn_4, column="d")],
            ),
        ],
        timestamp=_ts(20),
        query_type=QueryType.INSERT,
    )

    aggregator.add_known_query_lineage(known_query_lineage)

    mcps = list(aggregator.gen_metadata())

    # Verify all 4 tables are now present
    lineage_mcps = [mcp for mcp in mcps if mcp.aspectName == "upstreamLineage"]
    assert len(lineage_mcps) == 1

    lineage_aspect = lineage_mcps[0].aspect
    assert lineage_aspect is not None
    assert hasattr(lineage_aspect, "upstreams")
    upstream_urns = {u.dataset for u in lineage_aspect.upstreams}

    assert len(upstream_urns) == 4
    assert upstream_urn_1 in upstream_urns
    assert upstream_urn_2 in upstream_urns  # Added by fix
    assert upstream_urn_3 in upstream_urns  # Added by fix
    assert upstream_urn_4 in upstream_urns  # Added by fix

    # Verify metrics: 3 tables were added (2, 3, 4)
    assert aggregator.report.num_tables_added_from_column_lineage == 3
    assert aggregator.report.num_queries_with_lineage_inconsistencies_fixed == 1


@freeze_time(FROZEN_TIME)
def test_parallel_batch_keeps_order() -> None:
    """Test that parallel batch processing maintains correct query order with large volume."""
    # Generate ~1000 queries with varying complexity
    num_queries = 1000
    queries = generate_queries_at_scale(num_queries, seed=42)

    # Ensure queries are sorted by timestamp (generate_queries_at_scale should produce mostly sorted)
    queries.sort(key=lambda q: q.timestamp or datetime.min)

    # Sequential processing (max_workers=1)
    with patch.dict(os.environ, {"SQL_AGGREGATOR_PARSING_WORKERS": "1"}):
        aggregator_sequential = SqlParsingAggregator(
            platform="redshift",
            generate_lineage=True,
            generate_usage_statistics=False,
            generate_operations=False,
        )
    for query in queries:
        aggregator_sequential.add(query)

    sequential_mcps = list(aggregator_sequential.gen_metadata())

    # Parallel processing (max_workers=4)
    with patch.dict(os.environ, {"SQL_AGGREGATOR_PARSING_WORKERS": "4"}):
        aggregator_parallel = SqlParsingAggregator(
            platform="redshift",
            generate_lineage=True,
            generate_usage_statistics=False,
            generate_operations=False,
        )
    aggregator_parallel.add_batch(queries)

    parallel_mcps = list(aggregator_parallel.gen_metadata())

    # Results should be identical
    assert len(sequential_mcps) == len(parallel_mcps)

    # Verify both produced the same lineage
    # Extract dataset URNs from lineage aspects
    def get_lineage_urns(mcps):
        lineage_map = {}
        for mcp in mcps:
            if mcp.aspectName == "upstreamLineage":
                dataset_urn = mcp.entityUrn
                upstreams = {u.dataset for u in mcp.aspect.upstreams}
                lineage_map[dataset_urn] = upstreams
        return lineage_map

    sequential_lineage = get_lineage_urns(sequential_mcps)
    parallel_lineage = get_lineage_urns(parallel_mcps)

    assert sequential_lineage == parallel_lineage

    # Verify batch metrics were tracked
    assert aggregator_parallel.report.num_batch_calls == 1
    assert aggregator_parallel.report.num_queries_processed_in_batch == num_queries

    # Sequential should have 0 batch calls
    assert aggregator_sequential.report.num_batch_calls == 0
    assert aggregator_sequential.report.num_queries_processed_in_batch == 0


@freeze_time(FROZEN_TIME)
def test_batch_processing_empty() -> None:
    """Test batch processing with empty list."""
    aggregator = make_basic_aggregator()

    aggregator.add_batch([])

    assert aggregator.report.num_batch_calls == 1
    assert aggregator.report.num_queries_processed_in_batch == 0


@freeze_time(FROZEN_TIME)
def test_batch_processing_parsing_failures() -> None:
    """Test batch processing handles parsing failures gracefully."""
    with patch.dict(os.environ, {"SQL_AGGREGATOR_PARSING_WORKERS": "2"}):
        aggregator = SqlParsingAggregator(
            platform="redshift",
            generate_lineage=True,
            generate_usage_statistics=False,
            generate_operations=False,
        )

    queries = [
        ObservedQuery(
            query="SELECT * FROM table1",
            timestamp=_ts(100),
        ),
        ObservedQuery(
            query="THIS IS NOT VALID SQL;;;",
            timestamp=_ts(101),
        ),
        ObservedQuery(
            query="SELECT * FROM table2",
            timestamp=_ts(102),
        ),
    ]

    aggregator.add_batch(queries)

    assert aggregator.report.num_batch_calls == 1
    assert aggregator.report.num_queries_processed_in_batch == 3


@freeze_time(FROZEN_TIME)
def test_batch_processing_sequential_path() -> None:
    """Test that max_workers=1 uses sequential path."""
    with patch.dict(os.environ, {"SQL_AGGREGATOR_PARSING_WORKERS": "1"}):
        agg_sequential = SqlParsingAggregator(
            platform="redshift",
            generate_lineage=True,
            generate_usage_statistics=False,
            generate_operations=False,
        )

    queries = [
        ObservedQuery(query="SELECT * FROM table1", timestamp=_ts(100)),
        ObservedQuery(query="SELECT * FROM table2", timestamp=_ts(101)),
    ]

    agg_sequential.add_batch(queries)

    assert agg_sequential.report.num_queries_processed_in_batch == 2


@freeze_time(FROZEN_TIME)
def test_batch_processing_exception_handling() -> None:
    """Test that exceptions during parallel parsing are handled gracefully."""
    with patch.dict(os.environ, {"SQL_AGGREGATOR_PARSING_WORKERS": "2"}):
        aggregator = SqlParsingAggregator(
            platform="redshift",
            generate_lineage=True,
        )

    def failing_parser(query: ObservedQuery) -> Optional[Tuple[PreparsedQuery, bool]]:
        if "fail" in query.query.lower():
            raise RuntimeError("Intentional failure for testing")
        return aggregator._parse_observed_query_for_batch(query)

    with patch.object(
        aggregator, "_parse_observed_query_for_batch", side_effect=failing_parser
    ):
        queries = [
            ObservedQuery(query="SELECT * FROM table1", timestamp=_ts(100)),
            ObservedQuery(query="SELECT FAIL FROM table2", timestamp=_ts(101)),
            ObservedQuery(query="SELECT * FROM table3", timestamp=_ts(102)),
        ]

        aggregator.add_batch(queries)

        assert aggregator.report.num_queries_processed_in_batch == 3


@freeze_time(FROZEN_TIME)
def test_batch_processing_with_session_ids() -> None:
    """Test batch processing with queries that have session IDs."""
    with patch.dict(os.environ, {"SQL_AGGREGATOR_PARSING_WORKERS": "2"}):
        aggregator = SqlParsingAggregator(
            platform="redshift",
            generate_lineage=True,
        )

    queries = [
        ObservedQuery(
            query="SELECT * FROM table1",
            timestamp=_ts(100),
            session_id="session_1",
        ),
        ObservedQuery(
            query="SELECT * FROM table2",
            timestamp=_ts(101),
            session_id="session_2",
        ),
    ]

    aggregator.add_batch(queries)

    assert aggregator.report.num_queries_processed_in_batch == 2


@freeze_time(FROZEN_TIME)
def test_batch_processing_schema_resolver_error() -> None:
    """Test that schema resolver errors are handled gracefully."""
    with patch.dict(os.environ, {"SQL_AGGREGATOR_PARSING_WORKERS": "2"}):
        aggregator = SqlParsingAggregator(
            platform="redshift",
            generate_lineage=True,
        )

    def failing_resolver(session_id: str) -> NoReturn:
        raise RuntimeError("Schema resolver failed")

    with patch.object(
        aggregator, "_make_schema_resolver_for_session", side_effect=failing_resolver
    ):
        queries = [
            ObservedQuery(
                query="SELECT * FROM table1",
                timestamp=_ts(100),
                session_id="test_session",
            ),
        ]

        aggregator.add_batch(queries)

        assert aggregator.report.num_queries_processed_in_batch == 1


@freeze_time(FROZEN_TIME)
def test_batch_processing_column_errors() -> None:
    """Test that column-level parsing errors are tracked correctly."""
    with patch.dict(os.environ, {"SQL_AGGREGATOR_PARSING_WORKERS": "2"}):
        aggregator = SqlParsingAggregator(
            platform="redshift",
            generate_lineage=True,
        )

    def mock_parser(*args, **kwargs):
        result = SqlParsingResult(
            query_type=QueryType.SELECT,
            in_tables=[],
            out_tables=[],
            column_lineage=[],
        )
        result.debug_info.column_error = CooperativeTimeoutError(
            "Column parsing timed out"
        )
        return result

    with patch.object(aggregator, "_run_sql_parser", side_effect=mock_parser):
        queries = [
            ObservedQuery(
                query="SELECT * FROM table1",
                timestamp=_ts(100),
            ),
        ]

        aggregator.add_batch(queries)

        assert aggregator.report.num_observed_queries_column_failed >= 1
        assert aggregator.report.num_observed_queries_column_timeout >= 1


logger = logging.getLogger(__name__)


def generate_queries_at_scale(
    num_queries: int, seed: Optional[int] = None
) -> List[ObservedQuery]:
    """Generate test queries at different scales with varying complexity and randomness.

    Args:
        num_queries: Number of queries to generate
        seed: Optional random seed for reproducibility

    Returns:
        List of ObservedQuery objects with varying complexity and randomness
    """
    if seed is not None:
        random.seed(seed)

    queries = []
    base_timestamp = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    # Query templates with varying complexity
    simple_queries = [
        "SELECT * FROM table_{i}",
        "SELECT col1, col2 FROM table_{i} WHERE col1 > {val}",
        "INSERT INTO target_{i} SELECT * FROM source_{i}",
        "SELECT COUNT(*) FROM table_{i} WHERE col1 = {val}",
    ]

    medium_queries = [
        "SELECT t1.col1, t2.col2 FROM table_{i} t1 JOIN table_{i}_2 t2 ON t1.id = t2.id WHERE t1.col1 > {val}",
        "CREATE TABLE result_{i} AS SELECT col1, col2, col3 FROM table_{i} WHERE col1 > {val}",
        "INSERT INTO target_{i} (col1, col2) SELECT col1, col2 FROM source_{i} WHERE col1 IS NOT NULL AND col2 < {val}",
        "SELECT t1.*, t2.col3 FROM table_{i} t1 LEFT JOIN table_{i}_2 t2 ON t1.id = t2.id WHERE t1.col1 BETWEEN {val} AND {val2}",
    ]

    complex_queries = [
        "SELECT t1.col1, t2.col2, t3.col3 FROM table_{i} t1 "
        "LEFT JOIN table_{i}_2 t2 ON t1.id = t2.id "
        "INNER JOIN table_{i}_3 t3 ON t2.id = t3.id "
        "WHERE t1.col1 > {val} AND t2.col2 < {val2} AND t3.col3 = {val3}",
        "CREATE TABLE result_{i} AS "
        "SELECT col1, SUM(col2) as total, COUNT(*) as cnt, AVG(col3) as avg_val "
        "FROM table_{i} "
        "WHERE col1 > {val} "
        "GROUP BY col1 "
        "HAVING total > {val2}",
        "WITH cte_{i} AS (SELECT col1, col2 FROM table_{i} WHERE col1 > {val}) "
        "SELECT c.col1, c.col2, t.col3 FROM cte_{i} c JOIN table_{i}_2 t ON c.col1 = t.col1 WHERE t.col3 < {val2}",
    ]

    all_templates = simple_queries + medium_queries + complex_queries

    for i in range(num_queries):
        # Randomly select template and add randomness to values
        template = random.choice(all_templates)

        # Generate random values for placeholders
        val = random.randint(1, 1000)
        val2 = random.randint(100, 2000)
        val3 = random.randint(1, 500)

        query_text = template.format(i=i, val=val, val2=val2, val3=val3)

        # Add randomness to other fields
        user_idx = random.randint(0, 19)  # 20 different users
        timestamp_offset = random.randint(0, num_queries * 2)  # Random but increasing

        queries.append(
            ObservedQuery(
                query=query_text,
                default_db="dev",
                default_schema="public",
                timestamp=base_timestamp
                + timedelta(seconds=i + timestamp_offset % 100),  # Mostly increasing
                user=CorpUserUrn(f"user{user_idx}"),
                extra_info={"sequence": i},  # Track original sequence for validation
            )
        )

    return queries


def test_sequential_vs_batch(pytestconfig: pytest.Config) -> None:
    """Test sequential vs batch processing with ordering validation.

    This test:
    - Generates queries BEFORE timing starts
    - Measures only the processing time (sequential and parallel) for comparison
    - DOES validate ordering (ensures queries are processed in the same order)

    By default, tests with 1000 queries, but can be configured via environment variable.
    """
    num_queries = int(os.getenv("SQL_AGGREGATOR_TEST_QUERY_COUNT", "1000"))
    batch_size = int(os.getenv("SQL_AGGREGATOR_TEST_BATCH_SIZE", "200"))
    seed = int(
        os.getenv("SQL_AGGREGATOR_TEST_SEED", "42")
    )  # Fixed seed for reproducibility

    # Generate queries BEFORE timing starts
    logger.info(
        f"Generating {num_queries} queries for comparison (this time is excluded from measurements)"
    )
    all_queries = generate_queries_at_scale(num_queries, seed=seed)

    # Split queries into multiple batches to simulate real-world usage
    query_batches = [
        all_queries[i : i + batch_size] for i in range(0, len(all_queries), batch_size)
    ]
    num_batches = len(query_batches)
    logger.info(f"Split into {num_batches} batches")

    # Track order of queries as they're processed (by sequence number)
    sequential_order: List[int] = []
    parallel_order: List[int] = []

    # Test sequential processing (workers=1)
    with patch.dict(os.environ, {"SQL_AGGREGATOR_PARSING_WORKERS": "1"}):
        aggregator_sequential = SqlParsingAggregator(
            platform="redshift",
            generate_lineage=True,
            generate_usage_statistics=False,
            generate_operations=False,
        )

    # Patch to track order - create a wrapper that captures sequence numbers
    original_add_preparsed_sequential = aggregator_sequential.add_preparsed_query

    def track_sequential_order(
        self: "SqlParsingAggregator",
        parsed: PreparsedQuery,
        is_known_temp_table: bool = False,
        require_out_table_schema: bool = False,
        session_has_temp_tables: bool = True,
        _is_internal: bool = False,
    ) -> None:
        if parsed.extra_info and "sequence" in parsed.extra_info:
            sequential_order.append(parsed.extra_info["sequence"])
        # original_add_preparsed_sequential is already a bound method, so call it without self
        return original_add_preparsed_sequential(
            parsed,
            is_known_temp_table=is_known_temp_table,
            require_out_table_schema=require_out_table_schema,
            session_has_temp_tables=session_has_temp_tables,
            _is_internal=_is_internal,
        )

    aggregator_sequential.add_preparsed_query = types.MethodType(  # type: ignore[method-assign]
        track_sequential_order, aggregator_sequential
    )

    # Measure ONLY sequential processing time (data generation already done)
    start_time = time.perf_counter()
    for batch in query_batches:
        aggregator_sequential.add_batch(batch)
    sequential_time = time.perf_counter() - start_time
    aggregator_sequential.close()

    sequential_avg_time = sequential_time / num_queries if num_queries > 0 else 0.0
    sequential_throughput = (
        num_queries / sequential_time if sequential_time > 0 else 0.0
    )

    logger.info(
        f"Sequential processing: {sequential_time:.2f}s total, "
        f"{sequential_avg_time * 1000:.2f}ms per query, "
        f"{sequential_throughput:.2f} queries/sec"
    )

    # Test batch processing with multiple workers (if available)
    # Note: This will use sequential if SQL_AGGREGATOR_PARSING_WORKERS=1
    aggregator_batch = SqlParsingAggregator(
        platform="redshift",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
    )

    # Patch to track order - create a wrapper that captures sequence numbers
    original_add_preparsed_parallel = aggregator_batch.add_preparsed_query

    def track_parallel_order(
        self: "SqlParsingAggregator",
        parsed: PreparsedQuery,
        is_known_temp_table: bool = False,
        require_out_table_schema: bool = False,
        session_has_temp_tables: bool = True,
        _is_internal: bool = False,
    ) -> None:
        if parsed.extra_info and "sequence" in parsed.extra_info:
            parallel_order.append(parsed.extra_info["sequence"])
        # original_add_preparsed_parallel is already a bound method, so call it without self
        return original_add_preparsed_parallel(
            parsed,
            is_known_temp_table=is_known_temp_table,
            require_out_table_schema=require_out_table_schema,
            session_has_temp_tables=session_has_temp_tables,
            _is_internal=_is_internal,
        )

    aggregator_batch.add_preparsed_query = types.MethodType(  # type: ignore[method-assign]
        track_parallel_order, aggregator_batch
    )

    # Measure ONLY batch processing time (data generation already done)
    start_time = time.perf_counter()
    for batch in query_batches:
        aggregator_batch.add_batch(batch)
    batch_time = time.perf_counter() - start_time
    aggregator_batch.close()

    batch_avg_time = batch_time / num_queries if num_queries > 0 else 0.0
    batch_throughput = num_queries / batch_time if batch_time > 0 else 0.0

    logger.info(
        f"Batch processing: {batch_time:.2f}s total, "
        f"{batch_avg_time * 1000:.2f}ms per query, "
        f"{batch_throughput:.2f} queries/sec"
    )

    # Verify both produce the same results (same number of queries processed)
    assert aggregator_sequential.report.num_observed_queries == num_queries
    assert aggregator_batch.report.num_observed_queries == num_queries

    # Validate that queries were processed in the same order
    # Both should process queries in the same sequence order
    assert len(sequential_order) == num_queries, (
        f"Expected {num_queries} queries in sequential order, got {len(sequential_order)}"
    )
    assert len(parallel_order) == num_queries, (
        f"Expected {num_queries} queries in parallel order, got {len(parallel_order)}"
    )

    # The order should match - queries should be processed in the same sequence
    # Since add_batch maintains order, the sequences should match exactly
    assert sequential_order == parallel_order, (
        f"Query processing order mismatch. "
        f"First mismatch at index {next((i for i, (s, p) in enumerate(zip(sequential_order, parallel_order)) if s != p), None)}: "
        f"sequential={sequential_order[:20]}, parallel={parallel_order[:20]}"
    )

    logger.info("✓ Order validation passed: queries processed in identical order")

    # Log performance comparison
    if batch_time < sequential_time:
        speedup = sequential_time / batch_time
        logger.info(f"Batch processing is {speedup:.2f}x faster than sequential")
    else:
        logger.info(
            "Batch processing performance similar to sequential (may be due to workers=1 or overhead)"
        )

    # Performance assertions
    assert sequential_time > 0, "Sequential processing should take some time"
    assert batch_time > 0, "Batch processing should take some time"
    assert sequential_throughput > 0, "Sequential throughput should be positive"
    assert batch_throughput > 0, "Batch throughput should be positive"
