import pathlib
from datetime import datetime

import pytest

import datahub.emitter.mce_builder as builder
from datahub.metadata.urns import DatasetUrn
from datahub.sql_parsing.sql_parsing_aggregator_v2 import SqlParsingAggregator
from tests.test_helpers import mce_helpers

RESOURCE_DIR = pathlib.Path(__file__).parent / "aggregator_goldens"


def test_basic_lineage(pytestconfig: pytest.Config) -> None:
    aggregator = SqlParsingAggregator(
        platform="redshift",
        platform_instance=None,
        env=builder.DEFAULT_ENV,
        generate_view_lineage=True,
        generate_observed_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
    )

    aggregator.add_observed_query(
        query="create table foo as select a, b from bar",
        default_db="dev",
        default_schema="public",
    )

    mcps = list(aggregator.gen_metadata())

    mce_helpers.check_goldens_stream(
        pytestconfig,
        outputs=mcps,
        golden_path=RESOURCE_DIR / "test_basic_lineage.json",
    )


def test_overlapping_inserts(pytestconfig: pytest.Config) -> None:
    aggregator = SqlParsingAggregator(
        platform="redshift",
        platform_instance=None,
        env=builder.DEFAULT_ENV,
        generate_view_lineage=True,
        generate_observed_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
    )

    aggregator.add_observed_query(
        query="insert into downstream (a, b) select a, b from upstream1",
        default_db="dev",
        default_schema="public",
        query_timestamp=datetime.fromtimestamp(20),
    )
    aggregator.add_observed_query(
        query="insert into downstream (a, c) select a, c from upstream2",
        default_db="dev",
        default_schema="public",
        query_timestamp=datetime.fromtimestamp(25),
    )

    mcps = list(aggregator.gen_metadata())

    mce_helpers.check_goldens_stream(
        pytestconfig,
        outputs=mcps,
        golden_path=RESOURCE_DIR / "test_overlapping_inserts.json",
    )


def test_temp_table(pytestconfig: pytest.Config) -> None:
    aggregator = SqlParsingAggregator(
        platform="redshift",
        platform_instance=None,
        env=builder.DEFAULT_ENV,
        generate_view_lineage=True,
        generate_observed_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
    )

    aggregator._schema_resolver.add_raw_schema_info(
        DatasetUrn("redshift", "dev.public.bar").urn(),
        {"a": "int", "b": "int", "c": "int"},
    )

    aggregator.add_observed_query(
        query="create table foo as select a, 2*b as b from bar",
        default_db="dev",
        default_schema="public",
        session_id="session1",
    )
    aggregator.add_observed_query(
        query="create temp table foo as select a, b+c as c from bar",
        default_db="dev",
        default_schema="public",
        session_id="session2",
    )
    aggregator.add_observed_query(
        query="create table foo_session2 as select * from foo",
        default_db="dev",
        default_schema="public",
        session_id="session2",
    )
    aggregator.add_observed_query(
        query="create table foo_session3 as select * from foo",
        default_db="dev",
        default_schema="public",
        session_id="session3",
    )

    # foo_session2 should come from the temp foo table, have columns a and c, and depend on bar.b and bar.c
    # foo_session3 should come from the true foo table, have columns a and b, and depend on bar.b

    mcps = list(aggregator.gen_metadata())

    mce_helpers.check_goldens_stream(
        pytestconfig,
        outputs=mcps,
        golden_path=RESOURCE_DIR / "test_temp_table.json",
    )
