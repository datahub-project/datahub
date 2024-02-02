import pathlib

import pytest

import datahub.emitter.mce_builder as builder
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
    )
    aggregator.add_observed_query(
        query="insert into downstream (a, c) select a, c from upstream2",
        default_db="dev",
        default_schema="public",
    )

    mcps = list(aggregator.gen_metadata())

    mce_helpers.check_goldens_stream(
        pytestconfig,
        outputs=mcps,
        golden_path=RESOURCE_DIR / "test_overlapping_inserts.json",
    )
