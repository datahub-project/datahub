"""
Verifies the corrected semantics of native_query_parsing=False.

Old (buggy) behaviour: native_query_parsing=False suppressed ALL M-Query parsing.
New (correct) behaviour: native_query_parsing=False suppresses only expressions
    containing Value.NativeQuery; all other expressions are parsed normally.
"""

import pytest

import datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes as powerbi_data_classes
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.powerbi.config import (
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
    create_dataplatform_instance_resolver,
)


def _make_table(expression: str) -> powerbi_data_classes.Table:
    """Create a Table with the given M-Query expression."""
    return powerbi_data_classes.Table(
        columns=[],
        measures=[],
        expression=expression,
        name="test_table",
        full_name="test_dataset.test_table",
    )


def _get_instances(native_query_parsing: bool):
    config = PowerBiDashboardSourceConfig.model_validate(
        {
            "tenant_id": "fake",
            "client_id": "foo",
            "client_secret": "bar",
            "enable_advance_lineage_sql_construct": False,
            "extract_column_level_lineage": False,
            "native_query_parsing": native_query_parsing,
        }
    )
    platform_instance_resolver = create_dataplatform_instance_resolver(config)
    ctx = PipelineContext(run_id="fake")
    reporter = PowerBiDashboardSourceReport()
    return ctx, config, platform_instance_resolver, reporter


SNOWFLAKE_EXPR = (
    "let\n"
    '    Source = Snowflake.Databases("bu10758.ap-unknown-2.fakecomputing.com","PBI_TEST_WAREHOUSE_PROD",[Role="PBI_TEST_MEMBER"]),\n'
    '    PBI_TEST_Database = Source{[Name="PBI_TEST",Kind="Database"]}[Data],\n'
    '    TEST_Schema = PBI_TEST_Database{[Name="TEST",Kind="Schema"]}[Data],\n'
    '    TESTTABLE_Table = TEST_Schema{[Name="TESTTABLE",Kind="Table"]}[Data]\n'
    "in\n"
    "    TESTTABLE_Table"
)

NATIVE_QUERY_EXPR = (
    "let\n"
    '    Source = Snowflake.Databases("account"),\n'
    '    q = Value.NativeQuery(Source, "SELECT 1")\n'
    "in\n"
    "    q"
)


@pytest.mark.integration_batch_5
def test_native_query_false_skips_native_query_expression():
    """With native_query_parsing=False, NativeQuery expressions return empty lineage."""
    from datahub.ingestion.source.powerbi.m_query.parser import get_upstream_tables

    ctx, config, platform_instance_resolver, reporter = _get_instances(
        native_query_parsing=False
    )
    result = get_upstream_tables(
        table=_make_table(NATIVE_QUERY_EXPR),
        reporter=reporter,
        platform_instance_resolver=platform_instance_resolver,
        ctx=ctx,
        config=config,
    )
    assert result == []
    assert reporter.m_query_native_query_skipped == 1


@pytest.mark.integration_batch_5
def test_native_query_false_still_parses_non_native_expression():
    """With native_query_parsing=False, non-NativeQuery expressions still produce lineage."""
    from datahub.ingestion.source.powerbi.m_query.parser import get_upstream_tables

    ctx, config, platform_instance_resolver, reporter = _get_instances(
        native_query_parsing=False
    )
    result = get_upstream_tables(
        table=_make_table(SNOWFLAKE_EXPR),
        reporter=reporter,
        platform_instance_resolver=platform_instance_resolver,
        ctx=ctx,
        config=config,
    )
    # Should return non-empty lineage (Snowflake table found)
    assert result != []
    upstreams = result[0].upstreams
    assert len(upstreams) == 1
    assert "snowflake" in upstreams[0].urn


@pytest.mark.integration_batch_5
def test_native_query_true_parses_both():
    """With native_query_parsing=True (default), both expression types are attempted."""
    from datahub.ingestion.source.powerbi.m_query.parser import get_upstream_tables

    # Both should be attempted without raising an exception
    for expr in [SNOWFLAKE_EXPR, NATIVE_QUERY_EXPR]:
        ctx, config, platform_instance_resolver, reporter = _get_instances(
            native_query_parsing=True
        )
        get_upstream_tables(
            table=_make_table(expr),
            reporter=reporter,
            platform_instance_resolver=platform_instance_resolver,
            ctx=ctx,
            config=config,
        )
