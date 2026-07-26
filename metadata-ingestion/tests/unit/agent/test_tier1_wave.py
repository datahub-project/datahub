import pytest

from datahub.ingestion.agent.probe import probe_hierarchy


def test_snowflake_summary_inherits_probe():
    # SnowflakeSummaryConfig extends SnowflakeConnectionConfig → probe inherited.
    pytest.importorskip("snowflake.connector")
    assert probe_hierarchy("snowflake-summary")


def test_snowflake_queries_delegates_probe():
    pytest.importorskip("snowflake.connector")
    assert probe_hierarchy("snowflake-queries")


def test_bigquery_queries_delegates_probe():
    pytest.importorskip("google.cloud.bigquery")
    assert probe_hierarchy("bigquery-queries")
