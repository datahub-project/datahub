"""Config-parse tests for the SqlParsingParallelismConfig mixin.

Verifies that each connector's config correctly exposes use_parallel_sql_parsing
and sql_parsing_workers, and that values round-trip without error.
"""

import pytest
from pydantic import ValidationError

from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingParallelismConfig

# ---------------------------------------------------------------------------
# Mixin standalone tests
# ---------------------------------------------------------------------------


def test_mixin_defaults() -> None:
    class _Cfg(SqlParsingParallelismConfig):
        pass

    cfg = _Cfg()
    assert cfg.use_parallel_sql_parsing is False
    assert cfg.sql_parsing_workers is None


def test_mixin_accepts_values() -> None:
    class _Cfg(SqlParsingParallelismConfig):
        pass

    cfg = _Cfg(use_parallel_sql_parsing=True, sql_parsing_workers=4)
    assert cfg.use_parallel_sql_parsing is True
    assert cfg.sql_parsing_workers == 4


def test_mixin_accepts_none_workers() -> None:
    """None (auto-detect) must remain valid after adding ge=1."""

    class _Cfg(SqlParsingParallelismConfig):
        pass

    cfg = _Cfg(sql_parsing_workers=None)
    assert cfg.sql_parsing_workers is None


@pytest.mark.parametrize("bad_value", [0, -1])
def test_mixin_rejects_non_positive_workers(bad_value: int) -> None:
    """Values ≤ 0 must be rejected at config-parse time (ge=1 constraint)."""

    class _Cfg(SqlParsingParallelismConfig):
        pass

    with pytest.raises(ValidationError):
        _Cfg(sql_parsing_workers=bad_value)


# ---------------------------------------------------------------------------
# Snowflake
# ---------------------------------------------------------------------------


def test_snowflake_queries_extractor_config_parallel_defaults() -> None:
    from datahub.ingestion.source.snowflake.snowflake_queries import (
        SnowflakeQueriesExtractorConfig,
    )

    cfg = SnowflakeQueriesExtractorConfig()
    assert cfg.use_parallel_sql_parsing is False
    assert cfg.sql_parsing_workers is None


def test_snowflake_queries_extractor_config_parallel_values() -> None:
    from datahub.ingestion.source.snowflake.snowflake_queries import (
        SnowflakeQueriesExtractorConfig,
    )

    cfg = SnowflakeQueriesExtractorConfig(
        use_parallel_sql_parsing=True, sql_parsing_workers=4
    )
    assert cfg.use_parallel_sql_parsing is True
    assert cfg.sql_parsing_workers == 4


# ---------------------------------------------------------------------------
# BigQuery
# ---------------------------------------------------------------------------


def test_bigquery_queries_extractor_config_parallel_defaults() -> None:
    from datahub.ingestion.source.bigquery_v2.queries_extractor import (
        BigQueryQueriesExtractorConfig,
    )

    cfg = BigQueryQueriesExtractorConfig()
    assert cfg.use_parallel_sql_parsing is False
    assert cfg.sql_parsing_workers is None


def test_bigquery_queries_extractor_config_parallel_values() -> None:
    from datahub.ingestion.source.bigquery_v2.queries_extractor import (
        BigQueryQueriesExtractorConfig,
    )

    cfg = BigQueryQueriesExtractorConfig(
        use_parallel_sql_parsing=True, sql_parsing_workers=4
    )
    assert cfg.use_parallel_sql_parsing is True
    assert cfg.sql_parsing_workers == 4


# ---------------------------------------------------------------------------
# Redshift
# ---------------------------------------------------------------------------


def test_redshift_config_parallel_defaults() -> None:
    from datahub.ingestion.source.redshift.config import RedshiftConfig

    cfg = RedshiftConfig(host_port="localhost:5439", database="test")
    assert cfg.use_parallel_sql_parsing is False
    assert cfg.sql_parsing_workers is None


def test_redshift_config_parallel_values() -> None:
    from datahub.ingestion.source.redshift.config import RedshiftConfig

    cfg = RedshiftConfig(
        host_port="localhost:5439",
        database="test",
        use_parallel_sql_parsing=True,
        sql_parsing_workers=4,
    )
    assert cfg.use_parallel_sql_parsing is True
    assert cfg.sql_parsing_workers == 4


# ---------------------------------------------------------------------------
# Unity Catalog / Databricks
# ---------------------------------------------------------------------------


def test_unity_catalog_config_parallel_defaults() -> None:
    from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig

    cfg = UnityCatalogSourceConfig(
        workspace_url="https://adb-123.azuredatabricks.net",
        token="dapi-test-token",
    )
    assert cfg.use_parallel_sql_parsing is False
    assert cfg.sql_parsing_workers is None


def test_unity_catalog_config_parallel_values() -> None:
    from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig

    cfg = UnityCatalogSourceConfig(
        workspace_url="https://adb-123.azuredatabricks.net",
        token="dapi-test-token",
        use_parallel_sql_parsing=True,
        sql_parsing_workers=4,
    )
    assert cfg.use_parallel_sql_parsing is True
    assert cfg.sql_parsing_workers == 4
