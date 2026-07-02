"""Config-parse tests for the SqlParsingParallelismConfig mixin.

Connector-specific config wiring (Snowflake, BigQuery, Redshift, Unity) is tested
in each connector's own change; here we only cover the shared mixin.
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
    """Values <= 0 must be rejected at config-parse time (ge=1 constraint)."""

    class _Cfg(SqlParsingParallelismConfig):
        pass

    with pytest.raises(ValidationError):
        _Cfg(sql_parsing_workers=bad_value)
