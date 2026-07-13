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


def test_parallel_enabled_without_workers_raises() -> None:
    """use_parallel_sql_parsing=True with no sql_parsing_workers must raise ValidationError.

    Auto-detection via os.cpu_count() is unsafe on virtualized/containerized hosts
    where the reported CPU count is that of the underlying node, not the cgroup limit.
    Operators must supply an explicit worker count.
    """

    class _Cfg(SqlParsingParallelismConfig):
        pass

    with pytest.raises(ValidationError, match="sql_parsing_workers must be set"):
        _Cfg(use_parallel_sql_parsing=True)


def test_parallel_enabled_with_explicit_workers_accepted() -> None:
    """use_parallel_sql_parsing=True with an explicit worker count must be accepted."""

    class _Cfg(SqlParsingParallelismConfig):
        pass

    cfg = _Cfg(use_parallel_sql_parsing=True, sql_parsing_workers=4)
    assert cfg.use_parallel_sql_parsing is True
    assert cfg.sql_parsing_workers == 4


def test_parallel_disabled_workers_none_accepted() -> None:
    """use_parallel_sql_parsing=False with workers unset must remain valid."""

    class _Cfg(SqlParsingParallelismConfig):
        pass

    cfg = _Cfg(use_parallel_sql_parsing=False)
    assert cfg.sql_parsing_workers is None


def test_mixin_rejects_excessive_workers() -> None:
    """Values > 512 must be rejected at config-parse time (le=512 constraint)."""

    class _Cfg(SqlParsingParallelismConfig):
        pass

    with pytest.raises(ValidationError):
        _Cfg(sql_parsing_workers=1024)
