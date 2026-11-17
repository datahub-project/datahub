"""Failures blamed on the right party: the caller (exit 2), the environment
(exit 1) or the source (exit 3)."""

from typing import Dict, List

import pytest

from datahub.configuration.common import ConfigModel
from datahub.ingestion.agent import probe_methods
from datahub.ingestion.agent.probe_methods import probe_method, run_probe_method
from datahub.ingestion.agent.verdicts import (
    ProbeArgumentError,
    ProbeConnectionError,
    ProbeInternalError,
)


class _DriverError(Exception):
    """A DB-API error carrying its SQLSTATE the way generic_error_code reads."""

    def __init__(self, sqlstate: str) -> None:
        super().__init__(f"server said something about {sqlstate}")
        self.sqlstate = sqlstate


class _MySqlProtocolError(Exception):
    """A MySQL-protocol driver error: an errno, and no SQLSTATE to read."""

    def __init__(self, number: int) -> None:
        super().__init__(number, "server said something")
        self.errno = number


class _GatewayError(Exception):
    """A wrapper whose own code (an HTTP status) is not the SQL failure's."""

    status_code = 400


def _failure(mode: str) -> Exception:
    if mode.startswith("errno:"):
        return _MySqlProtocolError(int(mode.split(":", 1)[1]))
    if mode.startswith("wrapped:"):
        try:
            raise _DriverError(mode.split(":", 1)[1])
        except _DriverError as cause:
            wrapper = _GatewayError("gateway")
            wrapper.__cause__ = cause
            return wrapper
    return _DriverError(mode)


class _Provider:
    sql_dialect = "postgres"

    def __init__(self, mode: str) -> None:
        self.mode = mode

    @classmethod
    def for_config(cls, config: "_Config") -> "_Provider":
        if config.mode == "no-driver":
            raise ModuleNotFoundError("No module named 'somedriver'", name="somedriver")
        if config.mode == "refused":
            raise ConnectionRefusedError(61, "Connection refused")
        return cls(config.mode)

    def __enter__(self) -> "_Provider":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    @probe_method(name="sql", scoped_sql_param="query", shapes_own_result=True)
    def sql(self, query: str) -> Dict[str, object]:
        """Run a catalog query."""
        raise _failure(self.mode)

    @probe_method(name="things")
    def things(self) -> List[str]:
        """List things."""
        raise _DriverError(self.mode)


class _Config(ConfigModel):
    mode: str = ""

    @classmethod
    def probe_provider_class(cls) -> type:
        return _Provider


_REAL_CONFIG_CLASS_FOR = probe_methods.config_class_for


@pytest.fixture(autouse=True)
def _fake_source(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(probe_methods, "config_class_for", lambda _st: _Config)


_QUERY = {"query": "SELECT column_nam FROM information_schema.columns"}


def test_the_callers_own_sql_failing_with_sqlstate_class_42_is_theirs() -> None:
    with pytest.raises(ProbeArgumentError):
        run_probe_method("fake", {"mode": "42703"}, "sql", dict(_QUERY))


@pytest.mark.parametrize(
    "mode",
    [
        # Unknown column, missing table, syntax error: SQLSTATE 42S22, 42S02
        # and 42000, which a MySQL-protocol driver reports as errno only.
        "errno:1054",
        "errno:1146",
        "errno:1064",
        # A class-42 SQLSTATE under a wrapper whose own code is not one.
        "wrapped:42703",
    ],
)
def test_the_callers_sql_failing_with_a_class_42_error_anywhere_is_theirs(
    mode: str,
) -> None:
    with pytest.raises(ProbeArgumentError):
        run_probe_method("fake", {"mode": mode}, "sql", dict(_QUERY))


@pytest.mark.parametrize("mode", ["08006", "errno:2013", "wrapped:08006"])
def test_the_callers_sql_failing_with_another_code_is_the_sources(mode: str) -> None:
    with pytest.raises(ProbeConnectionError):
        run_probe_method("fake", {"mode": mode}, "sql", dict(_QUERY))


def test_sqlstate_42_from_a_listing_the_connector_wrote_is_still_the_sources() -> None:
    with pytest.raises(ProbeConnectionError):
        run_probe_method("fake", {"mode": "42P01"}, "things", {})


def test_a_missing_driver_is_the_environment_and_names_the_module() -> None:
    with pytest.raises(ProbeInternalError) as info:
        run_probe_method("fake", {"mode": "no-driver"}, "things", {})
    assert "somedriver" in str(info.value)


def test_a_refused_connection_stays_the_sources() -> None:
    with pytest.raises(ProbeConnectionError):
        run_probe_method("fake", {"mode": "refused"}, "things", {})


@pytest.mark.parametrize(
    "uri", ["nosuchdialect://h/db", "postgresql+nosuchdriver://u@h/db", "not a url"]
)
def test_a_url_sqlalchemy_cannot_use_is_the_callers(
    monkeypatch: pytest.MonkeyPatch, uri: str
) -> None:
    # The real registry, restored alone: undo() would also revert every other
    # fixture's patches on this monkeypatch.
    monkeypatch.setattr(probe_methods, "config_class_for", _REAL_CONFIG_CLASS_FOR)
    with pytest.raises(ProbeArgumentError) as info:
        run_probe_method(
            "sqlalchemy", {"connect_uri": uri, "platform": "x"}, "containers", {}
        )
    assert "@h" not in str(info.value)
