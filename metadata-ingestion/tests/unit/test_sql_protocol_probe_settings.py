"""The MySQL protocol's best-effort statement ceiling: tried after connecting,
in MySQL's spelling and then MariaDB's, and survivable when neither applies."""

import logging
import sqlite3
from typing import Any, List

import pytest
from sqlalchemy import create_engine

from datahub.ingestion.agent.sql_passthrough import QueryBudget
from datahub.ingestion.source.sql.protocol_probe_settings import (
    probe_settings_for_url,
)
from datahub.ingestion.source.sql.sql_generic import SQLAlchemyGenericConfig


class _Cursor:
    """A DB-API cursor that answers SET SESSION statements the way one MySQL
    flavour would, and hands everything else to a real sqlite cursor."""

    def __init__(self, real: sqlite3.Cursor, accepted: str, executed: List[str]):
        self._real = real
        self._accepted = accepted
        self._executed = executed

    def execute(self, statement: str, *args: Any) -> Any:
        if statement.startswith("SET SESSION"):
            self._executed.append(statement)
            if self._accepted in statement:
                return None
            raise sqlite3.OperationalError("Unknown system variable")
        return self._real.execute(statement, *args)

    def close(self) -> None:
        self._real.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)


class _Connection:
    def __init__(self, accepted: str, executed: List[str]) -> None:
        self._real = sqlite3.connect(":memory:")
        self._accepted = accepted
        self._executed = executed

    def cursor(self, *args: Any, **kwargs: Any) -> _Cursor:
        return _Cursor(
            self._real.cursor(*args, **kwargs), self._accepted, self._executed
        )

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)


def _connect_with_ceiling(accepted: str) -> List[str]:
    config = SQLAlchemyGenericConfig(
        platform="mysql", connect_uri="mysql+pymysql://u@h/db"
    )
    settings = probe_settings_for_url(config, QueryBudget(timeout_seconds=3))
    assert settings.prepare is not None
    # The ceiling is a best effort, so the budget does not claim it.
    assert not settings.timeout_applies

    executed: List[str] = []
    engine = create_engine("sqlite://", creator=lambda: _Connection(accepted, executed))
    settings.prepare(engine)
    with engine.connect():
        pass
    engine.dispose()
    return executed


def test_mysql_spelling_is_tried_first() -> None:
    assert _connect_with_ceiling("max_execution_time") == [
        "SET SESSION max_execution_time=3000"
    ]


def test_mariadb_spelling_is_the_fallback() -> None:
    assert _connect_with_ceiling("max_statement_time") == [
        "SET SESSION max_execution_time=3000",
        "SET SESSION max_statement_time=3",
    ]


def test_a_server_with_neither_variable_still_connects(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(
        logging.DEBUG, logger="datahub.ingestion.source.sql.protocol_probe_settings"
    ):
        executed = _connect_with_ceiling("nothing matches")
    assert len(executed) == 2
    assert "unbounded" in caplog.text
