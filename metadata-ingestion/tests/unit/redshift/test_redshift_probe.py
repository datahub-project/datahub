from typing import Any, Dict, List, Optional, Sequence, Tuple

import pytest

from datahub.ingestion.agent.sql_passthrough import PROBE_QUERY_LABEL
from datahub.ingestion.source.redshift.config import RedshiftConfig
from datahub.ingestion.source.redshift.redshift import RedshiftSource
from datahub.ingestion.source.redshift.redshift_probe import RedshiftMetadataProbe

# (substring identifying the query, column names, rows). Matched in order, so
# the more specific needles come first.
Route = Tuple[str, Sequence[str], Sequence[Sequence[Any]]]


class _FakeCursor:
    def __init__(self, conn: "_FakeConnection") -> None:
        self._conn = conn
        self.description: Optional[List[Tuple[str]]] = None
        self._rows: List[Sequence[Any]] = []

    def execute(self, query: str, args: Any = None) -> "_FakeCursor":
        self._conn.executed.append(query)
        self._conn.bound.append(args)
        if self._conn.fail_on and self._conn.fail_on in query:
            raise RuntimeError("canceling statement due to statement timeout")
        if "statement_timeout" in query:
            return self
        for needle, columns, rows in self._conn.routes:
            if needle in query:
                self.description = [(c,) for c in columns]
                self._rows = [list(r) for r in rows]
                return self
        raise AssertionError(f"unrouted query: {query[:300]}")

    def fetchall(self) -> List[Sequence[Any]]:
        rows, self._rows = self._rows, []
        return rows

    def fetchone(self) -> Optional[Sequence[Any]]:
        return self._rows.pop(0) if self._rows else None

    def fetchmany(self, size: Optional[int] = None) -> List[Sequence[Any]]:
        n = len(self._rows) if size is None else size
        out, self._rows = self._rows[:n], self._rows[n:]
        return out

    def close(self) -> None:
        pass


class _FakeConnection:
    def __init__(
        self, routes: Sequence[Route] = (), fail_on: Optional[str] = None
    ) -> None:
        self.routes = list(routes)
        self.fail_on = fail_on
        self.executed: List[str] = []
        self.bound: List[Any] = []
        self.autocommit = True
        self.closed = False

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def close(self) -> None:
        self.closed = True


# No password: the fake connection never authenticates, and an IAM-style
# recipe is the case the ingestion builder exists to get right.
_RECIPE: Dict[str, Any] = {
    "host_port": "cluster.example:5439",
    "database": "dev",
    "username": "probe_user",
}


def _probe(conn: _FakeConnection, **overrides: Any) -> RedshiftMetadataProbe:
    return RedshiftMetadataProbe(
        conn, RedshiftConfig.model_validate({**_RECIPE, **overrides})
    )


def _connect_with(
    monkeypatch: pytest.MonkeyPatch,
    conn: _FakeConnection,
    seen: Optional[List[RedshiftConfig]] = None,
) -> None:
    def _connect(config: RedshiftConfig) -> _FakeConnection:
        if seen is not None:
            seen.append(config)
        return conn

    monkeypatch.setattr(
        RedshiftSource, "get_redshift_connection", staticmethod(_connect)
    )


def test_for_config_connects_through_the_ingestion_builder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: List[RedshiftConfig] = []
    conn = _FakeConnection()
    _connect_with(monkeypatch, conn, seen)
    recipe = {**_RECIPE, "extra_client_options": {"iam": True, "sslmode": "prefer"}}

    probe = RedshiftMetadataProbe.for_config(RedshiftConfig.model_validate(recipe))

    options = seen[0].extra_client_options
    assert options["iam"] is True and options["sslmode"] == "prefer"
    assert options["application_name"] == PROBE_QUERY_LABEL
    assert "SET statement_timeout = 30000" in conn.executed
    assert probe.catalog_scope == RedshiftConfig.probe_catalog_scope()


def test_recipe_application_name_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: List[RedshiftConfig] = []
    _connect_with(monkeypatch, _FakeConnection(), seen)
    recipe = {**_RECIPE, "extra_client_options": {"application_name": "etl"}}

    RedshiftMetadataProbe.for_config(RedshiftConfig.model_validate(recipe))

    assert seen[0].extra_client_options["application_name"] == "etl"


def test_labelling_does_not_change_the_recipe_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _connect_with(monkeypatch, _FakeConnection())
    config = RedshiftConfig.model_validate(_RECIPE)

    RedshiftMetadataProbe.for_config(config)

    assert "application_name" not in config.extra_client_options


def test_connection_is_closed_when_the_ceiling_cannot_be_applied(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _FakeConnection(fail_on="statement_timeout")
    _connect_with(monkeypatch, conn)

    with pytest.raises(RuntimeError):
        RedshiftMetadataProbe.for_config(RedshiftConfig.model_validate(_RECIPE))
    assert conn.closed


def test_exit_closes_the_connection() -> None:
    conn = _FakeConnection()
    with _probe(conn):
        pass
    assert conn.closed


def test_sql_passthrough_returns_columns_and_detects_truncation() -> None:
    conn = _FakeConnection(
        routes=[("svv_all_schemas", ["schema_name"], [["a"], ["b"], ["c"]])]
    )
    result = _probe(conn).sql(
        "SELECT schema_name FROM pg_catalog.svv_all_schemas", limit=2
    )
    assert result["columns"] == ["schema_name"]
    assert result["truncated"] is True
    assert result["row_count"] == 2
    # Sent without bind values, so the driver does not reinterpret a `%`
    # inside a LIKE literal as a paramstyle placeholder.
    assert conn.bound == [None]
