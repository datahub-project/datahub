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


_DB_COLUMNS = ["database_name", "database_type", "database_options"]
_DB_DETAILS: Route = ("database_options", _DB_COLUMNS, [["dev", "local", None]])
_SHARED_DB: Route = ("database_options", _DB_COLUMNS, [["dev", "shared", None]])
_SCHEMAS: Route = (
    "schema_type",
    [
        "schema_name",
        "schema_type",
        "schema_owner_name",
        "schema_option",
        "external_platform",
        "external_database",
    ],
    [
        ["public", "local", None, None, None, None],
        ["ext_schema", "external", None, None, "GLUE", "lake"],
    ],
)
_REL_COLUMNS = ["tabletype", "schema", "relname", "view_definition"]
_RELATIONS: Route = (
    "tabletype",
    _REL_COLUMNS,
    [
        ["TABLE", "public", "orders", None],
        ["VIEW", "public", "v_orders", "select * from orders"],
        ["MATERIALIZED VIEW", "public", "mv_orders", "select 1"],
        ["FOREIGN TABLE", "public", "f_orders", None],
        ["EXTERNAL_TABLE", "ext_schema", "clicks", None],
        ["TABLE", "other", "unrelated", None],
    ],
)

# Each one a way out of a quoted SQL literal or statement: a closing quote, a
# UNION, a second statement, and both comment markers.
_HOSTILE_NAMES = [
    "public' OR '1'='1",
    "x' UNION SELECT usename FROM pg_user --",
    "public'; DROP TABLE orders; --",
    "public' /* comment */",
    'public" --',
]


def _assert_never_sent(conn: _FakeConnection, payload: str) -> None:
    assert not any(payload in q for q in conn.executed), conn.executed
    assert not any(payload in str(b) for b in conn.bound if b is not None)


def test_containers_are_the_schemas_ingestion_walks() -> None:
    conn = _FakeConnection(routes=[_SCHEMAS])
    assert _probe(conn).containers(limit=200) == ["public", "ext_schema"]
    # extract_ownership would join pg_catalog.pg_user: user names are not
    # schema shape and the probe must not read them.
    assert not any("pg_user" in q for q in conn.executed)


def test_tables_and_views_split_like_ingestion() -> None:
    conn = _FakeConnection(routes=[_DB_DETAILS, _RELATIONS])
    probe = _probe(conn)
    assert probe.tables(schema="public", limit=200) == ["orders", "f_orders"]
    assert probe.views(schema="public", limit=200) == ["v_orders", "mv_orders"]
    assert not any("pg_user" in q for q in conn.executed)


def test_listing_never_enriches_from_operational_history() -> None:
    # get_tables_and_views would first run enrich_tables (svv_table_info joined
    # to stl_insert): grants a metadata probe should not need.
    conn = _FakeConnection(routes=[_DB_DETAILS, _RELATIONS])
    _probe(conn).tables(schema="public", limit=200)
    assert not any("stl_insert" in q or "svv_table_info" in q for q in conn.executed)


@pytest.mark.parametrize("hostile", _HOSTILE_NAMES)
@pytest.mark.parametrize("command", ["tables", "views"])
def test_listing_filters_the_schema_in_python_not_in_sql(
    command: str, hostile: str
) -> None:
    conn = _FakeConnection(routes=[_DB_DETAILS, _RELATIONS])
    assert getattr(_probe(conn), command)(schema=hostile, limit=200) == []
    _assert_never_sent(conn, hostile)


def test_skip_external_tables_is_honoured() -> None:
    conn = _FakeConnection(routes=[_DB_DETAILS, _RELATIONS])
    _probe(conn, skip_external_tables=True).tables(schema="ext_schema", limit=200)
    listing = [q for q in conn.executed if "tabletype" in q][0]
    assert "svv_external_tables" not in listing


def test_external_tables_are_listed_by_default() -> None:
    conn = _FakeConnection(routes=[_DB_DETAILS, _RELATIONS])
    assert _probe(conn).tables(schema="ext_schema", limit=200) == ["clicks"]
    listing = [q for q in conn.executed if "tabletype" in q][0]
    assert "svv_external_tables" in listing


def test_shared_database_lists_through_svv_redshift_tables() -> None:
    shared: Route = (
        "FROM svv_redshift_tables",
        _REL_COLUMNS,
        [["TABLE", "public", "orders", None]],
    )
    conn = _FakeConnection(routes=[_SHARED_DB, shared])
    assert _probe(conn).tables(schema="public", limit=200) == ["orders"]


def test_database_type_is_read_once_per_probe() -> None:
    conn = _FakeConnection(routes=[_DB_DETAILS, _RELATIONS])
    probe = _probe(conn)
    probe.tables(schema="public", limit=200)
    probe.views(schema="public", limit=200)
    assert sum("database_options" in q for q in conn.executed) == 1


def test_an_empty_schema_says_why_rather_than_looking_empty() -> None:
    conn = _FakeConnection(routes=[_DB_DETAILS, _RELATIONS])
    probe = _probe(conn)
    assert probe.views(schema="nothing_here", limit=200) == []
    assert probe.warnings


def test_a_failing_catalog_query_propagates() -> None:
    conn = _FakeConnection(routes=[_DB_DETAILS, _RELATIONS], fail_on="tabletype")
    with pytest.raises(RuntimeError):
        _probe(conn).tables(schema="public", limit=200)


def test_run_reports_kind_and_parent(monkeypatch: pytest.MonkeyPatch) -> None:
    from datahub.ingestion.agent.probe_methods import run_probe_method

    _connect_with(monkeypatch, _FakeConnection(routes=[_DB_DETAILS, _RELATIONS]))

    result = run_probe_method(
        "redshift", dict(_RECIPE), "views", {"schema": "public", "limit": 1}
    )

    assert result.kind == "View"
    assert result.parent_path == ["public"]
    assert result.result == ["v_orders"]
    assert result.truncated is True


def test_run_reports_containers_as_schemas(monkeypatch: pytest.MonkeyPatch) -> None:
    from datahub.ingestion.agent.probe_methods import run_probe_method

    _connect_with(monkeypatch, _FakeConnection(routes=[_SCHEMAS]))

    result = run_probe_method("redshift", dict(_RECIPE), "containers", {})

    assert result.kind == "Schema"
    assert result.result == ["public", "ext_schema"]


def test_a_catalog_timeout_is_not_reported_as_an_empty_listing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from datahub.ingestion.agent.probe_methods import run_probe_method

    _connect_with(
        monkeypatch,
        _FakeConnection(routes=[_DB_DETAILS, _RELATIONS], fail_on="tabletype"),
    )

    with pytest.raises(RuntimeError, match="statement timeout"):
        run_probe_method("redshift", dict(_RECIPE), "tables", {"schema": "public"})


def test_probe_methods_advertises_what_the_provider_serves() -> None:
    from datahub.ingestion.agent.probe_methods import list_probe_methods

    commands = {spec.command for spec in list_probe_methods("redshift")}
    assert {"containers", "tables", "views", "sql"} <= commands
    assert not commands & {"foreign_keys", "primary_key", "indexes", "table_comment"}


_COLUMN_FIELDS = [
    "schema",
    "table_name",
    "name",
    "encode",
    "type",
    "distkey",
    "sortkey",
    "notnull",
    "comment",
    "attnum",
    "default",
]
_COLUMNS: Route = (
    "pg_attribute",
    _COLUMN_FIELDS,
    [
        ["public", "orders", "id", "az64", "integer", True, 1, True, "order key", 1, None],
        ["public", "orders", "note", "lzo", "varchar(256)", False, 0, False, None, 2, "'n/a'"],
        ["public", "other", "x", None, "integer", False, 0, False, None, 1, None],
    ],
)  # fmt: skip


def test_columns_come_from_ingestion_column_query() -> None:
    conn = _FakeConnection(routes=[_DB_DETAILS, _COLUMNS, _SCHEMAS])
    cols = _probe(conn).columns(schema="public", table="orders")
    assert [c["name"] for c in cols] == ["id", "note"]
    # Upper-cased, as get_columns_for_schema hands it to ingestion.
    assert cols[0] == {
        "name": "id",
        "type": "INTEGER",
        "nullable": False,
        "default": None,
        "comment": "order key",
    }
    assert cols[1]["default"] == "'n/a'"


@pytest.mark.parametrize("hostile", _HOSTILE_NAMES)
def test_columns_refuses_a_schema_the_catalog_does_not_list(hostile: str) -> None:
    conn = _FakeConnection(routes=[_DB_DETAILS, _COLUMNS, _SCHEMAS])
    with pytest.raises(ValueError, match="containers"):
        _probe(conn).columns(schema=hostile, table="orders")
    _assert_never_sent(conn, hostile)
    # Refused before the column query is even built.
    assert not any("pg_attribute" in q for q in conn.executed)


@pytest.mark.parametrize("hostile", _HOSTILE_NAMES)
def test_columns_matches_the_table_in_python_not_in_sql(hostile: str) -> None:
    conn = _FakeConnection(routes=[_DB_DETAILS, _COLUMNS, _SCHEMAS])
    probe = _probe(conn)
    assert probe.columns(schema="public", table=hostile) == []
    _assert_never_sent(conn, hostile)
    assert probe.warnings


def test_columns_refuses_a_listed_schema_whose_name_would_break_the_query() -> None:
    # Validation against the catalog is what keeps caller input out of
    # list_columns' f-strings; a name the catalog itself returned with a quote
    # in it would still close the literal, so it is refused too.
    quoted = "it's_mine"
    schemas: Route = (
        "schema_type",
        _SCHEMAS[1],
        [[quoted, "local", None, None, None, None]],
    )
    conn = _FakeConnection(routes=[_DB_DETAILS, _COLUMNS, schemas])
    with pytest.raises(ValueError):
        _probe(conn).columns(schema=quoted, table="orders")
    assert not any("pg_attribute" in q for q in conn.executed)


def test_columns_on_a_shared_database_use_svv_redshift_columns() -> None:
    shared_cols: Route = (
        "SVV_REDSHIFT_COLUMNS",
        _COLUMN_FIELDS,
        [["public", "orders", "id", None, "integer", None, 0, False, None, 1, None]],
    )
    conn = _FakeConnection(routes=[_SHARED_DB, shared_cols, _SCHEMAS])
    cols = _probe(conn).columns(schema="public", table="orders")
    assert [c["name"] for c in cols] == ["id"]


def test_columns_of_an_unknown_table_are_empty_with_a_reason() -> None:
    conn = _FakeConnection(routes=[_DB_DETAILS, _COLUMNS, _SCHEMAS])
    probe = _probe(conn)
    assert probe.columns(schema="public", table="missing") == []
    assert probe.warnings


def test_an_unknown_schema_exits_as_a_bad_argument(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from datahub.ingestion.agent.probe_methods import run_probe_method

    _connect_with(
        monkeypatch, _FakeConnection(routes=[_DB_DETAILS, _COLUMNS, _SCHEMAS])
    )

    # ValueError is the framework's "you named something that isn't there"
    # (exit 2), not a connection failure.
    with pytest.raises(ValueError):
        run_probe_method(
            "redshift",
            dict(_RECIPE),
            "columns",
            {"schema": "missing", "table": "orders"},
        )


def test_view_definition_is_the_ddl_ingestion_publishes() -> None:
    conn = _FakeConnection(routes=[_DB_DETAILS, _RELATIONS])
    probe = _probe(conn)
    assert probe.view_definition(schema="public", view="v_orders") == (
        "select * from orders"
    )
    assert probe.view_definition(schema="public", view="orders") is None


@pytest.mark.parametrize("hostile", _HOSTILE_NAMES)
def test_view_definition_matches_names_in_python_not_in_sql(hostile: str) -> None:
    conn = _FakeConnection(routes=[_DB_DETAILS, _RELATIONS])
    probe = _probe(conn)
    assert probe.view_definition(schema=hostile, view="v_orders") is None
    assert probe.view_definition(schema="public", view=hostile) is None
    _assert_never_sent(conn, hostile)


def test_probe_methods_advertises_the_per_object_commands() -> None:
    from datahub.ingestion.agent.probe_methods import list_probe_methods

    commands = {spec.command for spec in list_probe_methods("redshift")}
    assert {"columns", "view_definition"} <= commands
