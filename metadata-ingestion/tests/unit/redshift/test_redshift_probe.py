from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple, cast

import pytest
import redshift_connector

from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.probe_methods import list_probe_methods, run_probe_method
from datahub.ingestion.agent.sql_passthrough import PROBE_QUERY_LABEL
from datahub.ingestion.agent.verdicts import ProbeConnectionError
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.redshift.config import RedshiftConfig
from datahub.ingestion.source.redshift.redshift import RedshiftSource
from datahub.ingestion.source.redshift.redshift_probe import RedshiftMetadataProbe
from datahub.metadata.schema_classes import SubTypesClass
from datahub.metadata.urns import DatasetUrn
from tests.test_helpers.probe_parity import (
    EmittedIndex,
    FanOut,
    JudgedRecord,
    ParityListing,
    assert_probe_parity,
)

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
        self,
        routes: Sequence[Route] = (),
        fail_on: Optional[str] = None,
        close_error: Optional[Exception] = None,
    ) -> None:
        self.routes = list(routes)
        self.fail_on = fail_on
        self.close_error = close_error
        self.executed: List[str] = []
        self.bound: List[Any] = []
        self.autocommit = True
        self.closed = False

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def close(self) -> None:
        self.closed = True
        if self.close_error is not None:
            raise self.close_error


# No password: the fake connection never authenticates, and an IAM-style
# recipe is the case the ingestion builder exists to get right.
_RECIPE: Dict[str, Any] = {
    "host_port": "cluster.example:5439",
    "database": "dev",
    "username": "probe_user",
}


def _probe(conn: _FakeConnection, **overrides: Any) -> RedshiftMetadataProbe:
    return RedshiftMetadataProbe(
        cast(redshift_connector.Connection, conn),
        RedshiftConfig.model_validate({**_RECIPE, **overrides}),
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
    probe.__exit__(None, None, None)


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


def test_a_failing_close_does_not_replace_the_probe_error() -> None:
    # A broken connection (after a statement timeout, say) can refuse to
    # close; the caller must still see why the probe failed.
    conn = _FakeConnection(close_error=redshift_connector.InterfaceError("broken"))
    with pytest.raises(KeyError), _probe(conn):
        raise KeyError("the probe's own error")
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
# The columns RedshiftCommonQuery.list_tables returns.
_REL_COLUMNS = [
    "tabletype",
    "schema",
    "relname",
    "creation_time",
    "diststyle",
    "owner_name",
    "location",
    "parameters",
    "input_format",
    "output_format",
    "serde_parameters",
    "table_description",
    "view_definition",
]


def _rel(
    tabletype: str, schema: str, name: str, ddl: Optional[str] = None
) -> List[Any]:
    row: Dict[str, Any] = dict.fromkeys(_REL_COLUMNS)
    row.update(tabletype=tabletype, schema=schema, relname=name, view_definition=ddl)
    return [row[c] for c in _REL_COLUMNS]


_RELATIONS: Route = (
    "tabletype",
    _REL_COLUMNS,
    [
        _rel("TABLE", "public", "orders"),
        _rel("VIEW", "public", "v_orders", "select * from orders"),
        _rel("MATERIALIZED VIEW", "public", "mv_orders", "select 1"),
        _rel("FOREIGN TABLE", "public", "f_orders"),
        _rel("EXTERNAL_TABLE", "ext_schema", "clicks"),
        _rel("TABLE", "other", "unrelated"),
    ],
)

# What a schema-taking listing reads: the database type, the schema listing it
# checks the caller's schema against, and the relations.
_LISTING: List[Route] = [_DB_DETAILS, _RELATIONS, _SCHEMAS]

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
    conn = _FakeConnection(routes=_LISTING)
    probe = _probe(conn)
    assert probe.tables(schema="public", limit=200) == ["orders", "f_orders"]
    assert probe.views(schema="public", limit=200) == ["v_orders", "mv_orders"]
    assert not any("pg_user" in q for q in conn.executed)


def test_listing_never_enriches_from_operational_history() -> None:
    # get_tables_and_views would first run enrich_tables (svv_table_info joined
    # to stl_insert): grants a metadata probe should not need.
    conn = _FakeConnection(routes=_LISTING)
    _probe(conn).tables(schema="public", limit=200)
    assert not any("stl_insert" in q or "svv_table_info" in q for q in conn.executed)


@pytest.mark.parametrize("hostile", _HOSTILE_NAMES)
@pytest.mark.parametrize("command", ["tables", "views"])
def test_listing_refuses_a_schema_the_catalog_does_not_list(
    command: str, hostile: str
) -> None:
    conn = _FakeConnection(routes=_LISTING)
    with pytest.raises(ValueError):
        getattr(_probe(conn), command)(schema=hostile, limit=200)
    _assert_never_sent(conn, hostile)


def test_skip_external_tables_is_honoured() -> None:
    conn = _FakeConnection(routes=_LISTING)
    _probe(conn, skip_external_tables=True).tables(schema="ext_schema", limit=200)
    listing = [q for q in conn.executed if "tabletype" in q][0]
    assert "svv_external_tables" not in listing


def test_external_tables_are_listed_by_default() -> None:
    conn = _FakeConnection(routes=_LISTING)
    assert _probe(conn).tables(schema="ext_schema", limit=200) == ["clicks"]
    listing = [q for q in conn.executed if "tabletype" in q][0]
    assert "svv_external_tables" in listing


def test_shared_database_lists_through_svv_redshift_tables() -> None:
    shared: Route = (
        "FROM svv_redshift_tables",
        _REL_COLUMNS,
        [_rel("TABLE", "public", "orders")],
    )
    conn = _FakeConnection(routes=[_SHARED_DB, shared, _SCHEMAS])
    assert _probe(conn).tables(schema="public", limit=200) == ["orders"]


def test_database_type_is_read_once_per_probe() -> None:
    conn = _FakeConnection(routes=_LISTING)
    probe = _probe(conn)
    probe.tables(schema="public", limit=200)
    probe.views(schema="public", limit=200)
    assert sum("database_options" in q for q in conn.executed) == 1


def test_an_empty_schema_says_why_rather_than_looking_empty() -> None:
    schemas: Route = (
        "schema_type",
        _SCHEMAS[1],
        [["empty_schema", "local", None, None, None, None]],
    )
    conn = _FakeConnection(routes=[_DB_DETAILS, _RELATIONS, schemas])
    probe = _probe(conn)
    assert probe.views(schema="empty_schema", limit=200) == []
    assert probe.warnings


@pytest.mark.parametrize("command", ["tables", "views"])
def test_an_unlisted_schema_is_a_bad_argument_for_listings_too(
    monkeypatch: pytest.MonkeyPatch, command: str
) -> None:
    _connect_with(monkeypatch, _FakeConnection(routes=_LISTING))

    # The same answer `columns` gives (exit 2), not an empty listing at exit 0.
    with pytest.raises(ValueError):
        run_probe_method("redshift", dict(_RECIPE), command, {"schema": "missing"})


def test_a_schema_differing_only_in_case_points_at_the_listed_one() -> None:
    conn = _FakeConnection(routes=_LISTING)
    with pytest.raises(ValueError) as refused:
        _probe(conn).tables(schema="Public", limit=200)
    assert "'public'" in str(refused.value)


def test_a_failing_catalog_query_propagates() -> None:
    conn = _FakeConnection(routes=_LISTING, fail_on="tabletype")
    with pytest.raises(RuntimeError):
        _probe(conn).tables(schema="public", limit=200)


def test_run_reports_kind_and_parent(monkeypatch: pytest.MonkeyPatch) -> None:
    _connect_with(monkeypatch, _FakeConnection(routes=_LISTING))

    result = run_probe_method(
        "redshift", dict(_RECIPE), "views", {"schema": "public", "limit": 1}
    )

    assert result.kind == "View"
    assert result.parent_path == ["public"]
    assert result.result == ["v_orders"]
    assert result.truncated is True


def test_run_reports_containers_as_schemas(monkeypatch: pytest.MonkeyPatch) -> None:
    _connect_with(monkeypatch, _FakeConnection(routes=[_SCHEMAS]))

    result = run_probe_method("redshift", dict(_RECIPE), "containers", {})

    assert result.kind == "Schema"
    assert result.result == ["public", "ext_schema"]


def test_a_catalog_timeout_is_not_reported_as_an_empty_listing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _connect_with(
        monkeypatch,
        _FakeConnection(routes=_LISTING, fail_on="tabletype"),
    )

    # The driver's text is withheld as foreign, but the listing still fails
    # (exit 3) rather than coming back empty.
    with pytest.raises(ProbeConnectionError) as caught:
        run_probe_method("redshift", dict(_RECIPE), "tables", {"schema": "public"})
    assert "statement timeout" not in str(caught.value)


def test_probe_methods_advertises_what_the_provider_serves() -> None:
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
    with pytest.raises(ValueError):
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


def test_a_table_differing_only_in_case_points_at_the_listed_one() -> None:
    conn = _FakeConnection(routes=[_DB_DETAILS, _COLUMNS, _SCHEMAS])
    probe = _probe(conn)
    assert probe.columns(schema="public", table="Orders") == []
    assert any("'orders'" in w for w in probe.warnings)


def test_columns_of_an_unknown_table_are_empty_with_a_reason() -> None:
    conn = _FakeConnection(routes=[_DB_DETAILS, _COLUMNS, _SCHEMAS])
    probe = _probe(conn)
    assert probe.columns(schema="public", table="missing") == []
    assert probe.warnings


def test_an_unknown_schema_exits_as_a_bad_argument(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
    conn = _FakeConnection(routes=_LISTING)
    assert _probe(conn).view_definition(schema="public", view="v_orders") == (
        "select * from orders"
    )


def test_view_definition_of_a_table_is_null_and_says_so() -> None:
    conn = _FakeConnection(routes=_LISTING)
    probe = _probe(conn)
    assert probe.view_definition(schema="public", view="orders") is None
    assert probe.warnings


@pytest.mark.parametrize("view", ["missing", "V_orders"])
def test_view_definition_of_an_unlisted_view_is_a_bad_argument(view: str) -> None:
    # Not a null: that would read as "the catalog holds no SQL for it".
    conn = _FakeConnection(routes=_LISTING)
    with pytest.raises(ValueError):
        _probe(conn).view_definition(schema="public", view=view)


def test_view_definition_points_at_a_view_differing_only_in_case() -> None:
    conn = _FakeConnection(routes=_LISTING)
    with pytest.raises(ValueError) as refused:
        _probe(conn).view_definition(schema="public", view="V_orders")
    assert "'v_orders'" in str(refused.value)


@pytest.mark.parametrize("hostile", _HOSTILE_NAMES)
def test_view_definition_matches_names_in_python_not_in_sql(hostile: str) -> None:
    conn = _FakeConnection(routes=_LISTING)
    probe = _probe(conn)
    with pytest.raises(ValueError):
        probe.view_definition(schema=hostile, view="v_orders")
    with pytest.raises(ValueError):
        probe.view_definition(schema="public", view=hostile)
    _assert_never_sent(conn, hostile)


def test_probe_methods_advertises_the_per_object_commands() -> None:
    commands = {spec.command for spec in list_probe_methods("redshift")}
    assert {"columns", "view_definition"} <= commands


def test_a_redshift_error_is_labelled_with_its_sqlstate_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    planted = "PLANTED relation text"
    error = redshift_connector.ProgrammingError(
        {"S": "ERROR", "C": "42P01", "M": planted}
    )
    assert RedshiftMetadataProbe.probe_error_code(error) == "SQLSTATE 42P01"
    # Another library's error, or one without the fields, carries no code.
    assert RedshiftMetadataProbe.probe_error_code(RuntimeError({"C": "42P01"})) is None
    assert (
        RedshiftMetadataProbe.probe_error_code(redshift_connector.InterfaceError("x"))
        is None
    )

    class _FailingCursor(_FakeCursor):
        def execute(self, query: str, args: Any = None) -> "_FakeCursor":
            if "schema_type" in query:
                raise error
            return super().execute(query, args)

    class _FailingConnection(_FakeConnection):
        def cursor(self) -> _FakeCursor:
            return _FailingCursor(self)

    _connect_with(monkeypatch, _FailingConnection(routes=[_SCHEMAS]))
    with pytest.raises(ProbeConnectionError) as caught:
        run_probe_method("redshift", dict(_RECIPE), "containers", {})
    assert "SQLSTATE 42P01" in str(caught.value)
    assert planted not in str(caught.value)


def _view_verdict(**recipe: Any) -> Tuple[bool, Optional[str]]:
    result = check_filters(
        source_type="redshift",
        config_dict={**_RECIPE, **recipe},
        kind="View",
        parent_path=["public"],
        names=["v_orders"],
    )
    verdict = result.results[0]
    return verdict.included, verdict.excluded_by


def test_a_view_is_also_judged_by_table_pattern() -> None:
    assert _view_verdict(
        table_pattern={"allow": [r"^dev\.public\.orders$"]},
        view_pattern={"allow": [".*"]},
    ) == (False, "table_pattern")


def test_a_view_refused_by_both_patterns_names_view_pattern() -> None:
    # Ingestion checks view_pattern first.
    assert _view_verdict(
        table_pattern={"deny": [".*"]}, view_pattern={"deny": [".*"]}
    ) == (False, "view_pattern")


def test_try_allow_on_views_still_meets_table_pattern() -> None:
    # --try-allow replaces only view_pattern; table_pattern still decides, as
    # it would in a run with the edited recipe.
    result = check_filters(
        source_type="redshift",
        config_dict={
            **_RECIPE,
            "table_pattern": {"deny": [r"^dev\.public\.v_orders$"]},
            "view_pattern": {"allow": ["^nothing$"]},
        },
        kind="View",
        parent_path=["public"],
        names=["v_orders"],
        try_allow=[".*"],
    )
    assert result.results[0].excluded_by == "table_pattern"


def test_switching_views_off_outranks_table_pattern() -> None:
    assert _view_verdict(include_views=False, table_pattern={"deny": [".*"]}) == (
        False,
        "include_views",
    )


def test_a_view_in_a_denied_schema_names_schema_pattern() -> None:
    assert _view_verdict(
        schema_pattern={"deny": ["^public$"]}, table_pattern={"deny": [".*"]}
    ) == (False, "schema_pattern")


def test_a_table_is_not_judged_by_view_pattern() -> None:
    result = check_filters(
        source_type="redshift",
        config_dict={**_RECIPE, "view_pattern": {"deny": [".*"]}},
        kind="Table",
        parent_path=["public"],
        names=["orders"],
    )
    assert result.results[0].included


class _AnswerEverythingConnection(_FakeConnection):
    """Routes what it knows and answers every other query with no rows, so a
    whole ingestion run can go through it."""

    def cursor(self) -> _FakeCursor:
        return _EmptyByDefaultCursor(self)


class _EmptyByDefaultCursor(_FakeCursor):
    def execute(self, query: str, args: Any = None) -> "_FakeCursor":
        try:
            return super().execute(query, args)
        except AssertionError:
            self.description = []
            self._rows = []
            return self


_PARITY_SCHEMAS: Route = (
    "schema_type",
    _SCHEMAS[1],
    [
        ["public", "local", None, None, None, None],
        ["scratch", "local", None, None, None, None],
    ],
)
_PARITY_RELATIONS: Route = (
    "tabletype",
    _REL_COLUMNS,
    [
        _rel("TABLE", "public", "orders"),
        _rel("TABLE", "public", "orders_tmp"),
        _rel("VIEW", "public", "v_orders", "select * from orders"),
        _rel("VIEW", "public", "v_denied", "select 1"),
        _rel("VIEW", "public", "v_hidden", "select 2"),
        _rel("MATERIALIZED VIEW", "public", "mv_orders", "select 3"),
        _rel("TABLE", "scratch", "s1"),
    ],
)
_PARITY_ROUTES: List[Route] = [_DB_DETAILS, _PARITY_RELATIONS, _PARITY_SCHEMAS]


def _parity_connection(config: RedshiftConfig) -> _FakeConnection:
    return _AnswerEverythingConnection(routes=_PARITY_ROUTES)


def _ingest(recipe: Dict[str, object]) -> EmittedIndex:
    source = RedshiftSource(
        RedshiftConfig.model_validate(recipe),
        PipelineContext(run_id="redshift-probe-parity"),
    )
    index = EmittedIndex.from_workunits(source.get_workunits())
    assert not source.report.failures
    return index


def _datasets(sub_type: str) -> Callable[[EmittedIndex], Set[str]]:
    def emitted(index: EmittedIndex) -> Set[str]:
        return {
            DatasetUrn.from_string(urn).name
            for urn in index.urns("dataset", with_aspect=SubTypesClass)
            if any(
                isinstance(aspect, SubTypesClass) and sub_type in aspect.typeNames
                for aspect in index.aspects[urn]
            )
        }

    return emitted


def _qualified(record: JudgedRecord) -> str:
    return ".".join(("dev", *record.parent_path, record.name))


def test_probe_verdicts_match_ingestion(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        RedshiftSource, "get_redshift_connection", staticmethod(_parity_connection)
    )
    recipe: Dict[str, object] = {
        **_RECIPE,
        "schema_pattern": {"deny": ["^scratch$"]},
        "table_pattern": {"deny": [r".*\.orders_tmp$", r".*\.v_hidden$"]},
        "view_pattern": {"deny": [r".*\.v_denied$"]},
        "include_table_lineage": False,
        "include_usage_statistics": False,
    }
    fan_out = FanOut("containers", "schema")

    report = assert_probe_parity(
        "redshift",
        recipe,
        _ingest,
        [
            ParityListing(
                "schemas",
                "containers",
                emitted=lambda index: index.container_names(
                    DatasetContainerSubTypes.SCHEMA
                ),
            ),
            ParityListing(
                "tables",
                "tables",
                emitted=_datasets(DatasetSubTypes.TABLE),
                fan_out=fan_out,
                identity=_qualified,
            ),
            ParityListing(
                "views",
                "views",
                emitted=_datasets(DatasetSubTypes.VIEW),
                fan_out=fan_out,
                identity=_qualified,
            ),
        ],
    )

    assert report.excluded_by("views") == {
        "dev.public.v_denied": "view_pattern",
        "dev.public.v_hidden": "table_pattern",
    }
    assert report.excluded_by("tables") == {
        "dev.public.orders_tmp": "table_pattern",
        "dev.scratch.s1": "schema_pattern",
    }
