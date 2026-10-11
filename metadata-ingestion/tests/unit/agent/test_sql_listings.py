"""The SQL family's listings, and the parent that comes with them.

Before these existed, the only way to find out what tables a source had was to write
a catalog query -- so every caller had to know its dialect's catalog, remember the
schema it queried, and pass that schema back as --parent to get verdicts. DB2 and
Vertica could not do it at all, because sqlglot has no dialect for either and `sql`
therefore fails closed.
"""

from typing import Dict, List, Type

import pytest

from datahub.ingestion.agent.probe_methods import (
    ProbeMethodResult,
    ProbeMethodSpec,
    _iter_specs,
    config_class_for,
    probe_method,
    run_probe_method,
)
from datahub.ingestion.agent.verdicts import ProbeArgumentError
from datahub.ingestion.source.sql import sqlalchemy_probe
from datahub.ingestion.source.sql.sql_config import SQLCommonConfig
from datahub.ingestion.source.sql.sqlalchemy_probe import SqlAlchemyMetadataProbe


class _FakeInspector:
    def __init__(self, materialized_views_fail: bool = False) -> None:
        self.asked_for: List[str] = []
        self.materialized_views_fail = materialized_views_fail

    def get_schema_names(self) -> List[str]:
        return ["analytics", "information_schema"]

    def get_table_names(self, schema: str) -> List[str]:
        self.asked_for.append(f"tables:{schema}")
        return ["orders", "shipments"] if schema == "analytics" else []

    def get_view_names(self, schema: str) -> List[str]:
        self.asked_for.append(f"views:{schema}")
        return ["orders_v"] if schema == "analytics" else []

    def get_materialized_view_names(self, schema: str) -> List[str]:
        self.asked_for.append(f"materialized_views:{schema}")
        if self.materialized_views_fail:
            raise RuntimeError("permission denied for relation pg_class")
        return ["orders_mv"] if schema == "analytics" else []


def _sql_config_class(source_type: str) -> Type[SQLCommonConfig]:
    config_cls = config_class_for(source_type)
    assert config_cls is not None and issubclass(config_cls, SQLCommonConfig)
    return config_cls


def _probe(source_type: str = "postgres") -> SqlAlchemyMetadataProbe:
    # __new__ because __init__ builds an engine; these commands only touch the
    # Inspector, which is what ingestion enumerates through too.
    probe = SqlAlchemyMetadataProbe.__new__(SqlAlchemyMetadataProbe)
    probe._insp = _FakeInspector()  # type: ignore[assignment]
    probe.container_kind = str(_sql_config_class(source_type).probe_container_kind())
    return probe


def _spec(command: str) -> ProbeMethodSpec:
    spec = getattr(getattr(SqlAlchemyMetadataProbe, command), "__probe_command__", None)
    assert isinstance(spec, ProbeMethodSpec)
    return spec


def test_a_listing_declares_the_container_it_was_asked_about():
    # The whole point: a caller that passed `schema` to list tables should not have to
    # restate it as --parent, which is how it ends up missing -- and a missing parent
    # gives MySQL the opposite verdict.
    assert _spec("tables").parent_params == ("schema",)
    assert _spec("views").parent_params == ("schema",)
    # `containers` has none: it is the top of the walk.
    assert _spec("containers").parent_params == ()


def test_declaring_a_parent_param_that_does_not_exist_is_rejected_at_import():

    with pytest.raises(ValueError, match="no such parameter"):

        class Broken:
            @probe_method(parent_params=("shema",))
            def tables(self, schema: str) -> List[str]:
                """Typo in the declared parent parameter."""
                return []


def test_tables_and_views_are_separate_listings():
    # information_schema.tables returns both kinds in one result set, so a caller
    # judging that listing as tables gives a view a verdict from table_pattern when
    # ingestion would have used view_pattern.
    probe = _probe()
    assert probe.tables("analytics") == ["orders", "shipments"]
    assert probe.views("analytics") == ["orders_v"]
    assert _spec("tables").kind == "Table"
    assert _spec("views").kind == "View"


def _run_views(
    monkeypatch: pytest.MonkeyPatch,
    source_type: str,
    config: Dict[str, object],
    inspector: _FakeInspector,
) -> ProbeMethodResult:
    # for_config builds a real engine (over SQLite); the listing is then read
    # through the fake Inspector, so only what the config wires in is tested.
    monkeypatch.setattr(sqlalchemy_probe, "inspect", lambda engine: inspector)
    return run_probe_method(source_type, config, "views", {"schema": "analytics"})


@pytest.mark.parametrize(
    "source_type, config, views",
    [
        # PostgresSource._get_view_names ingests materialized views as views,
        # judged by view_pattern.
        (
            "postgres",
            {"host_port": "h:5432", "sqlalchemy_uri": "sqlite://"},
            ["orders_v", "orders_mv"],
        ),
        # Ingestion lists get_view_names alone everywhere else, the generic
        # source on a Postgres server included.
        (
            "sqlalchemy",
            {"connect_uri": "sqlite://", "platform": "postgres"},
            ["orders_v"],
        ),
        (
            "mysql",
            {"host_port": "h:3306", "sqlalchemy_uri": "sqlite://"},
            ["orders_v"],
        ),
    ],
)
def test_views_lists_what_the_connectors_own_view_listing_ingests(
    monkeypatch: pytest.MonkeyPatch,
    source_type: str,
    config: Dict[str, object],
    views: List[str],
) -> None:
    result = _run_views(monkeypatch, source_type, config, _FakeInspector())
    assert result.result == views
    assert result.kind == "View"


def test_a_materialized_view_listing_that_fails_warns_as_ingestion_does(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = _run_views(
        monkeypatch,
        "postgres",
        {"host_port": "h:5432", "sqlalchemy_uri": "sqlite://"},
        _FakeInspector(materialized_views_fail=True),
    )
    assert result.result == ["orders_v"]
    assert any("materialized views" in w.lower() for w in result.warnings)
    assert not any("pg_class" in w for w in result.warnings)


def test_containers_are_reported_as_the_kind_the_recipes_tier_makes_them():
    """get_schema_names() means different things per tier, and the pattern differs too.

    Three-tier sources filter schemas with schema_pattern; two-tier ones return
    databases and filter them with database_pattern, where schema_pattern is
    deprecated. One provider class serves both, so the kind cannot be a class-level
    declaration -- it comes from the config.
    """
    kinds: Dict[str, str] = {
        source_type: str(_sql_config_class(source_type).probe_container_kind())
        for source_type in ("postgres", "mssql", "snowflake", "mysql", "hive")
    }
    assert kinds["postgres"] == "Schema"
    assert kinds["mssql"] == "Schema"
    assert kinds["snowflake"] == "Schema"
    assert kinds["mysql"] == "Database"
    assert kinds["hive"] == "Database"


def test_the_config_class_declares_the_kind_containers_reports():
    assert _sql_config_class("postgres").probe_kind_overrides() == {
        "containers": "Schema"
    }
    assert _sql_config_class("mysql").probe_kind_overrides() == {
        "containers": "Database"
    }
    # The spec itself declares none, because the provider class cannot know it.
    assert _spec("containers").kind is None


def test_a_two_tier_provider_names_its_containers_databases():
    from datahub.ingestion.source.sql.mysql import MySQLConfig

    # An in-memory engine: for_config builds a real one, and only the kind
    # the config declares is under test.
    config = MySQLConfig.model_validate(
        {"host_port": "h:3306", "sqlalchemy_uri": "sqlite://"}
    )
    probe = SqlAlchemyMetadataProbe.for_config(config)
    try:
        assert probe.container_kind == "Database"
        with pytest.raises(ProbeArgumentError, match="no database named"):
            probe.tables("nope")
    finally:
        probe.__exit__(None, None, None)


def test_a_denied_container_is_still_listed():
    # information_schema would be dropped by default_schemas, and reporting only what
    # survives would leave `probe filter` nothing to explain.
    assert "information_schema" in _probe().containers()


def test_listings_are_bounded_by_the_framework():
    for command in ("containers", "tables", "views"):
        assert _spec(command).row_limit_param == "limit"


def test_every_sql_connector_can_now_enumerate_without_a_query():
    """Including the ones `sql` cannot serve at all.

    sqlglot has no dialect for DB2 or Vertica, so the gate refuses every query on
    them; before these listings existed, their probe could enumerate nothing.
    """
    for source_type in ("db2", "vertica", "oracle", "teradata", "postgres", "mysql"):
        commands = {
            c
            for c, _ in _iter_specs(
                _sql_config_class(source_type).probe_provider_class()
            )
        }
        assert {"containers", "tables", "views"} <= commands, source_type


def test_passthrough_sql_is_not_reparsed_for_bind_parameters():
    """execute(text(sql)) parses the SQL for `:name` binds, and the regex
    fires on a colon after any non-word character -- inside a string literal,
    or an array slice. A query the gate had already cleared then died at
    execute with StatementError, which recipe_cli maps to EXIT_CONNECTION:
    the agent is told the source is unreachable when the connection was fine.
    Verified against a live MySQL before the fix -- exit 3.

    Asserted on the call rather than the outcome, because the outcome needs a
    server: what matters is that the driver gets the string untouched.
    """
    sent = []

    class _Result:
        def fetchmany(self, n):
            return []

        def keys(self):
            return []

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def exec_driver_sql(self, query):
            sent.append(query)
            return _Result()

        def execute(self, *args, **kwargs):  # pragma: no cover
            raise AssertionError(
                "execute(text(...)) reparses the SQL; use exec_driver_sql"
            )

    class _Engine:
        def connect(self):
            return _Conn()

    probe = SqlAlchemyMetadataProbe.__new__(SqlAlchemyMetadataProbe)
    probe._engine = _Engine()  # type: ignore[assignment]

    sql = "SELECT * FROM information_schema.columns WHERE column_default = '{\"k\":v}'"
    probe.execute_catalog_query(sql, limit=10)
    assert sent == [sql], "the driver must receive the query verbatim"


def test_a_two_tier_recipe_lists_only_the_databases_it_reads():
    """The pin is the promise that `containers` matches what ingestion reads.

    It was taken from the singular `database` only, and Teradata's documented
    way to name several is the plural `databases` list -- "List of databases
    to ingest. If not specified, all databases will be ingested." A recipe
    using it left the pin empty, so `containers` reported every database on
    the server while TeradataSource.get_inspectors() enumerated two. The
    probe advertised containers ingestion would never read, which is the one
    thing this command exists not to do.
    """
    probe = _probe("mysql")

    # Nothing pinned: every container the server shows, which is right for a
    # recipe that names none.
    probe.pinned_containers = frozenset()
    assert probe.containers() == ["analytics", "information_schema"]

    # One named, singular or plural -- same answer either way.
    probe.pinned_containers = frozenset({"analytics"})
    assert probe.containers() == ["analytics"]

    # Several named: all of them, and nothing else.
    probe.pinned_containers = frozenset({"analytics", "information_schema"})
    assert probe.containers() == ["analytics", "information_schema"]

    # A typo is still absent rather than echoed back, which is why the pin
    # filters the server's listing instead of replacing it.
    probe.pinned_containers = frozenset({"analytcis"})
    assert probe.containers() == []
    assert SqlAlchemyMetadataProbe.pinned_containers == frozenset()


def test_the_pin_reads_both_the_singular_and_the_plural_field():
    """for_config's half of the same contract, without building an engine."""
    from types import SimpleNamespace

    from datahub.ingestion.source.sql.sqlalchemy_probe import _pinned_containers

    two_tier = "Database"
    assert _pinned_containers(
        SimpleNamespace(database="one", databases=None), two_tier
    ) == frozenset({"one"})
    assert _pinned_containers(
        SimpleNamespace(database=None, databases=["a", "b"]), two_tier
    ) == frozenset({"a", "b"})
    assert (
        _pinned_containers(SimpleNamespace(database=None, databases=None), two_tier)
        == frozenset()
    )
    # Both set: the singular wins. That is the precedence
    # TeradataSource.get_inspectors applies -- `[self.config.database]` when
    # it is set, the plural list only otherwise -- so unioning the two
    # reported databases ingestion never opens, which is this pin's own bug
    # one size smaller.
    assert _pinned_containers(
        SimpleNamespace(database="one", databases=["a", "b"]), two_tier
    ) == frozenset({"one"})
    # Three-tier: `database` names the connection's database, not a filter
    # over the schemas `containers` returns, so nothing is pinned there.
    assert (
        _pinned_containers(SimpleNamespace(database="one", databases=None), "Schema")
        == frozenset()
    )


def test_doris_probes_the_catalog_ingestion_reads():
    """The probe dialled a different catalog than ingestion.

    Ingestion passes the QUALIFIED `catalog.database` as current_db for an
    external catalog (DorisSource._qualified_database), because that is what
    Doris expects over the MySQL protocol. The probe built its engine from
    `config.get_sql_alchemy_url()` with no argument, which produced the bare
    database -- so it landed in the session's default (internal) catalog and
    read a different `sales` than ingestion reads.

    Fixed with a probe-scoped hook rather than by defaulting
    get_sql_alchemy_url(), which is the second half of this test. Defaulting
    reached MySQLSource._usage_connection -- inherited by DorisSource with no
    override, and include_usage_statistics lives on MySQLConfig -- so a Doris
    recipe with a catalog and usage enabled had its usage connection moved to
    a different catalog by a change meant for the probe.
    """
    from datahub.ingestion.source.sql.doris.doris_source import DorisConfig, DorisSource

    config = DorisConfig.model_validate(
        {
            "host_port": "h:9030",
            "username": "u",
            "database": "iceberg_catalog.sales",
        }
    )
    assert (config.catalog, config.database) == ("iceberg_catalog", "sales")

    source = DorisSource.__new__(DorisSource)
    source.config = config
    source._session_catalog = config.catalog
    source._catalog_detection_failed = False
    assert config.database is not None
    ingestion_url = config.get_sql_alchemy_url(
        current_db=source._qualified_database(config.database)
    )

    # The probe dials what ingestion dials.
    assert config.probe_sql_alchemy_url() == ingestion_url

    # And every other caller is untouched. _usage_connection calls this bare;
    # it must still get the unqualified database it always got.
    assert config.get_sql_alchemy_url().endswith("/sales")
    assert config.get_sql_alchemy_url() != ingestion_url

    # An internal-catalog recipe has no catalog to qualify with, so even the
    # probe gets the bare name.
    plain = DorisConfig.model_validate(
        {"host_port": "h:9030", "username": "u", "database": "sales"}
    )
    assert plain.probe_sql_alchemy_url() == plain.get_sql_alchemy_url()


def test_doris_reports_the_database_spelling_ingestion_matches():
    """Which spelling the Inspector returns on an external-catalog
    connection is not settled -- ingestion enumerates with SHOW DATABASES
    after SWITCH and never through the Inspector, so the connector does not
    answer it.

    The first attempt at this accepted BOTH spellings in the pin. That
    fixed the filtering and broke what came after: `containers` output is
    passed straight back as --parent, so a qualified name would have
    get_identifier build `catalog.database.table` while ingestion matches
    `database.table` -- and a listing carrying both spellings would report
    one database twice.

    Normalizing instead. Ingestion strips the prefix
    (_short_database_name), so the probe reports the stripped form and the
    pin needs only one spelling.
    """
    from datahub.ingestion.source.sql.doris.doris_source import DorisConfig
    from datahub.ingestion.source.sql.sqlalchemy_probe import (
        _container_normalizer,
        _pinned_containers,
    )

    config = DorisConfig.model_validate(
        {"host_port": "h:9030", "username": "u", "database": "iceberg_catalog.sales"}
    )
    # One spelling, the one ingestion matches on.
    assert _pinned_containers(config, str(config.probe_container_kind())) == {"sales"}

    probe = _probe("mysql")
    probe.pinned_containers = frozenset({"sales"})
    probe.container_normalizer = _container_normalizer(config)  # type: ignore[assignment]

    # Whichever spelling the server lists, the probe reports the short one.
    probe._insp.get_schema_names = lambda **kw: ["iceberg_catalog.sales", "other"]  # type: ignore[method-assign]
    assert probe.containers() == ["sales"]
    probe._insp.get_schema_names = lambda **kw: ["sales", "other"]  # type: ignore[method-assign]
    assert probe.containers() == ["sales"]

    # And both spellings in one listing are one database, not two.
    probe._insp.get_schema_names = lambda **kw: ["sales", "iceberg_catalog.sales"]  # type: ignore[method-assign]
    assert probe.containers() == ["sales"]

    # An internal-catalog recipe normalizes to identity.
    plain = DorisConfig.model_validate(
        {"host_port": "h:9030", "username": "u", "database": "sales"}
    )
    assert _container_normalizer(plain)("sales") == "sales"
    # A name that merely starts with something dotted is left alone.
    assert _container_normalizer(config)("other_catalog.sales") == "other_catalog.sales"


def test_closing_the_probe_disposes_the_engine_then_runs_the_base_closers() -> None:
    events: List[str] = []

    class _Engine:
        def dispose(self) -> None:
            events.append("dispose")

    probe = SqlAlchemyMetadataProbe.__new__(SqlAlchemyMetadataProbe)
    probe._engine = _Engine()  # type: ignore[assignment]
    probe._on_exit(lambda: events.append("closer"))
    probe.__exit__(None, None, None)
    assert events == ["dispose", "closer"]
