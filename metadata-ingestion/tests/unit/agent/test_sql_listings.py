"""The SQL family's listings, and the parent that comes with them.

Before these existed, the only way to find out what tables a source had was to write
a catalog query -- so every caller had to know its dialect's catalog, remember the
schema it queried, and pass that schema back as --parent to get verdicts. DB2 and
Vertica could not do it at all, because sqlglot has no dialect for either and `sql`
therefore fails closed.
"""

from typing import Dict, List

import pytest

from datahub.ingestion.agent.probe_methods import (
    ProbeMethodSpec,
    _iter_specs,
    config_class_for,
    probe_method,
)
from datahub.ingestion.source.sql.sqlalchemy_probe import SqlAlchemyMetadataProbe


class _FakeInspector:
    def __init__(self) -> None:
        self.asked_for: List[str] = []

    def get_schema_names(self) -> List[str]:
        return ["analytics", "information_schema"]

    def get_table_names(self, schema: str) -> List[str]:
        self.asked_for.append(f"tables:{schema}")
        return ["orders", "shipments"] if schema == "analytics" else []

    def get_view_names(self, schema: str) -> List[str]:
        self.asked_for.append(f"views:{schema}")
        return ["orders_v"] if schema == "analytics" else []


def _probe(source_type: str = "postgres") -> SqlAlchemyMetadataProbe:
    # __new__ because __init__ builds an engine; these commands only touch the
    # Inspector, which is what ingestion enumerates through too.
    probe = SqlAlchemyMetadataProbe.__new__(SqlAlchemyMetadataProbe)
    probe._insp = _FakeInspector()  # type: ignore[assignment]
    probe.kind_overrides = {
        "containers": str(config_class_for(source_type).probe_container_kind())
    }
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


def test_containers_are_reported_as_the_kind_the_recipes_tier_makes_them():
    """get_schema_names() means different things per tier, and the pattern differs too.

    Three-tier sources filter schemas with schema_pattern; two-tier ones return
    databases and filter them with database_pattern, where schema_pattern is
    deprecated. One provider class serves both, so the kind cannot be a class-level
    declaration -- it comes from the config.
    """
    kinds: Dict[str, str] = {
        source_type: str(config_class_for(source_type).probe_container_kind())
        for source_type in ("postgres", "mssql", "snowflake", "mysql", "hive")
    }
    assert kinds["postgres"] == "Schema"
    assert kinds["mssql"] == "Schema"
    assert kinds["snowflake"] == "Schema"
    assert kinds["mysql"] == "Database"
    assert kinds["hive"] == "Database"


def test_the_provider_reports_the_runtime_kind_for_containers():
    assert _probe("postgres").kind_overrides["containers"] == "Schema"
    assert _probe("mysql").kind_overrides["containers"] == "Database"
    # The spec itself declares none, because the class cannot know it.
    assert _spec("containers").kind is None


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
                config_class_for(source_type).probe_provider_class()
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
    from datahub.ingestion.source.sql.sqlalchemy_probe import (
        SqlAlchemyMetadataProbe,
    )

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
    from datahub.ingestion.source.sql.sqlalchemy_probe import SqlAlchemyMetadataProbe

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
    # Three-tier: `database` names the connection's database, not a filter
    # over the schemas `containers` returns, so nothing is pinned there.
    assert (
        _pinned_containers(SimpleNamespace(database="one", databases=None), "Schema")
        == frozenset()
    )
