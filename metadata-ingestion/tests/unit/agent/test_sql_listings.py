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
    from datahub.ingestion.agent.probe_methods import probe_method

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
