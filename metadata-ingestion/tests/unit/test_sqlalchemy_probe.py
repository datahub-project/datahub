"""The SQLAlchemy probe provider over a real engine: the per-object metadata
commands, and the engine-building refusals that happen before any connection."""

from typing import Callable

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine

from datahub.ingestion.agent.verdicts import ProbeArgumentError
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.sql.sql_generic import SQLAlchemyGenericConfig
from datahub.ingestion.source.sql.sqlalchemy_probe import SqlAlchemyMetadataProbe
from datahub.ingestion.source.sql.two_tier_sql_source import TwoTierSQLAlchemyConfig


@pytest.fixture
def engine(sqlite_engine: Engine) -> Engine:
    """An in-memory parent/child schema with an index and a view."""
    with sqlite_engine.begin() as conn:
        conn.execute(text("CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)"))
        conn.execute(
            text(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, "
                "parent_id INTEGER REFERENCES parent(id), note TEXT DEFAULT 'n/a')"
            )
        )
        conn.execute(text("CREATE INDEX child_parent_idx ON child (parent_id)"))
        conn.execute(text("CREATE VIEW child_v AS SELECT id FROM child"))
    return sqlite_engine


def test_constraint_and_index_commands_describe_the_resolved_table(
    engine: Engine,
) -> None:
    probe = SqlAlchemyMetadataProbe(engine)

    foreign_keys = probe.foreign_keys(schema="main", table="child")
    assert len(foreign_keys) == 1
    assert foreign_keys[0]["referred_table"] == "parent"
    assert foreign_keys[0]["constrained_columns"] == ["parent_id"]

    assert probe.primary_key(schema="main", table="child")["constrained_columns"] == [
        "id"
    ]
    indexes = probe.indexes(schema="main", table="child")
    assert [(ix["name"], ix["column_names"]) for ix in indexes] == [
        ("child_parent_idx", ["parent_id"])
    ]
    columns = probe.columns(schema="main", table="child")
    assert [c["name"] for c in columns] == ["id", "parent_id", "note"]
    assert columns[2]["default"] == "'n/a'"


def test_table_arguments_may_name_a_view_but_not_a_typo(engine: Engine) -> None:
    probe = SqlAlchemyMetadataProbe(engine)

    assert [c["name"] for c in probe.columns(schema="main", table="child_v")] == ["id"]
    with pytest.raises(ProbeArgumentError):
        probe.columns(schema="main", table="Child")
    with pytest.raises(ProbeArgumentError):
        probe.foreign_keys(schema="nope", table="child")


def test_table_comment_is_a_command_sqlite_lacks(engine: Engine) -> None:
    # The framework maps NotImplementedError to "command the source lacks".
    with pytest.raises(NotImplementedError):
        SqlAlchemyMetadataProbe(engine).table_comment(schema="main", table="child")


def test_a_malformed_recipe_url_is_refused_before_connecting() -> None:
    config = SQLAlchemyGenericConfig(platform="x", connect_uri="not a url at all")
    with pytest.raises(ProbeArgumentError, match="connection URL"):
        SqlAlchemyMetadataProbe.for_config(config)


# --- the recipe path: a probe built from a config, over its own engine ------


@pytest.fixture
def sqlite_file(sqlite_file_db: Callable[..., str]) -> str:
    return sqlite_file_db(
        "main",
        "CREATE TABLE parent (id INTEGER PRIMARY KEY);"
        "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER);"
        "CREATE VIEW child_v AS SELECT id FROM child;",
    )


def test_a_probe_built_from_a_recipe_lists_what_ingestion_enumerates(
    sqlite_file: str,
) -> None:
    config = SQLAlchemyGenericConfig(platform="sqlite", connect_uri=sqlite_file)

    with SqlAlchemyMetadataProbe.for_config(config) as probe:
        assert probe.container_kind == str(DatasetContainerSubTypes.SCHEMA)
        assert probe.containers() == ["main"]
        assert probe.tables(schema="main") == ["child", "parent"]
        assert probe.tables(schema="main", limit=1) == ["child"]
        # Views come from the connector's own listing hook, as ingestion's do.
        assert probe.views(schema="main") == ["child_v"]
        definition = probe.view_definition(schema="main", view="child_v")
        assert definition is not None and "SELECT id FROM child" in definition
        assert probe.sql_dialect == "sqlite"
        rows = probe.execute_catalog_query(
            "SELECT name FROM sqlite_master WHERE type = 'view'", limit=5
        )
        assert (rows.columns, rows.rows) == (["name"], [["child_v"]])
        # sqlite's protocol settings claim no ceiling, so the budget reports none.
        assert probe.query_budget.timeout_seconds is None
        assert probe.probe_report is not None


def test_a_two_tier_recipe_naming_its_database_lists_only_that_one(
    sqlite_file: str,
) -> None:
    """A pinned two-tier recipe lists only its database, and only when the
    server lists it too; a misspelt one is absent, never echoed back."""

    def _probe(**overrides: object) -> SqlAlchemyMetadataProbe:
        config = TwoTierSQLAlchemyConfig.model_validate(
            {
                "host_port": "",
                "scheme": "sqlite",
                "sqlalchemy_uri": sqlite_file,
                **overrides,
            }
        )
        return SqlAlchemyMetadataProbe.for_config(config)

    with _probe() as probe:
        assert probe.container_kind == str(DatasetContainerSubTypes.DATABASE)
        assert probe.containers() == ["main"]
    with _probe(database="main") as probe:
        assert probe.containers() == ["main"]
    # A name the server does not list shows as absent rather than echoed back.
    with _probe(database="mian") as probe:
        assert probe.containers() == []
    with pytest.raises(ProbeArgumentError, match="database"):
        with _probe() as probe:
            probe.tables(schema="mian")
