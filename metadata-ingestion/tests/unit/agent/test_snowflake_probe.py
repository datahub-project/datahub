from types import SimpleNamespace

import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.agent.models import ProbeLeafKind
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.snowflake.snowflake_probe import list_snowflake_children


class _Row:
    def __init__(self, name: str) -> None:
        self._mapping = {"name": name}


class _FakeConn:
    def __init__(self, log: list) -> None:
        self._log = log

    def execute(self, clause):
        sql = str(clause)
        self._log.append(sql)
        if "SHOW TERSE DATABASES" in sql:
            return [_Row("ANALYTICS"), _Row("RAW")]
        if "SHOW TERSE SCHEMAS" in sql:
            return [_Row("PUBLIC"), _Row("INFORMATION_SCHEMA")]
        return []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None


class _FakeInspector:
    def get_table_names(self, schema=None):
        return ["ORDERS", "SYS$LOG"]

    def get_view_names(self, schema=None):
        return ["V_ORDERS"]

    def get_columns(self, table, schema=None):
        return [{"name": "ID"}, {"name": "AMOUNT"}]


class _FakeEngine:
    def __init__(self, log: list) -> None:
        self._log = log
        self.disposed = False

    def connect(self):
        return _FakeConn(self._log)

    def dispose(self):
        self.disposed = True


@pytest.fixture
def snowflake(monkeypatch):
    import sqlalchemy

    log: list = []
    engine = _FakeEngine(log)
    monkeypatch.setattr(sqlalchemy, "create_engine", lambda url, **kw: engine)
    monkeypatch.setattr(sqlalchemy, "inspect", lambda conn: _FakeInspector())
    config = SimpleNamespace(
        get_sql_alchemy_url=lambda: "snowflake://acct/",
        get_options=lambda: {},
        database_pattern=AllowDenyPattern(allow=[".*"], deny=["^RAW$"]),
        schema_pattern=AllowDenyPattern(allow=[".*"]),
        match_fully_qualified_names=False,
        table_pattern=AllowDenyPattern(allow=[".*"]),
        view_pattern=AllowDenyPattern(allow=[".*"]),
    )
    return SimpleNamespace(config=config, engine=engine, log=log)


def test_databases_apply_database_pattern(snowflake):
    result = list_snowflake_children(snowflake.config, [], 100)
    by_name = {n.name: n for n in result.nodes}
    assert set(by_name) == {"ANALYTICS", "RAW"}
    assert by_name["ANALYTICS"].kind == DatasetContainerSubTypes.DATABASE
    assert by_name["ANALYTICS"].fqn == "ANALYTICS"
    assert by_name["ANALYTICS"].pattern_field == "database_pattern"
    assert by_name["ANALYTICS"].included is True
    assert by_name["RAW"].included is False
    assert by_name["RAW"].excluded_by == "database_pattern"
    assert snowflake.engine.disposed is True


def test_schemas_are_db_qualified_and_drop_information_schema(snowflake):
    result = list_snowflake_children(snowflake.config, ["ANALYTICS"], 100)
    by_name = {n.name: n for n in result.nodes}
    assert by_name["PUBLIC"].fqn == "ANALYTICS.PUBLIC"
    assert by_name["PUBLIC"].kind == DatasetContainerSubTypes.SCHEMA
    assert by_name["PUBLIC"].included is True
    assert by_name["INFORMATION_SCHEMA"].included is False
    assert by_name["INFORMATION_SCHEMA"].excluded_by == "default_schema"
    assert any('SHOW TERSE SCHEMAS IN DATABASE "ANALYTICS"' in s for s in snowflake.log)


def test_tables_merge_views_drop_sys_objects_and_pin_database(snowflake):
    result = list_snowflake_children(snowflake.config, ["ANALYTICS", "PUBLIC"], 100)
    by_name = {n.name: n for n in result.nodes}
    assert by_name["ORDERS"].kind == DatasetSubTypes.TABLE
    assert by_name["ORDERS"].pattern_field == "table_pattern"
    assert by_name["ORDERS"].fqn == "ANALYTICS.PUBLIC.ORDERS"
    assert by_name["ORDERS"].included is True
    assert by_name["V_ORDERS"].kind == DatasetSubTypes.VIEW
    assert by_name["V_ORDERS"].pattern_field == "view_pattern"
    assert by_name["SYS$LOG"].included is False
    assert by_name["SYS$LOG"].excluded_by == "system_object"
    assert any('USE DATABASE "ANALYTICS"' in s for s in snowflake.log)


def test_columns_are_fully_qualified_leaves(snowflake):
    result = list_snowflake_children(
        snowflake.config, ["ANALYTICS", "PUBLIC", "ORDERS"], 100
    )
    assert [n.name for n in result.nodes] == ["ID", "AMOUNT"]
    assert all(n.kind == ProbeLeafKind.COLUMN for n in result.nodes)
    assert all(n.pattern_field is None for n in result.nodes)
    assert result.nodes[0].fqn == "ANALYTICS.PUBLIC.ORDERS.ID"


def test_table_pattern_matches_fully_qualified_name(snowflake):
    snowflake.config.table_pattern = AllowDenyPattern(
        allow=[".*"], deny=[".*PUBLIC.ORDERS$"]
    )
    result = list_snowflake_children(snowflake.config, ["ANALYTICS", "PUBLIC"], 100)
    by_name = {n.name: n for n in result.nodes}
    assert by_name["ORDERS"].included is False
    assert by_name["ORDERS"].excluded_by == "table_pattern"
