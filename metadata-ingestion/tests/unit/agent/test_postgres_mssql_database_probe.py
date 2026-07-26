import pytest

from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.sql.mssql.source import SQLServerConfig
from datahub.ingestion.source.sql.postgres import PostgresConfig
from datahub.ingestion.source.sql.sql_probe import (
    MSSQL_PROBE_HIERARCHY,
    POSTGRES_PROBE_HIERARCHY,
    list_mssql_children,
    list_postgres_children,
)


class _FakeInspector:
    def __init__(self, schemas=(), tables=(), views=(), columns=()):
        self._schemas = list(schemas)
        self._tables = list(tables)
        self._views = list(views)
        self._columns = list(columns)

    def get_schema_names(self):
        return self._schemas

    def get_table_names(self, schema=None):
        return self._tables

    def get_view_names(self, schema=None):
        return self._views

    def get_columns(self, table, schema=None):
        return self._columns


class _FakeConnection:
    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False


class _FakeEngine:
    def __init__(self):
        self.disposed = False

    def connect(self):
        return _FakeConnection()

    def dispose(self):
        self.disposed = True


@pytest.fixture
def fake_sqlalchemy(monkeypatch):
    """Records every create_engine URL and hands back a fresh _FakeEngine each
    time -- the Database-topped build opens a new connection per level (see
    sql_probe.py's _build), not one shared client, so a single test typically
    sees several create_engine calls."""
    import sqlalchemy

    engines = []
    urls = []

    def fake_create_engine(url, **kwargs):
        engine = _FakeEngine()
        engines.append(engine)
        urls.append(url)
        return engine

    inspector = _FakeInspector(
        schemas=["public"],
        tables=["orders", "v_orders"],
        views=["v_orders"],
        columns=[{"name": "id"}, {"name": "amount"}],
    )

    monkeypatch.setattr(sqlalchemy, "create_engine", fake_create_engine)
    monkeypatch.setattr(sqlalchemy, "inspect", lambda eng: inspector)
    return urls, engines


def test_postgres_and_mssql_declare_the_database_level():
    assert POSTGRES_PROBE_HIERARCHY == [
        DatasetContainerSubTypes.DATABASE,
        DatasetContainerSubTypes.SCHEMA,
        DatasetSubTypes.TABLE,
        "Column",
    ]
    assert MSSQL_PROBE_HIERARCHY == [
        DatasetContainerSubTypes.DATABASE,
        DatasetContainerSubTypes.SCHEMA,
        DatasetSubTypes.TABLE,
        "Column",
    ]


def test_postgres_database_level_reports_a_default_database_excluded(
    fake_sqlalchemy, monkeypatch
):
    config = PostgresConfig(host_port="localhost:5432")
    monkeypatch.setattr(
        PostgresConfig,
        "list_databases",
        lambda self, conn: ["salesdb", "template0"],
    )
    result = list_postgres_children(config, [], 100)
    by_name = {n.name: n for n in result.nodes}
    assert by_name["salesdb"].included is True
    assert by_name["salesdb"].kind == DatasetContainerSubTypes.DATABASE
    assert by_name["template0"].included is False
    assert by_name["template0"].excluded_by == "default_database"


def test_mssql_database_level_reports_a_default_database_excluded(
    fake_sqlalchemy, monkeypatch
):
    config = SQLServerConfig()
    monkeypatch.setattr(
        SQLServerConfig,
        "list_databases",
        lambda self, conn: ["salesdb", "master"],
    )
    result = list_mssql_children(config, [], 100)
    by_name = {n.name: n for n in result.nodes}
    assert by_name["salesdb"].included is True
    assert by_name["master"].included is False
    assert by_name["master"].excluded_by == "default_database"


def test_postgres_schema_level_connects_to_the_named_database(fake_sqlalchemy):
    """The part most likely to be wrong: with a Database level, the schema
    lister must open a connection to the database named in parent_path[0]
    (via PostgresConfig.get_sql_alchemy_url(database=...)) rather than
    reusing the shared/default connection."""
    urls, _ = fake_sqlalchemy
    config = PostgresConfig(host_port="localhost:5432")
    result = list_postgres_children(config, ["salesdb"], 100)
    assert config.get_sql_alchemy_url(database="salesdb") in urls
    assert {n.name for n in result.nodes} == {"public"}
    assert result.nodes[0].kind == DatasetContainerSubTypes.SCHEMA


def test_mssql_schema_level_connects_to_the_named_database(fake_sqlalchemy):
    urls, _ = fake_sqlalchemy
    config = SQLServerConfig()
    list_mssql_children(config, ["salesdb"], 100)
    assert config.get_sql_alchemy_url(current_db="salesdb") in urls


def test_postgres_table_level_fqn_and_filter_target_include_the_database(
    fake_sqlalchemy,
):
    """Before this level existed, the Table fqn was schema.table (two parts)
    and the identifier shim fell back to whatever config.database/the default
    connection happened to be, which is wrong once a recipe iterates several
    databases. With the Database level, the fqn is database.schema.table,
    matching PostgresSource.get_identifier's own three-part output exactly --
    no double-prefixing, and the same string both ways."""
    config = PostgresConfig(host_port="localhost:5432")
    result = list_postgres_children(config, ["salesdb", "public"], 100)
    by_name = {n.name: n for n in result.nodes}
    assert by_name["orders"].fqn == "salesdb.public.orders"
    assert by_name["orders"].kind == DatasetSubTypes.TABLE
    assert by_name["v_orders"].kind == DatasetSubTypes.VIEW


def test_postgres_column_level_connects_to_the_named_database(fake_sqlalchemy):
    urls, _ = fake_sqlalchemy
    config = PostgresConfig(host_port="localhost:5432")
    result = list_postgres_children(config, ["salesdb", "public", "orders"], 100)
    assert config.get_sql_alchemy_url(database="salesdb") in urls
    assert {n.name for n in result.nodes} == {"id", "amount"}


def test_postgres_root_engine_falls_back_to_initial_database(fake_sqlalchemy):
    """The Database level's own listing (list_databases) needs somewhere valid
    to connect before any database name is known; bare get_sql_alchemy_url()
    resolves to config.database, which is unset in exactly this multi-db mode
    -- the root connection must use initial_database instead, mirroring
    PostgresSource.get_inspectors()'s own initial connection."""
    urls, _ = fake_sqlalchemy
    config = PostgresConfig(host_port="localhost:5432", initial_database="bootstrap")
    import datahub.ingestion.source.sql.sql_probe as sql_probe_module

    sql_probe_module._postgres_root_engine(config)
    assert config.get_sql_alchemy_url(database="bootstrap") in urls
