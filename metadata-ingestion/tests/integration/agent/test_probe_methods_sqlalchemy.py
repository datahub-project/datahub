import pytest
from sqlalchemy import create_engine

from datahub.ingestion.source.sql.sqlalchemy_probe import SqlAlchemyMetadataProbe


@pytest.fixture
def engine(tmp_path):
    eng = create_engine(f"sqlite:///{tmp_path}/t.db")
    with eng.begin() as c:
        c.exec_driver_sql(
            "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, "
            "status TEXT DEFAULT 'active')"
        )
        c.exec_driver_sql(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, "
            "customer_id INTEGER REFERENCES customers(id))"
        )
        c.exec_driver_sql("CREATE VIEW active_orders AS SELECT * FROM orders")
        c.exec_driver_sql("CREATE INDEX ix_orders_cust ON orders(customer_id)")
    try:
        yield eng
    finally:
        eng.dispose()


def test_columns_returns_types_no_values(engine):
    with SqlAlchemyMetadataProbe(engine) as p:
        cols = p.columns(schema="main", table="orders")
    names = {c["name"] for c in cols}
    assert names == {"id", "customer_id"}
    assert all("type" in c and "nullable" in c for c in cols)


def test_columns_default_is_string_or_none(engine):
    with SqlAlchemyMetadataProbe(engine) as p:
        customers = {c["name"]: c for c in p.columns(schema="main", table="customers")}
        orders = {c["name"]: c for c in p.columns(schema="main", table="orders")}
    assert isinstance(customers["status"]["default"], str)
    assert "active" in customers["status"]["default"]
    assert orders["id"]["default"] is None


def test_foreign_keys(engine):
    with SqlAlchemyMetadataProbe(engine) as p:
        fks = p.foreign_keys(schema="main", table="orders")
    assert fks and fks[0]["referred_table"] == "customers"


def test_view_definition_and_indexes_and_pk(engine):
    with SqlAlchemyMetadataProbe(engine) as p:
        assert "orders" in (
            p.view_definition(schema="main", view="active_orders") or ""
        )
        assert any(
            i["name"] == "ix_orders_cust"
            for i in p.indexes(schema="main", table="orders")
        )
        assert p.primary_key(schema="main", table="orders")["constrained_columns"] == [
            "id"
        ]
