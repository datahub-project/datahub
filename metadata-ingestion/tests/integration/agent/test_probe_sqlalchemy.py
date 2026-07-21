from typing import Dict

import pytest
from sqlalchemy import create_engine, text

from datahub.ingestion.agent.models import ProbeLeafKind
from datahub.ingestion.agent.probe import probe
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)


@pytest.fixture
def sqlite_db(tmp_path):
    db = tmp_path / "probe.db"
    engine = create_engine(f"sqlite:///{db}")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE orders (id INTEGER, amount REAL)"))
        conn.execute(text("CREATE VIEW v_orders AS SELECT id FROM orders"))
    engine.dispose()
    return str(db)


def test_probe_tables_and_columns(sqlite_db):
    config: Dict[str, object] = {
        "connect_uri": f"sqlite:///{sqlite_db}",
        "platform": "sqlite",
    }
    # sqlite has one (default) schema; list tables directly at level 1 using "main".
    tables = probe("sqlalchemy", config, parent_path=["main"], limit=100)
    assert tables.supported is True
    kinds = {n.name: n.kind for n in tables.nodes}
    assert kinds.get("orders") == DatasetSubTypes.TABLE
    assert kinds.get("v_orders") == DatasetSubTypes.VIEW

    columns = probe("sqlalchemy", config, parent_path=["main", "orders"], limit=100)
    col_names = {n.name for n in columns.nodes}
    assert {"id", "amount"} <= col_names
    assert all(n.kind == ProbeLeafKind.COLUMN for n in columns.nodes)


def test_probe_schemas(sqlite_db):
    config: Dict[str, object] = {
        "connect_uri": f"sqlite:///{sqlite_db}",
        "platform": "sqlite",
    }
    schemas = probe("sqlalchemy", config, parent_path=[], limit=100)
    assert schemas.supported is True
    names = {n.name for n in schemas.nodes}
    assert "main" in names
    assert all(n.kind == DatasetContainerSubTypes.SCHEMA for n in schemas.nodes)


def test_probe_unsupported_source_returns_fallback():
    # `file` is registered but implements no probe contract.
    result = probe("file", {}, parent_path=[], limit=10)
    assert result.supported is False
    assert result.fallback
