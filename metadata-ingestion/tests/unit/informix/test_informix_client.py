from typing import Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.source.informix.client import InformixClient
from datahub.ingestion.source.informix.config import InformixSourceConfig
from datahub.ingestion.source.informix.constants import (
    SQL_COLUMNS,
    SQL_FK,
    SQL_PK,
    SQL_TABLES,
    SQL_VIEW_DEF,
)
from datahub.ingestion.source.informix.models import ExtendedType, InformixTable

_PASSWORD = "sup3rs3cr3t"


def _client_with_rows(rows_by_sql: Dict[str, List[List[object]]]) -> InformixClient:
    # Bypass __init__ so no JVM is started; every read method goes through _query,
    # which is the only seam these tests need.
    client = InformixClient.__new__(InformixClient)
    client._query = lambda sql, params: rows_by_sql.get(sql, [])  # type: ignore[method-assign]
    return client


def _config(**overrides: object) -> InformixSourceConfig:
    values: Dict[str, object] = {
        "host_port": "informix.invalid:9088",
        "server": "informix_srv",
        "database": "mydb",
        "username": "myuser",
        "password": _PASSWORD,
        "driver_jar_paths": ["/tmp/jdbc.jar"],
    }
    values.update(overrides)
    return InformixSourceConfig.model_validate(values)


def _table(name: str = "customers", owner: str = "informix") -> InformixTable:
    return InformixTable(name=name, owner=owner)


def test_get_tables_maps_rows_and_detects_views():
    client = _client_with_rows(
        {
            SQL_TABLES: [
                ["customers", "informix", "T", 42],
                ["active_customers", "informix", "V", 0],
            ]
        }
    )
    tables = client.get_tables()
    assert [(t.name, t.owner, t.is_view) for t in tables] == [
        ("customers", "informix", False),
        ("active_customers", "informix", True),
    ]


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("2.0", 2),  # systables.nrows is a FLOAT, so the driver can hand back "2.0"
        (42, 42),
        (-1, None),  # Informix has not computed an estimate yet
        (0, None),
        (None, None),
    ],
)
def test_get_tables_parses_nrows(raw: object, expected: Optional[int]) -> None:
    client = _client_with_rows({SQL_TABLES: [["customers", "informix", "T", raw]]})
    assert client.get_tables()[0].nrows == expected


def test_get_tables_strips_padded_identifiers():
    client = _client_with_rows(
        {SQL_TABLES: [["  customers  ", "  informix  ", " T ", 1]]}
    )
    table = client.get_tables()[0]
    assert (table.name, table.owner, table.is_view) == ("customers", "informix", False)


def test_get_columns_marks_primary_keys():
    client = _client_with_rows(
        {
            SQL_PK: [["id"]],
            SQL_COLUMNS: [
                ["id", 258, 4, 1, None, None, None],
                ["email", 13, 100, 2, None, None, None],
            ],
        }
    )
    columns = client.get_columns(_table())
    assert [(c.name, c.is_pk) for c in columns] == [("id", True), ("email", False)]


def test_get_columns_carries_extended_type_columns():
    # extended_id = 0 yields SQL NULLs from the outer join; an extended type
    # yields its sysxtdtypes name and mode, plus the source type's name when the
    # column is a DISTINCT declared over another extended type.
    client = _client_with_rows(
        {
            SQL_COLUMNS: [
                ["plain", 258, 4, 1, None, None, None],
                ["flag", 41, 1, 2, "  boolean  ", "B", None],
                ["published", 18473, 1, 3, "flag_type", "D", "  boolean  "],
            ],
        }
    )
    columns = client.get_columns(_table())
    assert [c.extended for c in columns] == [
        None,
        ExtendedType(name="boolean", mode="B", source_name=None),
        ExtendedType(name="flag_type", mode="D", source_name="boolean"),
    ]


def test_get_foreign_keys_pairs_single_column_key():
    client = _client_with_rows(
        {SQL_FK: [["fk_orders_cust", "customer_id", "customers", "informix", "id"]]}
    )
    fks = client.get_foreign_keys(_table("orders"))
    assert len(fks) == 1
    assert fks[0].child_columns == ["customer_id"]
    assert fks[0].parent_columns == ["id"]
    assert (fks[0].parent_table, fks[0].parent_owner) == ("customers", "informix")


def test_get_foreign_keys_dedups_composite_cross_product():
    # SQL_FK joins child and parent index parts independently, so a 2-column key
    # comes back as the 4-row cross product rather than pairwise-ordered rows.
    client = _client_with_rows(
        {
            SQL_FK: [
                ["fk_composite", "region", "regions", "informix", "region_code"],
                ["fk_composite", "region", "regions", "informix", "country_code"],
                ["fk_composite", "country", "regions", "informix", "region_code"],
                ["fk_composite", "country", "regions", "informix", "country_code"],
            ]
        }
    )
    fks = client.get_foreign_keys(_table("sales"))
    assert len(fks) == 1
    assert fks[0].child_columns == ["region", "country"]
    assert fks[0].parent_columns == ["region_code", "country_code"]


def test_get_foreign_keys_groups_multiple_constraints():
    client = _client_with_rows(
        {
            SQL_FK: [
                ["fk_a", "customer_id", "customers", "informix", "id"],
                ["fk_b", "product_id", "products", "informix", "id"],
            ]
        }
    )
    fks = client.get_foreign_keys(_table("orders"))
    assert sorted(fk.name for fk in fks) == ["fk_a", "fk_b"]


def test_get_foreign_keys_skips_mismatched_column_counts():
    # A constraint backed by a wider pre-existing index yields more child columns
    # than the reference has parent columns. The client drops these rather than
    # constructing an InformixForeignKey that would fail the length invariant.
    client = _client_with_rows(
        {
            SQL_FK: [
                ["fk_wide", "region", "regions", "informix", "region_code"],
                ["fk_wide", "country", "regions", "informix", "region_code"],
            ]
        }
    )
    assert client.get_foreign_keys(_table("sales")) == []


def test_get_tables_skips_malformed_rows():
    client = _client_with_rows(
        {
            SQL_TABLES: [
                ["customers", "informix", "T", 42],
                ["broken", "informix", "T", object()],  # nrows cannot be parsed
                ["active_customers", "informix", "V", 0],
            ]
        }
    )
    tables = client.get_tables()
    assert [(t.name, t.is_view) for t in tables] == [
        ("customers", False),
        ("active_customers", True),
    ]


def test_get_view_definition_reassembles_chunks_in_seqno_order():
    client = _client_with_rows(
        {
            SQL_VIEW_DEF: [
                ["create view active_customers (id) as select x0.id "],
                ["from informix.customers x0 where x0.active = 'Y';"],
            ]
        }
    )
    assert client.get_view_definition(_table("active_customers")) == (
        "create view active_customers (id) as select x0.id "
        "from informix.customers x0 where x0.active = 'Y';"
    )


def test_get_view_definition_returns_none_without_rows():
    client = _client_with_rows({SQL_VIEW_DEF: []})
    assert client.get_view_definition(_table("active_customers")) is None


class _FakeSQLException(Exception):
    # Stands in for java.sql.SQLException, whose message echoes the full JDBC URL.
    def __init__(self, url: str) -> None:
        super().__init__(f"Failed to connect using {url}")

    def getSQLState(self) -> str:
        return "08001"

    def getErrorCode(self) -> int:
        return -908


def _connect_failure(error: Exception) -> Exception:
    with patch(
        "datahub.ingestion.source.informix.client.resolve_driver_jars",
        return_value=["/tmp/jdbc.jar"],
    ):
        with patch("datahub.ingestion.source.informix.client.jpype") as fake_jpype:
            fake_jpype.isJVMStarted.return_value = True
            driver_manager = MagicMock()
            driver_manager.getConnection.side_effect = error
            fake_jpype.JClass.return_value = driver_manager
            with pytest.raises(ConfigurationError) as exc_info:
                InformixClient(_config())
    return exc_info.value


def test_connect_failure_never_leaks_the_password():
    error = _connect_failure(
        _FakeSQLException(
            "jdbc:informix-sqli://informix.invalid:9088/mydb:"
            f"INFORMIXSERVER=informix_srv;user=myuser;password={_PASSWORD}"
        )
    )
    rendered = str(error)
    assert _PASSWORD not in rendered
    assert "jdbc:informix-sqli" not in rendered
    # Dropping __cause__ is what keeps the URL out of the traceback.
    assert error.__cause__ is None
    # SQLSTATE carries no credentials and is what operators triage on.
    assert "08001" in rendered
    assert "-908" in rendered


def test_connect_failure_reports_context_without_sqlstate():
    # A JVM-level failure has no getSQLState(); the error must still name the server,
    # host and database rather than being swallowed.
    error = _connect_failure(RuntimeError(f"JVM blew up with password={_PASSWORD}"))
    rendered = str(error)
    assert _PASSWORD not in rendered
    assert "informix_srv" in rendered
    assert "informix.invalid:9088" in rendered
    assert "mydb" in rendered
