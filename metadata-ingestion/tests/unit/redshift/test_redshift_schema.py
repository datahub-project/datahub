from typing import Optional
from unittest.mock import MagicMock, patch

from datahub.ingestion.source.redshift.redshift_schema import RedshiftDataDictionary


def _make_schema_cursor(rows: list, field_names: Optional[list] = None) -> MagicMock:
    if field_names is None:
        field_names = [
            "schema_name",
            "schema_type",
            "schema_owner_name",
            "schema_option",
            "external_platform",
            "external_database",
        ]
    cursor = MagicMock()
    cursor.description = [[name] for name in field_names]
    cursor.fetchall.return_value = rows
    return cursor


@patch.object(RedshiftDataDictionary, "get_query_result")
def test_get_schemas_populates_owner(mock_get_query_result: MagicMock) -> None:
    mock_get_query_result.return_value = _make_schema_cursor(
        [("public", "local", "etluser", None, None, None)]
    )
    conn = MagicMock()
    schemas = RedshiftDataDictionary.get_schemas(conn, "mydb")

    assert len(schemas) == 1
    assert schemas[0].name == "public"
    assert schemas[0].owner == "etluser"
    assert schemas[0].database == "mydb"
    assert schemas[0].type == "local"


@patch.object(RedshiftDataDictionary, "get_query_result")
def test_get_schemas_mixed_owned_and_external(mock_get_query_result: MagicMock) -> None:
    mock_get_query_result.return_value = _make_schema_cursor(
        [
            ("public", "local", "alice", None, None, None),
            ("analytics", "local", "bob", None, None, None),
            ("external_schema", "external", None, None, "glue", "my_db"),
        ]
    )
    conn = MagicMock()
    schemas = RedshiftDataDictionary.get_schemas(conn, "mydb")

    assert len(schemas) == 3
    assert schemas[0].owner == "alice"
    assert schemas[1].owner == "bob"
    assert schemas[2].owner is None
    assert schemas[2].external_platform == "glue"
