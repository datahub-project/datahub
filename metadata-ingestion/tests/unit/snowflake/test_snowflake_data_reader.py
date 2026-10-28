"""SnowflakeDataReader samples rows for classification."""

from types import SimpleNamespace
from unittest.mock import MagicMock

from datahub.ingestion.source.snowflake.snowflake_data_reader import (
    SnowflakeDataReader,
)


def test_sample_query_is_quoted_and_columns_are_normalised() -> None:
    cursor = MagicMock()
    cursor.__enter__.return_value = cursor
    cursor.fetchall.return_value = [(1, "a"), (2, None)]
    cursor.description = [SimpleNamespace(name="ID"), SimpleNamespace(name="Name")]
    connection = MagicMock()
    connection.native_connection.return_value.cursor.return_value = cursor

    reader = SnowflakeDataReader.create(connection, str.lower)
    sample = reader.get_sample_data_for_table(["MY_DB", "MY_SCHEMA", "T"], 2)

    sql = cursor.execute.call_args.args[0]
    assert '"MY_DB"."MY_SCHEMA"."T"' in sql
    assert "sample (2 rows)" in sql
    assert sample == {"id": [1, 2], "name": ["a", None]}
    reader.close()  # the connection is owned by the caller and must stay open
    connection.close.assert_not_called()
