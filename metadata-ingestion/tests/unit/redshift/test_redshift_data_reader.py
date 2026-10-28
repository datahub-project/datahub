from unittest.mock import MagicMock

import pandas as pd
import pytest

from datahub.ingestion.source.redshift.redshift_data_reader import RedshiftDataReader


def _connection_returning(df: pd.DataFrame) -> MagicMock:
    conn = MagicMock()
    cursor = conn.cursor.return_value.__enter__.return_value
    cursor.fetch_dataframe.return_value = df
    return conn


def test_get_sample_data_for_table_queries_fully_qualified_table() -> None:
    conn = _connection_returning(pd.DataFrame({"id": [1, 2], "name": ["a", "b"]}))
    reader = RedshiftDataReader.create(conn)

    sample = reader.get_sample_data_for_table(["dev", "public", "orders"], 2)

    assert sample == {"id": [1, 2], "name": ["a", "b"]}
    executed_sql = conn.cursor.return_value.__enter__.return_value.execute.call_args[0][
        0
    ]
    assert "dev.public.orders" in executed_sql
    assert "limit 2" in executed_sql


def test_get_sample_data_for_table_requires_three_part_identifier() -> None:
    reader = RedshiftDataReader.create(_connection_returning(pd.DataFrame()))
    with pytest.raises(AssertionError):
        reader.get_sample_data_for_table(["public", "orders"], 1)


def test_close_leaves_externally_managed_connection_open() -> None:
    conn = MagicMock()
    RedshiftDataReader.create(conn).close()
    conn.close.assert_not_called()
