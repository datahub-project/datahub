from unittest.mock import MagicMock

from datahub.ingestion.source.bigquery_v2.bigquery_data_reader import (
    BigQueryDataReader,
)


def test_no_sample_percentage_means_no_query_is_issued() -> None:
    client = MagicMock()
    reader = BigQueryDataReader.create(client)

    assert reader.get_sample_data_for_table(["p", "d", "t"], 100) == {}
    client.query_and_wait.assert_not_called()


def test_sample_uses_tablesample_with_percentage_and_returns_column_lists() -> None:
    client = MagicMock()
    df = client.query_and_wait.return_value.to_dataframe.return_value
    df.to_dict.return_value = {"id": [1, 2], "name": ["a", "b"]}
    reader = BigQueryDataReader(client)

    sample = reader.get_sample_data_for_table(
        ["my-project", "my_dataset", "my_table"], 100, sample_size_percent=0.005
    )

    assert sample == {"id": [1, 2], "name": ["a", "b"]}
    sql = client.query_and_wait.call_args.args[0]
    assert "`my-project.my_dataset.my_table`" in sql
    assert "TABLESAMPLE SYSTEM (0.50000000 percent)" in sql
    df.to_dict.assert_called_once_with(orient="list")


def test_close_releases_client() -> None:
    client = MagicMock()
    BigQueryDataReader(client).close()
    client.close.assert_called_once()
