from unittest.mock import MagicMock

from google.cloud.bigquery.table import _EmptyRowIterator

from datahub.ingestion.source.bigquery_v2.bigquery_schema import BigQuerySchemaApi
from tests.unit.bigquery.query_result_fakes import FakeResultPage


class _Rows:
    total_rows = 3
    api_request = object()

    def __init__(self) -> None:
        self.pages_accessed = False

    @property
    def pages(self) -> list[FakeResultPage]:
        self.pages_accessed = True
        return [FakeResultPage(["a", "b"]), FakeResultPage(["c"])]


def _schema_api(result: object) -> BigQuerySchemaApi:
    job = MagicMock()
    job.job_id = "job_1"
    job.result.return_value = result
    schema_api = BigQuerySchemaApi.__new__(BigQuerySchemaApi)
    schema_api.bq_client = MagicMock()
    schema_api.bq_client.query.return_value = job
    return schema_api


def test_query_result_streams_pages_lazily() -> None:
    rows = _Rows()
    result = _schema_api(rows).get_query_result("select 1")

    assert not rows.pages_accessed
    assert list(result) == ["a", "b", "c"]
    assert rows.pages_accessed


def test_query_result_handles_empty_row_iterator() -> None:
    result = _schema_api(_EmptyRowIterator()).get_query_result("select 1")
    assert list(result) == []
