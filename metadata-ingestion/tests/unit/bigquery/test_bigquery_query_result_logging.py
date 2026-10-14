import logging
from typing import Any, List
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.source.bigquery_v2 import bigquery_schema
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigQuerySchemaApi,
    _iter_bigquery_results,
)


def test_query_result_logs_completion_before_downloading_pages(
    caplog: pytest.LogCaptureFixture,
) -> None:
    job = MagicMock()
    job.job_id = "job_1"
    rows = MagicMock()
    rows.total_rows = 2
    rows.pages = [["a", "b"]]
    job.result.return_value = rows

    schema_api = BigQuerySchemaApi.__new__(BigQuerySchemaApi)
    schema_api.bq_client = MagicMock()
    schema_api.bq_client.query.return_value = job

    with caplog.at_level(logging.DEBUG):
        result = schema_api.get_query_result("select 1")
        completion = [
            record.message
            for record in caplog.records
            if "job_1 completed" in record.message
        ]
        assert completion == [
            "BigQuery job job_1 completed; results are ready to download (2 rows)"
        ]
        assert not any("returned page" in record.message for record in caplog.records)

        assert list(result) == ["a", "b"]

    messages = [record.message for record in caplog.records]
    assert (
        "BigQuery job job_1 returned page 1 to DataHub (2 rows on this page, 2 rows in the job)"
        in messages
    )
    assert "Finished reading BigQuery job job_1 (2 rows, 1 pages)" in messages


def test_query_result_logs_unknown_when_total_rows_unavailable(
    caplog: pytest.LogCaptureFixture,
) -> None:
    job = MagicMock()
    job.job_id = "job_missing_count"
    rows = MagicMock()
    rows.total_rows = None
    rows.pages = [[]]
    job.result.return_value = rows

    schema_api = BigQuerySchemaApi.__new__(BigQuerySchemaApi)
    schema_api.bq_client = MagicMock()
    schema_api.bq_client.query.return_value = job

    with caplog.at_level(logging.DEBUG):
        assert list(schema_api.get_query_result("select 1")) == []

    assert (
        "BigQuery job job_missing_count completed; results are ready to download (unknown rows)"
        in [record.message for record in caplog.records]
    )


def test_query_result_logs_read_progress(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(bigquery_schema, "_QUERY_RESULT_PROGRESS_SECONDS", 0)

    class _Rows:
        total_rows = 2

        @property
        def pages(self) -> List[List[str]]:
            return [["a", "b"]]

    with caplog.at_level(logging.DEBUG):
        rows: Any = _Rows()
        assert list(_iter_bigquery_results(rows, "job_2")) == ["a", "b"]

    assert "DataHub has read 1 of 2 rows from BigQuery job job_2" in [
        record.message for record in caplog.records
    ]
