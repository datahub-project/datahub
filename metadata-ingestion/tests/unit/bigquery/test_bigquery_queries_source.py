"""BigQueryQueriesSource: the standalone queries-v2 source is a thin shell
around BigQueryQueriesExtractor."""

from unittest.mock import MagicMock, patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.bigquery_v2.bigquery_connection import (
    BigQueryConnectionConfig,
)
from datahub.ingestion.source.bigquery_v2.bigquery_queries import (
    BigQueryQueriesSource,
)
from tests.unit.bigquery.conftest import as_mock

MODULE = "datahub.ingestion.source.bigquery_v2.bigquery_queries"


def make_source() -> BigQueryQueriesSource:
    with (
        patch.object(BigQueryConnectionConfig, "get_bigquery_client"),
        patch.object(BigQueryConnectionConfig, "get_projects_client"),
    ):
        return BigQueryQueriesSource.create(
            {
                "window": {
                    "start_time": "2024-01-01T00:00:00Z",
                    "end_time": "2024-01-02T00:00:00Z",
                }
            },
            PipelineContext(run_id="test"),
        )


def test_workunits_come_from_the_queries_extractor_and_window_is_reported() -> None:
    source = make_source()
    sentinel = [MagicMock()]
    with patch.object(
        source.queries_extractor, "get_workunits_internal", return_value=sentinel
    ):
        assert list(source.get_workunits_internal()) == sentinel

    report = source.get_report()
    assert report.window is source.config.window
    assert report.queries_extractor is source.queries_extractor.report


def test_close_releases_extractor_and_connection() -> None:
    source = make_source()
    with patch.object(source.queries_extractor, "close") as extractor_close:
        source.close()

    extractor_close.assert_called_once()
    as_mock(source.connection).close.assert_called_once()
