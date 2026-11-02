"""BigQueryMetadataProbe wiring: client construction from the connector's own
config, lifecycle, and error-code mapping. Query execution and budget behaviour are
covered in tests/unit/agent."""

from unittest.mock import MagicMock, patch

from google.api_core.exceptions import Forbidden

from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_connection import (
    BigQueryConnectionConfig,
)
from datahub.ingestion.source.bigquery_v2.bigquery_probe import BigQueryMetadataProbe


def test_probe_reuses_connector_client_and_closes_it_on_exit() -> None:
    """for_config must keep the recipe config, or projects() lists the credential's
    whole view instead of the recipe's project_ids."""
    config = BigQueryV2Config.model_validate({"project_ids": ["p2", "p1"]})
    with patch.object(BigQueryConnectionConfig, "get_bigquery_client") as get_client:
        with BigQueryMetadataProbe.for_config(config) as probe:
            assert isinstance(probe, BigQueryMetadataProbe)
            get_client.assert_called_once()
            assert probe.projects() == ["p1", "p2"]
            get_client.return_value.list_projects.assert_not_called()
        get_client.return_value.close.assert_called_once()

    assert BigQueryConnectionConfig.probe_provider_class() is BigQueryMetadataProbe


def test_google_api_errors_map_to_http_status_and_others_do_not() -> None:
    assert BigQueryMetadataProbe.probe_error_code(Forbidden("denied")) == "HTTP 403"
    assert BigQueryMetadataProbe.probe_error_code(RuntimeError("x")) is None


def test_catalog_scope_names_only_information_schema_views_without_jobs() -> None:
    relations = BigQueryMetadataProbe.catalog_scope.relations
    assert "information_schema.tables" in relations
    # CatalogScope matches case-insensitively, so an upper-case JOBS entry would
    # admit the query log just the same.
    assert not any(r.lower().endswith(".jobs") for r in relations)
    assert BigQueryMetadataProbe(MagicMock()).query_budget.max_bytes_billed == 1024**3
