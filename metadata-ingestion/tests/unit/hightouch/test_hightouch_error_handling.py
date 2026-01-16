from unittest.mock import Mock, patch

import pytest
from pydantic import ValidationError
from requests.exceptions import ConnectionError, HTTPError, RequestException, Timeout

from datahub.ingestion.run.pipeline import PipelineContext
from datahub.ingestion.source.hightouch.config import (
    HightouchAPIConfig,
    HightouchSourceConfig,
)
from datahub.ingestion.source.hightouch.hightouch import HightouchSource
from datahub.ingestion.source.hightouch.hightouch_api import HightouchAPIClient


class TestHightouchAPIFailures:
    def test_api_401_unauthorized(self):
        api_config = HightouchAPIConfig(
            api_key="invalid_key", base_url="https://api.hightouch.com"
        )
        api_client = HightouchAPIClient(api_config)

        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.raise_for_status.side_effect = HTTPError(response=mock_response)

        with (
            patch.object(api_client.session, "request", return_value=mock_response),
            pytest.raises(HTTPError),
        ):
            api_client.get_workspaces()

    def test_api_429_rate_limit(self):
        api_config = HightouchAPIConfig(
            api_key="test_key", base_url="https://api.hightouch.com"
        )
        api_client = HightouchAPIClient(api_config)

        mock_response = Mock()
        mock_response.status_code = 429
        mock_response.raise_for_status.side_effect = HTTPError(response=mock_response)

        with (
            patch.object(api_client.session, "request", return_value=mock_response),
            pytest.raises(HTTPError),
        ):
            api_client.get_workspaces()

    def test_api_500_server_error(self):
        api_config = HightouchAPIConfig(
            api_key="test_key", base_url="https://api.hightouch.com"
        )
        api_client = HightouchAPIClient(api_config)

        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = HTTPError(response=mock_response)

        with (
            patch.object(api_client.session, "request", return_value=mock_response),
            pytest.raises(HTTPError),
        ):
            api_client.get_workspaces()

    def test_api_timeout(self):
        api_config = HightouchAPIConfig(
            api_key="test_key", base_url="https://api.hightouch.com"
        )
        api_client = HightouchAPIClient(api_config)

        with (
            patch.object(
                api_client.session, "request", side_effect=Timeout("Request timed out")
            ),
            pytest.raises(Timeout),
        ):
            api_client.get_workspaces()

    def test_api_connection_error(self):
        api_config = HightouchAPIConfig(
            api_key="test_key", base_url="https://api.hightouch.com"
        )
        api_client = HightouchAPIClient(api_config)

        with (
            patch.object(
                api_client.session,
                "request",
                side_effect=ConnectionError("Failed to establish connection"),
            ),
            pytest.raises(ConnectionError),
        ):
            api_client.get_workspaces()

    def test_api_malformed_json_response(self):
        api_config = HightouchAPIConfig(
            api_key="test_key", base_url="https://api.hightouch.com"
        )
        api_client = HightouchAPIClient(api_config)

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None
        mock_response.json.side_effect = ValueError("Invalid JSON")

        with (
            patch.object(api_client.session, "request", return_value=mock_response),
            pytest.raises(ValueError),
        ):
            api_client.get_workspaces()


class TestHightouchLargeDatasets:
    @patch("datahub.ingestion.source.hightouch.hightouch_api.HightouchAPIClient")
    def test_large_number_of_syncs(self, mock_api_client):
        config = HightouchSourceConfig(
            api_config={"api_key": "test_key", "base_url": "https://api.hightouch.com"}
        )
        ctx = PipelineContext(run_id="test_run")

        mock_client_instance = Mock()
        mock_api_client.return_value = mock_client_instance

        syncs = []
        models_map = {}
        sources_map = {}
        dests_map = {}

        for i in range(1500):
            sync_mock = Mock()
            sync_mock.id = f"sync_{i}"
            sync_mock.slug = f"sync_slug_{i}"
            sync_mock.model_id = f"model_{i % 100}"
            sync_mock.destination_id = f"dest_{i % 50}"
            sync_mock.disabled = False
            sync_mock.schedule = None
            sync_mock.configuration = {}
            sync_mock.workspace_id = f"workspace_{i % 10}"
            sync_mock.tags = {}
            syncs.append(sync_mock)

            if sync_mock.model_id not in models_map:
                model_mock = Mock()
                model_mock.id = sync_mock.model_id
                model_mock.slug = f"model_{i % 100}"
                model_mock.name = f"Model {i % 100}"
                model_mock.source_id = f"source_{i % 20}"
                model_mock.query_type = "table"
                model_mock.raw_sql = None
                model_mock.query_schema = []
                model_mock.primary_key = "id"
                model_mock.workspace_id = None
                model_mock.folder_id = None
                model_mock.tags = {}
                model_mock.is_schema = False
                model_mock.created_at = None
                model_mock.updated_at = None
                model_mock.description = None
                models_map[sync_mock.model_id] = model_mock

                if model_mock.source_id not in sources_map:
                    source_mock = Mock()
                    source_mock.id = model_mock.source_id
                    source_mock.name = f"Source {i % 20}"
                    source_mock.type = "snowflake"
                    source_mock.configuration = {}
                    sources_map[source_mock.id] = source_mock

            if sync_mock.destination_id not in dests_map:
                dest_mock = Mock()
                dest_mock.id = sync_mock.destination_id
                dest_mock.name = f"Dest {i % 50}"
                dest_mock.type = "postgres"
                dest_mock.configuration = {}
                dests_map[dest_mock.id] = dest_mock

        mock_client_instance.get_syncs.return_value = syncs
        mock_client_instance.get_models.return_value = []
        mock_client_instance.get_workspaces.return_value = []
        mock_client_instance.get_folders.return_value = []
        mock_client_instance.get_contracts.return_value = []
        mock_client_instance.get_model_by_id.side_effect = lambda mid: models_map.get(
            mid
        )
        mock_client_instance.get_source_by_id.side_effect = lambda sid: sources_map.get(
            sid
        )
        mock_client_instance.get_destination_by_id.side_effect = (
            lambda did: dests_map.get(did)
        )

        with patch(
            "datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient",
            return_value=mock_client_instance,
        ):
            source = HightouchSource(config, ctx)
            list(source.get_workunits())

            assert len(syncs) == 1500
            assert source.report.syncs_scanned >= 1500

    @patch("datahub.ingestion.source.hightouch.hightouch_api.HightouchAPIClient")
    def test_large_number_of_models(self, mock_api_client):
        config = HightouchSourceConfig(
            api_config={"api_key": "test_key", "base_url": "https://api.hightouch.com"},
            emit_models_as_datasets=True,
        )
        ctx = PipelineContext(run_id="test_run")

        mock_client_instance = Mock()
        mock_api_client.return_value = mock_client_instance

        models = []
        for i in range(1200):
            model_mock = Mock()
            model_mock.id = f"model_{i}"
            model_mock.slug = f"model_slug_{i}"
            model_mock.name = f"Model {i}"
            model_mock.source_id = f"source_{i % 20}"
            model_mock.query_type = "table"
            model_mock.raw_sql = None
            model_mock.query_schema = []
            model_mock.primary_key = "id"
            model_mock.workspace_id = f"workspace_{i % 5}"
            model_mock.folder_id = None
            model_mock.tags = {}
            model_mock.is_schema = False
            model_mock.created_at = None
            model_mock.updated_at = None
            model_mock.description = None
            models.append(model_mock)

        mock_client_instance.get_models.return_value = models
        mock_client_instance.get_syncs.return_value = []
        mock_client_instance.get_workspaces.return_value = []
        mock_client_instance.get_folders.return_value = []
        mock_client_instance.get_contracts.return_value = []

        with patch(
            "datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient",
            return_value=mock_client_instance,
        ):
            source = HightouchSource(config, ctx)
            list(source.get_workunits())

            assert len(models) == 1200
            assert source.report.models_scanned >= 1200


class TestHightouchMalformedAPIResponses:
    @patch("datahub.ingestion.source.hightouch.hightouch_api.HightouchAPIClient")
    def test_sync_missing_required_fields(self, mock_api_client):
        config = HightouchSourceConfig(
            api_config={"api_key": "test_key", "base_url": "https://api.hightouch.com"}
        )
        ctx = PipelineContext(run_id="test_run")

        mock_client_instance = Mock()
        mock_api_client.return_value = mock_client_instance

        malformed_sync = Mock()
        malformed_sync.id = None
        malformed_sync.slug = None
        malformed_sync.model_id = None
        malformed_sync.destination_id = None

        mock_client_instance.get_syncs.return_value = [malformed_sync]
        mock_client_instance.get_workspaces.return_value = []
        mock_client_instance.get_folders.return_value = []
        mock_client_instance.get_contracts.return_value = []

        with patch(
            "datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient",
            return_value=mock_client_instance,
        ):
            source = HightouchSource(config, ctx)

            with pytest.raises((AttributeError, TypeError, ValueError)):
                list(source.get_workunits())

    @patch("datahub.ingestion.source.hightouch.hightouch_api.HightouchAPIClient")
    def test_model_with_invalid_query_schema(self, mock_api_client):
        config = HightouchSourceConfig(
            api_config={"api_key": "test_key", "base_url": "https://api.hightouch.com"},
            emit_models_as_datasets=True,
        )
        ctx = PipelineContext(run_id="test_run")

        mock_client_instance = Mock()
        mock_api_client.return_value = mock_client_instance

        malformed_model = Mock()
        malformed_model.id = "model_1"
        malformed_model.slug = "malformed_model"
        malformed_model.name = "Malformed Model"
        malformed_model.source_id = "source_1"
        malformed_model.query_type = "raw_sql"
        malformed_model.raw_sql = "SELECT * FROM table"
        malformed_model.query_schema = [{"invalid_field": "value"}]
        malformed_model.primary_key = None
        malformed_model.workspace_id = "workspace_1"
        malformed_model.folder_id = None
        malformed_model.tags = {}
        malformed_model.is_schema = False
        malformed_model.created_at = None
        malformed_model.updated_at = None
        malformed_model.description = None

        mock_source = Mock()
        mock_source.id = "source_1"
        mock_source.name = "Test Source"
        mock_source.type = "snowflake"
        mock_source.configuration = {}

        mock_client_instance.get_models.return_value = [malformed_model]
        mock_client_instance.get_syncs.return_value = []
        mock_client_instance.get_workspaces.return_value = []
        mock_client_instance.get_folders.return_value = []
        mock_client_instance.get_source_by_id.return_value = mock_source
        mock_client_instance.get_contracts.return_value = []

        with patch(
            "datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient",
            return_value=mock_client_instance,
        ):
            source = HightouchSource(config, ctx)
            list(source.get_workunits())

            assert source.report.model_schemas_skipped > 0

    @patch("datahub.ingestion.source.hightouch.hightouch_api.HightouchAPIClient")
    def test_destination_with_missing_configuration(self, mock_api_client):
        config = HightouchSourceConfig(
            api_config={"api_key": "test_key", "base_url": "https://api.hightouch.com"}
        )
        ctx = PipelineContext(run_id="test_run")

        mock_client_instance = Mock()
        mock_api_client.return_value = mock_client_instance

        sync_mock = Mock()
        sync_mock.id = "sync_1"
        sync_mock.slug = "test_sync"
        sync_mock.model_id = "model_1"
        sync_mock.destination_id = "dest_1"
        sync_mock.disabled = False
        sync_mock.schedule = None
        sync_mock.configuration = None
        sync_mock.workspace_id = "workspace_1"
        sync_mock.tags = {}
        sync_mock.referenced_columns = None

        model_mock = Mock()
        model_mock.id = "model_1"
        model_mock.slug = "test_model"
        model_mock.name = "Test Model"
        model_mock.source_id = "source_1"
        model_mock.query_type = "table"
        model_mock.raw_sql = None
        model_mock.query_schema = []
        model_mock.primary_key = "id"
        model_mock.workspace_id = None
        model_mock.folder_id = None
        model_mock.tags = {}
        model_mock.is_schema = False
        model_mock.created_at = None
        model_mock.updated_at = None
        model_mock.description = None

        dest_mock = Mock()
        dest_mock.id = "dest_1"
        dest_mock.name = "Test Destination"
        dest_mock.type = "postgres"
        dest_mock.configuration = None

        mock_client_instance.get_syncs.return_value = [sync_mock]
        mock_client_instance.get_models.return_value = []
        mock_client_instance.get_workspaces.return_value = []
        mock_client_instance.get_folders.return_value = []
        mock_client_instance.get_model_by_id.return_value = model_mock
        mock_client_instance.get_destination_by_id.return_value = dest_mock
        mock_client_instance.get_contracts.return_value = []

        source_mock = Mock()
        source_mock.id = "source_1"
        source_mock.name = "Test Source"
        source_mock.type = "snowflake"
        source_mock.configuration = {}
        mock_client_instance.get_source_by_id.return_value = source_mock

        with patch(
            "datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient",
            return_value=mock_client_instance,
        ):
            source = HightouchSource(config, ctx)
            workunits = list(source.get_workunits())

            assert any(
                hasattr(wu, "metadata")
                and hasattr(wu.metadata, "aspectName")
                and wu.metadata.aspectName == "dataFlowInfo"
                for wu in workunits
            )


class TestHightouchConfigValidation:
    def test_missing_api_key_fails_fast(self):
        with pytest.raises((ValueError, TypeError)):
            HightouchSourceConfig(api_config={})

    def test_invalid_base_url_format(self):
        config = HightouchSourceConfig(
            api_config={
                "api_key": "test_key",
                "base_url": "not-a-valid-url",
            }
        )
        ctx = PipelineContext(run_id="test_run")

        with pytest.raises(RequestException):
            source = HightouchSource(config, ctx)
            list(source.get_workunits())

    def test_negative_max_sync_runs(self):
        with pytest.raises(ValidationError) as exc_info:
            HightouchSourceConfig(
                api_config={"api_key": "test_key"},
                max_sync_runs_per_sync=-1,
            )

        assert "max_sync_runs_per_sync must be non-negative" in str(exc_info.value)
