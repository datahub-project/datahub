"""Tests for JsonSchemaSource http_headers support.

Verifies that custom HTTP headers are passed through to $ref resolution
when fetching remote schemas (e.g. behind CloudFront with auth).
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.source.schema.json_schema import JsonSchemaSource


class TestJsonloaderWithHeaders:
    """Tests for the _jsonloader_with_headers static method."""

    @patch("datahub.ingestion.source.schema.json_schema.requests.get")
    def test_passes_headers_to_requests(self, mock_get: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.json.return_value = {"type": "object"}
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        result = JsonSchemaSource._jsonloader_with_headers(
            "https://schema.example.com/my-schema.json",
            headers={"Authorization": "Bearer secret123"},
        )

        mock_get.assert_called_once_with(
            "https://schema.example.com/my-schema.json",
            headers={"Authorization": "Bearer secret123"},
        )
        assert result == {"type": "object"}

    @patch("datahub.ingestion.source.schema.json_schema.requests.get")
    def test_passes_empty_headers_when_none(self, mock_get: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.json.return_value = {"type": "string"}
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        JsonSchemaSource._jsonloader_with_headers(
            "https://schema.example.com/other.json",
            headers=None,
        )

        mock_get.assert_called_once_with(
            "https://schema.example.com/other.json",
            headers={},
        )

    @patch("datahub.ingestion.source.schema.json_schema.requests.get")
    def test_raises_on_http_error(self, mock_get: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = Exception("403 Forbidden")
        mock_get.return_value = mock_response

        with pytest.raises(Exception, match="403 Forbidden"):
            JsonSchemaSource._jsonloader_with_headers(
                "https://schema.example.com/secret.json",
                headers={"Authorization": "Bearer bad_token"},
            )

    def test_file_uri_does_not_use_requests(self, tmp_path: Path) -> None:
        schema = {"type": "object", "properties": {"name": {"type": "string"}}}
        schema_file = tmp_path / "local.json"
        schema_file.write_text(json.dumps(schema))

        result = JsonSchemaSource._jsonloader_with_headers(
            f"file://{schema_file}",
            headers={"Authorization": "Bearer should_not_matter"},
        )

        assert result == schema


class TestStringReplaceLoaderWithHeaders:
    """Tests for stringreplaceloader with headers support."""

    @patch(
        "datahub.ingestion.source.schema.json_schema.JsonSchemaSource._jsonloader_with_headers"
    )
    def test_passes_headers_through(self, mock_loader: MagicMock) -> None:
        mock_loader.return_value = {"type": "object"}

        result = JsonSchemaSource.stringreplaceloader(
            "https://old.example.com",
            "https://new.example.com",
            "https://old.example.com/schemas/event.json",
            headers={"X-Api-Key": "my-key"},
        )

        mock_loader.assert_called_once_with(
            "https://new.example.com/schemas/event.json",
            headers={"X-Api-Key": "my-key"},
        )
        assert result == {"type": "object"}

    @patch(
        "datahub.ingestion.source.schema.json_schema.JsonSchemaSource._jsonloader_with_headers"
    )
    def test_replaces_uri_before_loading(self, mock_loader: MagicMock) -> None:
        mock_loader.return_value = {}

        JsonSchemaSource.stringreplaceloader(
            "https://remote.host",
            "file:///local/path",
            "https://remote.host/schemas/v1/event.json",
            headers=None,
        )

        mock_loader.assert_called_once_with(
            "file:///local/path/schemas/v1/event.json",
            headers=None,
        )


class TestHttpHeadersConfig:
    """Tests that http_headers config is wired into the source correctly."""

    def test_config_accepts_http_headers(self, tmp_path: Path) -> None:
        """Verify the config field is accepted without errors."""
        schema = {"type": "object", "$id": "test-schema"}
        schema_file = tmp_path / "test.json"
        schema_file.write_text(json.dumps(schema))

        from datahub.ingestion.api.common import PipelineContext

        config = {
            "path": str(tmp_path),
            "platform": "test_platform",
            "http_headers": {
                "Authorization": "Bearer test-token",
                "X-Custom-Header": "custom-value",
            },
        }
        ctx = PipelineContext(run_id="test")
        source = JsonSchemaSource.create(config, ctx)

        assert source.config.http_headers == {
            "Authorization": "Bearer test-token",
            "X-Custom-Header": "custom-value",
        }

    def test_config_defaults_to_none(self, tmp_path: Path) -> None:
        schema = {"type": "object", "$id": "test-schema"}
        schema_file = tmp_path / "test.json"
        schema_file.write_text(json.dumps(schema))

        from datahub.ingestion.api.common import PipelineContext

        config = {
            "path": str(tmp_path),
            "platform": "test_platform",
        }
        ctx = PipelineContext(run_id="test")
        source = JsonSchemaSource.create(config, ctx)

        assert source.config.http_headers is None
