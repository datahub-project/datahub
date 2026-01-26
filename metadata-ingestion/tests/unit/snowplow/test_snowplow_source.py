"""Unit tests for Snowplow source business logic."""

from unittest.mock import Mock, patch

import pytest

from datahub.ingestion.api.source import SourceCapability
from datahub.ingestion.source.snowplow.snowplow import SnowplowSource


class TestSnowplowSourceTestConnection:
    """Tests for test_connection() static method."""

    def test_bdp_connection_success(self):
        """Test successful BDP connection test."""
        config_dict = {
            "bdp_connection": {
                "organization_id": "test-org",
                "api_key_id": "test-key-id",
                "api_key": "test-secret",
            }
        }

        with patch(
            "datahub.ingestion.source.snowplow.snowplow.SnowplowBDPClient"
        ) as mock_client_class:
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client_class.return_value = mock_client

            report = SnowplowSource.test_connection(config_dict)

            assert report.basic_connectivity is not None
            assert report.basic_connectivity.capable is True
            assert report.capability_report is not None
            assert (
                report.capability_report[SourceCapability.SCHEMA_METADATA].capable
                is True
            )
            mock_client.test_connection.assert_called_once()

    def test_bdp_connection_failure(self):
        """Test BDP connection failure when test_connection returns False."""
        config_dict = {
            "bdp_connection": {
                "organization_id": "test-org",
                "api_key_id": "test-key-id",
                "api_key": "test-secret",
            }
        }

        with patch(
            "datahub.ingestion.source.snowplow.snowplow.SnowplowBDPClient"
        ) as mock_client_class:
            mock_client = Mock()
            mock_client.test_connection.return_value = False
            mock_client_class.return_value = mock_client

            report = SnowplowSource.test_connection(config_dict)

            assert report.basic_connectivity is not None
            assert report.basic_connectivity.capable is False
            assert report.basic_connectivity.failure_reason is not None
            assert (
                "BDP API connection failed" in report.basic_connectivity.failure_reason
            )

    def test_bdp_connection_exception(self):
        """Test BDP connection error handling."""
        config_dict = {
            "bdp_connection": {
                "organization_id": "test-org",
                "api_key_id": "test-key-id",
                "api_key": "invalid-secret",
            }
        }

        with patch(
            "datahub.ingestion.source.snowplow.snowplow.SnowplowBDPClient"
        ) as mock_client_class:
            mock_client_class.side_effect = ValueError("Authentication failed")

            report = SnowplowSource.test_connection(config_dict)

            assert report.basic_connectivity is not None
            assert report.basic_connectivity.capable is False
            assert report.basic_connectivity.failure_reason is not None
            assert "BDP connection error" in report.basic_connectivity.failure_reason
            assert "Authentication failed" in report.basic_connectivity.failure_reason

    def test_iglu_connection_success(self):
        """Test successful Iglu connection test."""
        config_dict = {
            "iglu_connection": {
                "iglu_server_url": "http://iglu.example.com",
            }
        }

        with patch(
            "datahub.ingestion.source.snowplow.snowplow.IgluClient"
        ) as mock_client_class:
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client_class.return_value = mock_client

            report = SnowplowSource.test_connection(config_dict)

            assert report.basic_connectivity is not None
            assert report.basic_connectivity.capable is True
            assert report.capability_report is not None
            assert (
                report.capability_report[SourceCapability.SCHEMA_METADATA].capable
                is True
            )

    def test_iglu_connection_failure(self):
        """Test Iglu connection failure when test_connection returns False."""
        config_dict = {
            "iglu_connection": {
                "iglu_server_url": "http://iglu.example.com",
            }
        }

        with patch(
            "datahub.ingestion.source.snowplow.snowplow.IgluClient"
        ) as mock_client_class:
            mock_client = Mock()
            mock_client.test_connection.return_value = False
            mock_client_class.return_value = mock_client

            report = SnowplowSource.test_connection(config_dict)

            assert report.basic_connectivity is not None
            assert report.basic_connectivity.capable is False
            assert report.basic_connectivity.failure_reason is not None
            assert "Iglu connection failed" in report.basic_connectivity.failure_reason

    def test_iglu_connection_exception(self):
        """Test Iglu connection error handling."""
        config_dict = {
            "iglu_connection": {
                "iglu_server_url": "http://iglu.example.com",
            }
        }

        with patch(
            "datahub.ingestion.source.snowplow.snowplow.IgluClient"
        ) as mock_client_class:
            mock_client_class.side_effect = Exception("Connection refused")

            report = SnowplowSource.test_connection(config_dict)

            assert report.basic_connectivity is not None
            assert report.basic_connectivity.capable is False
            assert report.basic_connectivity.failure_reason is not None
            assert "Iglu connection error" in report.basic_connectivity.failure_reason

    def test_configuration_error(self):
        """Test handling of invalid configuration."""
        config_dict: dict = {}  # Missing required connection

        report = SnowplowSource.test_connection(config_dict)

        assert report.basic_connectivity is not None
        assert report.basic_connectivity.capable is False
        assert report.basic_connectivity.failure_reason is not None
        assert "Configuration error" in report.basic_connectivity.failure_reason

    def test_both_connections_success(self):
        """Test when both BDP and Iglu connections are configured and succeed."""
        config_dict = {
            "bdp_connection": {
                "organization_id": "test-org",
                "api_key_id": "test-key-id",
                "api_key": "test-secret",
            },
            "iglu_connection": {
                "iglu_server_url": "http://iglu.example.com",
            },
        }

        with (
            patch(
                "datahub.ingestion.source.snowplow.snowplow.SnowplowBDPClient"
            ) as mock_bdp_class,
            patch(
                "datahub.ingestion.source.snowplow.snowplow.IgluClient"
            ) as mock_iglu_class,
        ):
            mock_bdp = Mock()
            mock_bdp.test_connection.return_value = True
            mock_bdp_class.return_value = mock_bdp

            mock_iglu = Mock()
            mock_iglu.test_connection.return_value = True
            mock_iglu_class.return_value = mock_iglu

            report = SnowplowSource.test_connection(config_dict)

            assert report.basic_connectivity is not None
            assert report.basic_connectivity.capable is True
            assert report.capability_report is not None
            assert (
                report.capability_report[SourceCapability.SCHEMA_METADATA].capable
                is True
            )

    def test_bdp_fails_but_iglu_succeeds(self):
        """Test when BDP fails but Iglu succeeds - Iglu result takes precedence."""
        config_dict = {
            "bdp_connection": {
                "organization_id": "test-org",
                "api_key_id": "test-key-id",
                "api_key": "test-secret",
            },
            "iglu_connection": {
                "iglu_server_url": "http://iglu.example.com",
            },
        }

        with (
            patch(
                "datahub.ingestion.source.snowplow.snowplow.SnowplowBDPClient"
            ) as mock_bdp_class,
            patch(
                "datahub.ingestion.source.snowplow.snowplow.IgluClient"
            ) as mock_iglu_class,
        ):
            mock_bdp = Mock()
            mock_bdp.test_connection.return_value = False
            mock_bdp_class.return_value = mock_bdp

            mock_iglu = Mock()
            mock_iglu.test_connection.return_value = True
            mock_iglu_class.return_value = mock_iglu

            report = SnowplowSource.test_connection(config_dict)

            # Iglu success overwrites BDP failure (last write wins)
            assert report.basic_connectivity is not None
            assert report.basic_connectivity.capable is True


class TestSnowplowSourcePIIFieldExtraction:
    """Test PII field extraction from enrichment configuration."""

    @pytest.fixture
    def mock_bdp_client(self):
        """Create a mock BDP client."""
        return Mock()

    @pytest.fixture
    def mock_source_with_pii_enabled(self, mock_bdp_client):
        """Create a mock source with PII enrichment enabled."""
        with patch.object(SnowplowSource, "__init__", lambda self, config, ctx: None):
            source = SnowplowSource.__new__(SnowplowSource)
            source.bdp_client = mock_bdp_client
            source.config = Mock()
            source.config.field_tagging = Mock()
            source.config.field_tagging.use_pii_enrichment = True
            source.state = Mock()
            source.state.pii_fields_cache = None
            return source

    def test_extract_pii_fields_returns_cached_value(
        self, mock_source_with_pii_enabled
    ):
        """Test that cached PII fields are returned without API calls."""
        # Set cached value
        cached_fields = {"email", "phone"}
        mock_source_with_pii_enabled.state.pii_fields_cache = cached_fields

        result = mock_source_with_pii_enabled._extract_pii_fields()

        # Should return cached value without calling API
        assert result == cached_fields
        mock_source_with_pii_enabled.bdp_client.get_pipelines.assert_not_called()

    def test_extract_pii_fields_disabled(self, mock_bdp_client):
        """Test that empty set is returned when PII enrichment is disabled."""
        with patch.object(SnowplowSource, "__init__", lambda self, config, ctx: None):
            source = SnowplowSource.__new__(SnowplowSource)
            source.bdp_client = mock_bdp_client
            source.config = Mock()
            source.config.field_tagging = Mock()
            source.config.field_tagging.use_pii_enrichment = False
            source.state = Mock()
            source.state.pii_fields_cache = None

            result = source._extract_pii_fields()

            assert result == set()
            mock_bdp_client.get_pipelines.assert_not_called()

    def test_extract_pii_fields_no_bdp_client(self):
        """Test that empty set is returned when BDP client is not available."""
        with patch.object(SnowplowSource, "__init__", lambda self, config, ctx: None):
            source = SnowplowSource.__new__(SnowplowSource)
            source.bdp_client = None
            source.config = Mock()
            source.config.field_tagging = Mock()
            source.config.field_tagging.use_pii_enrichment = True
            source.state = Mock()
            source.state.pii_fields_cache = None

            result = source._extract_pii_fields()

            assert result == set()

    def test_extract_pii_fields_from_enrichment_list_format(
        self, mock_source_with_pii_enabled
    ):
        """Test extraction of PII fields from enrichment with list format."""
        # Mock pipeline and enrichment
        pipeline = Mock(id="pipeline-1")
        enrichment = Mock()
        enrichment.schema_ref = "iglu:com.snowplowanalytics.snowplow.enrichments/pii_pseudonymization_enrichment/jsonschema/2-0-0"
        enrichment.content = Mock()
        enrichment.content.data = Mock()
        enrichment.content.data.parameters = {
            "pii": [
                {"fieldName": "email"},
                {"fieldName": "phone"},
                {"fieldName": "ssn"},
            ]
        }

        mock_source_with_pii_enabled.bdp_client.get_pipelines.return_value = [pipeline]
        mock_source_with_pii_enabled.bdp_client.get_enrichments.return_value = [
            enrichment
        ]

        result = mock_source_with_pii_enabled._extract_pii_fields()

        assert result == {"email", "phone", "ssn"}

    def test_extract_pii_fields_from_enrichment_dict_format(
        self, mock_source_with_pii_enabled
    ):
        """Test extraction of PII fields from enrichment with dict format."""
        pipeline = Mock(id="pipeline-1")
        enrichment = Mock()
        enrichment.schema_ref = "iglu:com.snowplowanalytics.snowplow.enrichments/pii_pseudonymization_enrichment/jsonschema/2-0-0"
        enrichment.content = Mock()
        enrichment.content.data = Mock()
        enrichment.content.data.parameters = {
            "pii": {"fieldNames": ["email", "credit_card"]}
        }

        mock_source_with_pii_enabled.bdp_client.get_pipelines.return_value = [pipeline]
        mock_source_with_pii_enabled.bdp_client.get_enrichments.return_value = [
            enrichment
        ]

        result = mock_source_with_pii_enabled._extract_pii_fields()

        assert result == {"email", "credit_card"}

    def test_extract_pii_fields_from_multiple_pipelines(
        self, mock_source_with_pii_enabled
    ):
        """Test extraction from multiple pipelines."""
        pipeline1 = Mock(id="pipeline-1")
        pipeline2 = Mock(id="pipeline-2")

        enrichment1 = Mock()
        enrichment1.schema_ref = "pii_pseudonymization"
        enrichment1.content = Mock()
        enrichment1.content.data = Mock()
        enrichment1.content.data.parameters = {"pii": [{"fieldName": "email"}]}

        enrichment2 = Mock()
        enrichment2.schema_ref = "pii_pseudonymization"
        enrichment2.content = Mock()
        enrichment2.content.data = Mock()
        enrichment2.content.data.parameters = {"pii": [{"fieldName": "phone"}]}

        mock_source_with_pii_enabled.bdp_client.get_pipelines.return_value = [
            pipeline1,
            pipeline2,
        ]
        mock_source_with_pii_enabled.bdp_client.get_enrichments.side_effect = [
            [enrichment1],
            [enrichment2],
        ]

        result = mock_source_with_pii_enabled._extract_pii_fields()

        # Should combine fields from both pipelines
        assert result == {"email", "phone"}

    def test_extract_pii_fields_skips_non_pii_enrichments(
        self, mock_source_with_pii_enabled
    ):
        """Test that non-PII enrichments are skipped."""
        pipeline = Mock(id="pipeline-1")

        pii_enrichment = Mock()
        pii_enrichment.schema_ref = "pii_pseudonymization"
        pii_enrichment.content = Mock()
        pii_enrichment.content.data = Mock()
        pii_enrichment.content.data.parameters = {"pii": [{"fieldName": "email"}]}

        other_enrichment = Mock()
        other_enrichment.schema_ref = "ip_lookups"
        other_enrichment.content = Mock()
        other_enrichment.content.data = Mock()
        other_enrichment.content.data.parameters = {"database": "GeoIP2-City.mmdb"}

        mock_source_with_pii_enabled.bdp_client.get_pipelines.return_value = [pipeline]
        mock_source_with_pii_enabled.bdp_client.get_enrichments.return_value = [
            pii_enrichment,
            other_enrichment,
        ]

        result = mock_source_with_pii_enabled._extract_pii_fields()

        # Should only extract from PII enrichment
        assert result == {"email"}

    def test_extract_pii_fields_handles_missing_schema_ref(
        self, mock_source_with_pii_enabled
    ):
        """Test handling of enrichments with missing schema_ref."""
        pipeline = Mock(id="pipeline-1")

        enrichment_no_ref = Mock()
        enrichment_no_ref.schema_ref = None

        mock_source_with_pii_enabled.bdp_client.get_pipelines.return_value = [pipeline]
        mock_source_with_pii_enabled.bdp_client.get_enrichments.return_value = [
            enrichment_no_ref
        ]

        result = mock_source_with_pii_enabled._extract_pii_fields()

        # Should return empty set (no valid enrichments)
        assert result == set()

    def test_extract_pii_fields_handles_api_failures(
        self, mock_source_with_pii_enabled
    ):
        """Test that API failures are handled gracefully."""
        mock_source_with_pii_enabled.bdp_client.get_pipelines.side_effect = Exception(
            "API error"
        )

        result = mock_source_with_pii_enabled._extract_pii_fields()

        # Should return empty set and log warning
        assert result == set()

        # Should cache the empty result
        assert mock_source_with_pii_enabled.state.pii_fields_cache == set()

    def test_extract_pii_fields_handles_enrichment_fetch_failure(
        self, mock_source_with_pii_enabled
    ):
        """Test handling when enrichment fetch fails for a specific pipeline."""
        pipeline1 = Mock(id="pipeline-1")
        pipeline2 = Mock(id="pipeline-2")

        enrichment = Mock()
        enrichment.schema_ref = "pii_pseudonymization"
        enrichment.content = Mock()
        enrichment.content.data = Mock()
        enrichment.content.data.parameters = {"pii": [{"fieldName": "email"}]}

        mock_source_with_pii_enabled.bdp_client.get_pipelines.return_value = [
            pipeline1,
            pipeline2,
        ]

        # First pipeline succeeds, second fails
        mock_source_with_pii_enabled.bdp_client.get_enrichments.side_effect = [
            [enrichment],
            Exception("Fetch failed"),
        ]

        result = mock_source_with_pii_enabled._extract_pii_fields()

        # Should still return fields from successful pipeline
        assert result == {"email"}
