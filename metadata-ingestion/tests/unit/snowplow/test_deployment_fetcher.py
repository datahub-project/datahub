"""Unit tests for DeploymentFetcher service."""

from unittest.mock import Mock

import pytest

from datahub.ingestion.source.snowplow.models.snowplow_models import (
    DataStructure,
    DataStructureDeployment,
)
from datahub.ingestion.source.snowplow.services.deployment_fetcher import (
    DeploymentFetcher,
)
from datahub.ingestion.source.snowplow.snowplow_report import SnowplowSourceReport


class TestDeploymentFetcher:
    """Test deployment fetching service."""

    @pytest.fixture
    def mock_bdp_client(self):
        """Create mock BDP client."""
        return Mock()

    @pytest.fixture
    def fetcher_parallel(self, mock_bdp_client):
        """Create fetcher with parallel enabled."""
        return DeploymentFetcher(
            bdp_client=mock_bdp_client,
            enable_parallel=True,
            max_workers=3,
        )

    @pytest.fixture
    def fetcher_sequential(self, mock_bdp_client):
        """Create fetcher with parallel disabled."""
        return DeploymentFetcher(
            bdp_client=mock_bdp_client,
            enable_parallel=False,
            max_workers=3,
        )

    @pytest.fixture
    def sample_schemas(self):
        """Create sample data structures for testing."""
        return [
            DataStructure(
                hash="hash1",
                vendor="com.acme",
                name="event1",
                data=None,
            ),
            DataStructure(
                hash="hash2",
                vendor="com.acme",
                name="event2",
                data=None,
            ),
        ]

    def test_fetch_deployments_uses_parallel_when_multiple_schemas(
        self, fetcher_parallel, mock_bdp_client, sample_schemas
    ):
        """Test that parallel fetching is used when multiple schemas need deployments."""
        mock_bdp_client.get_data_structure_deployments.return_value = [
            DataStructureDeployment(
                version="1-0-0", env="PROD", ts="2024-01-01T00:00:00Z"
            )
        ]

        fetcher_parallel.fetch_deployments(sample_schemas)

        # Verify API was called for each schema
        assert mock_bdp_client.get_data_structure_deployments.call_count == 2
        # Verify deployments were set
        assert sample_schemas[0].deployments is not None
        assert sample_schemas[1].deployments is not None

    def test_fetch_deployments_uses_sequential_when_parallel_disabled(
        self, fetcher_sequential, mock_bdp_client, sample_schemas
    ):
        """Test that sequential fetching is used when parallel is disabled."""
        mock_bdp_client.get_data_structure_deployments.return_value = [
            DataStructureDeployment(
                version="1-0-0", env="PROD", ts="2024-01-01T00:00:00Z"
            )
        ]

        fetcher_sequential.fetch_deployments(sample_schemas)

        # Verify API was called for each schema
        assert mock_bdp_client.get_data_structure_deployments.call_count == 2
        # Verify deployments were set
        assert sample_schemas[0].deployments is not None
        assert sample_schemas[1].deployments is not None

    def test_fetch_deployments_parallel_handles_api_errors_gracefully(
        self, fetcher_parallel, mock_bdp_client, sample_schemas
    ):
        """Test that parallel fetching continues even when some schemas fail."""

        # First schema succeeds, second fails
        def side_effect(schema_hash):
            if schema_hash == "hash1":
                return [
                    DataStructureDeployment(
                        version="1-0-0", env="PROD", ts="2024-01-01T00:00:00Z"
                    )
                ]
            else:
                raise Exception("API timeout")

        mock_bdp_client.get_data_structure_deployments.side_effect = side_effect

        # Should not raise exception
        fetcher_parallel.fetch_deployments(sample_schemas)

        # First schema should have deployments
        assert sample_schemas[0].deployments is not None
        assert len(sample_schemas[0].deployments) > 0
        # Second schema should remain empty (error was logged but not raised)
        # deployments defaults to [] in the model
        assert sample_schemas[1].deployments == []

    def test_fetch_deployments_sequential_handles_api_errors_gracefully(
        self, fetcher_sequential, mock_bdp_client, sample_schemas
    ):
        """Test that sequential fetching continues even when some schemas fail."""

        # First schema succeeds, second fails
        def side_effect(schema_hash):
            if schema_hash == "hash1":
                return [
                    DataStructureDeployment(
                        version="1-0-0", env="PROD", ts="2024-01-01T00:00:00Z"
                    )
                ]
            else:
                raise Exception("API timeout")

        mock_bdp_client.get_data_structure_deployments.side_effect = side_effect

        # Should not raise exception
        fetcher_sequential.fetch_deployments(sample_schemas)

        # First schema should have deployments
        assert sample_schemas[0].deployments is not None
        assert len(sample_schemas[0].deployments) > 0
        # Second schema should remain empty (error was logged)
        assert sample_schemas[1].deployments == []

    def test_fetch_deployments_skips_schemas_without_hash(
        self, fetcher_parallel, mock_bdp_client
    ):
        """Test that schemas without hash are skipped."""
        schemas = [
            DataStructure(
                hash=None,  # No hash
                vendor="com.acme",
                name="event1",
                data=None,
            ),
        ]

        fetcher_parallel.fetch_deployments(schemas)

        # API should not be called
        mock_bdp_client.get_data_structure_deployments.assert_not_called()

    def test_fetch_deployments_no_bdp_client(self):
        """Test that fetching gracefully handles missing BDP client."""
        fetcher = DeploymentFetcher(bdp_client=None)
        schemas = [
            DataStructure(
                hash="hash1",
                vendor="com.acme",
                name="event1",
                data=None,
            ),
        ]

        # Should not raise exception
        fetcher.fetch_deployments(schemas)

        # Deployments should remain at default (empty list)
        assert schemas[0].deployments == []

    def test_fetch_deployments_empty_list(self, fetcher_parallel, mock_bdp_client):
        """Test that empty schema list is handled."""
        fetcher_parallel.fetch_deployments([])

        # API should not be called
        mock_bdp_client.get_data_structure_deployments.assert_not_called()

    def test_fetch_deployments_single_schema_uses_sequential(
        self, fetcher_parallel, mock_bdp_client
    ):
        """Test that single schema uses sequential even with parallel enabled."""
        schemas = [
            DataStructure(
                hash="hash1",
                vendor="com.acme",
                name="event1",
                data=None,
            ),
        ]

        mock_bdp_client.get_data_structure_deployments.return_value = [
            DataStructureDeployment(
                version="1-0-0", env="PROD", ts="2024-01-01T00:00:00Z"
            )
        ]

        fetcher_parallel.fetch_deployments(schemas)

        # API should be called once
        assert mock_bdp_client.get_data_structure_deployments.call_count == 1
        assert schemas[0].deployments is not None

    def test_fetch_deployments_parallel_reports_errors_to_report(
        self, mock_bdp_client, sample_schemas
    ):
        """Test that parallel fetching reports errors to the report when provided."""
        report = SnowplowSourceReport()
        fetcher = DeploymentFetcher(
            bdp_client=mock_bdp_client,
            enable_parallel=True,
            max_workers=3,
            report=report,
        )

        # First schema succeeds, second fails
        def side_effect(schema_hash):
            if schema_hash == "hash1":
                return [
                    DataStructureDeployment(
                        version="1-0-0", env="PROD", ts="2024-01-01T00:00:00Z"
                    )
                ]
            else:
                raise Exception("API timeout")

        mock_bdp_client.get_data_structure_deployments.side_effect = side_effect

        fetcher.fetch_deployments(sample_schemas)

        # Verify report was updated with failure
        assert report.num_deployment_fetch_failures == 1
        assert len(report.failed_deployment_fetches) == 1
        assert "com.acme/event2" in report.failed_deployment_fetches[0]
        assert "API timeout" in report.failed_deployment_fetches[0]

    def test_fetch_deployments_sequential_reports_errors_to_report(
        self, mock_bdp_client, sample_schemas
    ):
        """Test that sequential fetching reports errors to the report when provided."""
        report = SnowplowSourceReport()
        fetcher = DeploymentFetcher(
            bdp_client=mock_bdp_client,
            enable_parallel=False,
            max_workers=3,
            report=report,
        )

        # First schema succeeds, second fails
        def side_effect(schema_hash):
            if schema_hash == "hash1":
                return [
                    DataStructureDeployment(
                        version="1-0-0", env="PROD", ts="2024-01-01T00:00:00Z"
                    )
                ]
            else:
                raise Exception("Network error")

        mock_bdp_client.get_data_structure_deployments.side_effect = side_effect

        fetcher.fetch_deployments(sample_schemas)

        # Verify report was updated with failure
        assert report.num_deployment_fetch_failures == 1
        assert len(report.failed_deployment_fetches) == 1
        assert "com.acme/event2" in report.failed_deployment_fetches[0]
        assert "Network error" in report.failed_deployment_fetches[0]

    def test_fetch_deployments_without_report_still_works(
        self, mock_bdp_client, sample_schemas
    ):
        """Test that fetching works without a report (backwards compatibility)."""
        fetcher = DeploymentFetcher(
            bdp_client=mock_bdp_client,
            enable_parallel=True,
            max_workers=3,
            # No report provided
        )

        # Second schema fails
        def side_effect(schema_hash):
            if schema_hash == "hash1":
                return [
                    DataStructureDeployment(
                        version="1-0-0", env="PROD", ts="2024-01-01T00:00:00Z"
                    )
                ]
            else:
                raise Exception("API timeout")

        mock_bdp_client.get_data_structure_deployments.side_effect = side_effect

        # Should not raise exception even without report
        fetcher.fetch_deployments(sample_schemas)

        # First schema should have deployments
        assert sample_schemas[0].deployments is not None
        assert len(sample_schemas[0].deployments) > 0
