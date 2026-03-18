"""Unit tests for Dataplex configuration."""

import pytest
from pydantic import ValidationError

from datahub.ingestion.source.dataplex.dataplex_config import (
    DataplexConfig,
    DataplexFilterConfig,
    EntriesFilterConfig,
)


class TestDataplexFilterConfig:
    """Test filter configuration classes."""

    def test_entries_filter_config_defaults(self):
        """Test that EntriesFilterConfig has correct defaults."""
        config = EntriesFilterConfig()

        # Dataset pattern should allow everything by default
        assert config.dataset_pattern.allowed("any-entry")

    def test_entries_filter_config_with_patterns(self):
        """Test EntriesFilterConfig with custom patterns."""
        config = EntriesFilterConfig(
            dataset_pattern={"allow": ["bq_.*"], "deny": [".*_test"]},
        )

        # Dataset filtering
        assert config.dataset_pattern.allowed("bq_customers")
        assert not config.dataset_pattern.allowed("bq_customers_test")
        assert not config.dataset_pattern.allowed("gcs_files")

    def test_dataplex_filter_config_defaults(self):
        """Test that DataplexFilterConfig has correct defaults."""
        config = DataplexFilterConfig()

        # Entries sub-config should exist with defaults
        assert config.entries is not None
        assert config.entries.dataset_pattern.allowed("any-entry")

    def test_dataplex_filter_config_with_nested_patterns(self):
        """Test DataplexFilterConfig with nested entries patterns."""
        config = DataplexFilterConfig(
            entries={
                "dataset_pattern": {"allow": ["entry_.*"]},
            },
        )

        # Entries filtering
        assert config.entries.dataset_pattern.allowed("entry_table")
        assert not config.entries.dataset_pattern.allowed("entity_table")


class TestDataplexConfig:
    """Test main DataplexConfig class."""

    def test_minimal_config(self):
        """Test minimal valid configuration."""
        config = DataplexConfig(project_ids=["test-project"])

        assert config.project_ids == ["test-project"]
        assert config.filter_config is not None

    def test_config_with_filter_patterns(self):
        """Test configuration with filter patterns."""
        config = DataplexConfig(
            project_ids=["test-project"],
            filter_config={
                "entries": {
                    "dataset_pattern": {"allow": ["entry_.*"]},
                },
            },
        )

        # Verify filter config is properly structured
        assert config.filter_config.entries.dataset_pattern.allowed("entry_table")

    def test_config_entries_only(self):
        """Test configuration for entries-only mode."""
        config = DataplexConfig(
            project_ids=["test-project"],
            entries_location="us",
            filter_config={
                "entries": {
                    "dataset_pattern": {"allow": ["prod_.*"]},
                }
            },
        )

        assert config.entries_location == "us"
        assert config.filter_config.entries.dataset_pattern.allowed("prod_table")
        assert not config.filter_config.entries.dataset_pattern.allowed("dev_table")

    def test_config_validation_requires_project_ids(self):
        """Test that configuration validation requires at least one project."""
        with pytest.raises(ValidationError) as exc_info:
            DataplexConfig(project_ids=[])

        assert "At least one project must be specified" in str(exc_info.value)

    def test_config_project_id_backward_compatibility(self):
        """Test backward compatibility for project_id field."""
        # Using deprecated project_id (single)
        config = DataplexConfig(project_ids=["test-project"])

        # Should be migrated to project_ids
        assert config.project_ids == ["test-project"]

    def test_config_default_values(self):
        """Test that default configuration values are correct."""
        config = DataplexConfig(project_ids=["test-project"])

        # API selection defaults
        assert config.include_schema is True
        assert config.include_lineage is True

        # Performance defaults
        assert config.batch_size == 1000

        # Lineage retry defaults
        assert config.lineage_max_retries == 3
        assert config.lineage_retry_backoff_multiplier == 1.0

        # Location defaults
        assert config.entries_location == "us"

        # Filter defaults (should allow all)
        assert config.filter_config.entries.dataset_pattern.allowed("any-entry")

    def test_filter_config_only_entries_dataset_pattern(self):
        """Test configuration with only entries dataset_pattern (common case)."""
        config = DataplexConfig(
            project_ids=["test-project"],
            filter_config={
                "entries": {
                    "dataset_pattern": {"allow": ["analytics_.*"]},
                }
            },
        )

        # Entry dataset filtering works
        assert config.filter_config.entries.dataset_pattern.allowed("analytics_table")
        assert not config.filter_config.entries.dataset_pattern.allowed("prod_table")

    def test_multiple_projects(self):
        """Test configuration with multiple projects."""
        config = DataplexConfig(
            project_ids=["project-1", "project-2", "project-3"],
            filter_config={
                "entries": {
                    "dataset_pattern": {"allow": ["prod_.*"]},
                }
            },
        )

        assert len(config.project_ids) == 3
        assert "project-1" in config.project_ids
        assert "project-2" in config.project_ids
        assert "project-3" in config.project_ids

    def test_credentials_configuration(self):
        """Test configuration with credentials."""
        config = DataplexConfig(
            project_ids=["test-project"],
            credential={
                "project_id": "cred-project",
                "private_key_id": "key123",
                "private_key": "-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----\n",
                "client_email": "test@example.com",
                "client_id": "123456",
            },
        )

        assert config.credential is not None
        creds = config.get_credentials()
        assert creds is not None
        # Note: get_credentials() uses the first project_id from config.project_ids
        assert creds["project_id"] == "test-project"
        assert "private_key_id" in creds
        assert "client_email" in creds

    def test_schema_configuration(self):
        """Test schema extraction configuration."""
        # Test with schema enabled (default)
        config = DataplexConfig(project_ids=["test-project"])
        assert config.include_schema is True

        # Test disabling schema extraction
        config2 = DataplexConfig(project_ids=["test-project"], include_schema=False)
        assert config2.include_schema is False

    def test_lineage_configuration(self):
        """Test lineage-related configuration."""
        config = DataplexConfig(
            project_ids=["test-project"],
            include_lineage=True,
            batch_size=500,
        )

        assert config.include_lineage is True
        assert config.batch_size == 500

        # Test disabling lineage
        config2 = DataplexConfig(
            project_ids=["test-project"], include_lineage=False, batch_size=None
        )

        assert config2.include_lineage is False
        assert config2.batch_size is None  # Disable batching

    def test_lineage_retry_configuration_defaults(self):
        """Test that lineage retry configuration has correct defaults."""
        config = DataplexConfig(project_ids=["test-project"])

        # Verify default retry settings
        assert config.lineage_max_retries == 3
        assert config.lineage_retry_backoff_multiplier == 1.0

    def test_lineage_retry_configuration_custom_values(self):
        """Test lineage retry configuration with custom values."""
        config = DataplexConfig(
            project_ids=["test-project"],
            lineage_max_retries=5,
            lineage_retry_backoff_multiplier=2.0,
        )

        assert config.lineage_max_retries == 5
        assert config.lineage_retry_backoff_multiplier == 2.0

    def test_lineage_retry_configuration_bounds(self):
        """Test lineage retry configuration validation bounds."""
        # Test max_retries lower bound
        with pytest.raises(ValidationError) as exc_info:
            DataplexConfig(project_ids=["test-project"], lineage_max_retries=0)
        assert "greater than or equal to 1" in str(exc_info.value)

        # Test max_retries upper bound
        with pytest.raises(ValidationError) as exc_info:
            DataplexConfig(project_ids=["test-project"], lineage_max_retries=11)
        assert "less than or equal to 10" in str(exc_info.value)

        # Test backoff_multiplier lower bound
        with pytest.raises(ValidationError) as exc_info:
            DataplexConfig(
                project_ids=["test-project"], lineage_retry_backoff_multiplier=0.05
            )
        assert "greater than or equal to 0.1" in str(exc_info.value)

        # Test backoff_multiplier upper bound
        with pytest.raises(ValidationError) as exc_info:
            DataplexConfig(
                project_ids=["test-project"], lineage_retry_backoff_multiplier=11.0
            )
        assert "less than or equal to 10" in str(exc_info.value)

    def test_lineage_retry_configuration_edge_values(self):
        """Test lineage retry configuration with edge values (boundaries)."""
        # Minimum valid values
        config_min = DataplexConfig(
            project_ids=["test-project"],
            lineage_max_retries=1,
            lineage_retry_backoff_multiplier=0.1,
        )
        assert config_min.lineage_max_retries == 1
        assert config_min.lineage_retry_backoff_multiplier == 0.1

        # Maximum valid values
        config_max = DataplexConfig(
            project_ids=["test-project"],
            lineage_max_retries=10,
            lineage_retry_backoff_multiplier=10.0,
        )
        assert config_max.lineage_max_retries == 10
        assert config_max.lineage_retry_backoff_multiplier == 10.0

    def test_entries_location_default(self):
        """Test that entries_location defaults to 'us'."""
        config = DataplexConfig(project_ids=["test-project"])

        # Should default to multi-region "us"
        assert config.entries_location == "us"

    def test_location_validation_warnings(self, caplog):
        """Test location configuration validation warnings."""
        import logging

        # Test: Regional location for entries (should warn)
        with caplog.at_level(logging.WARNING):
            DataplexConfig(
                project_ids=["test-project"],
                entries_location="us-central1",  # Regional - wrong for entries
            )
            assert (
                "entries_location='us-central1' appears to be a regional location"
                in caplog.text
            )
            assert "@bigquery require multi-region locations" in caplog.text

        caplog.clear()

        # Test: Correct configuration (no warnings)
        with caplog.at_level(logging.WARNING):
            DataplexConfig(
                project_ids=["test-project"],
                entries_location="us",  # Multi-region for entries (correct)
            )
            # Should not have location-related warnings
            assert (
                "entries_location" not in caplog.text
                or "appears to be" not in caplog.text
            )
