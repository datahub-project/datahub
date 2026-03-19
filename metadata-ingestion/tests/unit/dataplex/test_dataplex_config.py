"""Unit tests for Dataplex configuration."""

import pytest
from pydantic import ValidationError

from datahub.ingestion.source.dataplex.dataplex_config import (
    DataplexConfig,
)


class TestDataplexConfig:
    """Test main DataplexConfig class."""

    def test_minimal_config(self):
        """Test minimal valid configuration."""
        config = DataplexConfig(project_ids=["test-project"])

        assert config.project_ids == ["test-project"]
        assert config.filter_config is not None

    def test_entry_groups_filter(self):
        """Test configuration with entry groups filter."""
        config = DataplexConfig(
            project_ids=["test-project"],
            filter_config={
                "entry_groups": {
                    "pattern": {"allow": ["@bigquery", "@pubsub"], "deny": ["@test"]}
                }
            },
        )

        assert config.filter_config.entry_groups.pattern.allowed("@bigquery")
        assert config.filter_config.entry_groups.pattern.allowed("@pubsub")
        assert not config.filter_config.entry_groups.pattern.allowed("@test")
        assert not config.filter_config.entry_groups.pattern.allowed("@gcs")

    def test_entries_pattern_filter(self):
        """Test configuration with entries name pattern filter."""
        config = DataplexConfig(
            project_ids=["test-project"],
            filter_config={
                "entries": {
                    "pattern": {
                        "allow": [".*/tables/users$"],
                        "deny": [".*_temp"],
                    }
                }
            },
        )

        # Entry names follow format: bigquery.googleapis.com/projects/my-project/datasets/analytics/tables/users
        assert config.filter_config.entries.pattern.allowed(
            "bigquery.googleapis.com/projects/my-project/datasets/analytics/tables/users"
        )
        assert not config.filter_config.entries.pattern.allowed(
            "bigquery.googleapis.com/projects/my-project/datasets/analytics/tables/users_temp"
        )
        assert not config.filter_config.entries.pattern.allowed(
            "bigquery.googleapis.com/projects/my-project/datasets/analytics/tables/orders"
        )

    def test_entries_fqn_pattern_filter(self):
        """Test configuration with entries FQN filter."""
        config = DataplexConfig(
            project_ids=["test-project"],
            filter_config={
                "entries": {
                    "fqn_pattern": {
                        "allow": ["bigquery:.*\\.production\\..*"],
                        "deny": [".*_test"],
                    }
                }
            },
        )

        assert config.filter_config.entries.fqn_pattern.allowed(
            "bigquery:my-project.production.users"
        )
        assert not config.filter_config.entries.fqn_pattern.allowed(
            "bigquery:my-project.production.users_test"
        )
        assert not config.filter_config.entries.fqn_pattern.allowed(
            "bigquery:my-project.staging.users"
        )

    def test_entries_both_filters(self):
        """Test configuration with both entry name pattern and FQN pattern."""
        config = DataplexConfig(
            project_ids=["test-project"],
            filter_config={
                "entries": {
                    "pattern": {"allow": [".*analytics.*"]},
                    "fqn_pattern": {"allow": ["bigquery:.*\\.production\\..*"]},
                }
            },
        )

        # Both patterns are independent - each is evaluated separately
        assert config.filter_config.entries.pattern.allowed(
            "bigquery.googleapis.com/projects/my-project/datasets/analytics/tables/users"
        )
        assert not config.filter_config.entries.pattern.allowed(
            "bigquery.googleapis.com/projects/my-project/datasets/sales/tables/orders"
        )
        assert config.filter_config.entries.fqn_pattern.allowed(
            "bigquery:my-project.production.users"
        )
        assert not config.filter_config.entries.fqn_pattern.allowed(
            "bigquery:my-project.staging.events"
        )

    def test_validate_project_ids_empty(self):
        """Test that empty project_ids raises validation error."""
        with pytest.raises(ValidationError) as exc_info:
            DataplexConfig(project_ids=[])

        assert "At least one project must be specified" in str(exc_info.value)

    def test_location_validation_warning(self, caplog):
        """Test that regional locations trigger a warning."""
        import logging

        with caplog.at_level(logging.WARNING):
            DataplexConfig(
                project_ids=["test-project"],
                entries_locations=["us-central1"],  # Regional location
            )

        assert "us-central1" in caplog.text
        assert "appears to be a regional location" in caplog.text
        assert "multi-region locations" in caplog.text
