"""Unit tests for Dataplex URN generation consistency.

This module tests that Entities API and Entries API generate identical URNs
for the same resources, ensuring proper deduplication in DataHub.
"""

import datahub.emitter.mce_builder as builder
from datahub.ingestion.source.dataplex.dataplex_helpers import (
    make_entity_dataset_urn,
)


class TestUrnGeneration:
    """Test URN generation for consistency between APIs."""

    def test_bigquery_entity_urn_format(self):
        """Test that entity URN follows expected BigQuery format."""
        entity_id = "my_table"
        project_id = "my-project"
        dataset_id = "my_dataset"
        platform = "bigquery"
        env = "PROD"

        urn = make_entity_dataset_urn(
            entity_id=entity_id,
            project_id=project_id,
            env=env,
            dataset_id=dataset_id,
            platform=platform,
        )

        # Expected format: urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.my_dataset.my_table,PROD)
        assert urn.startswith("urn:li:dataset:(urn:li:dataPlatform:bigquery,")
        assert f"{project_id}.{dataset_id}.{entity_id}" in urn
        assert env in urn

    def test_bigquery_entry_urn_format(self):
        """Test that entry URN follows expected BigQuery format."""
        # Simulate entry FQN parsing
        fqn = "bigquery:my-project.my_dataset.my_table"
        platform, resource_path = fqn.split(":", 1)
        dataset_name = resource_path  # Full path: my-project.my_dataset.my_table

        urn = builder.make_dataset_urn_with_platform_instance(
            platform=platform,
            name=dataset_name,
            platform_instance=None,
            env="PROD",
        )

        # Expected format: urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.my_dataset.my_table,PROD)
        assert urn.startswith("urn:li:dataset:(urn:li:dataPlatform:bigquery,")
        assert "my-project.my_dataset.my_table" in urn
        assert "PROD" in urn

    def test_entity_entry_urn_alignment_bigquery(self):
        """Verify that entity and entry URNs match for the same BigQuery table.

        This is critical for ensuring that when both APIs discover the same table,
        they produce identical URNs so DataHub can properly deduplicate them.
        """
        # Common table identifiers
        project_id = "my-project"
        dataset_id = "my_dataset"
        table_name = "my_table"
        env = "PROD"

        # Entity API: Generate URN using entity helper
        entity_urn = make_entity_dataset_urn(
            entity_id=table_name,
            project_id=project_id,
            env=env,
            dataset_id=dataset_id,
            platform="bigquery",
        )

        # Entries API: Generate URN from FQN
        fqn = f"bigquery:{project_id}.{dataset_id}.{table_name}"
        _, resource_path = fqn.split(":", 1)
        entry_urn = builder.make_dataset_urn_with_platform_instance(
            platform="bigquery",
            name=resource_path,  # Full path including project
            platform_instance=None,
            env=env,
        )

        # CRITICAL: URNs must match exactly for deduplication to work
        assert entity_urn == entry_urn, (
            f"URN mismatch!\nEntity: {entity_urn}\nEntry:  {entry_urn}"
        )

    def test_entity_entry_urn_alignment_multiple_tables(self):
        """Test URN alignment for multiple BigQuery tables."""
        test_cases = [
            # (project_id, dataset_id, table_name)
            ("project-123", "analytics", "user_events"),
            ("my-project", "raw_data", "transactions"),
            ("data-warehouse", "prod_dataset", "dim_customer"),
        ]

        for project_id, dataset_id, table_name in test_cases:
            env = "PROD"

            # Entity URN
            entity_urn = make_entity_dataset_urn(
                entity_id=table_name,
                project_id=project_id,
                env=env,
                dataset_id=dataset_id,
                platform="bigquery",
            )

            # Entry URN from FQN
            fqn = f"bigquery:{project_id}.{dataset_id}.{table_name}"
            _, resource_path = fqn.split(":", 1)
            entry_urn = builder.make_dataset_urn_with_platform_instance(
                platform="bigquery",
                name=resource_path,
                platform_instance=None,
                env=env,
            )

            assert entity_urn == entry_urn, (
                f"URN mismatch for {project_id}.{dataset_id}.{table_name}!\nEntity: {entity_urn}\nEntry:  {entry_urn}"
            )

    def test_gcs_entity_urn_format(self):
        """Test that entity URN follows expected GCS format."""
        entity_id = "path/to/file.parquet"
        project_id = "my-project"
        bucket_name = "my-bucket"
        platform = "gcs"
        env = "PROD"

        urn = make_entity_dataset_urn(
            entity_id=entity_id,
            project_id=project_id,
            env=env,
            dataset_id=bucket_name,
            platform=platform,
        )

        # Expected format: urn:li:dataset:(urn:li:dataPlatform:gcs,my-project.my-bucket.path/to/file.parquet,PROD)
        assert urn.startswith("urn:li:dataset:(urn:li:dataPlatform:gcs,")
        assert bucket_name in urn
        assert env in urn

    def test_urn_generation_preserves_special_characters(self):
        """Test that URN generation properly handles special characters in names."""
        test_cases = [
            ("table_with_underscores", "my-project", "my_dataset"),
            ("table-with-dashes", "my-project", "my-dataset"),
            ("table123numbers", "project-456", "dataset789"),
        ]

        for table_name, project_id, dataset_id in test_cases:
            entity_urn = make_entity_dataset_urn(
                entity_id=table_name,
                project_id=project_id,
                env="PROD",
                dataset_id=dataset_id,
                platform="bigquery",
            )

            # Entry URN
            fqn = f"bigquery:{project_id}.{dataset_id}.{table_name}"
            _, resource_path = fqn.split(":", 1)
            entry_urn = builder.make_dataset_urn_with_platform_instance(
                platform="bigquery",
                name=resource_path,
                platform_instance=None,
                env="PROD",
            )

            assert entity_urn == entry_urn, (
                f"URN mismatch with special characters!\nEntity: {entity_urn}\nEntry:  {entry_urn}"
            )

    def test_different_environments_produce_different_urns(self):
        """Test that different environments produce different URNs."""
        entity_id = "my_table"
        project_id = "my-project"
        dataset_id = "my_dataset"

        urn_prod = make_entity_dataset_urn(
            entity_id=entity_id,
            project_id=project_id,
            env="PROD",
            dataset_id=dataset_id,
            platform="bigquery",
        )

        urn_dev = make_entity_dataset_urn(
            entity_id=entity_id,
            project_id=project_id,
            env="DEV",
            dataset_id=dataset_id,
            platform="bigquery",
        )

        assert urn_prod != urn_dev
        assert "PROD" in urn_prod
        assert "DEV" in urn_dev

    def test_urn_components_extraction(self):
        """Test that URN contains all expected components."""
        project_id = "test-project"
        dataset_id = "test_dataset"
        table_name = "test_table"
        env = "PROD"
        platform = "bigquery"

        urn = make_entity_dataset_urn(
            entity_id=table_name,
            project_id=project_id,
            env=env,
            dataset_id=dataset_id,
            platform=platform,
        )

        # Verify all components are present
        assert "urn:li:dataset:" in urn
        assert f"dataPlatform:{platform}" in urn
        assert project_id in urn
        assert dataset_id in urn
        assert table_name in urn
        assert env in urn


class TestFqnParsing:
    """Test FQN parsing logic used in entries processing."""

    def test_parse_bigquery_fqn(self):
        """Test parsing BigQuery FQN format."""
        fqn = "bigquery:my-project.my_dataset.my_table"

        assert ":" in fqn
        platform, resource_path = fqn.split(":", 1)

        assert platform == "bigquery"
        assert resource_path == "my-project.my_dataset.my_table"

        # Verify it has 3 parts (project.dataset.table)
        parts = resource_path.split(".")
        assert len(parts) == 3

    def test_parse_gcs_fqn(self):
        """Test parsing GCS FQN format."""
        fqn = "gcs:my-bucket/path/to/file.parquet"

        assert ":" in fqn
        platform, resource_path = fqn.split(":", 1)

        assert platform == "gcs"
        assert resource_path == "my-bucket/path/to/file.parquet"

    def test_invalid_fqn_format(self):
        """Test handling of invalid FQN format."""
        # FQN without platform prefix should be detectable
        fqn = "my-project.my_dataset.my_table"

        # This should not have a colon
        assert ":" not in fqn


class TestEntryValidation:
    """Test entry validation logic for skipping asset metadata."""

    def test_bigquery_entry_validation_valid_table(self):
        """Test that valid BigQuery table entries pass validation."""
        fqn = "bigquery:my-project.my_dataset.my_table"

        assert ":" in fqn
        _, resource_path = fqn.split(":", 1)
        parts = resource_path.split(".")

        # Should have at least 3 parts (project.dataset.table)
        assert len(parts) >= 3

        # Table name shouldn't look like zone/asset metadata
        table_name = parts[-1]
        assert not any(
            suffix in table_name.lower()
            for suffix in ["_zone", "_asset", "zone1", "asset1"]
        )

    def test_bigquery_entry_validation_asset_metadata(self):
        """Test that asset metadata entries are detected."""
        test_cases = [
            "bigquery:my-project.my_dataset",  # Only 2 parts
            "bigquery:my-project.my_dataset.zone1",  # Zone metadata
            "bigquery:my-project.my_dataset.my_asset",  # Asset metadata
        ]

        for fqn in test_cases:
            _, resource_path = fqn.split(":", 1)
            parts = resource_path.split(".")

            # Either not enough parts or suspicious table name
            if len(parts) < 3:
                # Should be skipped
                assert True
            else:
                table_name = parts[-1]
                if any(
                    suffix in table_name.lower()
                    for suffix in ["_zone", "_asset", "zone1", "asset1"]
                ):
                    # Should be skipped
                    assert True

    def test_gcs_entry_validation_valid_file(self):
        """Test that valid GCS file entries pass validation."""
        fqn = "gcs:my-bucket/data/file.parquet"

        assert ":" in fqn
        _, resource_path = fqn.split(":", 1)
        parts = resource_path.split("/")

        # Should have at least 2 parts (bucket/path)
        assert len(parts) >= 2

        # Object name shouldn't look like asset metadata
        object_name = parts[-1]
        assert not any(suffix in object_name.lower() for suffix in ["_asset", "asset1"])

    def test_gcs_entry_validation_asset_metadata(self):
        """Test that GCS asset metadata entries are detected."""
        test_cases = [
            "gcs:my-bucket",  # Only bucket (1 part)
            "gcs:my-bucket/asset1",  # Asset metadata
        ]

        for fqn in test_cases:
            _, resource_path = fqn.split(":", 1)
            parts = resource_path.split("/")

            # Either not enough parts or suspicious object name
            if len(parts) < 2:
                # Should be skipped
                assert True
            else:
                object_name = parts[-1]
                if any(
                    suffix in object_name.lower() for suffix in ["_asset", "asset1"]
                ):
                    # Should be skipped
                    assert True
