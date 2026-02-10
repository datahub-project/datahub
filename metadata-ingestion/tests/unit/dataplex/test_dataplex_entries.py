"""Unit tests for Dataplex entry processing."""

from datetime import datetime
from threading import Lock
from typing import Optional
from unittest.mock import Mock, patch

import pytest
from google.cloud import dataplex_v1

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_entries import process_entry
from datahub.metadata.schema_classes import (
    ContainerClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    SchemaMetadataClass,
)


class TestProcessEntry:
    """Test process_entry function."""

    @pytest.fixture
    def config(self):
        """Create a default DataplexConfig for testing."""
        return DataplexConfig(project_ids=["test-project"], env="PROD")

    @pytest.fixture
    def entity_data_by_project(self):
        """Create entity data dictionary."""
        return {}

    @pytest.fixture
    def entity_data_lock(self):
        """Create entity data lock."""
        return Lock()

    @pytest.fixture
    def bq_containers(self):
        """Create BigQuery containers dictionary."""
        return {}

    @pytest.fixture
    def bq_containers_lock(self):
        """Create BigQuery containers lock."""
        return Lock()

    @pytest.fixture
    def construct_mcps_fn(self):
        """Create a mock construct_mcps function."""

        def _construct_mcps(dataset_urn, aspects):
            """Mock function that yields MCPs."""
            for aspect in aspects:
                mcp = Mock(spec=MetadataChangeProposalWrapper)
                mcp.aspect = aspect
                mcp.entityUrn = dataset_urn
                yield mcp

        return _construct_mcps

    def create_mock_entry(
        self,
        name: str,
        fully_qualified_name: Optional[str],
        entry_type: str = "TABLE",
        description: Optional[str] = None,
        create_time: Optional[datetime] = None,
        update_time: Optional[datetime] = None,
        aspects: Optional[dict] = None,
    ) -> dataplex_v1.Entry:
        """Create a mock Dataplex entry."""
        entry = Mock(spec=dataplex_v1.Entry)
        entry.name = name
        entry.fully_qualified_name = fully_qualified_name
        entry.entry_type = entry_type
        entry.aspects = aspects or {}

        # Mock entry_source
        entry_source = Mock()
        entry_source.description = description
        entry_source.create_time = create_time
        entry_source.update_time = update_time
        entry.entry_source = entry_source

        return entry

    def test_entry_without_fqn(
        self,
        config,
        entity_data_by_project,
        entity_data_lock,
        bq_containers,
        bq_containers_lock,
        construct_mcps_fn,
    ):
        """Test that entry without FQN is skipped."""
        entry = self.create_mock_entry(
            name="projects/test-project/locations/us/entryGroups/@bigquery/entries/test_entry",
            fully_qualified_name=None,
        )

        results = list(
            process_entry(
                project_id="test-project",
                entry=entry,
                entry_group_id="@bigquery",
                config=config,
                entity_data_by_project=entity_data_by_project,
                entity_data_lock=entity_data_lock,
                bq_containers=bq_containers,
                bq_containers_lock=bq_containers_lock,
                construct_mcps_fn=construct_mcps_fn,
            )
        )

        assert len(results) == 0
        assert len(entity_data_by_project) == 0

    def test_entry_filtered_by_pattern(
        self,
        entity_data_by_project,
        entity_data_lock,
        bq_containers,
        bq_containers_lock,
        construct_mcps_fn,
    ):
        """Test that entries are filtered correctly by pattern (allow/deny)."""
        config = DataplexConfig(
            project_ids=["test-project"],
            filter_config={
                "entries": {
                    "dataset_pattern": {"allow": ["prod_.*"], "deny": [".*_temp"]},
                }
            },
        )

        # Test 1: Entry that doesn't match allow pattern - should be rejected
        entry_dev = self.create_mock_entry(
            name="projects/test-project/locations/us/entryGroups/@bigquery/entries/dev_table",
            fully_qualified_name="bigquery:test-project.my_dataset.dev_table",
        )

        results_dev = list(
            process_entry(
                project_id="test-project",
                entry=entry_dev,
                entry_group_id="@bigquery",
                config=config,
                entity_data_by_project=entity_data_by_project,
                entity_data_lock=entity_data_lock,
                bq_containers=bq_containers,
                bq_containers_lock=bq_containers_lock,
                construct_mcps_fn=construct_mcps_fn,
            )
        )

        assert len(results_dev) == 0, (
            "Entry 'dev_table' should be rejected (doesn't match allow pattern)"
        )

        # Test 2: Entry that matches allow pattern - should be created
        entry_prod = self.create_mock_entry(
            name="projects/test-project/locations/us/entryGroups/@bigquery/entries/prod_table",
            fully_qualified_name="bigquery:test-project.my_dataset.prod_table",
        )

        results_prod = list(
            process_entry(
                project_id="test-project",
                entry=entry_prod,
                entry_group_id="@bigquery",
                config=config,
                entity_data_by_project=entity_data_by_project,
                entity_data_lock=entity_data_lock,
                bq_containers=bq_containers,
                bq_containers_lock=bq_containers_lock,
                construct_mcps_fn=construct_mcps_fn,
            )
        )

        assert len(results_prod) > 0, (
            "Entry 'prod_table' should be created (matches allow pattern)"
        )
        # Verify it has the expected aspects
        assert any(isinstance(r.aspect, DatasetPropertiesClass) for r in results_prod)
        assert any(
            isinstance(r.aspect, DataPlatformInstanceClass) for r in results_prod
        )

        # Test 3: Entry that matches allow pattern but also matches deny pattern - should be rejected
        entry_prod_temp = self.create_mock_entry(
            name="projects/test-project/locations/us/entryGroups/@bigquery/entries/prod_table_temp",
            fully_qualified_name="bigquery:test-project.my_dataset.prod_table_temp",
        )

        results_prod_temp = list(
            process_entry(
                project_id="test-project",
                entry=entry_prod_temp,
                entry_group_id="@bigquery",
                config=config,
                entity_data_by_project=entity_data_by_project,
                entity_data_lock=entity_data_lock,
                bq_containers=bq_containers,
                bq_containers_lock=bq_containers_lock,
                construct_mcps_fn=construct_mcps_fn,
            )
        )

        assert len(results_prod_temp) == 0, (
            "Entry 'prod_table_temp' should be rejected (matches deny pattern)"
        )

    def test_entry_with_invalid_fqn(
        self,
        config,
        entity_data_by_project,
        entity_data_lock,
        bq_containers,
        bq_containers_lock,
        construct_mcps_fn,
    ):
        """Test that entry with invalid FQN is skipped."""
        entry = self.create_mock_entry(
            name="projects/test-project/locations/us/entryGroups/@bigquery/entries/test_entry",
            fully_qualified_name="invalid-fqn-format",
        )

        results = list(
            process_entry(
                project_id="test-project",
                entry=entry,
                entry_group_id="@bigquery",
                config=config,
                entity_data_by_project=entity_data_by_project,
                entity_data_lock=entity_data_lock,
                bq_containers=bq_containers,
                bq_containers_lock=bq_containers_lock,
                construct_mcps_fn=construct_mcps_fn,
            )
        )

        assert len(results) == 0

    def test_bigquery_entry_valid(
        self,
        config,
        entity_data_by_project,
        entity_data_lock,
        bq_containers,
        bq_containers_lock,
        construct_mcps_fn,
    ):
        """Test processing a valid BigQuery entry."""
        create_time = datetime(2023, 1, 1, 12, 0, 0)
        update_time = datetime(2023, 1, 2, 12, 0, 0)

        entry = self.create_mock_entry(
            name="projects/test-project/locations/us/entryGroups/@bigquery/entries/my_table",
            fully_qualified_name="bigquery:test-project.my_dataset.my_table",
            description="Test table description",
            create_time=create_time,
            update_time=update_time,
        )

        results = list(
            process_entry(
                project_id="test-project",
                entry=entry,
                entry_group_id="@bigquery",
                config=config,
                entity_data_by_project=entity_data_by_project,
                entity_data_lock=entity_data_lock,
                bq_containers=bq_containers,
                bq_containers_lock=bq_containers_lock,
                construct_mcps_fn=construct_mcps_fn,
            )
        )

        # Should have at least DatasetProperties and DataPlatformInstance
        assert len(results) >= 2

        # Check DatasetProperties
        dataset_props = None
        platform_instance = None
        container = None
        for result in results:
            if isinstance(result.aspect, DatasetPropertiesClass):
                dataset_props = result.aspect
            elif isinstance(result.aspect, DataPlatformInstanceClass):
                platform_instance = result.aspect
            elif isinstance(result.aspect, ContainerClass):
                container = result.aspect

        assert dataset_props is not None
        assert dataset_props.name == "my_table"
        assert dataset_props.description == "Test table description"
        assert dataset_props.created is not None
        assert dataset_props.lastModified is not None

        assert platform_instance is not None
        assert "bigquery" in str(platform_instance.platform)

        # Should have container for BigQuery
        assert container is not None

        # Check entity data tracking
        assert "test-project" in entity_data_by_project
        entity_data = entity_data_by_project["test-project"]
        assert len(entity_data) == 1
        entity_tuple = next(iter(entity_data))
        assert entity_tuple.entity_id == "my_table"
        assert entity_tuple.source_platform == "bigquery"
        assert entity_tuple.is_entry is True

        # Check BigQuery container tracking
        assert "test-project" in bq_containers
        assert "my_dataset" in bq_containers["test-project"]

    def test_bigquery_entry_insufficient_parts(
        self,
        config,
        entity_data_by_project,
        entity_data_lock,
        bq_containers,
        bq_containers_lock,
        construct_mcps_fn,
    ):
        """Test that BigQuery entry with insufficient parts is skipped."""
        entry = self.create_mock_entry(
            name="projects/test-project/locations/us/entryGroups/@bigquery/entries/test_entry",
            fully_qualified_name="bigquery:test-project.my_dataset",
        )

        results = list(
            process_entry(
                project_id="test-project",
                entry=entry,
                entry_group_id="@bigquery",
                config=config,
                entity_data_by_project=entity_data_by_project,
                entity_data_lock=entity_data_lock,
                bq_containers=bq_containers,
                bq_containers_lock=bq_containers_lock,
                construct_mcps_fn=construct_mcps_fn,
            )
        )

        assert len(results) == 0

    def test_bigquery_entry_zone_metadata(
        self,
        config,
        entity_data_by_project,
        entity_data_lock,
        bq_containers,
        bq_containers_lock,
        construct_mcps_fn,
    ):
        """Test that BigQuery entry with zone metadata is skipped."""
        invalid_zone_patterns = ["_zone", "zone1"]

        for zone_pattern in invalid_zone_patterns:
            table_name = (
                f"test{zone_pattern}" if zone_pattern.startswith("_") else zone_pattern
            )
            entry = self.create_mock_entry(
                name=f"projects/test-project/locations/us/entryGroups/@bigquery/entries/{table_name}",
                fully_qualified_name=f"bigquery:test-project.my_dataset.{table_name}",
            )

            results = list(
                process_entry(
                    project_id="test-project",
                    entry=entry,
                    entry_group_id="@bigquery",
                    config=config,
                    entity_data_by_project=entity_data_by_project,
                    entity_data_lock=entity_data_lock,
                    bq_containers=bq_containers,
                    bq_containers_lock=bq_containers_lock,
                    construct_mcps_fn=construct_mcps_fn,
                )
            )

            assert len(results) == 0, (
                f"Entry with zone pattern '{zone_pattern}' should be skipped"
            )

    def test_bigquery_entry_asset_metadata(
        self,
        config,
        entity_data_by_project,
        entity_data_lock,
        bq_containers,
        bq_containers_lock,
        construct_mcps_fn,
    ):
        """Test that BigQuery entry with asset metadata is skipped."""
        invalid_asset_patterns = ["_asset", "asset1"]

        for asset_pattern in invalid_asset_patterns:
            table_name = (
                f"test{asset_pattern}"
                if asset_pattern.startswith("_")
                else asset_pattern
            )
            entry = self.create_mock_entry(
                name=f"projects/test-project/locations/us/entryGroups/@bigquery/entries/{table_name}",
                fully_qualified_name=f"bigquery:test-project.my_dataset.{table_name}",
            )

            results = list(
                process_entry(
                    project_id="test-project",
                    entry=entry,
                    entry_group_id="@bigquery",
                    config=config,
                    entity_data_by_project=entity_data_by_project,
                    entity_data_lock=entity_data_lock,
                    bq_containers=bq_containers,
                    bq_containers_lock=bq_containers_lock,
                    construct_mcps_fn=construct_mcps_fn,
                )
            )

            assert len(results) == 0, (
                f"Entry with asset pattern '{asset_pattern}' should be skipped"
            )

    def test_gcs_entry_valid(
        self,
        config,
        entity_data_by_project,
        entity_data_lock,
        bq_containers,
        bq_containers_lock,
        construct_mcps_fn,
    ):
        """Test processing a valid GCS entry."""
        entry = self.create_mock_entry(
            name="projects/test-project/locations/us/entryGroups/@gcs/entries/file.parquet",
            fully_qualified_name="gcs:my-bucket/path/to/file.parquet",
            description="GCS file",
        )

        results = list(
            process_entry(
                project_id="test-project",
                entry=entry,
                entry_group_id="@gcs",
                config=config,
                entity_data_by_project=entity_data_by_project,
                entity_data_lock=entity_data_lock,
                bq_containers=bq_containers,
                bq_containers_lock=bq_containers_lock,
                construct_mcps_fn=construct_mcps_fn,
            )
        )

        # Should have at least DatasetProperties and DataPlatformInstance
        assert len(results) >= 2

        dataset_props = None
        for result in results:
            if isinstance(result.aspect, DatasetPropertiesClass):
                dataset_props = result.aspect
                break

        assert dataset_props is not None
        assert dataset_props.description == "GCS file"

        # Check entity data tracking
        assert "test-project" in entity_data_by_project
        entity_data = entity_data_by_project["test-project"]
        assert len(entity_data) == 1
        entity_tuple = next(iter(entity_data))
        assert entity_tuple.source_platform == "gcs"
        assert entity_tuple.is_entry is True

        # GCS entries should not have containers
        assert len(bq_containers) == 0

    def test_gcs_entry_insufficient_parts(
        self,
        config,
        entity_data_by_project,
        entity_data_lock,
        bq_containers,
        bq_containers_lock,
        construct_mcps_fn,
    ):
        """Test that GCS entry with insufficient parts is skipped."""
        entry = self.create_mock_entry(
            name="projects/test-project/locations/us/entryGroups/@gcs/entries/test_entry",
            fully_qualified_name="gcs:my-bucket",
        )

        results = list(
            process_entry(
                project_id="test-project",
                entry=entry,
                entry_group_id="@gcs",
                config=config,
                entity_data_by_project=entity_data_by_project,
                entity_data_lock=entity_data_lock,
                bq_containers=bq_containers,
                bq_containers_lock=bq_containers_lock,
                construct_mcps_fn=construct_mcps_fn,
            )
        )

        assert len(results) == 0

    def test_gcs_entry_asset_metadata(
        self,
        config,
        entity_data_by_project,
        entity_data_lock,
        bq_containers,
        bq_containers_lock,
        construct_mcps_fn,
    ):
        """Test that GCS entry with asset metadata is skipped."""
        entry = self.create_mock_entry(
            name="projects/test-project/locations/us/entryGroups/@gcs/entries/asset1",
            fully_qualified_name="gcs:my-bucket/asset1",
        )

        results = list(
            process_entry(
                project_id="test-project",
                entry=entry,
                entry_group_id="@gcs",
                config=config,
                entity_data_by_project=entity_data_by_project,
                entity_data_lock=entity_data_lock,
                bq_containers=bq_containers,
                bq_containers_lock=bq_containers_lock,
                construct_mcps_fn=construct_mcps_fn,
            )
        )

        assert len(results) == 0

    def test_entry_with_schema_extraction_enabled(
        self,
        entity_data_by_project,
        entity_data_lock,
        bq_containers,
        bq_containers_lock,
        construct_mcps_fn,
    ):
        """Test entry processing with schema extraction enabled."""
        config = DataplexConfig(project_ids=["test-project"], include_schema=True)

        # Mock schema aspect
        schema_aspect = Mock(spec=dataplex_v1.Aspect)
        schema_aspect.data = {
            "fields": [
                {
                    "name": "column1",
                    "type": "STRING",
                    "mode": "NULLABLE",
                }
            ]
        }

        entry = self.create_mock_entry(
            name="projects/test-project/locations/us/entryGroups/@bigquery/entries/my_table",
            fully_qualified_name="bigquery:test-project.my_dataset.my_table",
            aspects={"dataplex.googleapis.com/schema": schema_aspect},
        )

        # Mock extract_schema_from_entry_aspects to return schema
        # Need to patch it in dataplex_entries module since it's imported there
        mock_schema = Mock(spec=SchemaMetadataClass)
        mock_schema.fields = [Mock()]

        with patch(
            "datahub.ingestion.source.dataplex.dataplex_entries.extract_schema_from_entry_aspects"
        ) as mock_extract:
            mock_extract.return_value = mock_schema

            results = list(
                process_entry(
                    project_id="test-project",
                    entry=entry,
                    entry_group_id="@bigquery",
                    config=config,
                    entity_data_by_project=entity_data_by_project,
                    entity_data_lock=entity_data_lock,
                    bq_containers=bq_containers,
                    bq_containers_lock=bq_containers_lock,
                    construct_mcps_fn=construct_mcps_fn,
                )
            )

            # Should have schema metadata
            schema_metadata = None
            for result in results:
                if isinstance(result.aspect, SchemaMetadataClass):
                    schema_metadata = result.aspect
                    break

            assert schema_metadata is not None
            assert mock_extract.called
            assert mock_extract.call_count == 1

    def test_entry_with_schema_extraction_disabled(
        self,
        entity_data_by_project,
        entity_data_lock,
        bq_containers,
        bq_containers_lock,
        construct_mcps_fn,
    ):
        """Test entry processing with schema extraction disabled."""
        config = DataplexConfig(project_ids=["test-project"], include_schema=False)

        entry = self.create_mock_entry(
            name="projects/test-project/locations/us/entryGroups/@bigquery/entries/my_table",
            fully_qualified_name="bigquery:test-project.my_dataset.my_table",
        )

        # Mock extract_schema_from_entry_aspects to ensure it's not called
        # Need to patch it in dataplex_entries module since it's imported there
        with patch(
            "datahub.ingestion.source.dataplex.dataplex_entries.extract_schema_from_entry_aspects"
        ) as mock_extract:
            mock_extract.return_value = None

            results = list(
                process_entry(
                    project_id="test-project",
                    entry=entry,
                    entry_group_id="@bigquery",
                    config=config,
                    entity_data_by_project=entity_data_by_project,
                    entity_data_lock=entity_data_lock,
                    bq_containers=bq_containers,
                    bq_containers_lock=bq_containers_lock,
                    construct_mcps_fn=construct_mcps_fn,
                )
            )

            # Should not have schema metadata
            schema_metadata = None
            for result in results:
                if isinstance(result.aspect, SchemaMetadataClass):
                    schema_metadata = result.aspect
                    break

            assert schema_metadata is None
            # Function should not be called when include_schema is False
            assert not mock_extract.called

    def test_entry_without_timestamps(
        self,
        config,
        entity_data_by_project,
        entity_data_lock,
        bq_containers,
        bq_containers_lock,
        construct_mcps_fn,
    ):
        """Test entry processing without timestamps."""
        entry = self.create_mock_entry(
            name="projects/test-project/locations/us/entryGroups/@bigquery/entries/my_table",
            fully_qualified_name="bigquery:test-project.my_dataset.my_table",
            create_time=None,
            update_time=None,
        )

        results = list(
            process_entry(
                project_id="test-project",
                entry=entry,
                entry_group_id="@bigquery",
                config=config,
                entity_data_by_project=entity_data_by_project,
                entity_data_lock=entity_data_lock,
                bq_containers=bq_containers,
                bq_containers_lock=bq_containers_lock,
                construct_mcps_fn=construct_mcps_fn,
            )
        )

        dataset_props = None
        for result in results:
            if isinstance(result.aspect, DatasetPropertiesClass):
                dataset_props = result.aspect
                break

        assert dataset_props is not None
        assert dataset_props.created is None
        assert dataset_props.lastModified is None

    def test_entry_without_entry_source(
        self,
        config,
        entity_data_by_project,
        entity_data_lock,
        bq_containers,
        bq_containers_lock,
        construct_mcps_fn,
    ):
        """Test entry processing without entry_source."""
        entry = self.create_mock_entry(
            name="projects/test-project/locations/us/entryGroups/@bigquery/entries/my_table",
            fully_qualified_name="bigquery:test-project.my_dataset.my_table",
        )
        entry.entry_source = None  # type: ignore[assignment]

        results = list(
            process_entry(
                project_id="test-project",
                entry=entry,
                entry_group_id="@bigquery",
                config=config,
                entity_data_by_project=entity_data_by_project,
                entity_data_lock=entity_data_lock,
                bq_containers=bq_containers,
                bq_containers_lock=bq_containers_lock,
                construct_mcps_fn=construct_mcps_fn,
            )
        )

        dataset_props = None
        for result in results:
            if isinstance(result.aspect, DatasetPropertiesClass):
                dataset_props = result.aspect
                break

        assert dataset_props is not None
        assert dataset_props.description == ""
        assert dataset_props.created is None
        assert dataset_props.lastModified is None

    def test_entry_custom_properties(
        self,
        config,
        entity_data_by_project,
        entity_data_lock,
        bq_containers,
        bq_containers_lock,
        construct_mcps_fn,
    ):
        """Test that custom properties are extracted from entry."""
        entry = self.create_mock_entry(
            name="projects/test-project/locations/us/entryGroups/@bigquery/entries/my_table",
            fully_qualified_name="bigquery:test-project.my_dataset.my_table",
            entry_type="TABLE",
        )

        results = list(
            process_entry(
                project_id="test-project",
                entry=entry,
                entry_group_id="@bigquery",
                config=config,
                entity_data_by_project=entity_data_by_project,
                entity_data_lock=entity_data_lock,
                bq_containers=bq_containers,
                bq_containers_lock=bq_containers_lock,
                construct_mcps_fn=construct_mcps_fn,
            )
        )

        dataset_props = None
        for result in results:
            if isinstance(result.aspect, DatasetPropertiesClass):
                dataset_props = result.aspect
                break

        assert dataset_props is not None
        assert dataset_props.customProperties is not None
        assert "dataplex_ingested" in dataset_props.customProperties
        assert dataset_props.customProperties["dataplex_ingested"] == "true"
        assert "dataplex_entry_id" in dataset_props.customProperties
        assert dataset_props.customProperties["dataplex_entry_id"] == "my_table"
        assert "dataplex_entry_group" in dataset_props.customProperties
        assert dataset_props.customProperties["dataplex_entry_group"] == "@bigquery"
        assert "dataplex_fully_qualified_name" in dataset_props.customProperties

    def test_multiple_entries_same_project(
        self,
        config,
        entity_data_by_project,
        entity_data_lock,
        bq_containers,
        bq_containers_lock,
        construct_mcps_fn,
    ):
        """Test processing multiple entries for the same project."""
        entry1 = self.create_mock_entry(
            name="projects/test-project/locations/us/entryGroups/@bigquery/entries/table1",
            fully_qualified_name="bigquery:test-project.dataset1.table1",
        )

        entry2 = self.create_mock_entry(
            name="projects/test-project/locations/us/entryGroups/@bigquery/entries/table2",
            fully_qualified_name="bigquery:test-project.dataset2.table2",
        )

        # Process first entry
        list(
            process_entry(
                project_id="test-project",
                entry=entry1,
                entry_group_id="@bigquery",
                config=config,
                entity_data_by_project=entity_data_by_project,
                entity_data_lock=entity_data_lock,
                bq_containers=bq_containers,
                bq_containers_lock=bq_containers_lock,
                construct_mcps_fn=construct_mcps_fn,
            )
        )

        # Process second entry
        list(
            process_entry(
                project_id="test-project",
                entry=entry2,
                entry_group_id="@bigquery",
                config=config,
                entity_data_by_project=entity_data_by_project,
                entity_data_lock=entity_data_lock,
                bq_containers=bq_containers,
                bq_containers_lock=bq_containers_lock,
                construct_mcps_fn=construct_mcps_fn,
            )
        )

        # Check entity data tracking
        assert "test-project" in entity_data_by_project
        entity_data = entity_data_by_project["test-project"]
        assert len(entity_data) == 2

        # Check BigQuery container tracking
        assert "test-project" in bq_containers
        assert "dataset1" in bq_containers["test-project"]
        assert "dataset2" in bq_containers["test-project"]

    def test_entry_fqn_without_colon(
        self,
        config,
        entity_data_by_project,
        entity_data_lock,
        bq_containers,
        bq_containers_lock,
        construct_mcps_fn,
    ):
        """Test entry with FQN that doesn't have colon (uses entry_id as dataset_name)."""
        entry = self.create_mock_entry(
            name="projects/test-project/locations/us/entryGroups/@bigquery/entries/my_table",
            fully_qualified_name="my_table",  # No colon, no platform prefix
        )

        results = list(
            process_entry(
                project_id="test-project",
                entry=entry,
                entry_group_id="@bigquery",
                config=config,
                entity_data_by_project=entity_data_by_project,
                entity_data_lock=entity_data_lock,
                bq_containers=bq_containers,
                bq_containers_lock=bq_containers_lock,
                construct_mcps_fn=construct_mcps_fn,
            )
        )

        # Should be skipped because parse_entry_fqn will fail
        assert len(results) == 0
