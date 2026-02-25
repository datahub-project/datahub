"""Unit tests for Dataplex custom properties extraction."""

from typing import Optional
from unittest.mock import Mock

from google.cloud import dataplex_v1

from datahub.ingestion.source.dataplex.dataplex_properties import (
    extract_aspects_to_custom_properties,
    extract_entity_custom_properties,
    extract_entry_custom_properties,
)


class TestExtractAspectsToCustomProperties:
    """Test extract_aspects_to_custom_properties function."""

    def test_extract_aspects_with_data(self):
        """Test extracting aspects with data fields."""
        custom_properties: dict[str, str] = {}

        # Mock aspect with data
        aspect_value = Mock()
        aspect_value.data = {
            "field1": "value1",
            "field2": 123,
            "field3": True,
        }

        aspects = {
            "dataplex.googleapis.com/schema": aspect_value,
            "dataplex.googleapis.com/partition": aspect_value,
        }

        extract_aspects_to_custom_properties(aspects, custom_properties)

        # Check aspect type properties
        assert "dataplex_aspect_schema" in custom_properties
        assert custom_properties["dataplex_aspect_schema"] == "schema"
        assert "dataplex_aspect_partition" in custom_properties
        assert custom_properties["dataplex_aspect_partition"] == "partition"

        # Check field properties for schema aspect
        assert "dataplex_schema_field1" in custom_properties
        assert "dataplex_schema_field2" in custom_properties
        assert "dataplex_schema_field3" in custom_properties

    def test_extract_aspects_without_data(self):
        """Test extracting aspects without data attribute."""
        custom_properties: dict[str, str] = {}

        # Mock aspect without data
        aspect_value = Mock()
        del aspect_value.data  # Remove data attribute

        aspects = {
            "dataplex.googleapis.com/schema": aspect_value,
        }

        extract_aspects_to_custom_properties(aspects, custom_properties)

        # Should still have aspect type property
        assert "dataplex_aspect_schema" in custom_properties
        assert custom_properties["dataplex_aspect_schema"] == "schema"

        # Should not have field properties
        assert "dataplex_schema_field1" not in custom_properties

    def test_extract_aspects_with_empty_data(self):
        """Test extracting aspects with empty data."""
        custom_properties: dict[str, str] = {}

        # Mock aspect with empty data
        aspect_value = Mock()
        aspect_value.data = {}

        aspects = {
            "dataplex.googleapis.com/schema": aspect_value,
        }

        extract_aspects_to_custom_properties(aspects, custom_properties)

        # Should have aspect type property
        assert "dataplex_aspect_schema" in custom_properties
        assert custom_properties["dataplex_aspect_schema"] == "schema"

        # Should not have field properties
        assert "dataplex_schema_field1" not in custom_properties

    def test_extract_aspects_with_none_data(self):
        """Test extracting aspects with None data."""
        custom_properties: dict[str, str] = {}

        # Mock aspect with None data
        aspect_value = Mock()
        aspect_value.data = None

        aspects = {
            "dataplex.googleapis.com/schema": aspect_value,
        }

        extract_aspects_to_custom_properties(aspects, custom_properties)

        # Should have aspect type property
        assert "dataplex_aspect_schema" in custom_properties
        assert custom_properties["dataplex_aspect_schema"] == "schema"

        # Should not have field properties
        assert "dataplex_schema_field1" not in custom_properties

    def test_extract_aspects_with_complex_path(self):
        """Test extracting aspects with complex path in aspect key."""
        custom_properties: dict[str, str] = {}

        aspect_value = Mock()
        aspect_value.data = {"field1": "value1"}

        aspects = {
            "projects/test-project/locations/us/entryGroups/@bigquery/aspects/complex_type": aspect_value,
        }

        extract_aspects_to_custom_properties(aspects, custom_properties)

        # Should extract the last part of the path as aspect type
        assert "dataplex_aspect_complex_type" in custom_properties
        assert custom_properties["dataplex_aspect_complex_type"] == "complex_type"
        assert "dataplex_complex_type_field1" in custom_properties

    def test_extract_aspects_updates_existing_dict(self):
        """Test that extract_aspects_to_custom_properties updates existing dict."""
        custom_properties = {
            "existing_key": "existing_value",
        }

        aspect_value = Mock()
        aspect_value.data = {"field1": "value1"}

        aspects = {
            "dataplex.googleapis.com/schema": aspect_value,
        }

        extract_aspects_to_custom_properties(aspects, custom_properties)

        # Should preserve existing keys
        assert "existing_key" in custom_properties
        assert custom_properties["existing_key"] == "existing_value"

        # Should add new keys
        assert "dataplex_aspect_schema" in custom_properties


class TestExtractEntryCustomProperties:
    """Test extract_entry_custom_properties function."""

    def create_mock_entry(
        self,
        entry_id: str = "test_entry",
        entry_group_id: str = "@bigquery",
        fully_qualified_name: str = "bigquery:project.dataset.table",
        entry_type: Optional[str] = "TABLE",
        parent_entry: Optional[str] = None,
        entry_source: Optional[Mock] = None,
        aspects: Optional[dict] = None,
    ) -> dataplex_v1.Entry:
        """Create a mock Dataplex entry."""
        entry = Mock(spec=dataplex_v1.Entry)
        entry.fully_qualified_name = fully_qualified_name
        entry.entry_type = entry_type
        entry.aspects = aspects or {}

        if parent_entry:
            entry.parent_entry = parent_entry

        if entry_source is None:
            entry_source = Mock()
        entry.entry_source = entry_source

        return entry

    def test_extract_entry_basic_properties(self):
        """Test extracting basic entry properties."""
        entry = self.create_mock_entry(
            entry_id="my_table",
            entry_group_id="@bigquery",
            fully_qualified_name="bigquery:project.dataset.my_table",
            entry_type="TABLE",
        )

        result = extract_entry_custom_properties(entry, "my_table", "@bigquery")

        assert result["dataplex_ingested"] == "true"
        assert result["dataplex_entry_id"] == "my_table"
        assert result["dataplex_entry_group"] == "@bigquery"
        assert (
            result["dataplex_fully_qualified_name"]
            == "bigquery:project.dataset.my_table"
        )
        assert result["dataplex_entry_type"] == "TABLE"

    def test_extract_entry_without_entry_type(self):
        """Test extracting entry properties without entry_type."""
        entry = self.create_mock_entry(entry_type=None)

        result = extract_entry_custom_properties(entry, "test_entry", "@bigquery")

        assert "dataplex_entry_type" not in result
        assert result["dataplex_ingested"] == "true"

    def test_extract_entry_with_parent_entry(self):
        """Test extracting entry with parent_entry."""
        entry = self.create_mock_entry(parent_entry="parent_entry_id")

        result = extract_entry_custom_properties(entry, "test_entry", "@bigquery")

        assert result["dataplex_parent_entry"] == "parent_entry_id"

    def test_extract_entry_without_parent_entry(self):
        """Test extracting entry without parent_entry attribute."""
        entry = self.create_mock_entry()
        # Remove parent_entry attribute
        if hasattr(entry, "parent_entry"):
            delattr(entry, "parent_entry")

        result = extract_entry_custom_properties(entry, "test_entry", "@bigquery")

        assert "dataplex_parent_entry" not in result

    def test_extract_entry_with_entry_source_resource(self):
        """Test extracting entry with entry_source.resource."""
        entry_source = Mock()
        entry_source.resource = "projects/test-project/datasets/my_dataset"
        entry = self.create_mock_entry(entry_source=entry_source)

        result = extract_entry_custom_properties(entry, "test_entry", "@bigquery")

        assert (
            result["dataplex_source_resource"]
            == "projects/test-project/datasets/my_dataset"
        )

    def test_extract_entry_with_entry_source_system(self):
        """Test extracting entry with entry_source.system."""
        entry_source = Mock()
        entry_source.system = "BIGQUERY"
        entry = self.create_mock_entry(entry_source=entry_source)

        result = extract_entry_custom_properties(entry, "test_entry", "@bigquery")

        assert result["dataplex_source_system"] == "BIGQUERY"

    def test_extract_entry_with_entry_source_platform(self):
        """Test extracting entry with entry_source.platform."""
        entry_source = Mock()
        entry_source.platform = "GCP"
        entry = self.create_mock_entry(entry_source=entry_source)

        result = extract_entry_custom_properties(entry, "test_entry", "@bigquery")

        assert result["dataplex_source_platform"] == "GCP"

    def test_extract_entry_with_entry_source_all_fields(self):
        """Test extracting entry with all entry_source fields."""
        entry_source = Mock()
        entry_source.resource = "projects/test-project/datasets/my_dataset"
        entry_source.system = "BIGQUERY"
        entry_source.platform = "GCP"
        entry = self.create_mock_entry(entry_source=entry_source)

        result = extract_entry_custom_properties(entry, "test_entry", "@bigquery")

        assert (
            result["dataplex_source_resource"]
            == "projects/test-project/datasets/my_dataset"
        )
        assert result["dataplex_source_system"] == "BIGQUERY"
        assert result["dataplex_source_platform"] == "GCP"

    def test_extract_entry_without_entry_source(self):
        """Test extracting entry without entry_source."""
        entry = self.create_mock_entry()
        # Explicitly set entry_source to None to test the None case
        entry.entry_source = None  # type: ignore[assignment]

        result = extract_entry_custom_properties(entry, "test_entry", "@bigquery")

        # Should still have basic properties
        assert result["dataplex_ingested"] == "true"
        # Should not have entry_source properties
        assert "dataplex_source_resource" not in result
        assert "dataplex_source_system" not in result
        assert "dataplex_source_platform" not in result

    def test_extract_entry_with_entry_source_missing_attributes(self):
        """Test extracting entry with entry_source but missing attributes."""
        entry_source = Mock()
        # Remove attributes
        if hasattr(entry_source, "resource"):
            delattr(entry_source, "resource")
        if hasattr(entry_source, "system"):
            delattr(entry_source, "system")
        if hasattr(entry_source, "platform"):
            delattr(entry_source, "platform")
        entry = self.create_mock_entry(entry_source=entry_source)

        result = extract_entry_custom_properties(entry, "test_entry", "@bigquery")

        assert "dataplex_source_resource" not in result
        assert "dataplex_source_system" not in result
        assert "dataplex_source_platform" not in result

    def test_extract_entry_with_aspects(self):
        """Test extracting entry with aspects."""
        aspect_value = Mock()
        aspect_value.data = {"field1": "value1", "field2": 123}

        aspects = {
            "dataplex.googleapis.com/schema": aspect_value,
        }

        entry = self.create_mock_entry(aspects=aspects)

        result = extract_entry_custom_properties(entry, "test_entry", "@bigquery")

        # Should have aspect properties
        assert "dataplex_aspect_schema" in result
        assert "dataplex_schema_field1" in result
        assert "dataplex_schema_field2" in result

    def test_extract_entry_without_aspects(self):
        """Test extracting entry without aspects."""
        entry = self.create_mock_entry(aspects=None)

        result = extract_entry_custom_properties(entry, "test_entry", "@bigquery")

        # Should not have aspect properties
        assert "dataplex_aspect_schema" not in result


class TestExtractEntityCustomProperties:
    """Test extract_entity_custom_properties function."""

    def create_mock_entity(
        self,
        entity_id: str = "test_entity",
        data_path: Optional[str] = None,
        system: Optional[Mock] = None,
        format: Optional[Mock] = None,
        asset: Optional[str] = None,
        catalog_entry: Optional[str] = None,
        compatibility: Optional[str] = None,
        aspects: Optional[dict] = None,
    ) -> dataplex_v1.Entity:
        """Create a mock Dataplex entity."""
        entity = Mock(spec=dataplex_v1.Entity)
        entity.data_path = data_path
        entity.system = system
        entity.format = format
        entity.asset = asset
        entity.aspects = aspects or {}

        if catalog_entry:
            entity.catalog_entry = catalog_entry
        if compatibility:
            entity.compatibility = compatibility

        return entity

    def test_extract_entity_basic_properties(self):
        """Test extracting basic entity properties."""
        entity = self.create_mock_entity()

        result = extract_entity_custom_properties(
            entity_full=entity,
            project_id="test-project",
            lake_id="test-lake",
            zone_id="test-zone",
            entity_id="test_entity",
            zone_metadata={},
        )

        assert result["dataplex_ingested"] == "true"
        assert result["dataplex_lake"] == "test-lake"
        assert result["dataplex_zone"] == "test-zone"
        assert result["dataplex_entity_id"] == "test_entity"

    def test_extract_entity_with_zone_type(self):
        """Test extracting entity with zone type from metadata."""
        entity = self.create_mock_entity()

        zone_metadata = {
            "test-project.test-lake.test-zone": "RAW",
        }

        result = extract_entity_custom_properties(
            entity_full=entity,
            project_id="test-project",
            lake_id="test-lake",
            zone_id="test-zone",
            entity_id="test_entity",
            zone_metadata=zone_metadata,
        )

        assert result["dataplex_zone_type"] == "RAW"

    def test_extract_entity_without_zone_type(self):
        """Test extracting entity without zone type in metadata."""
        entity = self.create_mock_entity()

        zone_metadata = {
            "test-project.other-lake.other-zone": "RAW",
        }

        result = extract_entity_custom_properties(
            entity_full=entity,
            project_id="test-project",
            lake_id="test-lake",
            zone_id="test-zone",
            entity_id="test_entity",
            zone_metadata=zone_metadata,
        )

        assert "dataplex_zone_type" not in result

    def test_extract_entity_with_data_path(self):
        """Test extracting entity with data_path."""
        entity = self.create_mock_entity(data_path="gs://my-bucket/path/to/data")

        result = extract_entity_custom_properties(
            entity_full=entity,
            project_id="test-project",
            lake_id="test-lake",
            zone_id="test-zone",
            entity_id="test_entity",
            zone_metadata={},
        )

        assert result["data_path"] == "gs://my-bucket/path/to/data"

    def test_extract_entity_with_system(self):
        """Test extracting entity with system."""
        system = Mock()
        system.name = "BIGQUERY"
        entity = self.create_mock_entity(system=system)

        result = extract_entity_custom_properties(
            entity_full=entity,
            project_id="test-project",
            lake_id="test-lake",
            zone_id="test-zone",
            entity_id="test_entity",
            zone_metadata={},
        )

        assert result["system"] == "BIGQUERY"

    def test_extract_entity_with_format(self):
        """Test extracting entity with format."""
        format_obj = Mock()
        format_obj.format_ = Mock()
        format_obj.format_.name = "PARQUET"
        entity = self.create_mock_entity(format=format_obj)

        result = extract_entity_custom_properties(
            entity_full=entity,
            project_id="test-project",
            lake_id="test-lake",
            zone_id="test-zone",
            entity_id="test_entity",
            zone_metadata={},
        )

        assert result["format"] == "PARQUET"

    def test_extract_entity_with_asset(self):
        """Test extracting entity with asset."""
        entity = self.create_mock_entity(asset="my-asset")

        result = extract_entity_custom_properties(
            entity_full=entity,
            project_id="test-project",
            lake_id="test-lake",
            zone_id="test-zone",
            entity_id="test_entity",
            zone_metadata={},
        )

        assert result["asset"] == "my-asset"

    def test_extract_entity_with_catalog_entry(self):
        """Test extracting entity with catalog_entry."""
        entity = self.create_mock_entity(catalog_entry="catalog-entry-id")

        result = extract_entity_custom_properties(
            entity_full=entity,
            project_id="test-project",
            lake_id="test-lake",
            zone_id="test-zone",
            entity_id="test_entity",
            zone_metadata={},
        )

        assert result["catalog_entry"] == "catalog-entry-id"

    def test_extract_entity_without_catalog_entry(self):
        """Test extracting entity without catalog_entry attribute."""
        entity = self.create_mock_entity()
        # Remove catalog_entry attribute
        if hasattr(entity, "catalog_entry"):
            delattr(entity, "catalog_entry")

        result = extract_entity_custom_properties(
            entity_full=entity,
            project_id="test-project",
            lake_id="test-lake",
            zone_id="test-zone",
            entity_id="test_entity",
            zone_metadata={},
        )

        assert "catalog_entry" not in result

    def test_extract_entity_with_compatibility(self):
        """Test extracting entity with compatibility."""
        entity = self.create_mock_entity(compatibility="COMPATIBLE")

        result = extract_entity_custom_properties(
            entity_full=entity,
            project_id="test-project",
            lake_id="test-lake",
            zone_id="test-zone",
            entity_id="test_entity",
            zone_metadata={},
        )

        assert result["compatibility"] == "COMPATIBLE"

    def test_extract_entity_without_compatibility(self):
        """Test extracting entity without compatibility attribute."""
        entity = self.create_mock_entity()
        # Remove compatibility attribute
        if hasattr(entity, "compatibility"):
            delattr(entity, "compatibility")

        result = extract_entity_custom_properties(
            entity_full=entity,
            project_id="test-project",
            lake_id="test-lake",
            zone_id="test-zone",
            entity_id="test_entity",
            zone_metadata={},
        )

        assert "compatibility" not in result

    def test_extract_entity_with_aspects(self):
        """Test extracting entity with aspects."""
        aspect_value = Mock()
        aspect_value.data = {"field1": "value1"}

        aspects = {
            "dataplex.googleapis.com/schema": aspect_value,
        }

        entity = self.create_mock_entity(aspects=aspects)

        result = extract_entity_custom_properties(
            entity_full=entity,
            project_id="test-project",
            lake_id="test-lake",
            zone_id="test-zone",
            entity_id="test_entity",
            zone_metadata={},
        )

        # Should have aspect properties
        assert "dataplex_aspect_schema" in result
        assert "dataplex_schema_field1" in result

    def test_extract_entity_without_aspects(self):
        """Test extracting entity without aspects attribute."""
        entity = self.create_mock_entity()
        # Remove aspects attribute
        if hasattr(entity, "aspects"):
            delattr(entity, "aspects")

        result = extract_entity_custom_properties(
            entity_full=entity,
            project_id="test-project",
            lake_id="test-lake",
            zone_id="test-zone",
            entity_id="test_entity",
            zone_metadata={},
        )

        # Should not have aspect properties
        assert "dataplex_aspect_schema" not in result

    def test_extract_entity_with_all_properties(self):
        """Test extracting entity with all possible properties."""
        system = Mock()
        system.name = "BIGQUERY"

        format_obj = Mock()
        format_obj.format_ = Mock()
        format_obj.format_.name = "PARQUET"

        aspect_value = Mock()
        aspect_value.data = {"field1": "value1"}

        aspects = {
            "dataplex.googleapis.com/schema": aspect_value,
        }

        entity = self.create_mock_entity(
            data_path="gs://my-bucket/path",
            system=system,
            format=format_obj,
            asset="my-asset",
            catalog_entry="catalog-id",
            compatibility="COMPATIBLE",
            aspects=aspects,
        )

        zone_metadata = {
            "test-project.test-lake.test-zone": "RAW",
        }

        result = extract_entity_custom_properties(
            entity_full=entity,
            project_id="test-project",
            lake_id="test-lake",
            zone_id="test-zone",
            entity_id="test_entity",
            zone_metadata=zone_metadata,
        )

        # Check all properties are present
        assert result["dataplex_ingested"] == "true"
        assert result["dataplex_lake"] == "test-lake"
        assert result["dataplex_zone"] == "test-zone"
        assert result["dataplex_entity_id"] == "test_entity"
        assert result["dataplex_zone_type"] == "RAW"
        assert result["data_path"] == "gs://my-bucket/path"
        assert result["system"] == "BIGQUERY"
        assert result["format"] == "PARQUET"
        assert result["asset"] == "my-asset"
        assert result["catalog_entry"] == "catalog-id"
        assert result["compatibility"] == "COMPATIBLE"
        assert "dataplex_aspect_schema" in result
