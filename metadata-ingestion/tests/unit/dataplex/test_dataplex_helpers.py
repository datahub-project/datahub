"""Unit tests for Dataplex helper functions and utilities."""

import json
from unittest.mock import Mock

from google.api_core import exceptions
from google.cloud import dataplex_v1

from datahub.ingestion.source.dataplex.dataplex_helpers import (
    EntityDataTuple,
    determine_entity_platform,
    extract_entity_metadata,
    make_audit_stamp,
    make_bigquery_dataset_container_key,
    make_dataplex_external_url,
    make_entity_dataset_urn,
    map_dataplex_field_to_datahub,
    map_dataplex_type_to_datahub,
    parse_entry_fqn,
    serialize_field_value,
)
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
)


class TestEntityDataTuple:
    """Test EntityDataTuple dataclass."""

    def test_create_entity_data_tuple(self):
        """Test creating EntityDataTuple."""
        entity_data = EntityDataTuple(
            lake_id="test-lake",
            zone_id="test-zone",
            entity_id="test-entity",
            asset_id="test-asset",
            source_platform="bigquery",
            dataset_id="test-dataset",
            is_entry=True,
        )

        assert entity_data.lake_id == "test-lake"
        assert entity_data.zone_id == "test-zone"
        assert entity_data.entity_id == "test-entity"
        assert entity_data.asset_id == "test-asset"
        assert entity_data.source_platform == "bigquery"
        assert entity_data.dataset_id == "test-dataset"
        assert entity_data.is_entry is True

    def test_entity_data_tuple_hashable(self):
        """Test that EntityDataTuple is hashable (frozen)."""
        entity_data1 = EntityDataTuple(
            lake_id="lake1",
            zone_id="zone1",
            entity_id="entity1",
            asset_id="asset1",
            source_platform="bigquery",
            dataset_id="dataset1",
        )

        entity_data2 = EntityDataTuple(
            lake_id="lake1",
            zone_id="zone1",
            entity_id="entity1",
            asset_id="asset1",
            source_platform="bigquery",
            dataset_id="dataset1",
        )

        # Should be able to add to set
        entity_set = {entity_data1, entity_data2}
        assert len(entity_set) == 1

    def test_entity_data_tuple_default_is_entry(self):
        """Test default value for is_entry."""
        entity_data = EntityDataTuple(
            lake_id="lake1",
            zone_id="zone1",
            entity_id="entity1",
            asset_id="asset1",
            source_platform="bigquery",
            dataset_id="dataset1",
        )

        assert entity_data.is_entry is False


class TestMakeBigQueryDatasetContainerKey:
    """Test make_bigquery_dataset_container_key function."""

    def test_make_container_key(self):
        """Test creating BigQuery dataset container key."""
        key = make_bigquery_dataset_container_key(
            project_id="test-project",
            dataset_id="test_dataset",
            platform="bigquery",
            env="PROD",
        )

        assert key.project_id == "test-project"
        assert key.dataset_id == "test_dataset"
        assert key.platform == "bigquery"
        assert key.env == "PROD"
        assert key.backcompat_env_as_instance is True


class TestMakeEntityDatasetUrn:
    """Test make_entity_dataset_urn function."""

    def test_make_dataset_urn_bigquery(self):
        """Test creating dataset URN for BigQuery."""
        urn = make_entity_dataset_urn(
            entity_id="my_table",
            project_id="test-project",
            env="PROD",
            dataset_id="my_dataset",
            platform="bigquery",
        )

        assert "urn:li:dataset:" in urn
        assert "bigquery" in urn
        assert "test-project.my_dataset.my_table" in urn

    def test_make_dataset_urn_gcs(self):
        """Test creating dataset URN for GCS."""
        urn = make_entity_dataset_urn(
            entity_id="file.parquet",
            project_id="test-project",
            env="PROD",
            dataset_id="my-bucket",
            platform="gcs",
        )

        assert "urn:li:dataset:" in urn
        assert "gcs" in urn


class TestMakeAuditStamp:
    """Test make_audit_stamp function."""

    def test_make_audit_stamp_with_timestamp(self):
        """Test creating audit stamp with valid timestamp."""
        mock_timestamp = Mock()
        mock_timestamp.timestamp.return_value = 1234567890.0

        result = make_audit_stamp(mock_timestamp)

        assert result is not None
        assert result["time"] == 1234567890000
        assert result["actor"] == "urn:li:corpuser:dataplex"

    def test_make_audit_stamp_none(self):
        """Test creating audit stamp with None timestamp."""
        result = make_audit_stamp(None)
        assert result is None


class TestMakeDataplexExternalUrl:
    """Test make_dataplex_external_url function."""

    def test_make_external_url(self):
        """Test generating Dataplex external URL."""
        url = make_dataplex_external_url(
            resource_type="lakes",
            resource_id="my-lake",
            project_id="test-project",
            location="us-central1",
            base_url="https://console.cloud.google.com/dataplex",
        )

        assert "https://console.cloud.google.com/dataplex/lakes" in url
        assert "us-central1" in url
        assert "my-lake" in url
        assert "test-project" in url


class TestDetermineEntityPlatform:
    """Test determine_entity_platform function."""

    def test_determine_platform_no_asset(self):
        """Test determination when entity has no asset."""
        entity = Mock(spec=dataplex_v1.Entity)
        entity.asset = None

        platform = determine_entity_platform(
            entity=entity,
            project_id="test-project",
            lake_id="test-lake",
            zone_id="test-zone",
            location="us-central1",
            dataplex_client=Mock(),
        )

        assert platform == "dataplex"

    def test_determine_platform_bigquery(self):
        """Test determination for BigQuery asset."""
        entity = Mock(spec=dataplex_v1.Entity)
        entity.asset = "test-asset"

        mock_asset = Mock()
        mock_asset.resource_spec = Mock()
        mock_asset.resource_spec.type_ = Mock()
        mock_asset.resource_spec.type_.name = "BIGQUERY_DATASET"

        mock_client = Mock()
        mock_client.get_asset.return_value = mock_asset

        platform = determine_entity_platform(
            entity=entity,
            project_id="test-project",
            lake_id="test-lake",
            zone_id="test-zone",
            location="us-central1",
            dataplex_client=mock_client,
        )

        assert platform == "bigquery"

    def test_determine_platform_gcs(self):
        """Test determination for GCS asset."""
        entity = Mock(spec=dataplex_v1.Entity)
        entity.asset = "test-asset"

        mock_asset = Mock()
        mock_asset.resource_spec = Mock()
        mock_asset.resource_spec.type_ = Mock()
        mock_asset.resource_spec.type_.name = "STORAGE_BUCKET"

        mock_client = Mock()
        mock_client.get_asset.return_value = mock_asset

        platform = determine_entity_platform(
            entity=entity,
            project_id="test-project",
            lake_id="test-lake",
            zone_id="test-zone",
            location="us-central1",
            dataplex_client=mock_client,
        )

        assert platform == "gcs"

    def test_determine_platform_api_error(self):
        """Test determination with API error."""
        entity = Mock(spec=dataplex_v1.Entity)
        entity.asset = "test-asset"

        mock_client = Mock()
        mock_client.get_asset.side_effect = exceptions.GoogleAPICallError("API error")

        platform = determine_entity_platform(
            entity=entity,
            project_id="test-project",
            lake_id="test-lake",
            zone_id="test-zone",
            location="us-central1",
            dataplex_client=mock_client,
        )

        assert platform == "dataplex"

    def test_determine_platform_attribute_error(self):
        """Test determination with unexpected asset structure."""
        entity = Mock(spec=dataplex_v1.Entity)
        entity.asset = "test-asset"

        mock_asset = Mock()
        mock_asset.resource_spec = None

        mock_client = Mock()
        mock_client.get_asset.return_value = mock_asset

        platform = determine_entity_platform(
            entity=entity,
            project_id="test-project",
            lake_id="test-lake",
            zone_id="test-zone",
            location="us-central1",
            dataplex_client=mock_client,
        )

        assert platform == "dataplex"


class TestMapDataplexTypeToDatahub:
    """Test map_dataplex_type_to_datahub function."""

    def test_map_known_type(self):
        """Test mapping known Dataplex type."""
        result = map_dataplex_type_to_datahub("STRING")
        assert isinstance(result, SchemaFieldDataTypeClass)
        assert isinstance(result.type, StringTypeClass)

    def test_map_unknown_type_defaults_to_string(self):
        """Test that unknown type defaults to StringType."""
        result = map_dataplex_type_to_datahub("UNKNOWN_TYPE")
        assert isinstance(result, SchemaFieldDataTypeClass)
        assert isinstance(result.type, StringTypeClass)


class TestMapDataplexFieldToDatahub:
    """Test map_dataplex_field_to_datahub function."""

    def test_map_field_simple_type(self):
        """Test mapping field with simple type."""
        field = Mock()
        field.type_ = dataplex_v1.types.Schema.Type.STRING
        field.mode = dataplex_v1.types.Schema.Mode.NULLABLE

        result = map_dataplex_field_to_datahub(field)

        assert isinstance(result, SchemaFieldDataTypeClass)

    def test_map_field_repeated_mode(self):
        """Test mapping field with REPEATED mode."""
        field = Mock()
        field.type_ = dataplex_v1.types.Schema.Type.STRING
        field.mode = dataplex_v1.types.Schema.Mode.REPEATED

        result = map_dataplex_field_to_datahub(field)

        assert isinstance(result, SchemaFieldDataTypeClass)
        assert isinstance(result.type, ArrayTypeClass)


class TestExtractEntityMetadata:
    """Test extract_entity_metadata function."""

    def test_extract_bigquery_metadata(self):
        """Test extracting metadata for BigQuery entity."""
        mock_asset = Mock()
        mock_asset.resource_spec = Mock()
        mock_asset.resource_spec.type_ = Mock()
        mock_asset.resource_spec.type_.name = "BIGQUERY_DATASET"
        mock_asset.resource_spec.name = "projects/test-project/datasets/my_dataset"

        mock_client = Mock()
        mock_client.get_asset.return_value = mock_asset

        platform, dataset_id = extract_entity_metadata(
            project_id="test-project",
            lake_id="test-lake",
            zone_id="test-zone",
            entity_id="test-entity",
            asset_id="test-asset",
            location="us-central1",
            dataplex_client=mock_client,
        )

        assert platform == "bigquery"
        assert dataset_id == "my_dataset"

    def test_extract_bigquery_metadata_simple_name(self):
        """Test extracting BigQuery metadata with simple resource name."""
        mock_asset = Mock()
        mock_asset.resource_spec = Mock()
        mock_asset.resource_spec.type_ = Mock()
        mock_asset.resource_spec.type_.name = "BIGQUERY_DATASET"
        mock_asset.resource_spec.name = "my_dataset"

        mock_client = Mock()
        mock_client.get_asset.return_value = mock_asset

        platform, dataset_id = extract_entity_metadata(
            project_id="test-project",
            lake_id="test-lake",
            zone_id="test-zone",
            entity_id="test-entity",
            asset_id="test-asset",
            location="us-central1",
            dataplex_client=mock_client,
        )

        assert platform == "bigquery"
        assert dataset_id == "my_dataset"

    def test_extract_gcs_metadata_full_path(self):
        """Test extracting metadata for GCS entity with full path."""
        mock_asset = Mock()
        mock_asset.resource_spec = Mock()
        mock_asset.resource_spec.type_ = Mock()
        mock_asset.resource_spec.type_.name = "STORAGE_BUCKET"
        mock_asset.resource_spec.name = "projects/test-project/buckets/my-bucket"

        mock_client = Mock()
        mock_client.get_asset.return_value = mock_asset

        platform, dataset_id = extract_entity_metadata(
            project_id="test-project",
            lake_id="test-lake",
            zone_id="test-zone",
            entity_id="test-entity",
            asset_id="test-asset",
            location="us-central1",
            dataplex_client=mock_client,
        )

        assert platform == "gcs"
        assert dataset_id == "my-bucket"

    def test_extract_gcs_metadata_gs_url(self):
        """Test extracting GCS metadata with gs:// URL."""
        mock_asset = Mock()
        mock_asset.resource_spec = Mock()
        mock_asset.resource_spec.type_ = Mock()
        mock_asset.resource_spec.type_.name = "STORAGE_BUCKET"
        mock_asset.resource_spec.name = "gs://my-bucket/path"

        mock_client = Mock()
        mock_client.get_asset.return_value = mock_asset

        platform, dataset_id = extract_entity_metadata(
            project_id="test-project",
            lake_id="test-lake",
            zone_id="test-zone",
            entity_id="test-entity",
            asset_id="test-asset",
            location="us-central1",
            dataplex_client=mock_client,
        )

        assert platform == "gcs"
        assert dataset_id == "my-bucket"

    def test_extract_gcs_metadata_simple_name(self):
        """Test extracting GCS metadata with simple bucket name."""
        mock_asset = Mock()
        mock_asset.resource_spec = Mock()
        mock_asset.resource_spec.type_ = Mock()
        mock_asset.resource_spec.type_.name = "STORAGE_BUCKET"
        mock_asset.resource_spec.name = "my-bucket"

        mock_client = Mock()
        mock_client.get_asset.return_value = mock_asset

        platform, dataset_id = extract_entity_metadata(
            project_id="test-project",
            lake_id="test-lake",
            zone_id="test-zone",
            entity_id="test-entity",
            asset_id="test-asset",
            location="us-central1",
            dataplex_client=mock_client,
        )

        assert platform == "gcs"
        assert dataset_id == "my-bucket"

    def test_extract_metadata_fallback_zone_id(self):
        """Test fallback to zone_id when resource_name is not available."""
        mock_asset = Mock()
        mock_asset.resource_spec = Mock()
        mock_asset.resource_spec.type_ = Mock()
        mock_asset.resource_spec.type_.name = "BIGQUERY_DATASET"
        mock_asset.resource_spec.name = None

        mock_client = Mock()
        mock_client.get_asset.return_value = mock_asset

        platform, dataset_id = extract_entity_metadata(
            project_id="test-project",
            lake_id="test-lake",
            zone_id="test-zone",
            entity_id="test-entity",
            asset_id="test-asset",
            location="us-central1",
            dataplex_client=mock_client,
        )

        assert platform == "bigquery"
        assert dataset_id == "test-zone"

    def test_extract_metadata_api_error(self):
        """Test extraction with API error."""
        mock_client = Mock()
        mock_client.get_asset.side_effect = exceptions.GoogleAPICallError("API error")

        platform, dataset_id = extract_entity_metadata(
            project_id="test-project",
            lake_id="test-lake",
            zone_id="test-zone",
            entity_id="test-entity",
            asset_id="test-asset",
            location="us-central1",
            dataplex_client=mock_client,
        )

        assert platform is None
        assert dataset_id is None

    def test_extract_metadata_attribute_error(self):
        """Test extraction with unexpected asset structure."""
        mock_asset = Mock()
        mock_asset.resource_spec = None

        mock_client = Mock()
        mock_client.get_asset.return_value = mock_asset

        platform, dataset_id = extract_entity_metadata(
            project_id="test-project",
            lake_id="test-lake",
            zone_id="test-zone",
            entity_id="test-entity",
            asset_id="test-asset",
            location="us-central1",
            dataplex_client=mock_client,
        )

        assert platform is None
        assert dataset_id is None


class TestSerializeFieldValue:
    """Test serialize_field_value function."""

    def test_serialize_none(self):
        """Test serializing None value."""
        result = serialize_field_value(None)
        assert result == ""

    def test_serialize_primitive_string(self):
        """Test serializing primitive string."""
        result = serialize_field_value("test_value")
        assert result == "test_value"

    def test_serialize_primitive_int(self):
        """Test serializing primitive int."""
        result = serialize_field_value(42)
        assert result == "42"

    def test_serialize_primitive_float(self):
        """Test serializing primitive float."""
        result = serialize_field_value(3.14)
        assert result == "3.14"

    def test_serialize_primitive_bool(self):
        """Test serializing primitive bool."""
        result = serialize_field_value(True)
        assert result == "True"

    def test_serialize_list_primitives(self):
        """Test serializing list of primitives."""
        result = serialize_field_value([1, 2, 3])
        assert result == "[1, 2, 3]"

    def test_serialize_regular_dict(self):
        """Test serializing regular dict."""
        result = serialize_field_value({"key": "value"})
        parsed = json.loads(result)
        assert parsed == {"key": "value"}

    def test_serialize_repeated_composite(self):
        """Test serializing RepeatedComposite proto object."""
        mock_item = Mock()
        mock_item.__class__.__name__ = "MapComposite"
        mock_item.items = Mock(return_value=[("field1", "value1")])

        mock_repeated = Mock()
        mock_repeated.__class__.__name__ = "RepeatedComposite"
        mock_repeated.__iter__ = Mock(return_value=iter([mock_item]))

        result = serialize_field_value(mock_repeated)
        assert isinstance(result, str)

    def test_serialize_map_composite(self):
        """Test serializing MapComposite proto object."""
        mock_map = Mock()
        mock_map.__class__.__name__ = "MapComposite"
        mock_map.items = Mock(return_value=[("key1", "value1")])

        result = serialize_field_value(mock_map)
        assert isinstance(result, str)


class TestParseEntryFqn:
    """Test parse_entry_fqn function."""

    def test_parse_bigquery_fqn_full(self):
        """Test parsing BigQuery FQN with full table reference."""
        platform, dataset_id = parse_entry_fqn(
            "bigquery:test-project.my_dataset.my_table"
        )

        assert platform == "bigquery"
        assert dataset_id == "test-project.my_dataset.my_table"

    def test_parse_bigquery_fqn_dataset_only(self):
        """Test parsing BigQuery FQN with dataset only."""
        platform, dataset_id = parse_entry_fqn("bigquery:test-project.my_dataset")

        assert platform == "bigquery"
        assert dataset_id == "test-project.my_dataset"

    def test_parse_gcs_fqn(self):
        """Test parsing GCS FQN."""
        platform, dataset_id = parse_entry_fqn("gcs:my-bucket/path/to/file.parquet")

        assert platform == "gcs"
        assert dataset_id == "my-bucket/path/to/file.parquet"

    def test_parse_fqn_no_colon(self):
        """Test parsing FQN without colon."""
        platform, dataset_id = parse_entry_fqn("invalid-fqn")

        assert platform == ""
        assert dataset_id == ""

    def test_parse_other_platform_fqn(self):
        """Test parsing FQN for other platforms."""
        platform, dataset_id = parse_entry_fqn("custom:resource/path")

        assert platform == "custom"
        assert dataset_id == "resource/path"

    def test_parse_bigquery_fqn_single_part(self):
        """Test parsing BigQuery FQN with only one part (unexpected format)."""
        platform, dataset_id = parse_entry_fqn("bigquery:project")

        assert platform == "bigquery"
        assert dataset_id == "project"


class TestParseEntryFqnEdgeCases:
    """Test parse_entry_fqn edge cases for coverage."""

    def test_parse_bigquery_fqn_one_part_warning(self):
        """Test parsing BigQuery FQN with unexpected single part format."""
        platform, dataset_id = parse_entry_fqn("bigquery:justonepart")

        # Should still return platform and resource_path
        assert platform == "bigquery"
        assert dataset_id == "justonepart"
