import pytest

from datahub_integrations.propagation.snowflake.description_extractor import (
    DescriptionExtractor,
)
from datahub_integrations.propagation.snowflake.event_processor import EventData


class TestDescriptionExtractor:
    """Test DescriptionExtractor class."""

    @pytest.fixture
    def extractor(self):
        """Create a DescriptionExtractor instance."""
        return DescriptionExtractor()

    def test_extract_from_entity_change_event_with_description(self, extractor):
        """Test extracting description from EntityChangeEvent."""
        event_data = EventData(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            event_type="EntityChangeEvent_v1",
            operation="ADD",
            category="DOCUMENTATION",
            parameters={"description": "This is a test table description"},
        )

        result = extractor.extract_description(event_data)

        assert result == "This is a test table description"

    def test_extract_from_entity_change_event_non_documentation_category(
        self, extractor
    ):
        """Test that non-DOCUMENTATION category events return None."""
        event_data = EventData(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            event_type="EntityChangeEvent_v1",
            operation="ADD",
            category="TAG",
            parameters={"description": "This should be ignored"},
        )

        result = extractor.extract_description(event_data)

        assert result is None

    def test_extract_from_entity_change_event_no_description(self, extractor):
        """Test EntityChangeEvent without description parameter."""
        event_data = EventData(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            event_type="EntityChangeEvent_v1",
            operation="ADD",
            category="DOCUMENTATION",
            parameters={},
        )

        result = extractor.extract_description(event_data)

        assert result is None

    def test_extract_table_description_from_mcl_event(self, extractor):
        """Test extracting table description from MetadataChangeLogEvent."""
        event_data = EventData(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            event_type="MetadataChangeLogEvent_v1",
            operation="UPSERT",
            aspect_name="editableDatasetProperties",
            change_type="UPSERT",
            aspect_data={"description": "Table description from MCL"},
        )

        result = extractor.extract_description(event_data)

        assert result == "Table description from MCL"

    def test_extract_table_description_from_mcl_event_no_description(self, extractor):
        """Test MCL event with editableDatasetProperties but no description."""
        event_data = EventData(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            event_type="MetadataChangeLogEvent_v1",
            operation="UPSERT",
            aspect_name="editableDatasetProperties",
            change_type="UPSERT",
            aspect_data={"someOtherField": "value"},
        )

        result = extractor.extract_description(event_data)

        assert result is None

    def test_extract_column_description_from_mcl_event(self, extractor):
        """Test extracting column description from editableSchemaMetadata."""
        event_data = EventData(
            entity_urn="urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD),column_name)",
            event_type="MetadataChangeLogEvent_v1",
            operation="UPSERT",
            aspect_name="editableSchemaMetadata",
            change_type="UPSERT",
            aspect_data={
                "editableSchemaFieldInfo": [
                    {
                        "fieldPath": "column_name",
                        "description": "This is a column description",
                    }
                ]
            },
        )

        result = extractor.extract_description(event_data)

        assert result == "This is a column description"

    def test_extract_column_description_multiple_fields(self, extractor):
        """Test extracting column description when multiple fields present (returns first)."""
        event_data = EventData(
            entity_urn="urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD),column1)",
            event_type="MetadataChangeLogEvent_v1",
            operation="UPSERT",
            aspect_name="editableSchemaMetadata",
            change_type="UPSERT",
            aspect_data={
                "editableSchemaFieldInfo": [
                    {"fieldPath": "column1", "description": "First column description"},
                    {
                        "fieldPath": "column2",
                        "description": "Second column description",
                    },
                ]
            },
        )

        result = extractor.extract_description(event_data)

        # Should return the first description found
        assert result == "First column description"

    def test_extract_column_description_no_description_field(self, extractor):
        """Test column extraction when field info has no description."""
        event_data = EventData(
            entity_urn="urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD),column_name)",
            event_type="MetadataChangeLogEvent_v1",
            operation="UPSERT",
            aspect_name="editableSchemaMetadata",
            change_type="UPSERT",
            aspect_data={
                "editableSchemaFieldInfo": [
                    {"fieldPath": "column_name", "otherField": "value"}
                ]
            },
        )

        result = extractor.extract_description(event_data)

        assert result is None

    def test_extract_column_description_empty_field_info(self, extractor):
        """Test column extraction with empty editableSchemaFieldInfo."""
        event_data = EventData(
            entity_urn="urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD),column_name)",
            event_type="MetadataChangeLogEvent_v1",
            operation="UPSERT",
            aspect_name="editableSchemaMetadata",
            change_type="UPSERT",
            aspect_data={"editableSchemaFieldInfo": []},
        )

        result = extractor.extract_description(event_data)

        assert result is None

    def test_extract_column_description_no_field_info(self, extractor):
        """Test column extraction without editableSchemaFieldInfo key."""
        event_data = EventData(
            entity_urn="urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD),column_name)",
            event_type="MetadataChangeLogEvent_v1",
            operation="UPSERT",
            aspect_name="editableSchemaMetadata",
            change_type="UPSERT",
            aspect_data={},
        )

        result = extractor.extract_description(event_data)

        assert result is None

    def test_extract_from_mcl_event_wrong_aspect(self, extractor):
        """Test MCL event with non-description aspect."""
        event_data = EventData(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            event_type="MetadataChangeLogEvent_v1",
            operation="UPSERT",
            aspect_name="schemaMetadata",  # Wrong aspect
            change_type="UPSERT",
            aspect_data={"description": "This should be ignored"},
        )

        result = extractor.extract_description(event_data)

        assert result is None

    def test_extract_from_mcl_event_delete_operation(self, extractor):
        """Test that DELETE operations are skipped."""
        event_data = EventData(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            event_type="MetadataChangeLogEvent_v1",
            operation="DELETE",
            aspect_name="editableDatasetProperties",
            change_type="DELETE",
            aspect_data={"description": "This should be ignored"},
        )

        result = extractor.extract_description(event_data)

        assert result is None

    def test_extract_from_mcl_event_no_aspect_name(self, extractor):
        """Test MCL event without aspect name."""
        event_data = EventData(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            event_type="MetadataChangeLogEvent_v1",
            operation="UPSERT",
            aspect_name=None,
            change_type="UPSERT",
            aspect_data={"description": "This should be ignored"},
        )

        result = extractor.extract_description(event_data)

        assert result is None

    def test_extract_unsupported_event_type(self, extractor):
        """Test extraction from unsupported event type."""
        event_data = EventData(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            event_type="UnsupportedEvent_v1",
            operation="ADD",
        )

        result = extractor.extract_description(event_data)

        assert result is None

    def test_extract_long_table_description(self, extractor):
        """Test extracting long table descriptions (verifies no truncation in extraction)."""
        long_description = "A" * 500  # Long description
        event_data = EventData(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            event_type="MetadataChangeLogEvent_v1",
            operation="UPSERT",
            aspect_name="editableDatasetProperties",
            change_type="UPSERT",
            aspect_data={"description": long_description},
        )

        result = extractor.extract_description(event_data)

        assert result == long_description
        assert len(result) == 500

    def test_extract_description_with_special_characters(self, extractor):
        """Test extracting descriptions with special characters."""
        special_description = "Table with 'quotes', \"double quotes\", and\nnewlines"
        event_data = EventData(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            event_type="EntityChangeEvent_v1",
            operation="ADD",
            category="DOCUMENTATION",
            parameters={"description": special_description},
        )

        result = extractor.extract_description(event_data)

        assert result == special_description
