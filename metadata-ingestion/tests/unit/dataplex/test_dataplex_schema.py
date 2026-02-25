"""Unit tests for Dataplex schema extraction utilities."""

from unittest.mock import Mock

from google.cloud import dataplex_v1

from datahub.ingestion.source.dataplex.dataplex_schema import (
    extract_field_value,
    extract_schema_from_entry_aspects,
    extract_schema_metadata,
    map_aspect_type_to_datahub,
    process_schema_field_item,
)
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    StringTypeClass,
    TimeTypeClass,
)


class TestExtractSchemaMetadata:
    """Test extract_schema_metadata function."""

    def test_extract_schema_no_schema(self):
        """Test extraction when entity has no schema."""
        entity = Mock(spec=dataplex_v1.Entity)
        entity.schema = None
        entity.id = "test_entity"

        result = extract_schema_metadata(entity, "urn:li:dataset:test", "bigquery")
        assert result is None

    def test_extract_schema_no_fields(self):
        """Test extraction when schema has no fields."""
        entity = Mock(spec=dataplex_v1.Entity)
        entity.schema = Mock()
        entity.schema.fields = []
        entity.id = "test_entity"

        result = extract_schema_metadata(entity, "urn:li:dataset:test", "bigquery")
        assert result is None

    def test_extract_schema_with_simple_fields(self):
        """Test extraction with simple fields."""
        entity = Mock(spec=dataplex_v1.Entity)
        entity.id = "test_entity"

        # Mock field with STRING type
        field1 = Mock()
        field1.name = "column1"
        field1.type_ = int(dataplex_v1.types.Schema.Type.STRING)
        field1.mode = int(dataplex_v1.types.Schema.Mode.NULLABLE)
        field1.description = "First column"
        field1.fields = []

        # Mock field with INT64 type
        field2 = Mock()
        field2.name = "column2"
        field2.type_ = int(dataplex_v1.types.Schema.Type.INT64)
        field2.mode = int(dataplex_v1.types.Schema.Mode.NULLABLE)
        field2.description = ""
        field2.fields = []

        entity.schema = Mock()
        entity.schema.fields = [field1, field2]

        result = extract_schema_metadata(entity, "urn:li:dataset:test", "bigquery")

        assert result is not None
        assert len(result.fields) == 2
        assert result.schemaName == "test_entity"
        assert result.fields[0].fieldPath == "column1"
        assert result.fields[0].description == "First column"
        assert result.fields[1].fieldPath == "column2"

    def test_extract_schema_with_nested_fields(self):
        """Test extraction with nested fields."""
        entity = Mock(spec=dataplex_v1.Entity)
        entity.id = "test_entity"

        # Nested field
        nested_field = Mock()
        nested_field.name = "nested_col"
        nested_field.type_ = int(dataplex_v1.types.Schema.Type.STRING)
        nested_field.mode = int(dataplex_v1.types.Schema.Mode.NULLABLE)
        nested_field.description = "Nested column"

        # Parent field with nested fields
        parent_field = Mock()
        parent_field.name = "parent"
        parent_field.type_ = int(dataplex_v1.types.Schema.Type.RECORD)
        parent_field.mode = int(dataplex_v1.types.Schema.Mode.NULLABLE)
        parent_field.description = "Parent column"
        parent_field.fields = [nested_field]

        entity.schema = Mock()
        entity.schema.fields = [parent_field]

        result = extract_schema_metadata(entity, "urn:li:dataset:test", "bigquery")

        assert result is not None
        # Should have parent + nested field
        assert len(result.fields) == 2
        assert result.fields[0].fieldPath == "parent.nested_col"
        assert result.fields[1].fieldPath == "parent"
        # Parent should be RECORD type
        assert isinstance(result.fields[1].type.type, RecordTypeClass)


class TestExtractFieldValue:
    """Test extract_field_value function."""

    def test_extract_from_dict_with_string_value(self):
        """Test extraction from dict with string_value attribute."""
        mock_value = Mock()
        mock_value.string_value = "test_value"

        field_data = {"name": mock_value}
        result = extract_field_value(field_data, "name")

        assert result == "test_value"

    def test_extract_from_dict_with_primitive(self):
        """Test extraction from dict with primitive value."""
        field_data = {"name": "test_value"}
        result = extract_field_value(field_data, "name")

        assert result == "test_value"

    def test_extract_from_dict_missing_key(self):
        """Test extraction from dict with missing key."""
        field_data = {"other": "value"}
        result = extract_field_value(field_data, "name", "default_value")

        assert result == "default_value"

    def test_extract_from_dict_none_value(self):
        """Test extraction from dict with None value."""
        field_data = {"name": None}
        result = extract_field_value(field_data, "name", "default_value")

        assert result == "default_value"

    def test_extract_from_object_with_attribute(self):
        """Test extraction from object with attribute."""
        field_data = Mock()
        field_data.name = "test_value"

        result = extract_field_value(field_data, "name")

        assert result == "test_value"

    def test_extract_from_object_missing_attribute(self):
        """Test extraction from object with missing attribute."""
        field_data = Mock(spec=[])
        result = extract_field_value(field_data, "name", "default_value")

        assert result == "default_value"


class TestProcessSchemaFieldItem:
    """Test process_schema_field_item function."""

    def test_process_struct_value(self):
        """Test processing protobuf Value with struct_value."""
        mock_field = Mock()
        mock_field.name = Mock()
        mock_field.name.string_value = "column1"

        mock_struct = Mock()
        mock_struct.fields = {"name": mock_field}

        field_value = Mock()
        field_value.struct_value = mock_struct

        result = process_schema_field_item(field_value, "entry_id")

        assert result is not None
        assert "name" in result

    def test_process_dict_like_object(self):
        """Test processing dict-like object."""
        field_value = {"name": "column1", "type": "STRING"}

        result = process_schema_field_item(field_value, "entry_id")

        assert result == field_value

    def test_process_none_value(self):
        """Test processing None value."""
        result = process_schema_field_item(None, "entry_id")
        assert result is None


class TestMapAspectTypeToDatahub:
    """Test map_aspect_type_to_datahub function."""

    def test_map_string_types(self):
        """Test mapping string types."""
        for type_str in ["STRING", "VARCHAR", "CHAR", "TEXT", "string"]:
            result = map_aspect_type_to_datahub(type_str)
            assert isinstance(result.type, StringTypeClass)

    def test_map_integer_types(self):
        """Test mapping integer types."""
        for type_str in ["INTEGER", "INT", "INT64", "LONG", "integer"]:
            result = map_aspect_type_to_datahub(type_str)
            assert isinstance(result.type, NumberTypeClass)

    def test_map_float_types(self):
        """Test mapping float types."""
        for type_str in ["FLOAT", "DOUBLE", "NUMERIC", "DECIMAL"]:
            result = map_aspect_type_to_datahub(type_str)
            assert isinstance(result.type, NumberTypeClass)

    def test_map_boolean_types(self):
        """Test mapping boolean types."""
        for type_str in ["BOOLEAN", "BOOL", "boolean"]:
            result = map_aspect_type_to_datahub(type_str)
            assert isinstance(result.type, BooleanTypeClass)

    def test_map_time_types(self):
        """Test mapping time types."""
        for type_str in ["TIMESTAMP", "DATETIME", "DATE", "TIME"]:
            result = map_aspect_type_to_datahub(type_str)
            assert isinstance(result.type, TimeTypeClass)

    def test_map_bytes_types(self):
        """Test mapping bytes types."""
        for type_str in ["BYTES", "BINARY"]:
            result = map_aspect_type_to_datahub(type_str)
            assert isinstance(result.type, BytesTypeClass)

    def test_map_record_types(self):
        """Test mapping record types."""
        for type_str in ["RECORD", "STRUCT"]:
            result = map_aspect_type_to_datahub(type_str)
            assert isinstance(result.type, RecordTypeClass)

    def test_map_array_type(self):
        """Test mapping array type."""
        result = map_aspect_type_to_datahub("ARRAY")
        assert isinstance(result.type, ArrayTypeClass)

    def test_map_unknown_type(self):
        """Test mapping unknown type defaults to string."""
        result = map_aspect_type_to_datahub("UNKNOWN_TYPE")
        assert isinstance(result.type, StringTypeClass)


class TestExtractSchemaFromEntryAspects:
    """Test extract_schema_from_entry_aspects function."""

    def test_extract_no_aspects(self):
        """Test extraction when entry has no aspects."""
        entry = Mock(spec=dataplex_v1.Entry)
        entry.aspects = {}

        result = extract_schema_from_entry_aspects(entry, "test_entry", "bigquery")

        assert result is None

    def test_extract_no_schema_aspect(self):
        """Test extraction when entry has aspects but no schema aspect."""
        entry = Mock(spec=dataplex_v1.Entry)
        entry.aspects = {
            "other-aspect": Mock(),
        }

        result = extract_schema_from_entry_aspects(entry, "test_entry", "bigquery")

        assert result is None

    def test_extract_schema_aspect_no_data(self):
        """Test extraction when schema aspect has no data."""
        schema_aspect = Mock()
        schema_aspect.data = None

        entry = Mock(spec=dataplex_v1.Entry)
        entry.aspects = {
            "dataplex-types.global.schema": schema_aspect,
        }

        result = extract_schema_from_entry_aspects(entry, "test_entry", "bigquery")

        assert result is None

    def test_extract_schema_aspect_no_fields(self):
        """Test extraction when schema aspect data has no column/field data."""
        schema_aspect = Mock()
        schema_aspect.data = {"other_key": "value"}

        entry = Mock(spec=dataplex_v1.Entry)
        entry.aspects = {
            "dataplex-types.global.schema": schema_aspect,
        }

        result = extract_schema_from_entry_aspects(entry, "test_entry", "bigquery")

        assert result is None

    def test_extract_schema_with_columns_list_value(self):
        """Test extraction with columns in list_value format."""
        # Mock field data
        field_name = Mock()
        field_name.string_value = "column1"

        field_type = Mock()
        field_type.string_value = "STRING"

        field_struct = Mock()
        field_struct.fields = {"name": field_name, "type": field_type}

        field_value = Mock()
        field_value.struct_value = field_struct

        list_value = Mock()
        list_value.values = [field_value]

        columns_data = Mock()
        columns_data.list_value = list_value

        schema_aspect = Mock()
        schema_aspect.data = {"columns": columns_data}

        entry = Mock(spec=dataplex_v1.Entry)
        entry.aspects = {
            "dataplex-types.global.schema": schema_aspect,
        }

        result = extract_schema_from_entry_aspects(entry, "test_entry", "bigquery")

        assert result is not None
        assert len(result.fields) == 1
        assert result.fields[0].fieldPath == "column1"
        assert result.fields[0].nativeDataType == "STRING"

    def test_extract_schema_with_fields_iterable(self):
        """Test extraction with fields as iterable."""
        fields_data = [
            {"name": "column1", "type": "STRING", "description": "Test column"},
            {"name": "column2", "type": "INTEGER"},
        ]

        schema_aspect = Mock()
        schema_aspect.data = {"fields": fields_data}

        entry = Mock(spec=dataplex_v1.Entry)
        entry.aspects = {
            "dataplex-types.global.schema": schema_aspect,
        }

        result = extract_schema_from_entry_aspects(entry, "test_entry", "bigquery")

        assert result is not None
        assert len(result.fields) == 2
        assert result.fields[0].fieldPath == "column1"
        assert result.fields[0].description == "Test column"
        assert result.fields[1].fieldPath == "column2"

    def test_extract_schema_fallback_aspect_key(self):
        """Test extraction with fallback schema aspect key."""
        fields_data = [{"name": "column1", "type": "STRING"}]

        schema_aspect = Mock()
        schema_aspect.data = {"columns": fields_data}

        entry = Mock(spec=dataplex_v1.Entry)
        entry.aspects = {
            "custom/schema": schema_aspect,
        }

        result = extract_schema_from_entry_aspects(entry, "test_entry", "bigquery")

        assert result is not None
        assert len(result.fields) == 1

    def test_extract_schema_with_datatype_fallback(self):
        """Test extraction using dataType field as fallback."""
        fields_data = [{"name": "column1", "dataType": "BOOLEAN"}]

        schema_aspect = Mock()
        schema_aspect.data = {"fields": fields_data}

        entry = Mock(spec=dataplex_v1.Entry)
        entry.aspects = {
            "dataplex-types.global.schema": schema_aspect,
        }

        result = extract_schema_from_entry_aspects(entry, "test_entry", "bigquery")

        assert result is not None
        assert len(result.fields) == 1
        assert isinstance(result.fields[0].type.type, BooleanTypeClass)

    def test_extract_schema_with_column_name_fallback(self):
        """Test extraction using column field as name fallback."""
        fields_data = [{"column": "column1", "type": "STRING"}]

        schema_aspect = Mock()
        schema_aspect.data = {"fields": fields_data}

        entry = Mock(spec=dataplex_v1.Entry)
        entry.aspects = {
            "dataplex-types.global.schema": schema_aspect,
        }

        result = extract_schema_from_entry_aspects(entry, "test_entry", "bigquery")

        assert result is not None
        assert len(result.fields) == 1
        assert result.fields[0].fieldPath == "column1"

    def test_extract_schema_exception_handling(self):
        """Test exception handling during extraction."""
        schema_aspect = Mock()
        # Make data raise exception when accessed
        schema_aspect.data = Mock()
        type(schema_aspect.data).__iter__ = Mock(side_effect=Exception("Test error"))

        entry = Mock(spec=dataplex_v1.Entry)
        entry.aspects = {
            "dataplex-types.global.schema": schema_aspect,
        }

        result = extract_schema_from_entry_aspects(entry, "test_entry", "bigquery")

        assert result is None

    def test_extract_schema_no_valid_fields(self):
        """Test extraction when no valid fields can be extracted."""
        # Fields without name should be skipped
        fields_data = [{"type": "STRING", "description": "No name"}]

        schema_aspect = Mock()
        schema_aspect.data = {"fields": fields_data}

        entry = Mock(spec=dataplex_v1.Entry)
        entry.aspects = {
            "dataplex-types.global.schema": schema_aspect,
        }

        result = extract_schema_from_entry_aspects(entry, "test_entry", "bigquery")

        assert result is None
