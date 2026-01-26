"""Unit tests for SnowplowSchemaParser."""

from datahub.ingestion.source.snowplow.utils.schema_parser import (
    ARRAY_TYPE_TOKEN,
    RECORD_TYPE_TOKEN,
    VERSION_PREFIX,
    SnowplowSchemaParser,
)
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    DateTypeClass,
    EnumTypeClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    StringTypeClass,
)


class TestParseSimpleScalarFields:
    """Tests for parsing basic scalar field types."""

    def test_parse_string_field(self):
        """Test parsing a simple string field."""
        schema_data = {
            "properties": {"user_id": {"type": "string", "description": "User ID"}}
        }
        result = SnowplowSchemaParser.parse_schema(
            schema_data, "com.test", "schema", "1-0-0"
        )

        assert len(result.fields) == 1
        field = result.fields[0]
        assert field.fieldPath == "user_id"
        assert field.description == "User ID"
        assert field.nativeDataType == "string"
        assert isinstance(field.type.type, StringTypeClass)

    def test_parse_integer_field(self):
        """Test parsing an integer field maps to NumberType."""
        schema_data = {"properties": {"age": {"type": "integer"}}}
        result = SnowplowSchemaParser.parse_schema(
            schema_data, "com.test", "schema", "1-0-0"
        )

        assert len(result.fields) == 1
        field = result.fields[0]
        assert field.fieldPath == "age"
        assert field.nativeDataType == "integer"
        assert isinstance(field.type.type, NumberTypeClass)

    def test_parse_number_field(self):
        """Test parsing a number field maps to NumberType."""
        schema_data = {"properties": {"price": {"type": "number"}}}
        result = SnowplowSchemaParser.parse_schema(
            schema_data, "com.test", "schema", "1-0-0"
        )

        field = result.fields[0]
        assert field.nativeDataType == "number"
        assert isinstance(field.type.type, NumberTypeClass)

    def test_parse_boolean_field(self):
        """Test parsing a boolean field."""
        schema_data = {"properties": {"active": {"type": "boolean"}}}
        result = SnowplowSchemaParser.parse_schema(
            schema_data, "com.test", "schema", "1-0-0"
        )

        field = result.fields[0]
        assert field.fieldPath == "active"
        assert field.nativeDataType == "boolean"
        assert isinstance(field.type.type, BooleanTypeClass)

    def test_parse_null_field(self):
        """Test parsing a null type field."""
        schema_data = {"properties": {"placeholder": {"type": "null"}}}
        result = SnowplowSchemaParser.parse_schema(
            schema_data, "com.test", "schema", "1-0-0"
        )

        field = result.fields[0]
        assert isinstance(field.type.type, NullTypeClass)


class TestParseNullableFields:
    """Tests for nullable field handling."""

    def test_nullable_field_with_type_array(self):
        """Test handling of nullable fields with type arrays like [\"string\", \"null\"]."""
        schema_data = {"properties": {"optional_field": {"type": ["string", "null"]}}}
        result = SnowplowSchemaParser.parse_schema(
            schema_data, "com.test", "schema", "1-0-0"
        )

        field = result.fields[0]
        assert field.nullable is True
        assert field.nativeDataType == "string"
        assert isinstance(field.type.type, StringTypeClass)

    def test_nullable_integer_field(self):
        """Test nullable integer field."""
        schema_data = {"properties": {"count": {"type": ["integer", "null"]}}}
        result = SnowplowSchemaParser.parse_schema(
            schema_data, "com.test", "schema", "1-0-0"
        )

        field = result.fields[0]
        assert field.nullable is True
        assert isinstance(field.type.type, NumberTypeClass)

    def test_required_field_not_nullable(self):
        """Test that required fields have nullable=False."""
        schema_data = {
            "properties": {"user_id": {"type": "string"}},
            "required": ["user_id"],
        }
        result = SnowplowSchemaParser.parse_schema(
            schema_data, "com.test", "schema", "1-0-0"
        )

        field = result.fields[0]
        assert field.nullable is False

    def test_optional_field_is_nullable(self):
        """Test that optional fields (not in required) have nullable=True."""
        schema_data = {
            "properties": {
                "optional": {"type": "string"},
                "required_field": {"type": "string"},
            },
            "required": ["required_field"],
        }
        result = SnowplowSchemaParser.parse_schema(
            schema_data, "com.test", "schema", "1-0-0"
        )

        fields_by_name = {f.fieldPath: f for f in result.fields}
        assert fields_by_name["optional"].nullable is True
        assert fields_by_name["required_field"].nullable is False


class TestParseEnumTypes:
    """Tests for enum field handling."""

    def test_enum_field(self):
        """Test enum fields are properly typed."""
        schema_data = {
            "properties": {
                "status": {"type": "string", "enum": ["active", "inactive", "pending"]}
            }
        }
        result = SnowplowSchemaParser.parse_schema(
            schema_data, "com.test", "schema", "1-0-0"
        )

        field = result.fields[0]
        assert field.fieldPath == "status"
        assert isinstance(field.type.type, EnumTypeClass)


class TestParseDateFormats:
    """Tests for date/datetime format handling."""

    def test_date_time_format(self):
        """Test date-time format maps to DateType."""
        schema_data = {
            "properties": {"created_at": {"type": "string", "format": "date-time"}}
        }
        result = SnowplowSchemaParser.parse_schema(
            schema_data, "com.test", "schema", "1-0-0"
        )

        field = result.fields[0]
        assert isinstance(field.type.type, DateTypeClass)

    def test_date_format(self):
        """Test date format maps to DateType."""
        schema_data = {
            "properties": {"birth_date": {"type": "string", "format": "date"}}
        }
        result = SnowplowSchemaParser.parse_schema(
            schema_data, "com.test", "schema", "1-0-0"
        )

        field = result.fields[0]
        assert isinstance(field.type.type, DateTypeClass)

    def test_time_format(self):
        """Test time format maps to DateType."""
        schema_data = {
            "properties": {"start_time": {"type": "string", "format": "time"}}
        }
        result = SnowplowSchemaParser.parse_schema(
            schema_data, "com.test", "schema", "1-0-0"
        )

        field = result.fields[0]
        assert isinstance(field.type.type, DateTypeClass)

    def test_uuid_format_stays_string(self):
        """Test uuid format stays as StringType."""
        schema_data = {"properties": {"id": {"type": "string", "format": "uuid"}}}
        result = SnowplowSchemaParser.parse_schema(
            schema_data, "com.test", "schema", "1-0-0"
        )

        field = result.fields[0]
        assert isinstance(field.type.type, StringTypeClass)

    def test_email_format_stays_string(self):
        """Test email format stays as StringType."""
        schema_data = {"properties": {"email": {"type": "string", "format": "email"}}}
        result = SnowplowSchemaParser.parse_schema(
            schema_data, "com.test", "schema", "1-0-0"
        )

        field = result.fields[0]
        assert isinstance(field.type.type, StringTypeClass)


class TestParseNestedObjects:
    """Tests for nested object/record field handling."""

    def test_simple_nested_object(self):
        """Test parsing a nested object uses V2 format."""
        schema_data = {
            "properties": {
                "user": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "email": {"type": "string"},
                    },
                }
            }
        }
        result = SnowplowSchemaParser.parse_schema(
            schema_data, "com.test", "schema", "1-0-0"
        )

        field_paths = [f.fieldPath for f in result.fields]
        # Object field uses V2 format
        assert f"{VERSION_PREFIX}.user" in field_paths
        # Nested fields use V2 path with record token
        assert f"{VERSION_PREFIX}.user.{RECORD_TYPE_TOKEN}.name" in field_paths
        assert f"{VERSION_PREFIX}.user.{RECORD_TYPE_TOKEN}.email" in field_paths

    def test_nested_object_parent_is_record_type(self):
        """Test that the parent object field has RecordType."""
        schema_data = {
            "properties": {
                "address": {
                    "type": "object",
                    "properties": {"street": {"type": "string"}},
                }
            }
        }
        result = SnowplowSchemaParser.parse_schema(
            schema_data, "com.test", "schema", "1-0-0"
        )

        parent_field = next(
            f for f in result.fields if f.fieldPath == f"{VERSION_PREFIX}.address"
        )
        assert isinstance(parent_field.type.type, RecordTypeClass)
        assert parent_field.nativeDataType == "object"

    def test_deeply_nested_structures(self):
        """Test parsing 3+ levels of nesting."""
        schema_data = {
            "properties": {
                "level1": {
                    "type": "object",
                    "properties": {
                        "level2": {
                            "type": "object",
                            "properties": {"level3": {"type": "string"}},
                        }
                    },
                }
            }
        }
        result = SnowplowSchemaParser.parse_schema(
            schema_data, "com.test", "schema", "1-0-0"
        )

        field_paths = [f.fieldPath for f in result.fields]
        # Verify deep nesting is preserved
        assert any(
            "level1" in p and "level2" in p and "level3" in p for p in field_paths
        )


class TestParseArrayFields:
    """Tests for array field handling."""

    def test_simple_array_of_strings(self):
        """Test parsing an array of strings."""
        schema_data = {
            "properties": {"tags": {"type": "array", "items": {"type": "string"}}}
        }
        result = SnowplowSchemaParser.parse_schema(
            schema_data, "com.test", "schema", "1-0-0"
        )

        # Array field uses V2 format
        field = next(
            f for f in result.fields if f.fieldPath == f"{VERSION_PREFIX}.tags"
        )
        assert isinstance(field.type.type, ArrayTypeClass)
        assert field.nativeDataType == "array"

    def test_array_of_objects(self):
        """Test parsing array of objects creates proper V2 paths."""
        schema_data = {
            "properties": {
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "price": {"type": "number"},
                            "quantity": {"type": "integer"},
                        },
                    },
                }
            }
        }
        result = SnowplowSchemaParser.parse_schema(
            schema_data, "com.test", "schema", "1-0-0"
        )

        field_paths = [f.fieldPath for f in result.fields]
        # Array field
        assert f"{VERSION_PREFIX}.items" in field_paths
        # Nested fields in array items use [type=array] token
        assert any(ARRAY_TYPE_TOKEN in p and "price" in p for p in field_paths)
        assert any(ARRAY_TYPE_TOKEN in p and "quantity" in p for p in field_paths)


class TestParseInvalidTypes:
    """Tests for handling invalid/unknown types."""

    def test_unknown_type_fallback_to_string(self):
        """Test that invalid types fall back to string with warning."""
        schema_data = {"properties": {"weird_field": {"type": "unknown_type"}}}
        result = SnowplowSchemaParser.parse_schema(
            schema_data, "com.test", "schema", "1-0-0"
        )

        # Should not crash, should use string as fallback
        assert len(result.fields) == 1
        field = result.fields[0]
        assert isinstance(field.type.type, StringTypeClass)

    def test_missing_type_defaults_to_string(self):
        """Test field with no type specified defaults to string."""
        schema_data = {
            "properties": {"no_type_field": {"description": "A field without type"}}
        }
        result = SnowplowSchemaParser.parse_schema(
            schema_data, "com.test", "schema", "1-0-0"
        )

        assert len(result.fields) == 1
        field = result.fields[0]
        # Should fallback gracefully
        assert isinstance(field.type.type, StringTypeClass)


class TestSchemaMetadata:
    """Tests for schema metadata properties."""

    def test_schema_name_format(self):
        """Test schema name follows vendor/name format."""
        schema_data = {"properties": {"field": {"type": "string"}}}
        result = SnowplowSchemaParser.parse_schema(
            schema_data, "com.example.vendor", "my_event", "1-0-0"
        )

        assert result.schemaName == "com.example.vendor/my_event"

    def test_platform_is_snowplow(self):
        """Test platform is set to snowplow."""
        schema_data = {"properties": {"field": {"type": "string"}}}
        result = SnowplowSchemaParser.parse_schema(
            schema_data, "com.test", "schema", "1-0-0"
        )

        assert result.platform == "urn:li:dataPlatform:snowplow"

    def test_empty_properties_returns_empty_fields(self):
        """Test schema with no properties returns empty fields list."""
        schema_data: dict = {"properties": {}}
        result = SnowplowSchemaParser.parse_schema(
            schema_data, "com.test", "schema", "1-0-0"
        )

        assert result.fields == []

    def test_missing_properties_key(self):
        """Test schema with missing properties key returns empty fields."""
        schema_data = {"type": "object"}
        result = SnowplowSchemaParser.parse_schema(
            schema_data, "com.test", "schema", "1-0-0"
        )

        assert result.fields == []


class TestSchemaVerParsing:
    """Tests for SchemaVer parsing utilities."""

    def test_parse_valid_schema_ver(self):
        """Test parsing valid SchemaVer format."""
        result = SnowplowSchemaParser.parse_schema_ver("1-0-0")

        assert result["model"] == 1
        assert result["revision"] == 0
        assert result["addition"] == 0

    def test_parse_complex_schema_ver(self):
        """Test parsing SchemaVer with non-zero parts."""
        result = SnowplowSchemaParser.parse_schema_ver("2-3-5")

        assert result["model"] == 2
        assert result["revision"] == 3
        assert result["addition"] == 5

    def test_parse_invalid_schema_ver_returns_zeros(self):
        """Test invalid SchemaVer returns all zeros."""
        result = SnowplowSchemaParser.parse_schema_ver("invalid")

        assert result["model"] == 0
        assert result["revision"] == 0
        assert result["addition"] == 0

    def test_parse_wrong_parts_count(self):
        """Test SchemaVer with wrong number of parts returns zeros."""
        result = SnowplowSchemaParser.parse_schema_ver("1-0")

        assert result["model"] == 0

    def test_schema_ver_to_string(self):
        """Test converting SchemaVer dict back to string."""
        version_dict = {"model": 1, "revision": 2, "addition": 3}
        result = SnowplowSchemaParser.schema_ver_to_string(version_dict)

        assert result == "1-2-3"

    def test_get_full_schema_name(self):
        """Test full schema name construction."""
        result = SnowplowSchemaParser.get_full_schema_name(
            "com.example", "event_name", "1-0-0"
        )

        assert result == "com.example/event_name/1-0-0"


class TestComplexSchemas:
    """Integration tests with realistic Snowplow schemas."""

    def test_realistic_page_view_schema(self):
        """Test parsing a realistic page_view event schema."""
        schema_data = {
            "properties": {
                "page_url": {
                    "type": "string",
                    "format": "uri",
                    "description": "Page URL",
                },
                "page_title": {"type": ["string", "null"], "description": "Page title"},
                "referrer": {"type": ["string", "null"], "format": "uri"},
                "timestamp": {"type": "string", "format": "date-time"},
                "is_active": {"type": "boolean"},
            },
            "required": ["page_url", "timestamp"],
        }
        result = SnowplowSchemaParser.parse_schema(
            schema_data, "com.snowplow", "page_view", "1-0-0"
        )

        assert len(result.fields) == 5
        fields_by_name = {f.fieldPath: f for f in result.fields}

        # Check required fields
        assert fields_by_name["page_url"].nullable is False
        assert fields_by_name["timestamp"].nullable is False

        # Check nullable fields
        assert fields_by_name["page_title"].nullable is True

        # Check date type
        assert isinstance(fields_by_name["timestamp"].type.type, DateTypeClass)

    def test_realistic_ecommerce_transaction(self):
        """Test parsing a realistic ecommerce transaction schema."""
        schema_data = {
            "properties": {
                "transaction_id": {"type": "string"},
                "total": {"type": "number"},
                "currency": {"type": "string", "enum": ["USD", "EUR", "GBP"]},
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "sku": {"type": "string"},
                            "name": {"type": "string"},
                            "price": {"type": "number"},
                            "quantity": {"type": "integer"},
                        },
                        "required": ["sku", "price"],
                    },
                },
                "shipping_address": {
                    "type": "object",
                    "properties": {
                        "street": {"type": "string"},
                        "city": {"type": "string"},
                        "country": {"type": "string"},
                    },
                },
            },
            "required": ["transaction_id", "total"],
        }
        result = SnowplowSchemaParser.parse_schema(
            schema_data, "com.shop", "transaction", "1-0-0"
        )

        field_paths = [f.fieldPath for f in result.fields]

        # Simple fields use V1 format
        assert "transaction_id" in field_paths
        assert "total" in field_paths

        # Enum field
        currency_field = next(f for f in result.fields if f.fieldPath == "currency")
        assert isinstance(currency_field.type.type, EnumTypeClass)

        # Array and nested objects use V2 format
        assert any(VERSION_PREFIX in p and "items" in p for p in field_paths)
        assert any(VERSION_PREFIX in p and "shipping_address" in p for p in field_paths)
