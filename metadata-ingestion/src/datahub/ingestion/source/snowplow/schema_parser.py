"""
Utility for parsing Snowplow JSON Schemas to DataHub schema format.

Converts JSON Schema definitions (used by Snowplow/Iglu) to DataHub's SchemaMetadata.

Schema V1/V2 Support:
- Simple scalar fields use V1 format: user_id, timestamp
- Complex types use V2 format: [version=2.0].items[type=array].price
"""

import logging
from typing import Any, Dict, List, Optional

from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    DateTypeClass,
    EnumTypeClass,
    NullTypeClass,
    NumberTypeClass,
    OtherSchemaClass,
    RecordTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
)

logger = logging.getLogger(__name__)

# Schema V2 tokens
VERSION_PREFIX = "[version=2.0]"
ARRAY_TYPE_TOKEN = "[type=array]"
RECORD_TYPE_TOKEN = "[type=record]"


class SnowplowSchemaParser:
    """
    Parser for converting Snowplow JSON Schemas to DataHub schema format.

    Handles:
    - JSON Schema type mapping to DataHub types
    - Nested objects and arrays
    - Enum types
    - Required fields
    - Field descriptions
    """

    # Mapping from JSON Schema types to DataHub types
    JSON_SCHEMA_TYPE_MAP = {
        "string": StringTypeClass,
        "integer": NumberTypeClass,
        "number": NumberTypeClass,
        "boolean": BooleanTypeClass,
        "object": RecordTypeClass,
        "array": ArrayTypeClass,
        "null": NullTypeClass,
    }

    # JSON Schema format to DataHub type mapping
    JSON_SCHEMA_FORMAT_MAP = {
        "date": DateTypeClass,
        "date-time": DateTypeClass,
        "time": DateTypeClass,
        "uuid": StringTypeClass,
        "email": StringTypeClass,
        "uri": StringTypeClass,
        "hostname": StringTypeClass,
        "ipv4": StringTypeClass,
        "ipv6": StringTypeClass,
    }

    @staticmethod
    def parse_schema(
        schema_data: Dict[str, Any],
        vendor: str,
        name: str,
        version: str,
    ) -> SchemaMetadataClass:
        """
        Parse JSON Schema to DataHub SchemaMetadata.

        Generates mixed V1/V2 schema:
        - Simple fields: V1 format (e.g., user_id)
        - Complex fields: V2 format (e.g., [version=2.0].items[type=array].price)

        Args:
            schema_data: JSON Schema definition (with 'properties' field)
            vendor: Schema vendor (for field path prefixes)
            name: Schema name
            version: Schema version

        Returns:
            DataHub SchemaMetadata
        """
        properties = schema_data.get("properties", {})
        required_fields = set(schema_data.get("required", []))

        fields: List[SchemaFieldClass] = []

        # Parse top-level properties (recursively for nested structures)
        for field_name, field_def in properties.items():
            parsed_fields = SnowplowSchemaParser._parse_field(
                field_name=field_name,
                field_def=field_def,
                parent_path="",
                required_fields=required_fields,
            )
            fields.extend(parsed_fields)

        return SchemaMetadataClass(
            schemaName=f"{vendor}/{name}",
            version=0,  # DataHub schema version (not Snowplow SchemaVer)
            hash="",
            platform="urn:li:dataPlatform:snowplow",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=fields,
        )

    @staticmethod
    def _parse_field(
        field_name: str,
        field_def: Dict[str, Any],
        parent_path: str,
        required_fields: set,
    ) -> List[SchemaFieldClass]:
        """
        Parse a field from JSON Schema, recursively handling nested structures.

        Returns a list of fields (1 for simple types, multiple for complex types with nested fields).

        Uses mixed V1/V2 format:
        - Simple scalar types: V1 paths (e.g., "user_id")
        - Complex types: V2 paths (e.g., "[version=2.0].items[type=array].price")

        Args:
            field_name: Field name
            field_def: Field definition from JSON Schema
            parent_path: Parent field path (for nested fields)
            required_fields: Set of required field names

        Returns:
            List of DataHub SchemaFields (single field for scalars, multiple for nested structures)
        """
        # Extract field metadata
        description = field_def.get("description", "")
        json_type = field_def.get("type")
        json_format = field_def.get("format")
        enum_values = field_def.get("enum")

        # Handle nullable fields (type can be array: ["string", "null"])
        is_nullable = False
        if isinstance(json_type, list):
            if "null" in json_type:
                is_nullable = True
                # Remove null and use the primary type
                json_type = next((t for t in json_type if t != "null"), "string")

        # Ensure json_type is a string for type checking
        json_type_str = str(json_type) if json_type is not None else None

        # Check if required
        is_required = field_name in required_fields
        is_nullable_final = is_nullable or not is_required

        # Build field path based on whether this is a complex type
        is_complex = json_type_str in ("array", "object")

        if is_complex and not parent_path:
            # Top-level complex field: use V2 format
            field_path = f"{VERSION_PREFIX}.{field_name}"
        elif parent_path:
            # Nested field: append to parent path
            field_path = f"{parent_path}.{field_name}"
        else:
            # Top-level simple field: use V1 format
            field_path = field_name

        # Handle different types
        if json_type_str == "array":
            return SnowplowSchemaParser._parse_array_field(
                field_path=field_path,
                field_def=field_def,
                description=description,
                is_nullable=is_nullable_final,
                required_fields=required_fields,
            )

        elif json_type_str == "object":
            return SnowplowSchemaParser._parse_object_field(
                field_path=field_path,
                field_def=field_def,
                description=description,
                is_nullable=is_nullable_final,
                required_fields=required_fields,
            )

        else:
            # Simple scalar field
            try:
                datahub_type = SnowplowSchemaParser._get_datahub_type(
                    json_type=json_type_str,
                    json_format=json_format,
                    enum_values=enum_values,
                    field_def=field_def,
                )
            except Exception as e:
                logger.warning(
                    f"Failed to determine type for field '{field_path}': {e}. "
                    f"Using StringType as fallback."
                )
                datahub_type = SchemaFieldDataTypeClass(type=StringTypeClass())

            return [
                SchemaFieldClass(
                    fieldPath=field_path,
                    type=datahub_type,
                    nativeDataType=json_type_str or "string",
                    description=description or None,
                    nullable=is_nullable_final,
                    isPartOfKey=False,
                )
            ]

    @staticmethod
    def _parse_array_field(
        field_path: str,
        field_def: Dict[str, Any],
        description: str,
        is_nullable: bool,
        required_fields: set,
    ) -> List[SchemaFieldClass]:
        """
        Parse an array field, creating the array field + nested fields for array items.

        For array of objects: Creates multiple fields with V2 paths like:
        - [version=2.0].items (array type)
        - [version=2.0].items[type=array].price (nested field in array items)

        Args:
            field_path: Field path for the array field
            field_def: Array field definition
            description: Field description
            is_nullable: Whether field is nullable
            required_fields: Set of required field names

        Returns:
            List of schema fields (array field + nested fields if items are objects)
        """
        items_def = field_def.get("items", {})
        item_type = items_def.get("type", "string")

        fields: List[SchemaFieldClass] = []

        # Create the array field itself
        array_field = SchemaFieldClass(
            fieldPath=field_path,
            type=SchemaFieldDataTypeClass(
                type=ArrayTypeClass(
                    nestedType=[item_type if isinstance(item_type, str) else "string"]
                )
            ),
            nativeDataType="array",
            description=description or None,
            nullable=is_nullable,
            isPartOfKey=False,
        )
        fields.append(array_field)

        # If array items are objects, recursively parse their properties
        if item_type == "object":
            items_properties = items_def.get("properties", {})
            items_required = set(items_def.get("required", []))

            # Parse each property of the array items
            for prop_name, prop_def in items_properties.items():
                # Build V2 path: parent_path.[type=array].property_name
                # Note: Dot before type annotation is required per DataHub V2 format
                nested_path = f"{field_path}.{ARRAY_TYPE_TOKEN}"
                nested_fields = SnowplowSchemaParser._parse_field(
                    field_name=prop_name,
                    field_def=prop_def,
                    parent_path=nested_path,
                    required_fields=items_required,
                )
                fields.extend(nested_fields)

        return fields

    @staticmethod
    def _parse_object_field(
        field_path: str,
        field_def: Dict[str, Any],
        description: str,
        is_nullable: bool,
        required_fields: set,
    ) -> List[SchemaFieldClass]:
        """
        Parse an object field, creating the record field + nested fields for object properties.

        For nested objects: Creates multiple fields with V2 paths like:
        - [version=2.0].address (record type)
        - [version=2.0].address[type=record].street (nested field in record)

        Args:
            field_path: Field path for the object field
            field_def: Object field definition
            description: Field description
            is_nullable: Whether field is nullable
            required_fields: Set of required field names

        Returns:
            List of schema fields (record field + nested property fields)
        """
        properties = field_def.get("properties", {})
        nested_required = set(field_def.get("required", []))

        fields: List[SchemaFieldClass] = []

        # Create the record field itself
        record_field = SchemaFieldClass(
            fieldPath=field_path,
            type=SchemaFieldDataTypeClass(type=RecordTypeClass()),
            nativeDataType="object",
            description=description or None,
            nullable=is_nullable,
            isPartOfKey=False,
        )
        fields.append(record_field)

        # Recursively parse nested properties
        for prop_name, prop_def in properties.items():
            # Build V2 path: parent_path.[type=record].property_name
            # Note: Dot before type annotation is required per DataHub V2 format
            nested_path = f"{field_path}.{RECORD_TYPE_TOKEN}"
            nested_fields = SnowplowSchemaParser._parse_field(
                field_name=prop_name,
                field_def=prop_def,
                parent_path=nested_path,
                required_fields=nested_required,
            )
            fields.extend(nested_fields)

        return fields

    @staticmethod
    def _get_datahub_type(
        json_type: Optional[str],
        json_format: Optional[str],
        enum_values: Optional[List[Any]],
        field_def: Dict[str, Any],
    ) -> SchemaFieldDataTypeClass:
        """
        Get DataHub field type from JSON Schema type.

        Note: This is for simple scalar types only. Complex types (array, object)
        are handled by _parse_array_field and _parse_object_field.

        Args:
            json_type: JSON Schema type (string, number, etc.)
            json_format: JSON Schema format (date-time, email, etc.)
            enum_values: Enum values if type is enum
            field_def: Full field definition

        Returns:
            DataHub SchemaFieldDataType
        """
        # Handle enum types
        if enum_values:
            return SchemaFieldDataTypeClass(type=EnumTypeClass())

        # Handle format-specific types (date, datetime, etc.)
        if json_format and json_format in SnowplowSchemaParser.JSON_SCHEMA_FORMAT_MAP:
            type_class = SnowplowSchemaParser.JSON_SCHEMA_FORMAT_MAP[json_format]
            return SchemaFieldDataTypeClass(type=type_class())  # type: ignore[arg-type]

        # Handle basic scalar types
        if json_type == "string":
            return SchemaFieldDataTypeClass(type=StringTypeClass())

        elif json_type == "integer" or json_type == "number":
            return SchemaFieldDataTypeClass(type=NumberTypeClass())

        elif json_type == "boolean":
            return SchemaFieldDataTypeClass(type=BooleanTypeClass())

        elif json_type == "null":
            return SchemaFieldDataTypeClass(type=NullTypeClass())

        else:
            # Default to string for unknown types
            logger.warning(f"Unknown JSON Schema type '{json_type}', using StringType")
            return SchemaFieldDataTypeClass(type=StringTypeClass())

    @staticmethod
    def parse_schema_ver(version: str) -> Dict[str, int]:
        """
        Parse Snowplow SchemaVer (MODEL-REVISION-ADDITION) format.

        Args:
            version: SchemaVer string (e.g., "1-0-0")

        Returns:
            Dict with model, revision, addition keys
        """
        try:
            parts = version.split("-")
            if len(parts) != 3:
                raise ValueError(f"Invalid SchemaVer format: {version}")

            return {
                "model": int(parts[0]),
                "revision": int(parts[1]),
                "addition": int(parts[2]),
            }
        except Exception as e:
            logger.warning(f"Failed to parse SchemaVer '{version}': {e}")
            return {"model": 0, "revision": 0, "addition": 0}

    @staticmethod
    def schema_ver_to_string(version_dict: Dict[str, int]) -> str:
        """
        Convert SchemaVer dict to string format.

        Args:
            version_dict: Dict with model, revision, addition keys

        Returns:
            SchemaVer string (e.g., "1-0-0")
        """
        return f"{version_dict['model']}-{version_dict['revision']}-{version_dict['addition']}"

    @staticmethod
    def get_full_schema_name(vendor: str, name: str, version: str) -> str:
        """
        Get full Iglu schema identifier.

        Args:
            vendor: Schema vendor
            name: Schema name
            version: SchemaVer version

        Returns:
            Full schema identifier (e.g., "com.example/event_name/1-0-0")
        """
        return f"{vendor}/{name}/{version}"
