"""
Utility for parsing Snowplow JSON Schemas to DataHub schema format.

Converts JSON Schema definitions (used by Snowplow/Iglu) to DataHub's SchemaMetadata.
"""

import logging
from typing import Any, Dict, List, Optional

from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    DateTypeClass,
    EnumTypeClass,
    MapTypeClass,
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

        # Parse top-level properties
        for field_name, field_def in properties.items():
            field = SnowplowSchemaParser._parse_field(
                field_name=field_name,
                field_def=field_def,
                parent_path="",
                required_fields=required_fields,
            )
            if field:
                fields.append(field)

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
    ) -> Optional[SchemaFieldClass]:
        """
        Parse a single field from JSON Schema.

        Args:
            field_name: Field name
            field_def: Field definition from JSON Schema
            parent_path: Parent field path (for nested fields)
            required_fields: Set of required field names

        Returns:
            DataHub SchemaField or None if parsing fails
        """
        # Build full field path
        field_path = f"{parent_path}.{field_name}" if parent_path else field_name

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

        # Determine DataHub type
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

        # Check if required
        is_required = field_name in required_fields

        return SchemaFieldClass(
            fieldPath=field_path,
            type=datahub_type,
            nativeDataType=json_type if isinstance(json_type, str) else str(json_type),
            description=description or None,
            nullable=is_nullable or not is_required,
            isPartOfKey=False,  # Snowplow events don't have explicit keys
        )

    @staticmethod
    def _get_datahub_type(
        json_type: Optional[str],
        json_format: Optional[str],
        enum_values: Optional[List[Any]],
        field_def: Dict[str, Any],
    ) -> SchemaFieldDataTypeClass:
        """
        Get DataHub field type from JSON Schema type.

        Args:
            json_type: JSON Schema type (string, number, etc.)
            json_format: JSON Schema format (date-time, email, etc.)
            enum_values: Enum values if type is enum
            field_def: Full field definition (for nested types)

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

        # Handle basic types
        if json_type == "string":
            return SchemaFieldDataTypeClass(type=StringTypeClass())

        elif json_type == "integer" or json_type == "number":
            return SchemaFieldDataTypeClass(type=NumberTypeClass())

        elif json_type == "boolean":
            return SchemaFieldDataTypeClass(type=BooleanTypeClass())

        elif json_type == "array":
            # Get array item type
            items_def = field_def.get("items", {})
            item_type = items_def.get("type", "string")

            # Determine the nested type string
            # nestedType should be a list of strings, not type class instances
            nested_type_str = item_type if isinstance(item_type, str) else "string"

            return SchemaFieldDataTypeClass(
                type=ArrayTypeClass(nestedType=[nested_type_str])
            )

        elif json_type == "object":
            # Nested object - would need recursive field parsing
            # For now, represent as Map type
            # keyType and valueType should be strings
            return SchemaFieldDataTypeClass(
                type=MapTypeClass(keyType="string", valueType="string")
            )

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
