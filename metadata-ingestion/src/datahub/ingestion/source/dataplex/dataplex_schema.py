"""Schema extraction utilities for Dataplex source."""

import logging
from typing import Any, Optional

from google.cloud import dataplex_v1

from datahub.ingestion.source.dataplex.dataplex_helpers import (
    map_dataplex_field_to_datahub,
)
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    NumberTypeClass,
    OtherSchemaClass,
    RecordTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    TimeTypeClass,
)
from datahub.metadata.urns import DataPlatformUrn

logger = logging.getLogger(__name__)


def extract_schema_metadata(
    entity: dataplex_v1.Entity, dataset_urn: str, platform: str
) -> Optional[SchemaMetadataClass]:
    """Extract schema metadata from Dataplex entity.

    Args:
        entity: Dataplex entity object
        dataset_urn: Dataset URN (not used, kept for compatibility)
        platform: Platform name (bigquery, gcs, etc.)

    Returns:
        SchemaMetadataClass if schema found, None otherwise
    """
    if not entity.schema or not entity.schema.fields:
        return None

    fields = []
    for field in entity.schema.fields:
        field_path = field.name

        field_type = map_dataplex_field_to_datahub(field)

        schema_field = SchemaFieldClass(
            fieldPath=field_path,
            type=field_type,
            nativeDataType=dataplex_v1.types.Schema.Type(field.type_).name,
            description=field.description or "",
            nullable=True,  # Dataplex doesn't explicitly track nullability
            recursive=False,
        )

        # Handle nested fields
        if field.fields:
            schema_field.type = SchemaFieldDataTypeClass(type=RecordTypeClass())
            # Add nested fields
            for nested_field in field.fields:
                nested_field_path = f"{field_path}.{nested_field.name}"
                nested_type = map_dataplex_field_to_datahub(nested_field)
                nested_schema_field = SchemaFieldClass(
                    fieldPath=nested_field_path,
                    type=nested_type,
                    nativeDataType=dataplex_v1.types.Schema.Type(
                        nested_field.type_
                    ).name,
                    description=nested_field.description or "",
                    nullable=True,
                    recursive=False,
                )
                fields.append(nested_schema_field)

        fields.append(schema_field)

    return SchemaMetadataClass(
        schemaName=entity.id,
        platform=str(DataPlatformUrn(platform)),
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=fields,
    )


def extract_field_value(field_data: Any, field_key: str, default: str = "") -> str:
    """Extract a field value from protobuf field data (dict or object).

    Args:
        field_data: Field data (dict or protobuf object)
        field_key: Primary key to look for
        default: Default value if not found

    Returns:
        Extracted value as string
    """
    if isinstance(field_data, dict):
        val = field_data.get(field_key)
        if val is None:
            return default
        return (
            val.string_value
            if hasattr(val, "string_value")
            else str(val)
            if val
            else default
        )
    else:
        val = getattr(field_data, field_key, None)
        return str(val) if val else default


def process_schema_field_item(field_value: Any, entry_id: str) -> Optional[Any]:
    """Process a single schema field item from protobuf data.

    Args:
        field_value: Field value from schema fields list
        entry_id: Entry ID for logging (not currently used)

    Returns:
        Field data object or None
    """
    if hasattr(field_value, "struct_value"):
        # Protobuf Value with struct_value
        return dict(field_value.struct_value.fields)
    elif hasattr(field_value, "__getitem__") or hasattr(field_value, "__dict__"):
        # Direct object or dict-like (proto.marshal objects)
        try:
            return dict(field_value) if hasattr(field_value, "items") else field_value
        except (TypeError, AttributeError):
            return field_value
    return None


def map_aspect_type_to_datahub(type_str: str) -> SchemaFieldDataTypeClass:
    """Map aspect schema type string to DataHub schema type.

    Args:
        type_str: Type string from aspect data (e.g., "STRING", "INTEGER", "BOOLEAN")

    Returns:
        SchemaFieldDataTypeClass for DataHub
    """
    type_str_upper = type_str.upper()

    # Map common types
    if type_str_upper in ("STRING", "VARCHAR", "CHAR", "TEXT"):
        return SchemaFieldDataTypeClass(type=StringTypeClass())
    elif type_str_upper in (
        "INTEGER",
        "INT",
        "INT64",
        "LONG",
    ) or type_str_upper in ("FLOAT", "DOUBLE", "NUMERIC", "DECIMAL"):
        return SchemaFieldDataTypeClass(type=NumberTypeClass())
    elif type_str_upper in ("BOOLEAN", "BOOL"):
        return SchemaFieldDataTypeClass(type=BooleanTypeClass())
    elif type_str_upper in ("TIMESTAMP", "DATETIME", "DATE", "TIME"):
        return SchemaFieldDataTypeClass(type=TimeTypeClass())
    elif type_str_upper in ("BYTES", "BINARY"):
        return SchemaFieldDataTypeClass(type=BytesTypeClass())
    elif type_str_upper in ("RECORD", "STRUCT"):
        return SchemaFieldDataTypeClass(type=RecordTypeClass())
    elif type_str_upper == "ARRAY":
        return SchemaFieldDataTypeClass(type=ArrayTypeClass(nestedType=["string"]))
    else:
        # Default to string for unknown types
        return SchemaFieldDataTypeClass(type=StringTypeClass())


def extract_schema_from_entry_aspects(
    entry: dataplex_v1.Entry, entry_id: str, platform: str
) -> Optional[SchemaMetadataClass]:
    """Extract schema metadata from Entry aspects.

    Looks for schema-type aspects in the entry and extracts column/field information.
    The schema aspect is typically stored at 'dataplex-types.global.schema'.

    Args:
        entry: Entry object from Catalog API
        entry_id: Entry ID for naming the schema
        platform: Platform name (bigquery, gcs, etc.)

    Returns:
        SchemaMetadataClass if schema aspect found, None otherwise
    """
    if not entry.aspects:
        logger.debug(f"Entry {entry_id} has no aspects")
        return None

    # Log all available aspect types for debugging
    aspect_keys = list(entry.aspects.keys())
    logger.debug(f"Entry {entry_id} has aspects: {aspect_keys}")

    # Look for the standard Dataplex schema aspect type
    schema_aspect = None
    schema_aspect_key = None

    # First, try the standard Dataplex schema aspect type
    for aspect_key in entry.aspects:
        # Check for the global schema aspect type (most common)
        if "dataplex-types.global.schema" in aspect_key or aspect_key.endswith(
            "/schema"
        ):
            schema_aspect = entry.aspects[aspect_key]
            schema_aspect_key = aspect_key
            logger.debug(
                f"Found schema aspect for entry {entry_id} at key: {aspect_key}"
            )
            break

    # Fallback: Look for any aspect with "schema" in the name
    if not schema_aspect:
        for aspect_key, aspect_value in entry.aspects.items():
            aspect_type = aspect_key.split("/")[-1]
            if "schema" in aspect_type.lower():
                schema_aspect = aspect_value
                schema_aspect_key = aspect_key
                logger.debug(
                    f"Found schema-like aspect for entry {entry_id} at key: {aspect_key}"
                )
                break

    if not schema_aspect:
        logger.debug(
            f"No schema aspect found for entry {entry_id}. Available aspects: {aspect_keys}"
        )
        return None

    if not hasattr(schema_aspect, "data") or not schema_aspect.data:
        logger.debug(
            f"Schema aspect {schema_aspect_key} for entry {entry_id} has no data"
        )
        return None

    # Extract schema fields from aspect data
    fields: list[SchemaFieldClass] = []
    try:
        # The aspect.data is a Struct (protobuf)
        data_dict = dict(schema_aspect.data)
        logger.debug(
            f"Schema aspect data keys for entry {entry_id}: {list(data_dict.keys())}"
        )

        # Common field names in schema aspects: columns, fields, schema
        schema_fields_data = (
            data_dict.get("columns")
            or data_dict.get("fields")
            or data_dict.get("schema")
        )

        if not schema_fields_data:
            logger.debug(
                f"No column/field data found in schema aspect for entry {entry_id}. "
                f"Available keys: {list(data_dict.keys())}"
            )
            return None

        # The schema_fields_data can be either:
        # 1. A protobuf Value with list_value attribute (from some aspects)
        # 2. A RepeatedComposite (proto.marshal list-like object) that can be iterated directly
        logger.debug(
            f"Processing schema fields for entry {entry_id}, type: {type(schema_fields_data).__name__}"
        )

        # Try to iterate schema_fields_data - it could be a RepeatedComposite or list_value
        schema_items = None
        if hasattr(schema_fields_data, "list_value"):
            # Protobuf Value with list_value
            schema_items = schema_fields_data.list_value.values
            logger.debug(
                f"Found {len(schema_items)} fields in list_value for entry {entry_id}"
            )
        elif hasattr(schema_fields_data, "__iter__"):
            # RepeatedComposite or other iterable (can iterate directly)
            schema_items = list(schema_fields_data)
            logger.debug(
                f"Found {len(schema_items)} fields in iterable for entry {entry_id}"
            )

        if schema_items:
            for field_value in schema_items:
                field_data = process_schema_field_item(field_value, entry_id)
                if field_data:
                    # Extract field name, type, and description using helper
                    field_name = extract_field_value(
                        field_data, "name"
                    ) or extract_field_value(field_data, "column")
                    field_type = extract_field_value(
                        field_data, "type"
                    ) or extract_field_value(field_data, "dataType", "string")
                    field_desc = extract_field_value(field_data, "description")

                    if field_name:
                        # Map the type string to DataHub schema type
                        datahub_type = map_aspect_type_to_datahub(str(field_type))

                        schema_field = SchemaFieldClass(
                            fieldPath=str(field_name),
                            type=datahub_type,
                            nativeDataType=str(field_type),
                            description=field_desc,
                            nullable=True,
                            recursive=False,
                        )
                        fields.append(schema_field)
                        logger.debug(
                            f"Extracted field '{field_name}' ({field_type}) for entry {entry_id}"
                        )

        if not fields:
            logger.debug(f"No schema fields extracted from entry {entry_id} aspects")
            return None

        logger.info(f"Extracted {len(fields)} schema fields for entry {entry_id}")
        return SchemaMetadataClass(
            schemaName=entry_id,
            platform=str(DataPlatformUrn(platform)),
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=fields,
        )

    except Exception as e:
        logger.warning(
            f"Failed to extract schema from entry {entry_id} aspects: {e}",
            exc_info=True,
        )
        return None
