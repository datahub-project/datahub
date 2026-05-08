"""Schema extraction utilities for Dataplex source."""

import logging
from typing import Any, Optional

from google.cloud import dataplex_v1

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


def _create_node_field(node_name: str) -> SchemaFieldClass:
    """Create a SchemaFieldClass for a graph node."""
    return SchemaFieldClass(
        fieldPath=f"[nodes].{node_name}",
        type=SchemaFieldDataTypeClass(type=RecordTypeClass()),
        nativeDataType="NODE",
        nullable=True,
        recursive=False,
    )


def _create_edge_field(
    edge_name: str, src_name: Optional[str], dst_name: Optional[str]
) -> SchemaFieldClass:
    """Create a SchemaFieldClass for a graph edge."""
    description = f"{src_name} → {dst_name}" if src_name and dst_name else None
    return SchemaFieldClass(
        fieldPath=f"[edges].{edge_name}",
        type=SchemaFieldDataTypeClass(type=RecordTypeClass()),
        nativeDataType="EDGE",
        description=description,
        nullable=True,
        recursive=False,
    )


def _extract_proto_edge_names(
    edge_fields: dict,
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Extract edge name and source/destination names from proto Value form."""
    name_val = edge_fields.get("name")
    edge_name = name_val.string_value if name_val else None

    src_val = edge_fields.get("source")
    src_name = None
    if src_val and hasattr(src_val, "struct_value"):
        src_name_val = dict(src_val.struct_value.fields).get("name")
        src_name = src_name_val.string_value if src_name_val else None

    dst_val = edge_fields.get("destination")
    dst_name = None
    if dst_val and hasattr(dst_val, "struct_value"):
        dst_name_val = dict(dst_val.struct_value.fields).get("name")
        dst_name = dst_name_val.string_value if dst_name_val else None

    return edge_name, src_name, dst_name


def extract_graph_schema_from_entry_aspects(
    entry: dataplex_v1.Entry, entry_id: str, platform: str
) -> Optional[SchemaMetadataClass]:
    """Extract schema from a Spanner Graph ``graph-schema`` aspect.

    Nodes are emitted as fields under ``[nodes].<name>`` and edges as fields
    under ``[edges].<name>``, with source→destination in the edge description.
    """
    if not entry.aspects:
        return None

    graph_aspect = None
    for aspect_key, aspect_value in entry.aspects.items():
        if "graph-schema" in aspect_key:
            graph_aspect = aspect_value
            break

    if graph_aspect is None:
        logger.debug(f"No graph-schema aspect found for entry {entry_id}")
        return None
    if not hasattr(graph_aspect, "data") or not graph_aspect.data:
        logger.debug(f"graph-schema aspect for entry {entry_id} has no data")
        return None

    fields: list[SchemaFieldClass] = []
    try:
        data_dict = dict(graph_aspect.data)

        nodes_data = data_dict.get("nodes")
        if nodes_data and hasattr(nodes_data, "list_value"):
            # Proto Value form
            for node_value in nodes_data.list_value.values:
                node_fields = dict(node_value.struct_value.fields)
                name_val = node_fields.get("name")
                node_name = name_val.string_value if name_val else None
                if node_name:
                    fields.append(_create_node_field(node_name))
        elif nodes_data and hasattr(nodes_data, "__iter__"):
            # Python-native list form (proto-plus auto-marshaled Struct → dict)
            for node_item in nodes_data:
                # MapComposite objects from proto-plus need dict conversion
                node_dict = (
                    dict(node_item) if not isinstance(node_item, dict) else node_item
                )
                node_name = node_dict.get("name")
                if node_name:
                    fields.append(_create_node_field(node_name))

        edges_data = data_dict.get("edges")
        if edges_data and hasattr(edges_data, "list_value"):
            # Proto Value form
            for edge_value in edges_data.list_value.values:
                edge_fields = dict(edge_value.struct_value.fields)
                edge_name, src_name, dst_name = _extract_proto_edge_names(edge_fields)
                if edge_name:
                    fields.append(_create_edge_field(edge_name, src_name, dst_name))
        elif edges_data and hasattr(edges_data, "__iter__"):
            # Python-native list form (proto-plus auto-marshaled Struct → dict)
            for edge_item in edges_data:
                # MapComposite objects from proto-plus need dict conversion
                edge_dict = (
                    dict(edge_item) if not isinstance(edge_item, dict) else edge_item
                )
                edge_name = edge_dict.get("name")
                src = edge_dict.get("source")
                src_name = dict(src).get("name") if src else None
                dst = edge_dict.get("destination")
                dst_name = dict(dst).get("name") if dst else None

                if edge_name:
                    fields.append(_create_edge_field(edge_name, src_name, dst_name))

        if not fields:
            logger.debug(
                f"No nodes or edges extracted from graph-schema for {entry_id}"
            )
            return None

        logger.info(f"Extracted {len(fields)} graph schema fields for entry {entry_id}")
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
            f"Failed to extract graph schema from entry {entry_id}: {e}",
            exc_info=True,
        )
        return None


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
