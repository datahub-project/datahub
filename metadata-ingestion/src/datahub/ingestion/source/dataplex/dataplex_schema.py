"""Schema extraction utilities for Dataplex source."""

import json
import logging
import re
from collections.abc import Iterable, Iterator
from itertools import count
from typing import Any, Dict, List, Optional

from google.cloud import dataplex_v1
from google.protobuf import struct_pb2

from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.extractor.schema_util import avro_schema_to_mce_fields
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
from datahub.utilities.hive_schema_to_avro import HiveColumnToAvroConverter

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


# e.g. ``array<struct<id:string>>``; a bare ``struct`` word has nothing to parse.
_HIVE_COMPLEX_NATIVE_TYPE = re.compile(
    r"^(struct|array|map|list|uniontype)<", re.IGNORECASE
)

# Fallback for leaf spellings the Hive parser cannot read, such as ``STRING(MAX)``.
_METADATA_TYPE_TO_AVRO: Dict[str, Dict[str, Any]] = {
    "STRING": {"type": "string"},
    "NUMBER": {"type": "double"},
    "BOOLEAN": {"type": "boolean"},
    "BYTES": {"type": "bytes"},
    "DATETIME": {"type": "int", "logicalType": "timestamp-millis"},
    "TIMESTAMP": {"type": "int", "logicalType": "timestamp-millis"},
}

_REPEATED_MODE = "REPEATED"
_REQUIRED_MODE = "REQUIRED"
_RECORD_TYPES = ("RECORD", "STRUCT")
_ARRAY_OR_MAP_TYPE = re.compile(r"^(array|list|map)(<|$)", re.IGNORECASE)
_MAP_TYPE = re.compile(r"^map(<|$)", re.IGNORECASE)
_MAP_KEY_FIELD = "key"
_MAP_VALUE_FIELD = "value"

_BIGQUERY_PLATFORM = "bigquery"
# Dataplex returns BigQuery REST type names; the BigQuery connector parses the
# standard SQL names, which the Hive parser maps to long/double, not int/float.
_BIGQUERY_REST_TO_STANDARD_SQL_TYPE: Dict[str, str] = {
    "INTEGER": "INT64",
    "FLOAT": "FLOAT64",
}

_FLAT_FALLBACK_TITLE = "Complex column emitted as a flat row"
_FLAT_FALLBACK_MESSAGE = (
    "Could not convert a column with nested structure to nested fieldPaths; "
    "emitted it as a single flat row instead."
)


class _NoStructureError(ValueError):
    """The column's type spelling yields nothing to nest, so it stays flat."""


def _extract_nested_field_items(field_data: Any) -> List[Any]:
    """A field's nested ``fields``, from either a protobuf Value or exported JSON."""
    if isinstance(field_data, dict):
        nested = field_data.get("fields")
    else:
        nested = getattr(field_data, "fields", None)
    if nested is None or isinstance(nested, (str, bytes)):
        return []
    if isinstance(nested, struct_pb2.Value):
        return list(nested.list_value.values)
    if isinstance(nested, Iterable):
        return list(nested)
    return []


def _is_complex_field(field_data: Any, data_type: str, mode: str) -> bool:
    """True when a field carries structure the flat mapping would drop."""
    return bool(
        mode == _REPEATED_MODE
        or _extract_nested_field_items(field_data)
        or _HIVE_COMPLEX_NATIVE_TYPE.match(data_type.strip())
    )


def _leaf_avro_type(
    data_type: str, metadata_type: str, platform: str
) -> Dict[str, Any]:
    """Avro node for a leaf field, parsed as the BigQuery connector parses one.

    Unknown types become ``null`` (NullType).
    """
    parsed_type = data_type
    if platform == _BIGQUERY_PLATFORM:
        parsed_type = _BIGQUERY_REST_TO_STANDARD_SQL_TYPE.get(
            data_type.upper(), data_type
        )
    node = dict(
        HiveColumnToAvroConverter._parse_basic_datatype_string(parsed_type.lower())
    )
    if node.get("type") == "null":
        node.update(_METADATA_TYPE_TO_AVRO.get(metadata_type.upper(), {}))
    node["native_data_type"] = data_type
    return node


def _element_unparsed(node: Any) -> bool:
    """True when an array or map parsed with an unreadable element type."""
    if not isinstance(node, dict):
        return False
    if node.get("type") == "array":
        element = node.get("items")
    elif node.get("type") == "map":
        element = node.get("values")
    else:
        return False
    return isinstance(element, dict) and element.get("type") == "null"


def _map_from_key_value_fields(
    data_type: str, child_fields: List[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """Avro map for a map column that Dataplex lists as key/value child fields."""
    if not _MAP_TYPE.match(data_type):
        return None
    child_types = {child["name"]: child["type"] for child in child_fields}
    if len(child_fields) != 2 or set(child_types) != {_MAP_KEY_FIELD, _MAP_VALUE_FIELD}:
        return None
    key_type = child_types[_MAP_KEY_FIELD]
    return {
        "type": "map",
        "values": child_types[_MAP_VALUE_FIELD],
        "native_data_type": data_type,
        "key_type": key_type,
        "key_native_data_type": (
            key_type.get("native_data_type", "") if isinstance(key_type, dict) else ""
        ),
    }


def _kc_field_to_avro_type(
    field_data: Any, entry_id: str, platform: str, struct_names: Iterator[int]
) -> Any:
    """Avro node for one catalog schema field, recursing into nested fields."""
    data_type = str(
        extract_field_value(field_data, "type")
        or extract_field_value(field_data, "dataType", "string")
    ).strip()
    # The BigQuery connector types fields from the type name alone; using
    # metadataType here would change fieldPath tokens on BigQuery entries.
    metadata_type = (
        ""
        if platform == _BIGQUERY_PLATFORM
        else str(extract_field_value(field_data, "metadataType"))
    )
    mode = str(extract_field_value(field_data, "mode")).upper()
    nested_items = _extract_nested_field_items(field_data)

    core: Any
    if nested_items:
        child_fields: List[Dict[str, Any]] = []
        for nested_item in nested_items:
            child_data = process_schema_field_item(nested_item, entry_id)
            if child_data is None:
                continue
            child_name = extract_field_value(child_data, "name") or extract_field_value(
                child_data, "column"
            )
            if not child_name:
                continue
            # The BigQuery connector parses the lowercased STRUCT type string, so
            # nested names (not top-level ones) come out lowercased there.
            if platform == _BIGQUERY_PLATFORM:
                child_name = child_name.lower()
            child_field: Dict[str, Any] = {
                "name": child_name,
                "type": _kc_field_to_avro_type(
                    child_data, entry_id, platform, struct_names
                ),
            }
            child_desc = extract_field_value(child_data, "description")
            if child_desc:
                child_field["doc"] = child_desc
            child_fields.append(child_field)
        core = _map_from_key_value_fields(data_type, child_fields)
        if core is not None and _element_unparsed(core):
            return {"type": "null", "native_data_type": data_type}
        if core is None:
            core = {
                # Must be unique per schema; schema_util renders it as [type=struct].
                "type": "record",
                "name": f"__struct_{next(struct_names)}",
                "fields": child_fields,
                "native_data_type": data_type,
            }
    elif _HIVE_COMPLEX_NATIVE_TYPE.match(data_type):
        # Private entry point on purpose: the public one wraps the result in an
        # extra record layer, which would double-nest the column.
        try:
            core = HiveColumnToAvroConverter._parse_datatype_string(data_type.lower())
        except Exception:
            logger.debug(
                "Could not parse native type %r for entry %s", data_type, entry_id
            )
            core = None
        if core is None or _element_unparsed(core):
            # e.g. ARRAY<STRING(MAX)>: a SQL spelling the Hive parser cannot read.
            # Returned before the REPEATED wrap, so no array<null> is emitted.
            return {"type": "null", "native_data_type": data_type}
    elif data_type.upper() in _RECORD_TYPES or metadata_type.upper() in _RECORD_TYPES:
        core = {
            "type": "record",
            "name": f"__struct_{next(struct_names)}",
            "fields": [],
            "native_data_type": data_type,
        }
    else:
        core = _leaf_avro_type(data_type, metadata_type, platform)

    kind = core.get("type") if isinstance(core, dict) else None
    # Dataplex marks map columns REPEATED, as a list of key/value pairs.
    if mode == _REPEATED_MODE and kind != "map":
        if kind != "array":
            if _ARRAY_OR_MAP_TYPE.match(data_type):
                array_native = data_type
            else:
                inner_native = (
                    core.get("native_data_type", data_type)
                    if isinstance(core, dict)
                    else data_type
                )
                array_native = f"ARRAY<{inner_native}>"
            core = {"type": "array", "items": core, "native_data_type": array_native}
        core["_nullable"] = False
        return core
    if isinstance(core, dict):
        core["_nullable"] = mode != _REQUIRED_MODE
    return core


def _convert_complex_column(
    field_data: Any, field_name: str, description: str, entry_id: str, platform: str
) -> List[SchemaFieldClass]:
    """Nested v2 SchemaFields for one complex column, via ``avro_schema_to_mce_fields``.

    Raises when no structure is found, so the caller falls back to a flat row.
    """
    avro_type = _kc_field_to_avro_type(field_data, entry_id, platform, count())
    if isinstance(avro_type, dict) and avro_type.get("type") == "null":
        raise _NoStructureError(f"no structure extracted for column {field_name}")
    column_field: Dict[str, Any] = {"name": field_name, "type": avro_type}
    if description:
        column_field["doc"] = description
    avro_schema = {"type": "record", "name": "__struct_", "fields": [column_field]}
    schema_fields = avro_schema_to_mce_fields(
        json.dumps(avro_schema), swallow_exceptions=False
    )
    if not schema_fields:
        raise ValueError(f"Avro conversion yielded no fields for column {field_name}")
    if description and not schema_fields[0].description:
        schema_fields[0].description = description
    return schema_fields


def _complex_column_fields(
    field_data: Any,
    field_name: str,
    field_type: str,
    description: str,
    entry_id: str,
    platform: str,
    report: Optional[SourceReport],
) -> Optional[List[SchemaFieldClass]]:
    """Nested v2 SchemaFields for a complex column; None means emit a flat row."""
    mode = str(extract_field_value(field_data, "mode")).upper()
    if not _is_complex_field(field_data, field_type, mode):
        return None
    try:
        schema_fields = _convert_complex_column(
            field_data, field_name, description, entry_id, platform
        )
    except _NoStructureError:
        logger.debug(
            f"Keeping column '{field_name}' ({field_type}) flat in entry "
            f"{entry_id}: nothing to nest"
        )
        return None
    except Exception as conversion_error:
        context = f"entry={entry_id}, column={field_name}, type={field_type}"
        if report is not None:
            report.warning(
                title=_FLAT_FALLBACK_TITLE,
                message=_FLAT_FALLBACK_MESSAGE,
                context=context,
                exc=conversion_error,
            )
        else:
            logger.warning(f"{_FLAT_FALLBACK_MESSAGE} {context}: {conversion_error}")
        return None
    logger.debug(
        f"Extracted complex field '{field_name}' ({field_type}) for entry {entry_id}"
    )
    return schema_fields


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
    ) or type_str_upper in (
        "FLOAT",
        # BigQuery spells these FLOAT64 / BIGNUMERIC; without them numeric
        # columns silently fall through to the StringType default below.
        "FLOAT64",
        "DOUBLE",
        "NUMERIC",
        "BIGNUMERIC",
        "DECIMAL",
    ):
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
    entry: dataplex_v1.Entry,
    entry_id: str,
    platform: str,
    report: Optional[SourceReport] = None,
) -> Optional[SchemaMetadataClass]:
    """Extract schema metadata from Entry aspects.

    Looks for schema-type aspects in the entry and extracts column/field information.
    The schema aspect is typically stored at 'dataplex-types.global.schema'.

    Args:
        entry: Entry object from Catalog API
        entry_id: Entry ID for naming the schema
        platform: Platform name (bigquery, gcs, etc.)
        report: Receives a warning when an unexpected error stops a complex
            column from converting

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
                        complex_fields = _complex_column_fields(
                            field_data,
                            str(field_name),
                            str(field_type),
                            field_desc,
                            entry_id,
                            platform,
                            report,
                        )
                        if complex_fields is not None:
                            fields.extend(complex_fields)
                            continue

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
