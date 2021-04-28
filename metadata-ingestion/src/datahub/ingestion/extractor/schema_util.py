import logging
from typing import Any, List, Optional, Union

import avro.schema

from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    EnumTypeClass,
    FixedTypeClass,
    MapTypeClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaField,
    SchemaFieldDataType,
    StringTypeClass,
    UnionTypeClass,
)

"""A helper file for Avro schema -> MCE schema transformations"""

logger = logging.getLogger(__name__)

_field_type_mapping = {
    "null": NullTypeClass,
    "bool": BooleanTypeClass,
    "boolean": BooleanTypeClass,
    "int": NumberTypeClass,
    "long": NumberTypeClass,
    "float": NumberTypeClass,
    "double": NumberTypeClass,
    "bytes": BytesTypeClass,
    "string": StringTypeClass,
    "record": RecordTypeClass,
    "map": MapTypeClass,
    "enum": EnumTypeClass,
    "array": ArrayTypeClass,
    "union": UnionTypeClass,
    "fixed": FixedTypeClass,
}


def _get_column_type(field_type: Union[str, dict]) -> SchemaFieldDataType:
    tp = field_type
    if hasattr(tp, "type"):
        tp = tp.type  # type: ignore
    tp = str(tp)
    TypeClass: Any = _field_type_mapping.get(tp)
    # Note: we could populate the nestedTypes field for unions and similar fields
    # for the other types as well. However, since we already populate the nativeDataType
    # field below, it is mostly ok to leave this as not fully initialized.
    dt = SchemaFieldDataType(type=TypeClass())
    return dt


def _is_nullable(schema: avro.schema.Schema) -> bool:
    if isinstance(schema, avro.schema.UnionSchema):
        return any(_is_nullable(sub_schema) for sub_schema in schema.schemas)
    elif isinstance(schema, avro.schema.PrimitiveSchema):
        return schema.name == "null"
    else:
        return False


def _recordschema_to_mce_fields(schema: avro.schema.RecordSchema) -> List[SchemaField]:
    fields: List[SchemaField] = []

    for parsed_field in schema.fields:
        description: Optional[str] = parsed_field.doc
        if parsed_field.has_default:
            description = description if description else "No description available."
            description = f"{description}\nField default value: {parsed_field.default}"
        field = SchemaField(
            fieldPath=parsed_field.name,
            nativeDataType=str(parsed_field.type),
            type=_get_column_type(parsed_field.type),
            description=description,
            recursive=False,
            nullable=_is_nullable(parsed_field.type),
        )

        fields.append(field)

    return fields


def _genericschema_to_mce_fields(schema: avro.schema.Schema) -> List[SchemaField]:
    fields: List[SchemaField] = []

    # In the generic (non-RecordSchema) case, only a single SchemaField will be returned
    # and the fieldPath will be set to empty to signal that the type refers to the
    # the whole object.
    field = SchemaField(
        fieldPath="",
        nativeDataType=str(schema.type),
        type=_get_column_type(schema.type),
        description=schema.props.get("doc", None),
        recursive=False,
        nullable=_is_nullable(schema),
    )
    fields.append(field)

    return fields


def avro_schema_to_mce_fields(avro_schema_string: str) -> List[SchemaField]:
    """Converts an avro schema into a schema compatible with MCE"""

    # Handle some library compatability issues.
    if hasattr(avro.schema, "parse"):
        schema_parse_fn = avro.schema.parse
    else:
        schema_parse_fn = avro.schema.Parse

    parsed_schema: avro.schema.Schema = schema_parse_fn(avro_schema_string)

    if isinstance(parsed_schema, avro.schema.RecordSchema):
        schema_convert_fn = _recordschema_to_mce_fields
    else:
        schema_convert_fn = _genericschema_to_mce_fields

    fields = schema_convert_fn(parsed_schema)

    return fields
