import logging
from typing import Any, Callable, Dict, List, Optional, Union

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


class AvroToMceSchemaConverter:
    field_type_mapping: Dict[str, Any] = {
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

    def __init__(self):
        # Initialize the prefix name stack for nested name generation.
        self._prefix_name_stack: List[str] = []
        self._record_types_seen: List[str] = []

    def _should_recurse(self, schema: avro.schema.Schema) -> bool:
        return isinstance(
            schema,
            (
                avro.schema.RecordSchema,
                avro.schema.UnionSchema,
                avro.schema.ArraySchema,
                avro.schema.MapSchema,
            ),
        ) and (
            not isinstance(schema, avro.schema.RecordSchema)
            or (schema.fullname not in self._record_types_seen)
        )

    def _get_column_type(self, field_type: Union[str, dict]) -> SchemaFieldDataType:
        tp = field_type
        if hasattr(tp, "type"):
            tp = tp.type  # type: ignore
        tp = str(tp)
        TypeClass: Any = self.field_type_mapping.get(tp)
        # Note: we could populate the nestedTypes field for unions and similar fields
        # for the other types as well. However, since we already populate the nativeDataType
        # field below, it is mostly ok to leave this as not fully initialized.
        dt = SchemaFieldDataType(type=TypeClass())
        return dt

    def _is_nullable(self, schema: avro.schema.Schema) -> bool:
        if isinstance(schema, avro.schema.UnionSchema):
            return any(self._is_nullable(sub_schema) for sub_schema in schema.schemas)
        elif isinstance(schema, avro.schema.PrimitiveSchema):
            return schema.name == "null"
        else:
            return False

    def _get_cur_field_path(self) -> str:
        return ".".join(self._prefix_name_stack)

    def _recordschema_to_mce_fields(
        self, schema: avro.schema.RecordSchema
    ) -> List[SchemaField]:
        fields: List[SchemaField] = []
        # Add the full name of this schema to the list of record types seen so far.
        self._record_types_seen.append(schema.fullname)

        # Generate the fields from the record schema's fields
        for parsed_field in schema.fields:
            # Generate the description.
            description: Optional[str] = (
                parsed_field.doc if parsed_field.doc else "No description available."
            )
            if parsed_field.has_default:
                description = (
                    f"{description}\nField default value: {parsed_field.default}"
                )
            # Add the field name to the prefix stack.
            self._prefix_name_stack.append(parsed_field.name)
            cur_sub_fields: List[SchemaField] = []
            if self._should_recurse(parsed_field.type):
                # Recursively explore sub-types.
                cur_sub_fields = self._to_mce_fields(parsed_field.type)
                fields.extend(cur_sub_fields)
            if not cur_sub_fields:
                # No subfields exist. So, this is the leaf field. Go ahead and construct the field and append it to the output.
                fields.append(
                    SchemaField(
                        fieldPath=self._get_cur_field_path(),
                        nativeDataType=str(parsed_field.type),
                        type=self._get_column_type(parsed_field.type),
                        description=description,
                        recursive=False,
                        nullable=self._is_nullable(parsed_field.type),
                    )
                )
            # Remove the name from prefix stack.
            self._prefix_name_stack.pop()
        return fields

    def _arrayschema_to_mce_fields(
        self, schema: avro.schema.ArraySchema
    ) -> List[SchemaField]:
        fields: List[SchemaField] = []
        # Recurse if needed.
        if self._should_recurse(schema.items):
            # Recursively explore sub-types
            fields.extend(self._to_mce_fields(schema.items))
        return fields

    def _mapschema_to_mce_fields(
        self, schema: avro.schema.MapSchema
    ) -> List[SchemaField]:
        fields: List[SchemaField] = []
        # Process the map schema
        if self._should_recurse(schema.values):
            fields.extend(self._to_mce_fields(schema.values))
        return fields

    def _unionschema_to_mce_fields(
        self, schema: avro.schema.UnionSchema
    ) -> List[SchemaField]:
        fields: List[SchemaField] = []
        # Process the union schemas.
        for sub_schema in schema.schemas:
            # Recursively explore sub-types
            if self._should_recurse(sub_schema):
                fields.extend(self._to_mce_fields(sub_schema))
        return fields

    def _non_recursive_to_mce_fields(
        self, schema: avro.schema.Schema
    ) -> List[SchemaField]:
        fields: List[SchemaField] = []
        # In the non-recursive case, only a single SchemaField will be returned
        # and the fieldPath will be set to empty to signal that the type refers to the
        # the whole object.
        field = SchemaField(
            fieldPath="",
            nativeDataType=str(schema.type),
            type=self._get_column_type(schema.type),
            description=schema.props.get("doc", None),
            recursive=False,
            nullable=self._is_nullable(schema),
        )
        fields.append(field)
        return fields

    def _to_mce_fields(self, avro_schema: avro.schema.Schema) -> List[SchemaField]:
        # Map of avro schema type to the conversion handler
        avro_type_to_mce_converter_map: Dict[
            avro.schema.RecordSchema, Callable[[avro.schema.Schema], List[SchemaField]]
        ] = {
            avro.schema.RecordSchema: self._recordschema_to_mce_fields,
            avro.schema.UnionSchema: self._unionschema_to_mce_fields,
            avro.schema.ArraySchema: self._arrayschema_to_mce_fields,
            avro.schema.MapSchema: self._mapschema_to_mce_fields,
            avro.schema.PrimitiveSchema: self._non_recursive_to_mce_fields,
            avro.schema.FixedSchema: self._non_recursive_to_mce_fields,
            avro.schema.EnumSchema: self._non_recursive_to_mce_fields,
        }
        # Invoke the relevant conversion handler for the schema element type.
        return avro_type_to_mce_converter_map[type(avro_schema)](avro_schema)

    @classmethod
    def to_mce_fields(cls, avro_schema_string: str) -> List[SchemaField]:
        # Prefer the `parse` function over the deprecated `Parse` function.
        avro_schema_parse_fn = getattr(avro.schema, "parse", "Parse")
        avro_schema = avro_schema_parse_fn(avro_schema_string)
        converter = cls()
        return converter._to_mce_fields(avro_schema)


def avro_schema_to_mce_fields(avro_schema_string: str) -> List[SchemaField]:
    """Converts an avro schema into a schema compatible with MCE"""
    return AvroToMceSchemaConverter.to_mce_fields(avro_schema_string)
