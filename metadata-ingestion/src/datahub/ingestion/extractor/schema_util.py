import logging
from typing import Any, Callable, Dict, Generator, List, Optional, Union

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

# ------------------------------------------------------------------------------


class AvroToMceSchemaConverter:
    """Converts an AVRO schema in JSON to MCE SchemaFields."""

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

    def __init__(self, is_key_schema: bool):
        # Initialize the prefix name stack for nested name generation.
        self._prefix_name_stack: List[str] = []
        self._record_types_seen: List[str] = []
        self._skip_emit_next_complex_type_once = False
        if is_key_schema:
            self._prefix_name_stack.append("[key=True]")

    @staticmethod
    def _is_complex_type(schema: avro.schema.Schema) -> bool:
        return isinstance(
            schema,
            (
                avro.schema.RecordSchema,
                avro.schema.UnionSchema,
                avro.schema.ArraySchema,
                avro.schema.MapSchema,
            ),
        )

    def _should_recurse(self, schema: avro.schema.Schema) -> bool:
        return self._is_complex_type(schema) and (
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

    @staticmethod
    def _get_annotation_type(schema: avro.schema.Schema) -> str:
        if isinstance(schema, avro.schema.UnionSchema) and len(schema.schemas) == 2:
            # Optional types as unions in AVRO. Return underlying non-null sub-type.
            (first, second) = schema.schemas
            if first.type == avro.schema.NULL:
                schema = second
            elif second.type == avro.schema.NULL:
                schema = first
        if isinstance(schema, avro.schema.RecordSchema):
            return schema.name
        return schema.type

    def _emit_schema_field(
        self, schema: avro.schema.Schema, description: Optional[str] = None
    ) -> Generator[SchemaField, None, None]:
        if description is None:
            description = schema.props.get("doc", None)
        field = SchemaField(
            fieldPath=self._get_cur_field_path(),
            nativeDataType=str(schema.type),
            type=self._get_column_type(schema.type),
            description=description,
            recursive=False,
            nullable=self._is_nullable(schema),
        )
        yield field

    def _test_and_skip_next_if_optional_as_union(
        self, schema: avro.schema.Schema
    ) -> bool:
        is_optional_as_union = (
            isinstance(schema, avro.schema.UnionSchema)
            and len(schema.schemas) == 2
            and any(s.type == avro.schema.NULL for s in schema.schemas)
        )
        if is_optional_as_union:
            self._skip_emit_next_complex_type_once = True
        return is_optional_as_union

    def _gen_recordschema_to_mce_fields(
        self, schema: avro.schema.RecordSchema
    ) -> Generator[SchemaField, None, None]:
        # Add the full name of this schema to the list of record types seen so far.
        # This will be used to prevent repeated exploration of recursive record type references.
        self._record_types_seen.append(schema.fullname)

        emit_schema_node: bool = True
        if self._skip_emit_next_complex_type_once:
            emit_schema_node = False
            self._skip_emit_next_complex_type_once = False

        if emit_schema_node:
            # process the record itself
            self._prefix_name_stack.append(
                f"[type={AvroToMceSchemaConverter._get_annotation_type(schema)}]"
            )
            yield from self._emit_schema_field(schema)

        # Process each field in the record schema.
        for parsed_field in schema.fields:
            self._prefix_name_stack.append(
                f"[type={AvroToMceSchemaConverter._get_annotation_type(parsed_field.type)}]{parsed_field.name}"
            )
            # Generate the description.
            description: Optional[str] = (
                parsed_field.doc if parsed_field.doc else "No description available."
            )
            if parsed_field.has_default:
                description = (
                    f"{description}\nField default value: {parsed_field.default}"
                )
            yield from self._emit_schema_field(parsed_field.type, description)
            if self._should_recurse(parsed_field.type):
                if self._is_complex_type(parsed_field.type):
                    # We already prepended the next complex type annotation as the field's type.
                    # Prevent it from being emitted.
                    self._skip_emit_next_complex_type_once = True
                # Recursively explore sub-types.
                yield from self._to_mce_fields(parsed_field.type)
            # Remove the name from prefix stack.
            self._prefix_name_stack.pop()

        if emit_schema_node:
            self._prefix_name_stack.pop()

    def _gen_nested_schema_helper(
        self,
        schema: avro.schema.Schema,
        prefix_gen: Callable[[avro.schema.Schema], str],
        sub_schemas: List[avro.schema.Schema],
        sub_item_prefix_gen: Callable[[avro.schema.Schema], str] = None,
        emit_sub_schema: bool = False,
    ) -> Generator[SchemaField, None, None]:
        emit_schema_node: bool = True
        if self._skip_emit_next_complex_type_once:
            emit_schema_node = False
            self._skip_emit_next_complex_type_once = False
        # Append to the field path tokens and emit the SchemaFiled corresponding to the schema itself.
        if emit_schema_node:
            self._prefix_name_stack.append(prefix_gen(schema))
            yield from self._emit_schema_field(schema)
        else:
            assert self._is_complex_type(schema)
        # Emit sub-schemas
        for sub_schema in sub_schemas:
            gen_sub_schema = not self._test_and_skip_next_if_optional_as_union(
                sub_schema
            )
            if sub_item_prefix_gen and gen_sub_schema:
                self._prefix_name_stack.append(sub_item_prefix_gen(sub_schema))
            if emit_sub_schema and gen_sub_schema:
                yield from self._emit_schema_field(sub_schema)
            # Recursively generate from sub-schemas
            if self._should_recurse(sub_schema):
                yield from self._to_mce_fields(sub_schema)
            if sub_item_prefix_gen and gen_sub_schema:
                self._prefix_name_stack.pop()
        if emit_schema_node:
            self._prefix_name_stack.pop()

    def _gen_arrayschema_to_mce_fields(
        self, schema: avro.schema.ArraySchema
    ) -> Generator[SchemaField, None, None]:
        """Generates SchemaFields from array schema."""

        def type_prefix_gen(x: avro.schema.ArraySchema) -> str:
            return f"[type={AvroToMceSchemaConverter._get_annotation_type(x)}]"

        sub_items = schema.items if isinstance(schema.items, list) else [schema.items]
        yield from self._gen_nested_schema_helper(schema, type_prefix_gen, sub_items)

    def _gen_mapschema_to_mce_fields(
        self, schema: avro.schema.MapSchema
    ) -> Generator[SchemaField, None, None]:
        """Generates SchemaFields from map schema."""

        def type_prefix_gen(x: avro.schema.MapSchema) -> str:
            return f"[type={AvroToMceSchemaConverter._get_annotation_type(x)}]"

        sub_items = (
            schema.values if isinstance(schema.values, list) else [schema.values]
        )
        yield from self._gen_nested_schema_helper(schema, type_prefix_gen, sub_items)

    def _gen_unionschema_to_mce_fields(
        self, schema: avro.schema.UnionSchema
    ) -> Generator[SchemaField, None, None]:
        # Process the union schemas.
        def prefix_gen(x: avro.schema.UnionSchema) -> str:
            return f"[type={AvroToMceSchemaConverter._get_annotation_type(x)}]"

        self._test_and_skip_next_if_optional_as_union(schema)
        yield from self._gen_nested_schema_helper(schema, prefix_gen, schema.schemas)

    def _gen_non_recursive_to_mce_fields(
        self, schema: avro.schema.Schema
    ) -> Generator[SchemaField, None, None]:
        # In the non-recursive case, only a single SchemaField will be returned
        # and the fieldPath will be set to empty to signal that the type refers to the
        # the whole object.
        yield from self._emit_schema_field(schema)

    def _to_mce_fields(
        self, avro_schema: avro.schema.Schema
    ) -> Generator[SchemaField, None, None]:
        # Map of avro schema type to the conversion handler
        avro_type_to_mce_converter_map: Dict[
            avro.schema.Schema,
            Callable[[avro.schema.Schema], Generator[SchemaField, None, None]],
        ] = {
            avro.schema.RecordSchema: self._gen_recordschema_to_mce_fields,
            avro.schema.UnionSchema: self._gen_unionschema_to_mce_fields,
            avro.schema.ArraySchema: self._gen_arrayschema_to_mce_fields,
            avro.schema.MapSchema: self._gen_mapschema_to_mce_fields,
            avro.schema.PrimitiveSchema: self._gen_non_recursive_to_mce_fields,
            avro.schema.FixedSchema: self._gen_non_recursive_to_mce_fields,
            avro.schema.EnumSchema: self._gen_non_recursive_to_mce_fields,
        }
        # Invoke the relevant conversion handler for the schema element type.
        yield from avro_type_to_mce_converter_map[type(avro_schema)](avro_schema)

    @classmethod
    def to_mce_fields(
        cls, avro_schema_string: str, is_key_schema: bool
    ) -> Generator[SchemaField, None, None]:
        # Prefer the `parse` function over the deprecated `Parse` function.
        avro_schema_parse_fn = getattr(avro.schema, "parse", "Parse")
        avro_schema = avro_schema_parse_fn(avro_schema_string)
        converter = cls(is_key_schema)
        yield from converter._to_mce_fields(avro_schema)


def avro_schema_to_mce_fields(
    avro_schema_string: str, is_key_schema: bool = False
) -> List[SchemaField]:
    """Converts an avro schema into a schema compatible with MCE"""
    return list(
        AvroToMceSchemaConverter.to_mce_fields(avro_schema_string, is_key_schema)
    )
