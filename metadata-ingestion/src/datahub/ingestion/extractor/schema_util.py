import json
import logging
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Type,
    Union,
    cast,
    overload,
)

import avro.schema

from datahub.emitter import mce_builder
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    EnumTypeClass,
    FixedTypeClass,
    MapTypeClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaField,
    SchemaFieldDataType,
    StringTypeClass,
    TimeTypeClass,
    UnionTypeClass,
)
from datahub.utilities.mapping import Constants, OperationProcessor

"""A helper file for Avro schema -> MCE schema transformations"""

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# Type aliases

PrefixNameStack = List[str]

AvroNestedSchemas = Union[
    avro.schema.RecordSchema,
    avro.schema.UnionSchema,
    avro.schema.ArraySchema,
    avro.schema.MapSchema,
]

ExtendedAvroNestedSchemas = Union[
    avro.schema.RecordSchema,
    avro.schema.UnionSchema,
    avro.schema.ArraySchema,
    avro.schema.MapSchema,
    avro.schema.Field,
]

AvroNonNestedSchemas = Union[
    avro.schema.EnumSchema,
    avro.schema.FixedSchema,
    avro.schema.PrimitiveSchema,
]

SchemaOrField = Union[avro.schema.Schema, avro.schema.Field]

FieldStack = List[avro.schema.Field]

# The latest avro code contains this type definition in a compatibility module,
# but that has not yet been released to PyPI. In the interim, we define it ourselves.
# https://github.com/apache/avro/blob/e5811b404ac01fac0d0d6e223d62441554c9cbe9/lang/py/avro/compatibility.py#L48
AVRO_TYPE_NULL = "null"

# ------------------------------------------------------------------------------
# AvroToMceSchemaConverter


class AvroToMceSchemaConverter:
    """Converts an AVRO schema in JSON to MCE SchemaFields."""

    # FieldPath format version.
    version_string: str = "[version=2.0]"

    field_type_mapping: Dict[str, Type] = {
        AVRO_TYPE_NULL: NullTypeClass,
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

    field_logical_type_mapping: Dict[str, Type] = {
        "date": DateTypeClass,
        "decimal": NumberTypeClass,
        "time-micros": TimeTypeClass,
        "time-millis": TimeTypeClass,
        "timestamp-micros": TimeTypeClass,
        "timestamp-millis": TimeTypeClass,
        "uuid": StringTypeClass,
    }

    def __init__(
        self,
        is_key_schema: bool,
        default_nullable: bool = False,
        meta_mapping_processor: Optional[OperationProcessor] = None,
        schema_tags_field: Optional[str] = None,
        tag_prefix: Optional[str] = None,
    ) -> None:
        # Tracks the prefix name stack for nested name generation.
        self._prefix_name_stack: PrefixNameStack = [self.version_string]
        # Tracks the fields on the current path.
        self._fields_stack: FieldStack = []
        # Tracks the record types seen so far. Used to prevent infinite recursion with recursive types.
        self._record_types_seen: List[str] = []
        # If part of the key-schema or value-schema.
        self._is_key_schema = is_key_schema
        # Default value of nullable for non-null schema.
        self.default_nullable = default_nullable
        if is_key_schema:
            # Helps maintain backwards-compatibility. Annotation for any field that is part of key-schema.
            self._prefix_name_stack.append("[key=True]")
        # Meta mapping
        self._meta_mapping_processor = meta_mapping_processor
        self._schema_tags_field = schema_tags_field
        self._tag_prefix = tag_prefix

        # Map of avro schema type to the conversion handler
        # TODO: Clean up this type... perhaps refactor
        self._avro_type_to_mce_converter_map: Mapping[
            Union[
                Type[avro.schema.Schema],
                Type[avro.schema.Field],
                Type[avro.schema.LogicalSchema],
            ],
            Callable[[SchemaOrField], Iterable[SchemaField]],
        ] = {
            avro.schema.RecordSchema: self._gen_from_non_field_nested_schemas,
            avro.schema.UnionSchema: self._gen_from_non_field_nested_schemas,
            avro.schema.ArraySchema: self._gen_from_non_field_nested_schemas,
            avro.schema.MapSchema: self._gen_from_non_field_nested_schemas,
            avro.schema.Field: self._gen_nested_schema_from_field,  # type: ignore
            avro.schema.PrimitiveSchema: self._gen_non_nested_to_mce_fields,
            avro.schema.FixedSchema: self._gen_non_nested_to_mce_fields,
            avro.schema.EnumSchema: self._gen_non_nested_to_mce_fields,
            avro.schema.LogicalSchema: self._gen_non_nested_to_mce_fields,
        }

    @staticmethod
    def _get_type_name(
        avro_schema: SchemaOrField, logical_if_present: bool = False
    ) -> str:
        logical_type_name: Optional[str] = None
        if logical_if_present:
            logical_type_name = cast(
                Optional[str],
                getattr(avro_schema, "logical_type", None)
                or avro_schema.props.get("logicalType"),
            )
        return logical_type_name or str(
            getattr(avro_schema.type, "type", avro_schema.type)
        )

    @staticmethod
    def _get_column_type(
        avro_schema: SchemaOrField, logical_type: Optional[str]
    ) -> SchemaFieldDataType:
        type_name: str = AvroToMceSchemaConverter._get_type_name(avro_schema)
        TypeClass: Optional[Type] = AvroToMceSchemaConverter.field_type_mapping.get(
            type_name
        )
        if logical_type is not None:
            TypeClass = AvroToMceSchemaConverter.field_logical_type_mapping.get(
                logical_type, TypeClass
            )
        assert TypeClass is not None
        dt = SchemaFieldDataType(type=TypeClass())
        # Handle Arrays and Maps
        if isinstance(dt.type, ArrayTypeClass) and isinstance(
            avro_schema, avro.schema.ArraySchema
        ):
            dt.type.nestedType = [
                AvroToMceSchemaConverter._get_type_name(
                    avro_schema.items, logical_if_present=True
                )
            ]
        elif isinstance(dt.type, MapTypeClass) and isinstance(
            avro_schema, avro.schema.MapSchema
        ):
            # Avro map's key is always a string. See: https://avro.apache.org/docs/current/spec.html#Maps
            dt.type.keyType = "string"
            dt.type.valueType = AvroToMceSchemaConverter._get_type_name(
                avro_schema.values, logical_if_present=True
            )
        return dt

    def _is_nullable(self, schema: SchemaOrField) -> bool:
        if isinstance(schema, avro.schema.Field):
            return self._is_nullable(schema.type)
        if isinstance(schema, avro.schema.UnionSchema):
            return any(self._is_nullable(sub_schema) for sub_schema in schema.schemas)
        if (
            isinstance(schema, avro.schema.PrimitiveSchema)
            and schema.type == AVRO_TYPE_NULL
        ):
            return True
        if isinstance(schema.props, dict):
            return schema.props.get("_nullable", self.default_nullable)
        return self.default_nullable

    def _get_cur_field_path(self) -> str:
        return ".".join(self._prefix_name_stack)

    @staticmethod
    def _strip_namespace(name_or_fullname: str) -> str:
        return name_or_fullname.rsplit(".", maxsplit=1)[-1]

    @staticmethod
    def _get_simple_native_type(schema: SchemaOrField) -> str:
        if isinstance(schema, (avro.schema.RecordSchema, avro.schema.Field)):
            # For Records, fields, always return the name.
            return AvroToMceSchemaConverter._strip_namespace(schema.name)

        # For optional, use the underlying non-null type
        if isinstance(schema, avro.schema.UnionSchema) and len(schema.schemas) == 2:
            # Optional types as unions in AVRO. Return underlying non-null sub-type.
            (first, second) = schema.schemas
            if first.type == AVRO_TYPE_NULL:
                return second.type
            elif second.type == AVRO_TYPE_NULL:
                return first.type

        # For everything else, use the schema's type
        return schema.type

    @staticmethod
    def _get_type_annotation(schema: SchemaOrField) -> str:
        simple_native_type = AvroToMceSchemaConverter._get_simple_native_type(schema)
        if simple_native_type.startswith("__struct_"):
            simple_native_type = "struct"
        elif simple_native_type.startswith("__structn_"):
            simple_native_type = "struct{}".format(simple_native_type.split("_")[3])
        if isinstance(schema, avro.schema.Field):
            return simple_native_type
        else:
            return f"[type={simple_native_type}]"

    @staticmethod
    @overload
    def _get_underlying_type_if_option_as_union(
        schema: SchemaOrField, default: SchemaOrField
    ) -> SchemaOrField: ...

    @staticmethod
    @overload
    def _get_underlying_type_if_option_as_union(
        schema: SchemaOrField, default: Optional[SchemaOrField] = None
    ) -> Optional[SchemaOrField]: ...

    @staticmethod
    def _get_underlying_type_if_option_as_union(
        schema: SchemaOrField, default: Optional[SchemaOrField] = None
    ) -> Optional[SchemaOrField]:
        if isinstance(schema, avro.schema.UnionSchema) and len(schema.schemas) == 2:
            (first, second) = schema.schemas
            if first.type == AVRO_TYPE_NULL:
                return second
            elif second.type == AVRO_TYPE_NULL:
                return first
        return default

    class SchemaFieldEmissionContextManager:
        """Context Manager for MCE SchemaFiled emission
        - handles prefix name stack management and AVRO record-field generation for non-complex types.
        - actual_schema contains the underlying no-null type's schema if the schema is a union
          This way we can use the type/description of the non-null type if needed.
        """

        # props to skip when building jsonProps
        json_props_to_skip = [
            "_nullable",
            "native_data_type",
        ]

        def __init__(
            self,
            schema: SchemaOrField,
            actual_schema: SchemaOrField,
            converter: "AvroToMceSchemaConverter",
            description: Optional[str] = None,
            default_value: Optional[str] = None,
        ):
            self._schema = schema
            self._actual_schema = actual_schema
            self._converter = converter
            self._description = description
            self._default_value = default_value

        def __enter__(self):
            type_annotation = self._converter._get_type_annotation(self._actual_schema)
            self._converter._prefix_name_stack.append(type_annotation)
            return self

        def emit(self) -> Iterable[SchemaField]:
            if (
                not isinstance(
                    self._actual_schema,
                    (
                        avro.schema.ArraySchema,
                        avro.schema.Field,
                        avro.schema.MapSchema,
                        avro.schema.RecordSchema,
                    ),
                )
                and self._converter._fields_stack
            ):
                # We are in the context of a non-nested(simple) field or the special-cased union.
                yield from self._converter._gen_from_last_field()
            else:
                # Just emit the SchemaField from schema provided in the Ctor.

                schema = self._schema
                actual_schema = self._actual_schema

                if isinstance(schema, avro.schema.Field):
                    # Field's schema is actually it's type.
                    schema = schema.type
                    actual_schema = (
                        self._converter._get_underlying_type_if_option_as_union(
                            schema, schema
                        )
                    )

                description = self._description
                if not description and actual_schema.props.get("doc"):
                    description = cast(Optional[str], actual_schema.props.get("doc"))

                if self._default_value is not None:
                    description = f"{description if description else ''}\nField default value: {self._default_value}"

                native_data_type = self._converter._prefix_name_stack[-1]
                if isinstance(schema, (avro.schema.Field, avro.schema.UnionSchema)):
                    native_data_type = self._converter._prefix_name_stack[-2]
                type_prefix = "[type="
                if native_data_type.startswith(type_prefix):
                    native_data_type = native_data_type[
                        slice(len(type_prefix), len(native_data_type) - 1)
                    ]
                native_data_type = cast(
                    str, actual_schema.props.get("native_data_type", native_data_type)
                )

                field_path = self._converter._get_cur_field_path()
                merged_props: Dict[str, Any] = {}
                merged_props.update(self._schema.other_props)
                merged_props.update(schema.other_props)
                merged_props.update(actual_schema.other_props)

                # Parse meta_mapping
                meta_aspects: Dict[str, Any] = {}
                if self._converter._meta_mapping_processor:
                    meta_aspects = self._converter._meta_mapping_processor.process(
                        merged_props
                    )

                tags: List[str] = []
                if self._converter._schema_tags_field:
                    for tag in merged_props.get(self._converter._schema_tags_field, []):
                        tags.append(self._converter._tag_prefix + tag)

                meta_tags_aspect = meta_aspects.get(Constants.ADD_TAG_OPERATION)
                if meta_tags_aspect:
                    tags += [
                        tag_association.tag[len("urn:li:tag:") :]
                        for tag_association in meta_tags_aspect.tags
                    ]

                if "deprecated" in merged_props:
                    description = (
                        f'<span style="color:red">DEPRECATED: {merged_props["deprecated"]}</span>\n'
                        + description
                        if description
                        else ""
                    )
                    tags.append("Deprecated")

                tags_aspect = None
                if tags:
                    tags_aspect = mce_builder.make_global_tag_aspect_with_tag_list(tags)

                meta_terms_aspect = meta_aspects.get(Constants.ADD_TERM_OPERATION)

                logical_type_name: Optional[str] = cast(
                    Optional[str],
                    # logicalType nested inside type
                    getattr(actual_schema, "logical_type", None)
                    or actual_schema.props.get("logicalType")
                    # bare logicalType
                    or self._actual_schema.props.get("logicalType"),
                )

                json_props: Optional[Dict[str, Any]] = (
                    {
                        k: v
                        for k, v in merged_props.items()
                        if k not in self.json_props_to_skip
                    }
                    if merged_props
                    else None
                )

                field = SchemaField(
                    fieldPath=field_path,
                    # Populate it with the simple native type for now.
                    nativeDataType=native_data_type,
                    type=self._converter._get_column_type(
                        actual_schema,
                        logical_type_name,
                    ),
                    description=description,
                    recursive=False,
                    nullable=self._converter._is_nullable(schema),
                    isPartOfKey=self._converter._is_key_schema,
                    globalTags=tags_aspect,
                    glossaryTerms=meta_terms_aspect,
                    jsonProps=json.dumps(json_props) if json_props else None,
                )
                yield field

        def __exit__(self, exc_type, exc_val, exc_tb):
            self._converter._prefix_name_stack.pop()

    def _get_sub_schemas(self, schema: SchemaOrField) -> Iterable[SchemaOrField]:
        """Responsible for generation for appropriate sub-schemas for every nested AVRO type."""

        def gen_items_from_list_tuple_or_scalar(
            val: Any,
        ) -> Iterable[avro.schema.Schema]:
            if isinstance(val, (list, tuple)):
                yield from val
            else:
                yield val

        # Array type
        if isinstance(schema, avro.schema.ArraySchema):
            yield from gen_items_from_list_tuple_or_scalar(schema.items)
        # Map type
        elif isinstance(schema, avro.schema.MapSchema):
            yield from gen_items_from_list_tuple_or_scalar(schema.values)
        # Union type
        elif isinstance(schema, avro.schema.UnionSchema):
            is_option_as_union_type = self._get_underlying_type_if_option_as_union(
                schema
            )
            if is_option_as_union_type is not None:
                yield is_option_as_union_type
            else:
                for sub_schema in schema.schemas:
                    if sub_schema.type != AVRO_TYPE_NULL:
                        yield sub_schema
        # Record type
        elif isinstance(schema, avro.schema.RecordSchema):
            yield from gen_items_from_list_tuple_or_scalar(schema.fields)
        # Field type
        elif isinstance(schema, avro.schema.Field):
            yield schema.type

    def _gen_nested_schema_from_field(
        self,
        field: avro.schema.Field,
    ) -> Iterable[SchemaField]:
        """Handles generation of MCE SchemaFields for an AVRO Field type."""
        # NOTE: Here we only manage the field stack and trigger MCE Field generation from this field's type.
        # The actual emitting of a field happens when
        #  (a) another nested record is encountered or
        #  (b) a non-nested type has been reached or
        #  (c) during the special-casing for unions.
        self._fields_stack.append(field)
        for sub_schema in self._get_sub_schemas(field):
            yield from self._to_mce_fields(sub_schema)
        self._fields_stack.pop()

    def _gen_from_last_field(
        self, schema_to_recurse: Optional[AvroNestedSchemas] = None
    ) -> Iterable[SchemaField]:
        """Emits the field most-recent field, optionally triggering sub-schema generation under the field."""
        last_field_schema = self._fields_stack[-1]
        # Generate the custom-description for the field.
        description = last_field_schema.doc if last_field_schema.doc else None
        with AvroToMceSchemaConverter.SchemaFieldEmissionContextManager(
            last_field_schema,
            last_field_schema,
            self,
            description,
            last_field_schema.default,
        ) as f_emit:
            yield from f_emit.emit()

            if schema_to_recurse is not None:
                # Generate the nested sub-schemas under the most-recent field.
                for sub_schema in self._get_sub_schemas(schema_to_recurse):
                    yield from self._to_mce_fields(sub_schema)

    def _gen_from_non_field_nested_schemas(
        self, schema: SchemaOrField
    ) -> Iterable[SchemaField]:
        """Handles generation of MCE SchemaFields for all standard AVRO nested types."""
        # Handle recursive record definitions
        recurse: bool = True
        if isinstance(schema, avro.schema.RecordSchema):
            if schema.fullname not in self._record_types_seen:
                self._record_types_seen.append(schema.fullname)
            else:
                recurse = False

        # Adjust actual schema if needed
        actual_schema = self._get_underlying_type_if_option_as_union(schema, schema)

        with AvroToMceSchemaConverter.SchemaFieldEmissionContextManager(
            schema,
            actual_schema,
            self,
        ) as fe_schema:
            if isinstance(
                actual_schema,
                (
                    avro.schema.UnionSchema,
                    avro.schema.PrimitiveSchema,
                    avro.schema.FixedSchema,
                    avro.schema.EnumSchema,
                ),
            ):
                # Emit non-AVRO field complex schemas(even optional unions that become primitives) and special-casing for extra union emission.
                yield from fe_schema.emit()

            if (
                isinstance(actual_schema, avro.schema.RecordSchema)
                and self._fields_stack
            ):
                # We have encountered a nested record, emit the most-recently seen field.
                yield from self._gen_from_last_field(actual_schema if recurse else None)
            else:
                # We are not yet in the context of any field. Generate all nested sub-schemas under the complex type.
                if recurse:
                    for sub_schema in self._get_sub_schemas(actual_schema):
                        yield from self._to_mce_fields(sub_schema)

    def _gen_non_nested_to_mce_fields(
        self, schema: SchemaOrField
    ) -> Iterable[SchemaField]:
        """Handles generation of MCE SchemaFields for non-nested AVRO types."""
        with AvroToMceSchemaConverter.SchemaFieldEmissionContextManager(
            schema,
            schema,
            self,
        ) as non_nested_emitter:
            yield from non_nested_emitter.emit()

    def _to_mce_fields(self, avro_schema: SchemaOrField) -> Iterable[SchemaField]:
        # Invoke the relevant conversion handler for the schema element type.
        schema_type = (
            type(avro_schema)
            if not isinstance(avro_schema, avro.schema.LogicalSchema)
            else avro.schema.LogicalSchema
        )
        yield from self._avro_type_to_mce_converter_map[schema_type](avro_schema)

    @classmethod
    def to_mce_fields(
        cls,
        avro_schema: avro.schema.Schema,
        is_key_schema: bool,
        default_nullable: bool = False,
        meta_mapping_processor: Optional[OperationProcessor] = None,
        schema_tags_field: Optional[str] = None,
        tag_prefix: Optional[str] = None,
    ) -> Iterable[SchemaField]:
        """
        Converts a key or value type AVRO schema string to appropriate MCE SchemaFields.
        :param avro_schema_string: String representation of the AVRO schema.
        :param is_key_schema: True if it is a key-schema.
        :return: An MCE SchemaField generator.
        """
        # avro_schema = avro.schema.parse(avro_schema)
        converter = cls(
            is_key_schema,
            default_nullable,
            meta_mapping_processor,
            schema_tags_field,
            tag_prefix,
        )
        yield from converter._to_mce_fields(avro_schema)


# ------------------------------------------------------------------------------
#  API


def avro_schema_to_mce_fields(
    avro_schema: Union[avro.schema.Schema, str],
    is_key_schema: bool = False,
    default_nullable: bool = False,
    meta_mapping_processor: Optional[OperationProcessor] = None,
    schema_tags_field: Optional[str] = None,
    tag_prefix: Optional[str] = None,
    swallow_exceptions: bool = True,
) -> List[SchemaField]:
    """
    Converts an avro schema into schema fields compatible with MCE.
    :param avro_schema: AVRO schema, either as a string or as an avro.schema.Schema object.
    :param is_key_schema: True if it is a key-schema. Default is False (value-schema).
    :param swallow_exceptions: True if the caller wants exceptions to be suppressed
    :param action_processor: Optional OperationProcessor to be used for meta mappings
    :return: The list of MCE compatible SchemaFields.
    """

    try:
        if isinstance(avro_schema, str):
            avro_schema = avro.schema.parse(avro_schema)

        return list(
            AvroToMceSchemaConverter.to_mce_fields(
                avro_schema,
                is_key_schema,
                default_nullable,
                meta_mapping_processor,
                schema_tags_field,
                tag_prefix,
            )
        )
    except Exception:
        if swallow_exceptions:
            logger.exception(f"Failed to parse {avro_schema} into mce fields.")
            return []
        else:
            raise
