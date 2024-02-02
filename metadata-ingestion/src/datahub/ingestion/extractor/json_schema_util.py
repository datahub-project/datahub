import json
import logging
import unittest.mock
from hashlib import md5
from typing import Any, Callable, Dict, Iterable, List, Optional, Type

import jsonref
import jsonschema
from pydantic import Field
from pydantic.dataclasses import dataclass

from datahub.ingestion.extractor.json_ref_patch import title_swapping_callback
from datahub.metadata.com.linkedin.pegasus2avro.schema import OtherSchema
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    EnumTypeClass,
    FixedTypeClass,
    MapTypeClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaFieldClass as SchemaField,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass as SchemaMetadata,
    StringTypeClass,
    UnionTypeClass,
)

logger = logging.getLogger(__name__)


@dataclass
class DataHubType:
    type: Type
    nested_type: Optional[str] = None

    def as_schema_field_type(self) -> SchemaFieldDataTypeClass:
        if self.type == UnionTypeClass:
            return SchemaFieldDataTypeClass(
                type=UnionTypeClass(
                    nestedTypes=[self.nested_type] if self.nested_type else None
                )
            )
        elif self.type == ArrayTypeClass:
            return SchemaFieldDataTypeClass(
                type=ArrayTypeClass(
                    nestedType=[self.nested_type] if self.nested_type else None
                )
            )
        elif self.type == MapTypeClass:
            return SchemaFieldDataTypeClass(
                type=MapTypeClass(keyType="string", valueType=self.nested_type)
            )
        raise Exception(f"Unexpected type {self.type}")


@dataclass
class FieldElement:
    type: List[str]
    schema_types: List[str]
    name: Optional[str] = None
    parent_type: Optional[DataHubType] = None

    def clone(self) -> "FieldElement":
        """Return a copy of this element"""
        new_element = FieldElement(
            name=self.name,
            type=[t for t in self.type],
            schema_types=[t for t in self.schema_types],
            parent_type=self.parent_type,
        )
        return new_element

    def as_string(self, v2_format: bool = True) -> str:
        if v2_format:
            type_prefix = ".".join([f"[type={inner_type}]" for inner_type in self.type])
            if self.name:
                return f"{type_prefix}.{self.name}"
            else:
                return f"{type_prefix}"
        else:
            return self.name or ""


@dataclass
class FieldPath:
    EMPTY_FIELD_NAME = " "
    path: List[FieldElement] = Field(default_factory=list)
    is_key_schema: bool = False
    use_v2_paths_always: bool = True

    def _needs_v2_path(self) -> bool:
        if self.use_v2_paths_always:
            return True
        if self.is_key_schema:
            return True
        for element in self.path:
            for t in element.type:
                if t in ["union", "array"]:
                    return True
        return False

    def _set_parent_type_if_not_exists(self, parent_type: DataHubType) -> None:
        """Assigns a parent type if one does not already exist"""
        assert self.path
        if self.path[-1].parent_type is None:
            self.path[-1].parent_type = parent_type

    def _get_type_override(self) -> Optional[SchemaFieldDataTypeClass]:
        if self.path and self.path[-1].parent_type:
            return self.path[-1].parent_type.as_schema_field_type()
        else:
            return None

    def _get_native_type_override(self) -> Optional[str]:
        type_override = self._get_type_override()
        if type_override:
            # return type specific native types for collection types
            if isinstance(type_override.type, ArrayTypeClass):
                return f"array({','.join(type_override.type.nestedType or [])})"
            elif isinstance(type_override.type, MapTypeClass):
                return f"map(str,{type_override.type.valueType})"
        return None

    def get_recursive(self, schema: Dict) -> Optional[str]:
        """Return a recursive type if found"""
        schema_str = str(schema)
        for p in self.path:
            for i, schema_type in enumerate(p.schema_types):
                if schema_type == schema_str:
                    # return the corresponding type for the schema that's a match
                    assert (
                        len(p.type) > i
                    ), f"p.type({len(p.type)})) and p.schema_types({len(p.schema_types)}) should have the same length"
                    return p.type[i]
        return None

    def clone_plus(self, element: FieldElement) -> "FieldPath":
        """Add a new element to a path and return a new fieldpath"""
        fpath = FieldPath(is_key_schema=self.is_key_schema)
        fpath.path = [x for x in self.path]
        fpath.path.append(element)
        return fpath

    def expand_type(self, type: str, type_schema: Dict) -> "FieldPath":
        """Expand the type of the last element of the path and return a new fieldpath"""
        fpath = FieldPath(is_key_schema=self.is_key_schema)
        fpath.path = [x.clone() for x in self.path]
        if fpath.path:
            fpath.path[-1].type.append(type)
            fpath.path[-1].schema_types.append(str(type_schema))
        else:
            fpath.path = [FieldElement(type=[type], schema_types=[str(type_schema)])]
        return fpath

    def has_field_name(self) -> bool:
        names = [f.name for f in self.path if f.name is not None]
        return len(names) > 0

    def ensure_field_name(self) -> bool:
        if not self.has_field_name():
            if not self.path:
                self.path = [FieldElement(type=[], schema_types=[])]
            self.path[-1].name = FieldPath.EMPTY_FIELD_NAME
        return True

    def as_string(self) -> str:
        v2_format = self._needs_v2_path()
        if v2_format:
            prefix = ["[version=2.0]"]
            if self.is_key_schema:
                prefix.append("[key=True]")
        else:
            prefix = []
        if self.path:
            return ".".join(
                prefix + [f.as_string(v2_format=v2_format) for f in self.path]
            )
        else:
            # this is a non-field (top-level) schema
            return ".".join(prefix)


class JsonSchemaTranslator:
    _INJECT_DEFAULTS_INTO_DESCRIPTION = True

    field_type_mapping: Dict[str, Type] = {
        "null": NullTypeClass,
        "bool": BooleanTypeClass,
        "boolean": BooleanTypeClass,
        "number": NumberTypeClass,
        "int": NumberTypeClass,
        "integer": NumberTypeClass,
        "long": NumberTypeClass,
        "float": NumberTypeClass,
        "double": NumberTypeClass,
        "bytes": BytesTypeClass,
        "string": StringTypeClass,
        "object": RecordTypeClass,
        "map": MapTypeClass,
        "enum": EnumTypeClass,
        "array": ArrayTypeClass,
        "union": UnionTypeClass,
        "fixed": FixedTypeClass,
    }

    @staticmethod
    def _is_nullable(schema: Dict) -> bool:
        if "type" in schema and isinstance(schema["type"], list):
            return "null" in schema["type"]
        return False

    @classmethod
    def _field_from_primitive(
        cls,
        datahub_field_type: Type,
        field_path: FieldPath,
        schema: Dict,
        required: bool = False,
        specific_type: Optional[str] = None,
    ) -> Iterable[SchemaField]:
        type_override = field_path._get_type_override()
        native_type = field_path._get_native_type_override() or str(
            schema.get("type") or ""
        )
        nullable = JsonSchemaTranslator._is_nullable(schema)
        if "format" in schema:
            native_type = f"{native_type}({schema['format']})"
        if datahub_field_type in [
            BooleanTypeClass,
            NumberTypeClass,
            StringTypeClass,
            NullTypeClass,
        ]:
            discriminated_type = (
                specific_type
                or JsonSchemaTranslator._get_discriminated_type_from_schema(schema)
            )
            primitive_path = field_path.expand_type(discriminated_type, schema)
            yield SchemaField(
                fieldPath=primitive_path.as_string(),
                type=type_override
                or SchemaFieldDataTypeClass(type=datahub_field_type()),
                description=JsonSchemaTranslator._get_description_from_any_schema(
                    schema
                ),
                nativeDataType=native_type,
                nullable=nullable,
                jsonProps=JsonSchemaTranslator._get_jsonprops_for_any_schema(
                    schema, required=required
                ),
                isPartOfKey=field_path.is_key_schema,
            )
        elif datahub_field_type in [EnumTypeClass]:
            # Convert enums to string representation
            schema_enums = list(map(json.dumps, schema["enum"]))
            yield SchemaField(
                fieldPath=field_path.expand_type("enum", schema).as_string(),
                type=type_override or SchemaFieldDataTypeClass(type=EnumTypeClass()),
                nativeDataType="Enum",
                description=f"One of: {', '.join(schema_enums)}",
                nullable=nullable,
                jsonProps=JsonSchemaTranslator._get_jsonprops_for_any_schema(
                    schema, required=required
                ),
                isPartOfKey=field_path.is_key_schema,
            )
        else:
            logger.error(f"No entry for {datahub_field_type}")
            raise Exception(f"No entry for {datahub_field_type}")
        pass

    @staticmethod
    def _get_type_from_schema(schema: Dict) -> str:
        """Returns a generic json type from a schema."""
        if Ellipsis in schema:
            return "object"
        if "oneOf" in schema or "anyOf" in schema or "allOf" in schema:
            return "union"
        elif "enum" in schema:
            return "enum"
        elif "type" in schema:
            if isinstance(schema["type"], list):
                # we have an array of types
                # if only one type, short-circuit
                if len(schema["type"]) == 1:
                    return schema["type"][0]
                # if this is a union with null, short-circuit
                elif len(schema["type"]) == 2 and "null" in schema["type"]:
                    return [t for t in schema["type"] if t != "null"][0]
                else:
                    return "union"
            elif schema["type"] != "object":
                return schema["type"]
            elif "additionalProperties" in schema and isinstance(
                schema["additionalProperties"], dict
            ):
                return "map"

        return "object"

    @staticmethod
    def _get_discriminated_type_from_schema(schema: Dict) -> str:
        """Returns a discriminated (specific) type from a schema. Use this for constructing field paths."""
        generic_type = JsonSchemaTranslator._get_type_from_schema(schema)
        if generic_type == "object" and "javaType" in schema:
            return schema["javaType"].split(".")[-1]
        if generic_type == "object" and "title" in schema and "properties" in schema:
            return schema["title"]
        if "format" in schema:
            return f"{generic_type}({schema['format']})"

        return generic_type

    @staticmethod
    def _get_description_from_any_schema(schema: Dict) -> str:
        description = ""
        if "description" in schema:
            description = str(schema.get("description"))
        elif "const" in schema:
            schema_const = schema.get("const")
            description = f"Const value: {schema_const}"
        if JsonSchemaTranslator._INJECT_DEFAULTS_INTO_DESCRIPTION:
            default = schema.get("default")
            if default is not None:
                description = f"{description}\nField default value: {default}"
        return description

    @staticmethod
    def _get_jsonprops_for_any_schema(
        schema: Dict, required: Optional[bool] = None
    ) -> Optional[str]:
        json_props = {}
        if "default" in schema:
            json_props["default"] = schema["default"]
        if required is not None:
            json_props["required"] = required

        return json.dumps(json_props) if json_props else None

    @classmethod
    def _field_from_complex_type(
        cls,
        datahub_field_type: Type,
        field_path: FieldPath,
        schema: Dict,
        required: bool = False,
        specific_type: Optional[str] = None,
    ) -> Iterable[SchemaField]:
        discriminated_type = (
            specific_type
            or JsonSchemaTranslator._get_discriminated_type_from_schema(schema)
        )
        type_override = field_path._get_type_override()
        native_type_override = field_path._get_native_type_override()
        nullable = JsonSchemaTranslator._is_nullable(schema)

        if Ellipsis in schema:
            # This happens in the case of recursive fields, we short-circuit by making this just be an object
            schema = {}

        if datahub_field_type == RecordTypeClass:
            # have we seen this schema before?
            recursive_type = field_path.get_recursive(schema)
            if recursive_type:
                yield SchemaField(
                    fieldPath=field_path.expand_type(
                        recursive_type, schema
                    ).as_string(),
                    nativeDataType=native_type_override or recursive_type,
                    type=type_override
                    or SchemaFieldDataTypeClass(type=RecordTypeClass()),
                    nullable=nullable,
                    description=JsonSchemaTranslator._get_description_from_any_schema(
                        schema
                    ),
                    jsonProps=JsonSchemaTranslator._get_jsonprops_for_any_schema(
                        schema, required
                    ),
                    isPartOfKey=field_path.is_key_schema,
                    recursive=True,
                )
                return  # important to break the traversal here
            if field_path.has_field_name():
                # generate a field for the struct if we have a field path
                yield SchemaField(
                    fieldPath=field_path.expand_type(
                        discriminated_type, schema
                    ).as_string(),
                    nativeDataType=native_type_override
                    or JsonSchemaTranslator._get_discriminated_type_from_schema(schema),
                    type=type_override
                    or SchemaFieldDataTypeClass(type=RecordTypeClass()),
                    nullable=nullable,
                    description=JsonSchemaTranslator._get_description_from_any_schema(
                        schema
                    ),
                    jsonProps=JsonSchemaTranslator._get_jsonprops_for_any_schema(
                        schema, required
                    ),
                    isPartOfKey=field_path.is_key_schema,
                )

            field_path = field_path.expand_type(discriminated_type, schema)

            for field_name, field_schema in schema.get("properties", {}).items():
                required_field: bool = field_name in schema.get("required", [])
                inner_field_path = field_path.clone_plus(
                    FieldElement(type=[], name=field_name, schema_types=[])
                )
                yield from JsonSchemaTranslator.get_fields(
                    JsonSchemaTranslator._get_type_from_schema(field_schema),
                    field_schema,
                    required_field,
                    inner_field_path,
                )
        elif datahub_field_type == ArrayTypeClass:
            field_path = field_path.expand_type("array", schema)
            # default items schema is string
            items_schema = schema.get("items", {"type": "string"})
            items_type = JsonSchemaTranslator._get_type_from_schema(items_schema)
            field_path._set_parent_type_if_not_exists(
                DataHubType(type=ArrayTypeClass, nested_type=items_type)
            )
            yield from JsonSchemaTranslator.get_fields(
                items_type, items_schema, required=False, base_field_path=field_path
            )

        elif datahub_field_type == MapTypeClass:
            field_path = field_path.expand_type("map", schema)
            # When additionalProperties is used alone, without properties, the object essentially functions as a map<string, T> where T is the type described in the additionalProperties sub-schema. Maybe that helps to answer your original question.
            value_type = JsonSchemaTranslator._get_discriminated_type_from_schema(
                schema["additionalProperties"]
            )
            field_path._set_parent_type_if_not_exists(
                DataHubType(type=MapTypeClass, nested_type=value_type)
            )
            # FIXME: description not set. This is present in schema["description"].
            yield from JsonSchemaTranslator.get_fields(
                JsonSchemaTranslator._get_type_from_schema(
                    schema["additionalProperties"]
                ),
                schema["additionalProperties"],
                required=required,
                base_field_path=field_path,
            )

        elif datahub_field_type == UnionTypeClass:
            # we need to determine which of the different types of unions we are dealing with here
            union_category_map = {
                "oneOf": schema.get("oneOf"),
                "anyOf": schema.get("anyOf"),
                "allOf": schema.get("allOf"),
            }
            # unions can also exist if the "type" field is of type array
            if "type" in schema and isinstance(schema["type"], list):
                union_category_map["anyOf"] = [{"type": t} for t in schema["type"]]
            (union_category, union_category_schema) = [
                (k, v) for k, v in union_category_map.items() if v
            ][0]
            if not field_path.has_field_name() and len(union_category_schema) == 1:
                # Special case: If this is a top-level field AND there is only one type in the
                # union, we collapse down the union to avoid extra nesting.
                union_schema = union_category_schema[0]
                merged_union_schema = (
                    JsonSchemaTranslator._retain_parent_schema_props_in_union(
                        union_schema=union_schema, parent_schema=schema
                    )
                )
                yield from JsonSchemaTranslator.get_fields(
                    JsonSchemaTranslator._get_type_from_schema(merged_union_schema),
                    merged_union_schema,
                    required=required,
                    base_field_path=field_path,
                )
                return  # this one is done
            if field_path.has_field_name():
                # The frontend expects the top-level field to be a record, so we only
                # include the UnionTypeClass if we're not at the top level.
                yield SchemaField(
                    fieldPath=field_path.expand_type("union", schema).as_string(),
                    type=type_override or SchemaFieldDataTypeClass(UnionTypeClass()),
                    nativeDataType=f"union({union_category})",
                    nullable=nullable,
                    description=JsonSchemaTranslator._get_description_from_any_schema(
                        schema
                    ),
                    jsonProps=JsonSchemaTranslator._get_jsonprops_for_any_schema(
                        schema, required
                    ),
                    isPartOfKey=field_path.is_key_schema,
                )
            for i, union_schema in enumerate(union_category_schema):
                union_type = JsonSchemaTranslator._get_discriminated_type_from_schema(
                    union_schema
                )
                if (
                    union_type == "object"
                ):  # we add an index to distinguish each union type
                    union_type = f"union_{i}"
                union_field_path = field_path.expand_type("union", schema)
                union_field_path._set_parent_type_if_not_exists(
                    DataHubType(type=UnionTypeClass, nested_type=union_type)
                )
                merged_union_schema = (
                    JsonSchemaTranslator._retain_parent_schema_props_in_union(
                        union_schema=union_schema, parent_schema=schema
                    )
                )
                yield from JsonSchemaTranslator.get_fields(
                    JsonSchemaTranslator._get_type_from_schema(merged_union_schema),
                    merged_union_schema,
                    required=required,
                    base_field_path=union_field_path,
                    specific_type=union_type,
                )
        else:
            raise Exception(f"Unhandled type {datahub_field_type}")

    @staticmethod
    def _retain_parent_schema_props_in_union(
        union_schema: Dict, parent_schema: Dict
    ) -> Dict:
        """Merge the "properties" and the "required" fields from the parent schema into the child union schema."""

        union_schema = union_schema.copy()
        if "properties" in parent_schema:
            union_schema["properties"] = {
                **parent_schema["properties"],
                **union_schema.get("properties", {}),
            }
        if "required" in parent_schema:
            union_schema["required"] = [
                *parent_schema["required"],
                *union_schema.get("required", []),
            ]

        return union_schema

    @staticmethod
    def get_type_mapping(json_type: str) -> Type:
        return JsonSchemaTranslator.field_type_mapping.get(json_type, NullTypeClass)

    datahub_type_to_converter_mapping: Dict[
        Any,
        Callable[[Type, Type, FieldPath, Dict[Any, Any], bool], Iterable[SchemaField]],
    ] = {
        RecordTypeClass: _field_from_complex_type,
        UnionTypeClass: _field_from_complex_type,
        ArrayTypeClass: _field_from_complex_type,
        EnumTypeClass: _field_from_primitive,
        BooleanTypeClass: _field_from_primitive,
        FixedTypeClass: _field_from_primitive,
        StringTypeClass: _field_from_primitive,
        BytesTypeClass: _field_from_primitive,
        NumberTypeClass: _field_from_primitive,
        MapTypeClass: _field_from_complex_type,
        NullTypeClass: _field_from_primitive,
    }

    @classmethod
    def get_fields(
        cls,
        json_type: str,
        schema_dict: Dict,
        required: bool,
        base_field_path: FieldPath = FieldPath(),
        specific_type: Optional[str] = None,
    ) -> Iterable[SchemaField]:
        datahub_type = JsonSchemaTranslator.get_type_mapping(json_type)

        if datahub_type:
            generator = cls.datahub_type_to_converter_mapping.get(datahub_type)
            if generator is None:
                raise Exception(
                    f"Failed to find a mapping for type {datahub_type}, schema was {schema_dict}"
                )
            yield from generator.__get__(cls)(
                datahub_type, base_field_path, schema_dict, required, specific_type
            )

    @classmethod
    def get_fields_from_schema(
        cls,
        schema_dict: Dict,
        is_key_schema: bool = False,
        swallow_exceptions: bool = True,
    ) -> Iterable[SchemaField]:
        """Takes a json schema which can contain references and returns an iterator over schema fields.
        Preserves behavior similar to schema_util.avro_schema_to_mce which swallows exceptions by default
        """
        with unittest.mock.patch("jsonref.JsonRef.callback", title_swapping_callback):
            try:
                try:
                    schema_string = json.dumps(schema_dict)
                except Exception:
                    # we assume that this is a jsonref schema to begin with
                    jsonref_schema_dict = schema_dict
                else:
                    # first validate the schema using a json validator
                    validator = jsonschema.validators.validator_for(schema_dict)
                    validator.check_schema(schema_dict)
                    # then apply jsonref
                    jsonref_schema_dict = jsonref.loads(schema_string)
            except Exception as e:
                if swallow_exceptions:
                    logger.error(
                        "Failed to get fields from schema, continuing...", exc_info=e
                    )
                    return []
                else:
                    raise
            json_type = cls._get_type_from_schema(jsonref_schema_dict)
            yield from JsonSchemaTranslator.get_fields(
                json_type,
                jsonref_schema_dict,
                required=False,
                base_field_path=FieldPath(is_key_schema=is_key_schema),
            )

    @staticmethod
    def _get_id_from_any_schema(schema_dict: Dict[Any, Any]) -> Optional[str]:
        return schema_dict.get("$id", schema_dict.get("id"))


def get_enum_description(
    authored_description: Optional[str], enum_symbols: List[str]
) -> str:
    description = authored_description or ""
    missed_symbols = [symbol for symbol in enum_symbols if symbol not in description]
    if missed_symbols:
        description = (
            (description + "." if description else "")
            + " Allowed symbols are "
            + ", ".join(enum_symbols)
        )

    return description


def get_schema_metadata(
    platform: str,
    name: str,
    json_schema: Dict[Any, Any],
    raw_schema_string: Optional[str] = None,
) -> SchemaMetadata:
    json_schema_as_string = raw_schema_string or json.dumps(json_schema)
    md5_hash: str = md5(json_schema_as_string.encode()).hexdigest()

    schema_fields = list(JsonSchemaTranslator.get_fields_from_schema(json_schema))

    schema_metadata = SchemaMetadata(
        schemaName=name,
        platform=f"urn:li:dataPlatform:{platform}",
        version=0,
        hash=md5_hash,
        platformSchema=OtherSchema(rawSchema=json_schema_as_string),
        fields=schema_fields,
    )
    return schema_metadata
