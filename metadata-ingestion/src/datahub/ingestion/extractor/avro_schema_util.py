import json
import hashlib
import copy
from typing import List, Dict, Any, Optional, Type, Union, TypeAlias
from dataclasses import dataclass

from datahub.metadata.schema_classes import (
    SchemaFieldClass as SchemaField,
    SchemaFieldDataTypeClass as SchemaFieldDataType,
    SchemaMetadataClass as SchemaMetadata,
    OtherSchemaClass,
    BooleanTypeClass,
    FixedTypeClass,
    StringTypeClass,
    BytesTypeClass,
    NumberTypeClass,
    DateTypeClass,
    TimeTypeClass,
    EnumTypeClass,
    NullTypeClass,
    MapTypeClass,
    ArrayTypeClass,
    UnionTypeClass,
    RecordTypeClass,
)

DataHubSchemaType: TypeAlias = Union[
    BooleanTypeClass,
    FixedTypeClass,
    StringTypeClass,
    BytesTypeClass,
    NumberTypeClass,
    DateTypeClass,
    TimeTypeClass,
    EnumTypeClass,
    NullTypeClass,
    MapTypeClass,
    ArrayTypeClass,
    UnionTypeClass,
    RecordTypeClass,
]


import avro.schema

#### NOTES

# use these type aliases instead of schema: Dict[str, Any]
# SchemaOrField = Union[avro.schema.Schema, avro.schema.Field]
# FieldStack = List[avro.schema.Field]

# I did many schema/field["type"] replacements with schema/field["schema"]
####

@dataclass
class DataHubType:
    type: Type
    nested_type: Optional[str] = None

    def as_schema_field_type(self) -> SchemaFieldDataType:
        if self.type == UnionTypeClass:
            return SchemaFieldDataType(
                type=UnionTypeClass(
                    nestedTypes=[self.nested_type] if self.nested_type else None
                )
            )
        elif self.type == ArrayTypeClass:
            return SchemaFieldDataType(
                type=ArrayTypeClass(
                    nestedType=[self.nested_type] if self.nested_type else None
                )
            )
        elif self.type == MapTypeClass:
            return SchemaFieldDataType(
                type=MapTypeClass(
                    keyType="string",
                    valueType=self.nested_type if self.nested_type else None,
                )
            )
        else:
            raise ValueError(f"Unexpected type {self.type}")


class FieldElement:
    def __init__(
        self,
        types: List[str],
        schema_types: List[str],
        name: Optional[str] = None,
        parent_type: Optional[Any] = None,
    ):
        self.types = types
        self.schema_types = schema_types
        self.name = name
        self.parent_type = parent_type

    def clone(self):
        return FieldElement(
            copy.deepcopy(self.types),
            copy.deepcopy(self.schema_types),
            self.name,
            self.parent_type,
        )

    def as_string(self, v2_format: bool) -> str:
        return self.name if self.name else ""


class FieldPath:
    EMPTY_FIELD_NAME = " "

    def __init__(self, is_key_schema: bool = False):
        self.path: List[FieldElement] = []
        self.is_key_schema = is_key_schema
        self.use_v2_paths_always: bool = True

    def set_path(self, path: List[FieldElement]) -> None:
        if path is None:
            raise ValueError("Path cannot be null")
        if any(element is None for element in path):
            raise ValueError("Path cannot contain null elements")
        self.path = path

    def needs_v2_path(self) -> bool:
        if self.use_v2_paths_always:
            return True
        if self.is_key_schema:
            return True
        return any(
            t in ["union", "array"] for element in self.path for t in element.types
        )

    def set_parent_type_if_not_exists(self, parent_type: Any) -> None:
        if self.path and self.path[-1].parent_type is None:
            self.path[-1].parent_type = parent_type

    def get_type_override(self) -> Optional[Any]:
        if self.path and self.path[-1].parent_type is not None:
            return self.path[-1].parent_type
        return None

    def get_native_type_override(self) -> Optional[str]:
        type_override = self.get_type_override()
        if type_override is not None:
            if isinstance(type_override, list):
                return f"array({','.join(type_override)})"
            elif isinstance(type_override, dict):
                return f"map(str,{type_override.get('value_type')})"
        return None

    def get_recursive(self, schema: Dict[str, Any]) -> Optional[str]:
        schema_str = str(schema)
        for element in self.path:
            for i, schema_type in enumerate(element.schema_types):
                if schema_type == schema_str:
                    return element.types[i]
        return None

    def pop_last(self) -> "FieldPath":
        fpath = FieldPath()
        fpath.is_key_schema = self.is_key_schema
        fpath.path = copy.deepcopy(self.path)
        fpath.path.pop()
        return fpath

    def clone_plus(self, element: FieldElement) -> "FieldPath":
        fpath = FieldPath()
        fpath.is_key_schema = self.is_key_schema
        fpath.path = copy.deepcopy(self.path)
        fpath.path.append(element)
        return fpath

    def expand_type(self, type: str, type_schema: Any) -> "FieldPath":
        fpath = FieldPath()
        fpath.is_key_schema = self.is_key_schema
        fpath.path = [element.clone() for element in self.path]

        if fpath.path:
            last_element = fpath.path[-1]
            last_element.types.append(type)
            last_element.schema_types.append(str(type_schema))
        else:
            fpath.path.append(FieldElement([type], [str(type_schema)], None, None))

        return fpath

    def has_field_name(self) -> bool:
        return any(f.name is not None for f in self.path)

    def ensure_field_name(self) -> bool:
        if not self.has_field_name():
            if not self.path:
                self.path.append(FieldElement([], [], None, None))
            self.path[-1].name = self.EMPTY_FIELD_NAME
        return True

    def as_string(self) -> str:
        v2_format = self.needs_v2_path()
        prefix = []

        if v2_format:
            prefix.append("[version=2.0]")
            if self.is_key_schema:
                prefix.append("[key=True]")

        if self.path:
            return (
                ".".join(prefix)
                + "."
                + ".".join(f.as_string(v2_format) for f in self.path)
            )
        else:
            return ".".join(prefix)

    def dump(self) -> str:
        sb = ["FieldPath: " + self.as_string()]
        for f in self.path:
            sb.append(f"{f.name} {f.schema_types}")
        return " ".join(sb)


class AvroSchemaConverter:
    LOGICAL_TYPE_MAPPING: Dict[str, Type[DataHubSchemaType]] = {
        "date": DateTypeClass,
        "time-micros": TimeTypeClass,
        "time-millis": TimeTypeClass,
        "timestamp-micros": TimeTypeClass,
        "timestamp-millis": TimeTypeClass,
        "decimal": NumberTypeClass,
        "uuid": StringTypeClass,
    }

    def __init__(self):
        self.object_mapper = json

    def get_type_from_logical_type(
        self, schema: Dict[str, Any]
    ) -> Type[DataHubSchemaType]:
        logical_type = schema.get("logicalType")
        if logical_type:
            return self.LOGICAL_TYPE_MAPPING[logical_type]
        return self.get_base_type(schema)

    def get_base_type(self, schema: Dict[str, Any]) -> Type[DataHubSchemaType]:
        schema_type = schema["type"]
        if schema_type == "boolean":
            return BooleanTypeClass
        elif schema_type in ["int", "long", "float", "double"]:
            return NumberTypeClass
        elif schema_type == "string":
            return StringTypeClass
        elif schema_type == "bytes":
            return BytesTypeClass
        elif schema_type == "fixed":
            return FixedTypeClass
        elif schema_type == "enum":
            return EnumTypeClass
        elif schema_type == "array":
            return ArrayTypeClass
        elif schema_type == "map":
            return MapTypeClass
        elif schema_type == "record":
            return RecordTypeClass
        elif schema_type == "union":
            return UnionTypeClass
        else:
            return NullTypeClass

    def get_field_type(self, schema: Dict[str, Any]) -> str:
        return schema["type"].lower()

    def get_native_data_type(self, schema: Dict[str, Any]) -> str:
        logical_type = schema.get("logicalType")
        if logical_type:
            return f"{schema['type'].lower()}({logical_type})"
        return schema["type"].lower()

    def to_datahub_schema(
        self,
        schema: Dict[str, Any],
        is_key_schema: bool,
        default_nullable: bool,
        platform_urn: str,
        raw_schema_string: Optional[str] = None,
    ) -> SchemaMetadata:
        try:
            if raw_schema_string:
                canonical_form = json.dumps(schema, separators=(",", ":"))
                fingerprint_bytes = hashlib.md5(canonical_form.encode("utf-8")).digest()
            else:
                fingerprint_bytes = hashlib.md5(
                    json.dumps(schema).encode("utf-8")
                ).digest()

            schema_hash = "".join(f"{byte:02x}" for byte in fingerprint_bytes)

            fields: List[SchemaField] = []
            base_path = FieldPath(is_key_schema=is_key_schema)

            if schema["type"] == "record":
                base_path = base_path.expand_type(schema["name"], json.dumps(schema))

            self.process_schema(schema, base_path, default_nullable, fields)

            return SchemaMetadata(
                schemaName=schema["name"],
                platform=platform_urn,
                version=0,
                hash=schema_hash,
                platformSchema=OtherSchemaClass(rawSchema=json.dumps(schema)),
                fields=fields,
            )
        except Exception as e:
            raise RuntimeError("Failed to convert Avro schema") from e

    def process_schema(
        self,
        schema: Dict[str, Any],
        field_path: FieldPath,
        default_nullable: bool,
        fields: List[SchemaField],
    ) -> None:
        if schema["type"] == "record":
            for field in schema["fields"]:
                self.process_field(field, field_path, default_nullable, fields)

    def process_field(
        self,
        field: Dict[str, Any],
        field_path: FieldPath,
        default_nullable: bool,
        fields: List[SchemaField],
        nullable_override: bool = False,
        type_override: Optional[DataHubType] = None,
    ) -> None:
        field_schema = field["type"]
        is_nullable = self.is_nullable(field_schema, default_nullable)
        if nullable_override:
            is_nullable = True
        if type_override:
            is_nullable = nullable_override

        discriminated_type = self.get_discriminated_type(field_schema)

        element = FieldElement([], [], field["name"], type_override)
        new_path = field_path.clone_plus(element)

        if field_schema["type"] == "record":
            self.process_record_field(
                field,
                new_path,
                discriminated_type,
                default_nullable,
                fields,
                is_nullable,
                type_override,
            )
        elif field_schema["type"] == "array":
            self.process_array_field(
                field,
                new_path,
                discriminated_type,
                default_nullable,
                fields,
                is_nullable,
            )
        elif field_schema["type"] == "map":
            self.process_map_field(
                field,
                new_path,
                discriminated_type,
                default_nullable,
                fields,
                is_nullable,
            )
        elif field_schema["type"] == "union":
            self.process_union_field(
                field,
                new_path,
                discriminated_type,
                default_nullable,
                fields,
                is_nullable,
                type_override,
            )
        elif field_schema["type"] == "enum":
            self.process_enum_field(
                field,
                new_path,
                discriminated_type,
                default_nullable,
                fields,
                is_nullable,
            )
        else:
            self.process_primitive_field(
                field,
                new_path,
                discriminated_type,
                default_nullable,
                fields,
                is_nullable,
            )

    def process_record_field(
        self,
        field: Dict[str, Any],
        field_path: "FieldPath",
        discriminated_type: str,
        default_nullable: bool,
        fields: List[SchemaField],
        is_nullable: bool,
        type_override: Optional[DataHubType],
    ) -> None:
        record_path = field_path.expand_type(
            discriminated_type, json.dumps(field["schema"])
        )

        data_type: SchemaFieldDataType = (
            type_override.as_schema_field_type()
            if type_override
            else SchemaFieldDataType(type=RecordTypeClass())
        )

        record_field = SchemaField(
            fieldPath=record_path.as_string(),
            type=data_type,
            nativeDataType=discriminated_type,
            nullable=is_nullable or default_nullable,
            isPartOfKey=field_path.is_key_schema,
        )

        self.populate_common_properties(field, record_field)
        fields.append(record_field)

        for nested_field in field["schema"]["fields"]:
            self.process_field(nested_field, record_path, default_nullable, fields)

    def populate_common_properties(
        self, field: Dict[str, Any], datahub_field: SchemaField
    ) -> None:
        combined_props = {**field.get("props", {}), **field["schema"].get("props", {})}
        if combined_props:
            datahub_field.jsonProps = json.dumps(combined_props)
        if field.get("doc"):
            datahub_field.description = field["doc"]
            assert datahub_field.description is not None
            if "default" in field:
                default_value = field["default"]
                if default_value is None:
                    datahub_field.description += "\nField default value: null"
                else:
                    datahub_field.description += (
                        f"\nField default value: {json.dumps(default_value)}"
                    )

    def process_array_field(
        self,
        field: Dict[str, Any],
        field_path: "FieldPath",
        discriminated_type: str,
        default_nullable: bool,
        fields: List[SchemaField],
        is_nullable: bool,
    ) -> None:
        array_schema = field["schema"]
        element_schema = array_schema["items"]
        element_type = self.get_discriminated_type(element_schema)

        field_path = field_path.expand_type("array", json.dumps(array_schema))

        array_datahubtype = DataHubType(type=ArrayTypeClass, nested_type=element_type)

        array_field = SchemaField(
            fieldPath=field_path.as_string(),
            type=array_datahubtype.as_schema_field_type(),
            nativeDataType=f"array({element_type})",
            nullable=is_nullable or default_nullable,
            isPartOfKey=field_path.is_key_schema,
        )

        self.populate_common_properties(field, array_field)
        fields.append(array_field)

        if element_schema["type"] in ["record", "array", "map", "union"]:
            element_field = {
                "name": "items",
                "type": element_schema,
                "doc": element_schema.get("doc", field.get("doc")),
            }
            self.process_field(
                element_field, field_path, default_nullable, fields, True
            )

    def process_map_field(
        self,
        field: Dict[str, Any],
        field_path: "FieldPath",
        discriminated_type: str,
        default_nullable: bool,
        fields: List[SchemaField],
        is_nullable: bool,
    ) -> None:
        map_schema = field["schema"]
        value_schema = map_schema["values"]
        value_type = self.get_discriminated_type(value_schema)

        field_path = field_path.expand_type("map", json.dumps(map_schema))

        map_datahubtype = DataHubType(type=MapTypeClass, nested_type=value_type)

        map_field = SchemaField(
            fieldPath=field_path.as_string(),
            type=map_datahubtype.as_schema_field_type(),
            nativeDataType=f"map<string,{value_type}>",
            nullable=is_nullable or default_nullable,
            isPartOfKey=field_path.is_key_schema,
        )

        self.populate_common_properties(field, map_field)
        fields.append(map_field)

        if value_schema["type"] in ["record", "array", "map", "union"]:
            value_field = {
                "name": "value",
                "type": value_schema,
                "doc": value_schema.get("doc", field.get("doc")),
            }
            self.process_field(value_field, field_path, default_nullable, fields, True)

    def process_union_field(
        self,
        field: Dict[str, Any],
        field_path: "FieldPath",
        discriminated_type: str,
        default_nullable: bool,
        fields: List[SchemaField],
        is_nullable: bool,
        type_override: Optional[DataHubType],
    ) -> None:
        union_types = field["type"]["types"]

        if len(union_types) == 2 and is_nullable:
            non_null_schema = next(s for s in union_types if s["type"] != "null")
            self.process_field(
                {
                    "name": field["name"],
                    "type": non_null_schema,
                    "doc": field.get("doc"),
                },
                field_path.pop_last(),
                default_nullable,
                fields,
                True,
            )
            return

        union_field_path = field_path.expand_type("union", json.dumps(field["type"]))

        union_datahubtype = DataHubType(
            type=UnionTypeClass, nested_type=discriminated_type
        )

        union_field = SchemaField(
            fieldPath=union_field_path.as_string(),
            type=union_datahubtype.as_schema_field_type()
            if type_override is None
            else type_override.as_schema_field_type(),
            nativeDataType="union",
            nullable=is_nullable or default_nullable,
            isPartOfKey=field_path.is_key_schema,
        )

        self.populate_common_properties(field, union_field)
        fields.append(union_field)

        union_description = field.get("doc", field["type"].get("doc"))

        for union_schema in union_types:
            if union_schema["type"] != "null":
                union_field_inner = {
                    "name": field["name"],
                    "type": union_schema,
                    "doc": union_schema.get("doc", union_description),
                }
                self.process_field(
                    union_field_inner,
                    field_path.pop_last().clone_plus(
                        FieldElement(["union"], [], None, None)
                    ),
                    default_nullable,
                    fields,
                )

    def process_enum_field(
        self,
        field: Dict[str, Any],
        field_path: "FieldPath",
        discriminated_type: str,
        default_nullable: bool,
        fields: List[SchemaField],
        is_nullable: bool,
    ) -> None:
        field_path = field_path.expand_type("enum", json.dumps(field["type"]))

        enum_description = (
            field.get("doc", "")
            + " Allowed symbols are: "
            + ", ".join(field["type"]["symbols"])
        )

        enum_field = SchemaField(
            fieldPath=field_path.as_string(),
            type=SchemaFieldDataType(type=EnumTypeClass()),
            nativeDataType="Enum",
            nullable=is_nullable or default_nullable,
            isPartOfKey=field_path.is_key_schema,
        )

        self.populate_common_properties(field, enum_field)
        if field.get("doc"):
            enum_field.description = enum_description

        fields.append(enum_field)

    def process_primitive_field(
        self,
        field: Dict[str, Any],
        field_path: "FieldPath",
        discriminated_type: str,
        default_nullable: bool,
        fields: List[SchemaField],
        is_nullable: bool,
    ) -> None:
        field_path = field_path.expand_type(
            discriminated_type, json.dumps(field["type"])
        )

        type_class = self.get_type_from_logical_type(field["type"])
        primitive_field = SchemaField(
            fieldPath=field_path.as_string(),
            type=SchemaFieldDataType(type=type_class()),
            nativeDataType=self.get_native_data_type(field["type"]),
            nullable=is_nullable or default_nullable,
            isPartOfKey=field_path.is_key_schema,
        )

        self.populate_common_properties(field, primitive_field)
        fields.append(primitive_field)

    def is_nullable(self, schema: Dict[str, Any], default_nullable: bool) -> bool:
        if isinstance(schema, list): # union
        #if schema.get("type") == "union": ### this requires revision, I've seen schema["type"] == ["t0", "t1"]
            return any(s["type"] == "null" for s in schema["types"])
        return default_nullable

    def get_discriminated_type(self, schema: Dict[str, Any]) -> str:
        if schema["type"] == "record":
            if "namespace" in schema:
                return schema["name"].replace(schema["namespace"] + ".", "")
            return schema["name"]
        elif (
            schema["type"] == "union"
            and len(schema["types"]) == 2
            and any(s["type"] == "null" for s in schema["types"])
        ):
            non_null_schema = next(s for s in schema["types"] if s["type"] != "null")
            return non_null_schema["name"]
        return schema["type"].lower()


def avro_schema_to_mce_fields(
        avro_schema: Union[avro.schema.Schema, str],
        is_key_schema: bool = False,
        default_nullable: bool = False,
        #meta_mapping_processor: Optional[OperationProcessor] = None,
        schema_tags_field: Optional[str] = None,
        tag_prefix: Optional[str] = None,
        swallow_exceptions: bool = True,
) -> List[SchemaField]:
    try:
        if isinstance(avro_schema, str):
            avro_schema = avro.schema.parse(avro_schema)

        schema_metadata = AvroSchemaConverter(
        ).to_datahub_schema(
            schema=avro_schema.to_canonical_json(),
            is_key_schema=is_key_schema,
            default_nullable=default_nullable,
            platform_urn="urn:li:dataPlatform",
            raw_schema_string=json.dumps(avro_schema.to_json()),
        )
        return schema_metadata.fields
    except Exception:
        if swallow_exceptions:
            logger.exception(f"Failed to parse {avro_schema} into mce fields.")
            return []
        else:
            raise
