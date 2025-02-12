import copy
import hashlib
import json
import logging
from dataclasses import dataclass
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Set,
    Type,
    TypeAlias,
    Union,
)

import avro.schema

from datahub.emitter import mce_builder
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    EnumTypeClass,
    FixedTypeClass,
    MapTypeClass,
    NullTypeClass,
    NumberTypeClass,
    OtherSchemaClass,
    RecordTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    TimeTypeClass,
    UnionTypeClass,
)
from datahub.utilities.mapping import (
    Constants,
    OperationProcessor,
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

# Code here is similar to code in AvroSchemaConverter.java in the Java SDK
# So, in order to preserve the feature parity,
# any update here should have the corresponding update there

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
        parent_type: Optional[DataHubType] = None,
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
        if v2_format:
            type_prefix = ".".join(f"[type={inner_type}]" for inner_type in self.types)
            return f"{type_prefix}.{self.name}" if self.name else type_prefix
        else:
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
    _records_seen: Set[str]  # track visited schema fullnames
    _meta_mapping_processor: Optional[OperationProcessor] = None
    _schema_tags_field: Optional[str] = None
    _tag_prefix: Optional[str] = None

    def __init__(
        self,
        meta_mapping_processor: Optional[OperationProcessor] = None,
        schema_tags_field: Optional[str] = None,
        tag_prefix: Optional[str] = None,
    ):
        self._records_seen = set()
        self._meta_mapping_processor = meta_mapping_processor
        self._schema_tags_field = schema_tags_field
        self._tag_prefix = tag_prefix

    def get_type_from_logical_type(
        self, field: avro.schema.Field
    ) -> Type[DataHubSchemaType]:
        logical_type = field.props.get("logicalType") or field.type.props.get(
            "logicalType"
        )
        if logical_type:
            assert isinstance(logical_type, str)
            return self.LOGICAL_TYPE_MAPPING[logical_type]
        return self.get_base_type(field.type)

    def get_base_type(self, schema: avro.schema.Schema) -> Type[DataHubSchemaType]:
        schema_type = schema.type
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

    def get_native_data_type(self, schema: avro.schema.Schema) -> str:
        logical_type = schema.props.get("logicalType")
        if logical_type:
            return f"{schema.type.lower()}({logical_type})"
        return schema.type.lower()

    def to_datahub_schema(
        self,
        schema: avro.schema.Schema,
        is_key_schema: bool,
        default_nullable: bool,
        platform_urn: str,
        raw_schema_string: Optional[str] = None,
    ) -> SchemaMetadataClass:
        try:
            if raw_schema_string:
                canonical_form = json.dumps(raw_schema_string, separators=(",", ":"))
                fingerprint_bytes = hashlib.md5(canonical_form.encode("utf-8")).digest()
            else:
                fingerprint_bytes = hashlib.md5(
                    json.dumps(schema.to_json()).encode("utf-8")
                ).digest()

            schema_hash = "".join(f"{byte:02x}" for byte in fingerprint_bytes)

            fields: List[SchemaFieldClass] = []
            base_path = FieldPath(is_key_schema=is_key_schema)

            # TOOD: this code could be moved within the process_schema method
            if schema.type == "record":
                assert isinstance(schema, avro.schema.RecordSchema)
                base_path = base_path.expand_type(
                    schema.name, json.dumps(schema.to_json())
                )
                self._records_seen.add(schema.fullname)
            elif schema.type == "union":
                assert isinstance(schema, avro.schema.UnionSchema)
                # just expand so this is pop up later in process_union_field
                base_path = base_path.expand_type("union", schema)

            self.process_schema(schema, base_path, default_nullable, fields)

            return SchemaMetadataClass(
                schemaName=schema.name if not isinstance(schema, (avro.schema.PrimitiveSchema | avro.schema.UnionSchema)) else None,  # type: ignore[attr-defined]
                platform=platform_urn,
                version=0,
                hash=schema_hash,
                platformSchema=OtherSchemaClass(rawSchema=json.dumps(schema.to_json())),
                fields=fields,
            )
        except Exception as e:
            raise RuntimeError("Failed to convert Avro schema") from e

    def process_schema(
        self,
        schema: avro.schema.Schema,
        field_path: FieldPath,
        default_nullable: bool,
        fields: List[SchemaFieldClass],
    ) -> None:
        if schema.type == "record":
            assert isinstance(schema, avro.schema.RecordSchema)
            for field in schema.fields:
                self.process_field(
                    field=field,
                    field_path=field_path,
                    default_nullable=default_nullable,
                    fields=fields,
                )
        elif isinstance(schema, avro.schema.PrimitiveSchema):
            self.process_primitive_field(
                field=self._new_avro_schema_field(
                    name="not-used but not-empty name",
                    schema=schema.to_json(),
                ),
                field_path=field_path,
                discriminated_type=schema.type,
                default_nullable=default_nullable,
                fields=fields,
                is_nullable=False,
            )
        elif isinstance(schema, avro.schema.UnionSchema):
            self.process_union_field(
                field=self._new_avro_schema_field(
                    name="unnamed_union",
                    schema=schema.to_json(),
                ),
                field_path=field_path,
                discriminated_type=schema.type,
                default_nullable=default_nullable,
                fields=fields,
                is_nullable=False,
                type_override=None,
            )

    def process_field(
        self,
        field: avro.schema.Field,
        field_path: FieldPath,
        default_nullable: bool,
        fields: List[SchemaFieldClass],
        nullable_override: bool = False,
        type_override: Optional[DataHubType] = None,
    ) -> None:
        field_schema: avro.schema.Schema = field.type
        is_nullable = self.is_nullable(field_schema, default_nullable)
        if nullable_override:
            is_nullable = True
        if type_override:
            is_nullable = nullable_override

        discriminated_type = self.get_discriminated_type(field_schema)

        element = FieldElement([], [], field.name, type_override)
        new_path = field_path.clone_plus(element)

        if field_schema.type == "record":
            assert isinstance(field_schema, avro.schema.RecordSchema)
            recursive = field_schema.fullname in self._records_seen
            if not recursive:
                self._records_seen.add(field_schema.fullname)
            self.process_record_field(
                field=field,
                field_path=new_path,
                discriminated_type=discriminated_type,
                default_nullable=default_nullable,
                fields=fields,
                is_nullable=is_nullable,
                type_override=type_override,
                recursive=recursive,
            )
        elif field_schema.type == "array":
            assert isinstance(field_schema, avro.schema.ArraySchema)
            self.process_array_field(
                field=field,
                field_path=new_path,
                discriminated_type=discriminated_type,
                default_nullable=default_nullable,
                fields=fields,
                is_nullable=is_nullable,
            )
        elif field_schema.type == "map":
            assert isinstance(field_schema, avro.schema.MapSchema)
            self.process_map_field(
                field=field,
                field_path=new_path,
                discriminated_type=discriminated_type,
                default_nullable=default_nullable,
                fields=fields,
                is_nullable=is_nullable,
            )
        elif field_schema.type == "union":
            assert isinstance(field_schema, avro.schema.UnionSchema)
            self.process_union_field(
                field=field,
                field_path=new_path,
                discriminated_type=discriminated_type,
                default_nullable=default_nullable,
                fields=fields,
                is_nullable=is_nullable,
                type_override=type_override,
            )
        elif field_schema.type == "enum":
            assert isinstance(field_schema, avro.schema.EnumSchema)
            self.process_enum_field(
                field=field,
                field_path=new_path,
                discriminated_type=discriminated_type,
                default_nullable=default_nullable,
                fields=fields,
                is_nullable=is_nullable,
            )
        else:
            self.process_primitive_field(
                field=field,
                field_path=new_path,
                discriminated_type=discriminated_type,
                default_nullable=default_nullable,
                fields=fields,
                is_nullable=is_nullable,
            )

    def process_record_field(
        self,
        field: avro.schema.Field,
        field_path: "FieldPath",
        discriminated_type: str,
        default_nullable: bool,
        fields: List[SchemaFieldClass],
        is_nullable: bool,
        type_override: Optional[DataHubType],
        recursive: bool = False,
    ) -> None:
        record_schema = field.type
        assert isinstance(record_schema, avro.schema.RecordSchema)
        record_path = field_path.expand_type(discriminated_type, record_schema)

        data_type: SchemaFieldDataTypeClass = (
            type_override.as_schema_field_type()
            if type_override
            else SchemaFieldDataTypeClass(type=RecordTypeClass())
        )

        record_field = SchemaFieldClass(
            fieldPath=record_path.as_string(),
            type=data_type,
            nativeDataType=discriminated_type,
            nullable=is_nullable or default_nullable,
            isPartOfKey=field_path.is_key_schema,
        )

        self.populate_common_properties_and_process_meta_mapping(field, record_field)
        fields.append(record_field)

        if not recursive:
            for nested_field in record_schema.fields:
                self.process_field(
                    field=nested_field,
                    field_path=record_path,
                    default_nullable=default_nullable,
                    fields=fields,
                )

    def populate_common_properties_and_process_meta_mapping(
        self, field: avro.schema.Field, datahub_field: SchemaFieldClass
    ) -> None:
        combined_props = {**field.other_props, **field.type.other_props}
        if combined_props:
            datahub_field.jsonProps = json.dumps(combined_props)

        description = field.doc or (
            field.type.doc if getattr(field.type, "doc", None) else None
        )
        if description:
            if "deprecated" in combined_props:
                description = (
                    f"<span style=\"color:red\">DEPRECATED: {combined_props['deprecated']}</span>\n"
                    + description
                )

            if field.has_default:
                default_value = field.default
                if default_value is None:
                    description += "\nField default value: null"
                else:
                    description += f"\nField default value: {json.dumps(default_value)}"

            datahub_field.description = description

        # process meta mapping to generate tags and/or terms

        meta_aspects: Dict[str, Any] = (
            self._meta_mapping_processor.process(combined_props)
            if self._meta_mapping_processor
            else {}
        )
        tags: List[str] = (
            [
                self._tag_prefix + tag
                for tag in combined_props.get(self._schema_tags_field, [])
            ]
            if self._schema_tags_field
            else []
        )
        meta_tags_aspect = meta_aspects.get(Constants.ADD_TAG_OPERATION)
        if meta_tags_aspect:
            tags += [
                tag_association.tag[len("urn:li:tag:") :]
                for tag_association in meta_tags_aspect.tags
            ]
        if "deprecated" in combined_props:
            tags.append("Deprecated")

        datahub_field.globalTags = (
            mce_builder.make_global_tag_aspect_with_tag_list(tags) if tags else None
        )
        datahub_field.glossaryTerms = meta_aspects.get(Constants.ADD_TERM_OPERATION)

    def process_array_field(
        self,
        field: avro.schema.Field,
        field_path: FieldPath,
        discriminated_type: str,
        default_nullable: bool,
        fields: List[SchemaFieldClass],
        is_nullable: bool,
    ) -> None:
        array_schema = field.type
        assert isinstance(array_schema, avro.schema.ArraySchema)
        element_schema = array_schema.items
        element_type = self.get_discriminated_type(element_schema)

        field_path = field_path.expand_type("array", array_schema)

        array_datahubtype = DataHubType(type=ArrayTypeClass, nested_type=element_type)

        array_field = SchemaFieldClass(
            fieldPath=field_path.as_string(),
            type=array_datahubtype.as_schema_field_type(),
            nativeDataType=f"array({element_type})",
            nullable=is_nullable or default_nullable,
            isPartOfKey=field_path.is_key_schema,
        )
        self.populate_common_properties_and_process_meta_mapping(field, array_field)
        fields.append(array_field)

        if element_schema.type in ["record", "array", "map", "union"]:
            element_field = self._new_avro_schema_field(
                name="items",
                schema=element_schema.to_json(),
                doc=field.doc,
                default=field.default,
                other_props=dict(field.other_props) if field.other_props else None,
            )
            self.process_field(
                element_field, field_path, default_nullable, fields, True
            )

    def process_map_field(
        self,
        field: avro.schema.Field,
        field_path: "FieldPath",
        discriminated_type: str,
        default_nullable: bool,
        fields: List[SchemaFieldClass],
        is_nullable: bool,
    ) -> None:
        map_schema = field.type
        assert isinstance(map_schema, avro.schema.MapSchema)
        value_schema = map_schema.values
        value_type = self.get_discriminated_type(value_schema)

        field_path = field_path.expand_type("map", map_schema)

        map_datahubtype = DataHubType(type=MapTypeClass, nested_type=value_type)

        map_field = SchemaFieldClass(
            fieldPath=field_path.as_string(),
            type=map_datahubtype.as_schema_field_type(),
            nativeDataType=f"map<string,{value_type}>",
            nullable=is_nullable or default_nullable,
            isPartOfKey=field_path.is_key_schema,
        )
        self.populate_common_properties_and_process_meta_mapping(field, map_field)
        fields.append(map_field)

        if value_schema.type in ["record", "array", "map", "union"]:
            value_field = self._new_avro_schema_field(
                name="value",
                schema=value_schema.to_json(),
                doc=field.doc,
                default=field.default,
                other_props=dict(field.other_props) if field.other_props else None,
            )
            self.process_field(
                field=value_field,
                field_path=field_path,
                default_nullable=default_nullable,
                fields=fields,
                # nullabel_override=True,
            )

    def process_union_field(
        self,
        field: avro.schema.Field,
        field_path: FieldPath,
        discriminated_type: str,
        default_nullable: bool,
        fields: List[SchemaFieldClass],
        is_nullable: bool,
        type_override: Optional[DataHubType],
    ) -> None:
        union_types = field.type.schemas

        if len(union_types) == 2 and is_nullable:
            non_null_schema = next(s for s in union_types if s.type != "null")
            self.process_field(
                field=self._new_avro_schema_field(
                    name=field.name,
                    schema=non_null_schema.to_json(),
                    doc=field.doc,
                    default=field.default,
                    other_props=dict(field.other_props) if field.other_props else None,
                ),
                field_path=field_path.pop_last(),
                default_nullable=default_nullable,
                fields=fields,
                nullable_override=True,
            )
            return

        union_field_path = field_path.expand_type("union", field.type)

        union_datahubtype = DataHubType(
            type=UnionTypeClass, nested_type=discriminated_type
        )

        union_field = SchemaFieldClass(
            fieldPath=union_field_path.as_string(),
            type=type_override.as_schema_field_type()
            if type_override
            else union_datahubtype.as_schema_field_type(),
            nativeDataType="union",
            nullable=is_nullable or default_nullable,
            isPartOfKey=field_path.is_key_schema,
        )
        self.populate_common_properties_and_process_meta_mapping(field, union_field)
        fields.append(union_field)

        for union_schema in union_types:
            if union_schema.type != "null":
                union_field_inner = self._new_avro_schema_field(
                    name=field.name,
                    schema=union_schema.to_json(),
                    doc=field.doc,
                    default=field.default,
                    other_props=dict(field.other_props) if field.other_props else None,
                )
                self.process_field(
                    field=union_field_inner,
                    field_path=field_path.pop_last().clone_plus(
                        FieldElement(["union"], [], None, None)
                    ),
                    default_nullable=default_nullable,
                    fields=fields,
                )

    def _new_avro_schema_field(
        self,
        name: str,
        schema: str,
        doc: Optional[str] = None,
        default: Optional[str] = None,
        other_props: Optional[Dict[str, Any]] = None,
    ) -> avro.schema.Field:
        # TODO: there should be an easier way to instantiate a Field object not requiring parsing!
        dummy_record_str = {
            "type": "record",
            "name": "dummy_record",
            "fields": [
                {
                    "name": name,
                    "type": schema,
                    "doc": doc,
                    "default": default,
                    **(other_props or {}),
                }
            ],
        }
        dummy_record_schema = avro.schema.parse(json.dumps(dummy_record_str))
        dummy_record_fields = dummy_record_schema.fields  # type: ignore[attr-defined]
        assert len(dummy_record_fields) == 1
        return dummy_record_fields[0]

    def process_enum_field(
        self,
        field: avro.schema.Field,
        field_path: "FieldPath",
        discriminated_type: str,
        default_nullable: bool,
        fields: List[SchemaFieldClass],
        is_nullable: bool,
    ) -> None:
        field_path = field_path.expand_type("enum", field.type)

        enum_description = field.doc or "" + " Allowed symbols are: " + ", ".join(
            field.type.symbols
        )

        enum_field = SchemaFieldClass(
            fieldPath=field_path.as_string(),
            type=SchemaFieldDataTypeClass(type=EnumTypeClass()),
            nativeDataType="Enum",
            nullable=is_nullable or default_nullable,
            isPartOfKey=field_path.is_key_schema,
        )
        self.populate_common_properties_and_process_meta_mapping(field, enum_field)
        if field.doc:
            enum_field.description = enum_description

        fields.append(enum_field)

    def process_primitive_field(
        self,
        field: avro.schema.Field,
        field_path: FieldPath,
        discriminated_type: str,
        default_nullable: bool,
        fields: List[SchemaFieldClass],
        is_nullable: bool,
    ) -> None:
        field_path = field_path.expand_type(discriminated_type, field.type)

        type_class = self.get_type_from_logical_type(field)
        primitive_field = SchemaFieldClass(
            fieldPath=field_path.as_string(),
            type=SchemaFieldDataTypeClass(type=type_class()),
            nativeDataType=self.get_native_data_type(field.type),
            nullable=is_nullable or default_nullable,
            isPartOfKey=field_path.is_key_schema,
        )
        self.populate_common_properties_and_process_meta_mapping(field, primitive_field)
        fields.append(primitive_field)

    def is_nullable(self, schema: avro.schema.Schema, default_nullable: bool) -> bool:
        if schema.type == "null":
            return True
        elif schema.type == "union":
            return any(s.type == "null" for s in schema.schemas)  # type: ignore[attr-defined]
        else:
            if isinstance(schema.props, dict):
                return schema.props.get("_nullable", default_nullable)
            return default_nullable

    def get_discriminated_type(self, schema: avro.schema.Schema) -> str:
        if schema.type == "record":
            assert isinstance(schema, avro.schema.RecordSchema)
            if schema.namespace:
                return schema.name.replace(schema.namespace + ".", "")
            return schema.name
        elif (
            schema.type == "union"
            and len(schema.schemas) == 2  # type: ignore[attr-defined]
            and any(s.type == "null" for s in schema.schemas)  # type: ignore[attr-defined]
        ):
            assert isinstance(schema, avro.schema.UnionSchema)
            non_null_schema = next(s for s in schema.schemas if s.type != "null")
            assert getattr(
                non_null_schema,
                "name",
                getattr(non_null_schema, "fullname", non_null_schema.type),
            ), f"Union schema must have a name {non_null_schema.to_json()}"
            return getattr(
                non_null_schema,
                "name",
                getattr(non_null_schema, "fullname", non_null_schema.type),
            )
        return schema.type.lower()


def avro_schema_to_mce_fields(
    avro_schema: Union[avro.schema.Schema, str],
    is_key_schema: bool = False,
    default_nullable: bool = False,
    meta_mapping_processor: Optional[OperationProcessor] = None,
    schema_tags_field: Optional[str] = None,
    tag_prefix: Optional[str] = None,
    swallow_exceptions: bool = True,
) -> List[SchemaFieldClass]:
    try:
        if isinstance(avro_schema, str):
            avro_schema = avro.schema.parse(avro_schema)

        schema_metadata = AvroSchemaConverter(
            meta_mapping_processor=meta_mapping_processor,
            schema_tags_field=schema_tags_field,
            tag_prefix=tag_prefix,
        ).to_datahub_schema(
            schema=avro_schema,
            is_key_schema=is_key_schema,
            default_nullable=default_nullable,
            platform_urn="urn:li:dataPlatform",
            raw_schema_string=json.dumps(avro_schema.to_json()),
        )
        return schema_metadata.fields
    except Exception as e:
        if swallow_exceptions:
            logger.exception(f"Failed to parse {avro_schema} into mce fields.")
            return []
        else:
            raise e
