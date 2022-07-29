import contextlib
import logging
import os
import re
import sys
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import (
    Any,
    Dict,
    Generator,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    cast,
)

try:
    import grpc
    import grpc.experimental
    import networkx as nx
except ImportError:
    raise ModuleNotFoundError(
        "The protobuf_util module requires Python 3.7 or newer because of the"
        " networkx.algorithms.dag.topological_generations dependency."
    )

from google.protobuf.descriptor import (
    Descriptor,
    DescriptorBase,
    EnumDescriptor,
    FieldDescriptor,
    FileDescriptor,
    OneofDescriptor,
)

from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    EnumTypeClass,
    FixedTypeClass,
    MapTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaField,
    SchemaFieldDataType,
    StringTypeClass,
    UnionTypeClass,
)

"""A helper file for Protobuf schema -> MCE schema transformations"""

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------------------
#  API
#
@dataclass
class ProtobufSchema:
    name: str
    content: str


def protobuf_schema_to_mce_fields(
    main_schema: ProtobufSchema,
    imported_schemas: Optional[List[ProtobufSchema]] = None,
    is_key_schema: bool = False,
) -> List[SchemaField]:
    """
    Converts a protobuf schema into a schema compatible with MCE
    :param protobuf_schema_string: String representation of the protobuf schema
    :param is_key_schema: True if it is a key-schema. Default is False (value-schema).
    :return: The list of MCE compatible SchemaFields.
    """
    descriptor: FileDescriptor = _from_protobuf_schema_to_descriptors(
        main_schema, imported_schemas
    )
    graph: nx.DiGraph = _populate_graph(descriptor)

    if nx.is_directed_acyclic_graph(graph):
        return _schema_fields_from_dag(graph, is_key_schema)
    else:
        raise Exception("Cyclic schemas are not supported")


#
# ------------------------------------------------------------------------------

_native_type_to_typeclass: Dict[str, Type] = {
    "bool": BooleanTypeClass,
    "bytes": BytesTypeClass,
    "double": NumberTypeClass,
    "enum": EnumTypeClass,
    "fixed32": FixedTypeClass,
    "fixed64": FixedTypeClass,
    "float": NumberTypeClass,
    "group": RecordTypeClass,
    "int32": NumberTypeClass,
    "int64": NumberTypeClass,
    "map": MapTypeClass,
    "message": RecordTypeClass,
    "oneof": UnionTypeClass,
    "repeated": ArrayTypeClass,
    "sfixed32": FixedTypeClass,
    "sfixed64": FixedTypeClass,
    "sint32": NumberTypeClass,
    "sint64": NumberTypeClass,
    "string": StringTypeClass,
    "uint32": NumberTypeClass,
    "uint64": NumberTypeClass,
}

_protobuf_type_to_native_type: Dict[int, str] = {
    FieldDescriptor.TYPE_BOOL: "bool",
    FieldDescriptor.TYPE_BYTES: "bytes",
    FieldDescriptor.TYPE_DOUBLE: "double",
    FieldDescriptor.TYPE_ENUM: "enum",
    FieldDescriptor.TYPE_FIXED32: "fixed32",
    FieldDescriptor.TYPE_FIXED64: "fixed64",
    FieldDescriptor.TYPE_FLOAT: "float",
    FieldDescriptor.TYPE_INT32: "int32",
    FieldDescriptor.TYPE_INT64: "int64",
    FieldDescriptor.TYPE_SFIXED32: "sfixed32",
    FieldDescriptor.TYPE_SFIXED64: "sfixed64",
    FieldDescriptor.TYPE_SINT32: "sint32",
    FieldDescriptor.TYPE_SINT64: "sint64",
    FieldDescriptor.TYPE_STRING: "string",
    FieldDescriptor.TYPE_UINT32: "uint32",
    FieldDescriptor.TYPE_UINT64: "uint64",
}

_protobuf_type_to_schema_type: Dict[int, str] = {
    FieldDescriptor.TYPE_BOOL: "bool",
    FieldDescriptor.TYPE_BYTES: "bytes",
    FieldDescriptor.TYPE_DOUBLE: "double",
    FieldDescriptor.TYPE_ENUM: "enum",
    FieldDescriptor.TYPE_FIXED32: "int",
    FieldDescriptor.TYPE_FIXED64: "long",
    FieldDescriptor.TYPE_FLOAT: "float",
    FieldDescriptor.TYPE_INT32: "int",
    FieldDescriptor.TYPE_INT64: "long",
    FieldDescriptor.TYPE_SFIXED32: "int",
    FieldDescriptor.TYPE_SFIXED64: "long",
    FieldDescriptor.TYPE_SINT32: "int",
    FieldDescriptor.TYPE_SINT64: "long",
    FieldDescriptor.TYPE_STRING: "string",
    FieldDescriptor.TYPE_UINT32: "int",
    FieldDescriptor.TYPE_UINT64: "long",
}


@dataclass
class _PathAndField:
    path: str
    field: SchemaField


def _add_field(graph: nx.DiGraph, parent_node: str, field: FieldDescriptor) -> None:
    field_node: str = _get_node_name(field)
    field_type: str = _get_type_ascription(field)
    if graph.nodes.get(field_node) is None:
        graph.add_node(field_node, node_type=field_type)
    if graph.get_edge_data(parent_node, field_node) is None:
        graph.add_edge(parent_node, field_node, fields=[])
    graph[parent_node][field_node]["fields"].append(field)


def _add_fields(
    graph: nx.DiGraph,
    fields: List[FieldDescriptor],
    parent_name: str,
    parent_type: str = "message",
    visited: Optional[Set[str]] = None,
) -> None:
    if visited is None:
        visited = set()

    for field in fields:
        if parent_type == "oneof" or field.containing_oneof is None:
            if field.message_type:
                _add_message(graph, field.message_type, visited)
            _add_field(graph, parent_name, field)


def _add_message(graph: nx.DiGraph, message: Descriptor, visited: Set[str]) -> None:
    node_name: str = _get_node_name(message)
    if node_name not in visited:
        visited.add(node_name)
        node_type: str = _get_type_ascription(message)
        graph.add_node(node_name, node_type=node_type)

        for nested in message.nested_types_by_name.values():
            _add_message(graph, nested, visited)

        _add_fields(graph, message.fields, node_name, visited=visited)

        for oneof in message.oneofs_by_name.values():
            _add_oneof(graph, node_name, oneof, visited)


def _add_oneof(
    graph: nx.DiGraph, parent_node: str, oneof: OneofDescriptor, visited: Set[str]
) -> None:
    node_name: str = _get_node_name(cast(DescriptorBase, oneof))
    node_type: str = _get_type_ascription(cast(DescriptorBase, oneof))
    graph.add_node(node_name, node_type=node_type)
    graph.add_edge(parent_node, node_name, fields=[oneof])

    _add_fields(graph, oneof.fields, node_name, parent_type="oneof", visited=visited)


@contextlib.contextmanager
def _add_sys_path(*paths: str) -> Iterator[None]:
    try:
        for path in paths:
            sys.path.insert(0, path)
            yield
    finally:
        for path in paths:
            sys.path.remove(path)


def _create_schema_field(path: List[str], field: FieldDescriptor) -> _PathAndField:
    field_path = ".".join(path)
    schema_field = SchemaField(
        fieldPath=".".join(path),
        nativeDataType=_get_simple_native_type(field),
        # Protobuf field are always nullable
        nullable=True,
        type=_get_column_type(field),
    )
    return _PathAndField(field_path, schema_field)


def _from_protobuf_schema_to_descriptors(
    main_schema: ProtobufSchema, imported_schemas: Optional[List[ProtobufSchema]] = None
) -> FileDescriptor:
    if imported_schemas is None:
        imported_schemas = []
    imported_schemas.insert(0, main_schema)
    with TemporaryDirectory() as tmpdir, _add_sys_path(tmpdir):
        for schema in imported_schemas:  # type: ProtobufSchema
            #
            # Ignore any google/protobuf modules
            #
            if not schema.name.startswith("google/protobuf"):
                #
                # This is just in case one of the referenced schemas has '/' in their name
                #
                full_path = os.path.join(tmpdir, schema.name)
                Path(full_path).parent.mkdir(parents=True, exist_ok=True)
                #
                with open(full_path, "w") as temp_file:
                    temp_file.writelines(schema.content)

        return grpc.protos(main_schema.name).DESCRIPTOR


def _get_column_type(descriptor: DescriptorBase) -> SchemaFieldDataType:
    native_type: str = _get_simple_native_type(descriptor)
    type_class: Any
    if getattr(descriptor, "label", None) == FieldDescriptor.LABEL_REPEATED:
        type_class = ArrayTypeClass(nestedType=[native_type])
    elif getattr(descriptor, "type", None) == FieldDescriptor.TYPE_ENUM:
        type_class = EnumTypeClass()
    #
    # TODO: Find a better way to detect maps
    #
    # elif simple_type == "map":
    #    type_class = MapTypeClass(
    #        keyType=descriptor.key_type,
    #        valueType=descriptor.val_type,
    #    )
    else:
        type_class = _native_type_to_typeclass.get(native_type, RecordTypeClass)()

    return SchemaFieldDataType(type=type_class)


def _get_field_path_type(descriptor: DescriptorBase) -> str:
    if isinstance(descriptor, Descriptor):
        return _sanitise_type(descriptor.full_name)
    elif isinstance(descriptor, EnumDescriptor):
        return "enum"
    elif isinstance(descriptor, FieldDescriptor):
        if descriptor.message_type:
            return _sanitise_type(descriptor.message_type.full_name)
        else:
            return _protobuf_type_to_schema_type[descriptor.type]
    elif isinstance(descriptor, OneofDescriptor):
        return "union"
    else:
        raise ValueError(f"Unknown descriptor type: {type(descriptor)}")


def _get_node_name(descriptor: DescriptorBase) -> str:
    if isinstance(descriptor, FieldDescriptor):
        if descriptor.message_type:
            return descriptor.message_type.full_name
        else:
            return _protobuf_type_to_schema_type[descriptor.type]
    elif isinstance(descriptor, (Descriptor, EnumDescriptor, OneofDescriptor)):
        return descriptor.full_name
    else:
        raise ValueError(f"Unknown descriptor type: {type(descriptor)}")


def _get_simple_native_type(descriptor: DescriptorBase) -> str:
    if isinstance(descriptor, FieldDescriptor):
        if descriptor.message_type:
            return descriptor.message_type.full_name
        elif descriptor.enum_type:
            return descriptor.enum_type.full_name
        else:
            return _protobuf_type_to_native_type[descriptor.type]
    elif isinstance(descriptor, OneofDescriptor):
        return "oneof"
    elif isinstance(descriptor, (Descriptor, EnumDescriptor)):
        return descriptor.full_name
    else:
        raise ValueError(f"Unknown descriptor type: {type(descriptor)}")


def _get_type_ascription(descriptor: DescriptorBase) -> str:
    return_list: List[str] = []

    if (
        isinstance(descriptor, FieldDescriptor)
        and descriptor.label == FieldDescriptor.LABEL_REPEATED
    ):
        return_list.append("[type=array]")

    return_list.append(f"[type={_get_field_path_type(descriptor)}]")

    return ".".join(return_list)


def _populate_graph(descriptor: FileDescriptor) -> nx.DiGraph:
    graph = nx.DiGraph()
    visited: Set[str] = set()

    for message in descriptor.message_types_by_name.values():
        _add_message(graph, message, visited)

    return graph


def _sanitise_type(name: str) -> str:
    sanitised: str = name if name[0] != "." else name[1:]
    return sanitised.replace(".", "_")


def _schema_fields_from_dag(
    graph: nx.DiGraph, is_key_schema: bool
) -> List[SchemaField]:
    generations: List = list(nx.algorithms.dag.topological_generations(graph))
    fields: Dict = {}

    if generations and generations[0]:
        roots = generations[0]
        leafs: List = [node for node in graph if graph.out_degree(node) == 0]
        type_of_nodes: Dict = nx.get_node_attributes(graph, "node_type")

        for root in roots:
            root_type = type_of_nodes[root]
            for leaf in leafs:
                paths = list(nx.all_simple_edge_paths(graph, root, leaf))
                if paths:
                    for path in paths:
                        stack: List[str] = ["[version=2.0]"]
                        if is_key_schema:
                            stack.append("[key=True]")
                        stack.append(root_type)
                        if len(roots) > 1:
                            stack.append(re.sub(r"^.*\.", "", root))
                            root_path = ".".join(stack)
                            fields[root_path] = SchemaField(
                                fieldPath=root_path,
                                nativeDataType="message",
                                type=SchemaFieldDataType(type=RecordTypeClass()),
                            )
                        for field in _traverse_path(graph, path, stack):
                            fields[field.path] = field.field

    return sorted(fields.values(), key=lambda sf: sf.fieldPath)


def _traverse_path(
    graph: nx.DiGraph, path: List[Tuple[str, str]], stack: List[str]
) -> Generator[_PathAndField, None, None]:
    if path:
        src, dst = path[0]
        for field in graph[src][dst]["fields"]:
            copy_of_stack: List[str] = deepcopy(stack)
            type_ascription: str = _get_type_ascription(field)
            copy_of_stack.append(f"{type_ascription}.{field.name}")
            yield _create_schema_field(copy_of_stack, field)
            yield from _traverse_path(graph, path[1:], copy_of_stack)
