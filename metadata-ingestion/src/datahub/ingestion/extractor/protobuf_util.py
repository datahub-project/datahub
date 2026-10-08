import contextlib
import hashlib
import json
import logging
import os
import re
import subprocess
import sys
import threading
from copy import deepcopy
from dataclasses import dataclass, field as dataclass_field
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

import grpc
import grpc.experimental
import grpc_tools
import networkx as nx
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from google.protobuf.descriptor import (
    Descriptor,
    DescriptorBase,
    EnumDescriptor,
    FieldDescriptor,
    FileDescriptor,
    OneofDescriptor,
)
from google.protobuf.json_format import MessageToDict

from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    EnumTypeClass,
    FixedTypeClass,
    MapTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaFieldClass as SchemaField,
    SchemaFieldDataTypeClass as SchemaFieldDataType,
    StringTypeClass,
    UnionTypeClass,
)

"""A helper file for Protobuf schema -> MCE schema transformations"""

logger = logging.getLogger(__name__)

_DESCRIPTOR_CACHE: Dict[str, Optional[FileDescriptor]] = {}
# protobuf compilation registers symbols in a process-global descriptor pool, so
# serialize cache access and compilation across profiling/schema-inference threads.
_DESCRIPTOR_CACHE_LOCK = threading.Lock()
_PROTOC_TIMEOUT_SECONDS = 60

GOOGLE_TYPE_DEFINITIONS = {
    "google/type/date.proto": """
syntax = "proto3";

package google.type;

option cc_enable_arenas = true;
option go_package = "google.golang.org/genproto/googleapis/type/date;date";
option java_multiple_files = true;
option java_outer_classname = "DateProto";
option java_package = "com.google.type";
option objc_class_prefix = "GTP";

// Represents a whole or partial calendar date, such as a birthday. The time of
// day and time zone are either specified elsewhere or are insignificant. The
// date is relative to the Gregorian Calendar. This can represent one of the
// following:
//
// * A full date, with non-zero year, month, and day values
// * A month and day value, with a zero year, such as an anniversary
// * A year on its own, with zero month and day values
// * A year and month value, with a zero day, such as a credit card expiration date
//
// Related types are [google.type.TimeOfDay][google.type.TimeOfDay] and
// `google.protobuf.Timestamp`.
message Date {
  // Year of the date. Must be from 1 to 9999, or 0 to specify a date without
  // a year.
  int32 year = 1;

  // Month of a year. Must be from 1 to 12, or 0 to specify a year without a
  // month and day.
  int32 month = 2;

  // Day of a month. Must be from 1 to 31 and valid for the year and month, or 0
  // to specify a year by itself or a year and month where the day isn't
  // significant.
  int32 day = 3;
}
""",
    "google/type/decimal.proto": """
syntax = "proto3";

package google.type;

option cc_enable_arenas = true;
option go_package = "google.golang.org/genproto/googleapis/type/decimal;decimal";
option java_multiple_files = true;
option java_outer_classname = "DecimalProto";
option java_package = "com.google.type";
option objc_class_prefix = "GTP";

// A representation of a decimal value, such as 2.5. Clients may convert values
// into language-native decimal formats, such as Java's [BigDecimal][] or
// Python's [decimal.Decimal][].
//
// [BigDecimal]: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/math/BigDecimal.html
// [decimal.Decimal]: https://docs.python.org/3/library/decimal.html
message Decimal {
  // The decimal value, as a string.
  //
  // The string representation consists of an optional sign, `+` (`U+002B`)
  // or `-` (`U+002D`), followed by a sequence of zero or more decimal digits
  // ("the integer"), optionally followed by a fraction, optionally followed
  // by an exponent.
  //
  // The fraction consists of a decimal point followed by zero or more decimal
  // digits. The string must contain at least one digit in either the integer
  // or the fraction. The number formed by the sign, the integer and the
  // fraction is referred to as the significand.
  //
  // The exponent consists of the character `e` (`U+0065`) or `E` (`U+0045`)
  // followed by one or more decimal digits.
  //
  // Services **should** normalize decimal values before storing them by:
  //
  //   - Removing an explicitly-provided `+` sign (`+2.5` -> `2.5`).
  //   - Replacing a zero-length integer value with `0` (`.5` -> `0.5`).
  //   - Coercing the exponent character to upper-case, with explicit sign
  //     (`2.5e8` -> `2.5E+8`).
  //   - Removing an explicitly-provided zero exponent (`2.5E0` -> `2.5`).
  //
  // Services **may** perform additional normalization based on its own needs
  // and the internal decimal implementation selected, such as shifting the
  // decimal point and exponent value together (example: `2.5E-1` <-> `0.25`).
  // Additionally, services **may** preserve trailing zeroes in the fraction
  // to indicate increased precision, but are not required to do so.
  //
  // Note that only the `.` character is supported to divide the integer
  // and the fraction; `,` is not supported.
  string value = 1;
}
""",
}


# ------------------------------------------------------------------------------
#  API
#
@dataclass
class ProtobufSchema:
    name: str
    content: str


@dataclass
class ProtobufAnnotation:
    """Comment and custom options authored on a message or a field."""

    description: Optional[str] = None
    # Custom option values keyed by the option's full name, nested on its dots:
    # `option (acme.meta.event) = {owner: "x"}` becomes {"acme": {"meta": {"event": {"owner": "x"}}}}.
    props: Dict[str, Any] = dataclass_field(default_factory=dict)


@dataclass
class ProtobufAnnotations:
    # Keyed by message full name, and by (message full name, field name).
    messages: Dict[str, ProtobufAnnotation] = dataclass_field(default_factory=dict)
    fields: Dict[Tuple[str, str], ProtobufAnnotation] = dataclass_field(
        default_factory=dict
    )
    # The first message of the main file: what Confluent serializers write by default.
    main_message: Optional[str] = None


def get_protobuf_annotations(
    main_schema: ProtobufSchema, imported_schemas: Optional[List[ProtobufSchema]] = None
) -> ProtobufAnnotations:
    """Compile the schema with source info into an isolated descriptor pool and read the
    comments and custom options that the generated-module path used for fields drops."""
    try:
        file_set = _compile_with_source_info(main_schema, imported_schemas or [])
    except Exception as e:
        logger.debug(f"Could not read annotations of {main_schema.name}: {e}")
        return ProtobufAnnotations()

    pool = descriptor_pool.DescriptorPool()
    for file_proto in file_set.file:
        pool.Add(file_proto)
    # Builds a class for every message and registers every extension in this pool, so
    # option messages re-parsed below expose custom options as extension fields.
    message_factory.GetMessageClassesForFiles([f.name for f in file_set.file], pool)

    annotations = ProtobufAnnotations()
    main_file = next(f for f in file_set.file if f.name == main_schema.name)
    if main_file.message_type:
        annotations.main_message = _qualified(
            main_file.package, main_file.message_type[0].name
        )
    comments = {
        tuple(location.path): (
            location.leading_comments or location.trailing_comments
        ).strip()
        for location in main_file.source_code_info.location
        if location.leading_comments or location.trailing_comments
    }
    for index, message in enumerate(main_file.message_type):
        _collect_message_annotations(
            pool, annotations, comments, main_file.package, message, (4, index)
        )
    return annotations


def protobuf_schema_to_mce_fields(
    main_schema: ProtobufSchema,
    imported_schemas: Optional[List[ProtobufSchema]] = None,
    is_key_schema: bool = False,
    annotations: Optional[ProtobufAnnotations] = None,
) -> List[SchemaField]:
    """
    Converts a protobuf schema into a schema compatible with MCE
    :param protobuf_schema_string: String representation of the protobuf schema
    :param is_key_schema: True if it is a key-schema. Default is False (value-schema).
    :return: The list of MCE compatible SchemaFields.
    """
    descriptor = _from_protobuf_schema_to_descriptors(main_schema, imported_schemas)

    # Handle case where descriptor compilation failed
    if descriptor is None:
        logger.warning(
            f"Failed to compile protobuf schema {main_schema.name}, returning empty fields"
        )
        return []

    graph: nx.DiGraph = _populate_graph(descriptor)

    if nx.is_directed_acyclic_graph(graph):
        return _schema_fields_from_dag(graph, is_key_schema, annotations)
    else:
        logger.warning(
            f"Cyclic schema detected in {main_schema.name}, returning empty fields"
        )
        return []


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


def _create_schema_field(
    path: List[str],
    field: FieldDescriptor,
    annotations: Optional[ProtobufAnnotations] = None,
) -> _PathAndField:
    field_path = ".".join(path)
    annotation = (
        annotations.fields.get((field.containing_type.full_name, field.name))
        if annotations and field.containing_type
        else None
    )
    schema_field = SchemaField(
        fieldPath=".".join(path),
        nativeDataType=_get_simple_native_type(field),
        # Protobuf field are always nullable
        nullable=True,
        type=_get_column_type(field),
        description=annotation.description if annotation else None,
        jsonProps=json.dumps(annotation.props)
        if annotation and annotation.props
        else None,
    )
    return _PathAndField(field_path, schema_field)


def _qualified(package: str, name: str) -> str:
    return f"{package}.{name}" if package else name


def _compile_with_source_info(
    main_schema: ProtobufSchema, imported_schemas: List[ProtobufSchema]
) -> descriptor_pb2.FileDescriptorSet:
    well_known_types = os.path.join(os.path.dirname(grpc_tools.__file__), "_proto")
    with TemporaryDirectory() as tmpdir:
        for schema in [main_schema, *imported_schemas]:
            if schema.name.startswith("google/protobuf/"):
                continue  # shipped with grpc_tools
            full_path = Path(tmpdir, schema.name)
            full_path.parent.mkdir(parents=True, exist_ok=True)
            full_path.write_text(schema.content)
        out = Path(tmpdir, "descriptor_set.pb")
        # A separate process: grpc_tools' in-process compiler keeps global import state,
        # and running it here breaks the later grpc.protos() compile of the same files.
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "grpc_tools.protoc",
                f"-I{tmpdir}",
                f"-I{well_known_types}",
                "--include_imports",
                "--include_source_info",
                f"--descriptor_set_out={out}",
                main_schema.name,
            ],
            capture_output=True,
            text=True,
            timeout=_PROTOC_TIMEOUT_SECONDS,
        )
        if result.returncode != 0:
            raise ValueError(f"protoc exited with {result.returncode}: {result.stderr}")
        return descriptor_pb2.FileDescriptorSet.FromString(out.read_bytes())


def _option_props(
    pool: descriptor_pool.DescriptorPool, options_type: str, options: Any
) -> Dict[str, Any]:
    if not options.ByteSize():
        return {}
    options_class = message_factory.GetMessageClass(
        pool.FindMessageTypeByName(options_type)
    )
    parsed = options_class.FromString(options.SerializeToString())
    props: Dict[str, Any] = {}
    for option_field, value in parsed.ListFields():
        if not option_field.is_extension:
            continue
        if option_field.message_type is not None:
            value = MessageToDict(value, preserving_proto_field_name=True)
        *parents, leaf = option_field.full_name.split(".")
        node = props
        for part in parents:
            node = node.setdefault(part, {})
        node[leaf] = value
    return props


def _collect_message_annotations(
    pool: descriptor_pool.DescriptorPool,
    annotations: ProtobufAnnotations,
    comments: Dict[Tuple[int, ...], str],
    scope: str,
    message: descriptor_pb2.DescriptorProto,
    path: Tuple[int, ...],
) -> None:
    # SourceCodeInfo paths: 4 = FileDescriptorProto.message_type,
    # 3 = DescriptorProto.nested_type, 2 = DescriptorProto.field.
    full_name = _qualified(scope, message.name)
    annotations.messages[full_name] = ProtobufAnnotation(
        description=comments.get(path) or None,
        props=_option_props(pool, "google.protobuf.MessageOptions", message.options),
    )
    for index, message_field in enumerate(message.field):
        annotation = ProtobufAnnotation(
            description=comments.get((*path, 2, index)) or None,
            props=_option_props(
                pool, "google.protobuf.FieldOptions", message_field.options
            ),
        )
        if annotation.description or annotation.props:
            annotations.fields[(full_name, message_field.name)] = annotation
    for index, nested in enumerate(message.nested_type):
        _collect_message_annotations(
            pool, annotations, comments, full_name, nested, (*path, 3, index)
        )


def _from_protobuf_schema_to_descriptors(
    main_schema: ProtobufSchema, imported_schemas: Optional[List[ProtobufSchema]] = None
) -> Optional[FileDescriptor]:
    if imported_schemas is None:
        imported_schemas = []
    imported_schemas.insert(0, main_schema)

    all_schema_content = "\n".join([schema.content for schema in imported_schemas])

    cache_key = hashlib.md5(all_schema_content.encode()).hexdigest()
    with _DESCRIPTOR_CACHE_LOCK:
        if cache_key in _DESCRIPTOR_CACHE:
            cached_descriptor = _DESCRIPTOR_CACHE[cache_key]
            if cached_descriptor is not None:
                logger.debug(
                    f"Reusing cached descriptor for {main_schema.name} (hash: {cache_key[:8]}...)"
                )
            return cached_descriptor

        google_types_referenced = []

        if "google.type.Date" in all_schema_content and not any(
            schema.name == "google/type/date.proto" for schema in imported_schemas
        ):
            google_types_referenced.append("google/type/date.proto")

        if "google.type.Decimal" in all_schema_content and not any(
            schema.name == "google/type/decimal.proto" for schema in imported_schemas
        ):
            google_types_referenced.append("google/type/decimal.proto")

        for google_type_file in google_types_referenced:
            if google_type_file in GOOGLE_TYPE_DEFINITIONS:
                logger.info(f"Adding fallback definition for {google_type_file}")
                imported_schemas.append(
                    ProtobufSchema(
                        name=google_type_file,
                        content=GOOGLE_TYPE_DEFINITIONS[google_type_file],
                    )
                )

        with TemporaryDirectory() as tmpdir, _add_sys_path(tmpdir):
            for schema in imported_schemas:
                #
                # Ignore google/protobuf modules but allow google/type modules
                # which contain common types like google.type.Date and google.type.Decimal
                #
                should_skip_schema = schema.name.startswith("google/protobuf") or (
                    schema.name.startswith("google/")
                    and not schema.name.startswith("google/type")
                )
                if not should_skip_schema:
                    #
                    # This is just in case one of the referenced schemas has '/' in their name
                    #
                    full_path = os.path.join(tmpdir, schema.name)
                    Path(full_path).parent.mkdir(parents=True, exist_ok=True)
                    with open(full_path, "w") as temp_file:
                        temp_file.writelines(schema.content)

            try:
                descriptor = grpc.protos(main_schema.name).DESCRIPTOR
                _DESCRIPTOR_CACHE[cache_key] = descriptor
                return descriptor
            except Exception as e:
                error_msg = str(e)

                if "duplicate symbol" in error_msg.lower():
                    logger.debug(
                        f"Protobuf schema {main_schema.name} contains symbols already registered in "
                        f"global descriptor pool (hash: {cache_key[:8]}...). This typically occurs when "
                        f"multiple topics share the same schema. Schema fields will be unavailable."
                    )
                elif "google.type" in error_msg:
                    logger.warning(
                        f"Failed to compile protobuf schema {main_schema.name}: {e}"
                    )
                    logger.error(
                        f"Google type definition error in {main_schema.name}. "
                        f"This may indicate missing google/type imports in the schema registry."
                    )
                elif "descriptor pool" in error_msg.lower():
                    logger.warning(
                        f"Failed to compile protobuf schema {main_schema.name}: {e}"
                    )
                    logger.error(
                        f"Descriptor pool error in {main_schema.name}. "
                        f"This may indicate conflicting protobuf definitions or circular dependencies."
                    )
                else:
                    logger.warning(
                        f"Failed to compile protobuf schema {main_schema.name}: {e}"
                    )

                _DESCRIPTOR_CACHE[cache_key] = None
                return None


def _is_repeated_field(descriptor: DescriptorBase) -> bool:
    # protobuf 7.x removed FieldDescriptor.label in favor of is_repeated, while
    # protobuf 5.29.x only exposes label. Prefer is_repeated, fall back to label.
    # Non-field descriptors have neither, so this correctly returns False.
    is_repeated = getattr(descriptor, "is_repeated", None)
    if is_repeated is not None:
        return bool(is_repeated)
    return getattr(descriptor, "label", None) == FieldDescriptor.LABEL_REPEATED


def _get_column_type(descriptor: DescriptorBase) -> SchemaFieldDataType:
    native_type: str = _get_simple_native_type(descriptor)
    type_class: Any
    if _is_repeated_field(descriptor):
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

    if _is_repeated_field(descriptor):
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
    graph: nx.DiGraph,
    is_key_schema: bool,
    annotations: Optional[ProtobufAnnotations] = None,
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
                        for field in _traverse_path(graph, path, stack, annotations):
                            fields[field.path] = field.field

    return sorted(fields.values(), key=lambda sf: sf.fieldPath)


def _traverse_path(
    graph: nx.DiGraph,
    path: List[Tuple[str, str]],
    stack: List[str],
    annotations: Optional[ProtobufAnnotations] = None,
) -> Generator[_PathAndField, None, None]:
    if path:
        src, dst = path[0]
        for field in graph[src][dst]["fields"]:
            copy_of_stack: List[str] = deepcopy(stack)
            type_ascription: str = _get_type_ascription(field)
            copy_of_stack.append(f"{type_ascription}.{field.name}")
            yield _create_schema_field(copy_of_stack, field, annotations)
            yield from _traverse_path(graph, path[1:], copy_of_stack, annotations)
