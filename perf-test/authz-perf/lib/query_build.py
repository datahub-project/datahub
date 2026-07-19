from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Set, Tuple, Union

from graphql import (
    BooleanValueNode,
    FieldNode,
    GraphQLField,
    GraphQLInterfaceType,
    GraphQLList,
    GraphQLNamedType,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLSchema,
    GraphQLUnionType,
    InlineFragmentNode,
    IntValueNode,
    ListValueNode,
    NamedTypeNode,
    NameNode,
    ObjectFieldNode,
    ObjectValueNode,
    OperationDefinitionNode,
    OperationType,
    SelectionSetNode,
    StringValueNode,
    VariableDefinitionNode,
    VariableNode,
    print_ast,
)
from graphql.language.ast import ValueNode

from lib.query_spec import ExpandTypeSpec, QuerySpec

try:
    from datahub.utilities.graphql_query_adapter import (
        RequiredFieldUnsupportedError,
        _minify_printed_graphql,
    )
except ImportError as exc:
    raise RuntimeError(
        "query_build requires acryl-datahub with graphql_query_adapter"
    ) from exc


GraphQLType = Union[
    GraphQLObjectType,
    GraphQLInterfaceType,
    GraphQLUnionType,
    GraphQLList,
    GraphQLNonNull,
]


def _unwrap_type(gql_type: GraphQLType) -> GraphQLNamedType:
    current = gql_type
    while isinstance(current, (GraphQLNonNull, GraphQLList)):
        current = current.of_type
    return current  # type: ignore[return-value]


def _schema_type_names(schema: GraphQLSchema) -> Set[str]:
    return set(schema.type_map.keys())


def _object_or_interface_fields(
    gql_type: GraphQLNamedType,
) -> Mapping[str, GraphQLField]:
    if isinstance(gql_type, GraphQLObjectType):
        return gql_type.fields
    if isinstance(gql_type, GraphQLInterfaceType):
        return gql_type.fields
    return {}


def _type_implements_or_equals(
    schema: GraphQLSchema, gql_type: GraphQLNamedType, type_name: str
) -> bool:
    if gql_type.name == type_name:
        return True
    if isinstance(gql_type, GraphQLObjectType):
        for iface in gql_type.interfaces:
            if iface.name == type_name:
                return True
    if isinstance(gql_type, GraphQLUnionType):
        for member in gql_type.types:
            if _type_implements_or_equals(schema, member, type_name):
                return True
    return False


def _resolve_field_type(
    schema: GraphQLSchema,
    parent_type: GraphQLNamedType,
    field_name: str,
) -> Optional[GraphQLNamedType]:
    fields = _object_or_interface_fields(parent_type)
    field = fields.get(field_name)
    if field is None:
        return None
    return _unwrap_type(field.type)


def _is_scalar_or_enum(gql_type: GraphQLNamedType) -> bool:
    kind = gql_type.ast_node.kind if gql_type.ast_node else None

    if kind in ("ScalarTypeDefinition", "EnumTypeDefinition"):
        return True
    return gql_type.name in {
        "String",
        "Int",
        "Float",
        "Boolean",
        "ID",
    }


def _expand_type_paths(
    schema: GraphQLSchema,
    expand: ExpandTypeSpec,
) -> List[str]:
    gql_type = schema.type_map.get(expand.type)
    if gql_type is None or not isinstance(gql_type, GraphQLObjectType):
        return []
    prefix = expand.parent_path.rstrip(".")
    paths: List[str] = []
    for field_name, field in gql_type.fields.items():
        if _is_scalar_or_enum(_unwrap_type(field.type)):
            paths.append(f"{prefix}.{field_name}")
    return paths


def _classify_path(
    path: str,
    spec: QuerySpec,
    schema_type_names: Set[str],
) -> str:
    first = path.split(".", 1)[0]
    if first == spec.root:
        return "root"
    if first in schema_type_names:
        return "type"
    raise ValueError(f"Path {path!r} must start with root {spec.root!r} or a schema type")


def _validate_root_path(
    schema: GraphQLSchema,
    spec: QuerySpec,
    path: str,
) -> bool:
    parts = path.split(".")
    if not parts or parts[0] != spec.root:
        return False
    query_type = _unwrap_type(schema.query_type)
    if not isinstance(query_type, GraphQLObjectType):
        return False
    current: Optional[GraphQLNamedType] = query_type
    for idx, segment in enumerate(parts):
        if idx == 0:
            if segment not in _object_or_interface_fields(current):
                return False
            current = _resolve_field_type(schema, current, segment)
            continue
        if current is None:
            return False
        fields = _object_or_interface_fields(current)
        if segment not in fields:
            return False
        if idx == len(parts) - 1:
            return True
        current = _resolve_field_type(schema, current, segment)
    return False


def _validate_type_path(schema: GraphQLSchema, path: str) -> bool:
    parts = path.split(".")
    type_name = parts[0]
    gql_type = schema.type_map.get(type_name)
    if gql_type is None or not isinstance(gql_type, (GraphQLObjectType, GraphQLInterfaceType)):
        return False
    current: Optional[GraphQLNamedType] = gql_type
    for idx, segment in enumerate(parts[1:], start=1):
        if current is None:
            return False
        fields = _object_or_interface_fields(current)
        if segment not in fields:
            return False
        if idx == len(parts) - 1:
            return True
        current = _resolve_field_type(schema, current, segment)
    return len(parts) == 1


def _collect_valid_paths(
    schema: GraphQLSchema,
    spec: QuerySpec,
) -> Tuple[List[str], List[str], List[str]]:
    schema_names = _schema_type_names(schema)
    expanded: List[str] = list(spec.paths)
    for expand in spec.expand_types:
        expanded.extend(_expand_type_paths(schema, expand))

    valid_root: List[str] = []
    valid_type: List[str] = []
    omitted: List[str] = []
    seen: Set[str] = set()
    for path in expanded:
        if path in seen:
            continue
        seen.add(path)
        try:
            kind = _classify_path(path, spec, schema_names)
        except ValueError:
            omitted.append(path)
            continue
        if kind == "root":
            if _validate_root_path(schema, spec, path):
                valid_root.append(path)
            else:
                omitted.append(path)
        else:
            if _validate_type_path(schema, path):
                valid_type.append(path)
            else:
                omitted.append(path)
    return valid_root, valid_type, omitted


@dataclass
class _TreeNode:
    children: Dict[str, "_TreeNode"] = field(default_factory=dict)
    is_leaf: bool = False


def _insert_path(tree: _TreeNode, segments: List[str]) -> None:
    node = tree
    for idx, segment in enumerate(segments):
        child = node.children.get(segment)
        if child is None:
            child = _TreeNode()
            node.children[segment] = child
        if idx == len(segments) - 1:
            child.is_leaf = True
        node = child


def _build_tree(paths: List[str], prefix_len: int) -> _TreeNode:
    root = _TreeNode()
    for path in paths:
        parts = path.split(".")
        if len(parts) <= prefix_len:
            continue
        _insert_path(root, parts[prefix_len:])
    return root


def _json_to_graphql_value(value: Any) -> ValueNode:
    if isinstance(value, str) and value.startswith("$"):
        return VariableNode(name=NameNode(value=value[1:]))
    if isinstance(value, bool):
        return BooleanValueNode(value=value)
    if isinstance(value, int):
        return IntValueNode(value=str(value))
    if isinstance(value, str):
        return StringValueNode(value=value)
    if isinstance(value, list):
        return ListValueNode(values=[_json_to_graphql_value(v) for v in value])
    if isinstance(value, dict):
        return ObjectValueNode(
            fields=[
                ObjectFieldNode(
                    name=NameNode(value=str(k)),
                    value=_json_to_graphql_value(v),
                )
                for k, v in value.items()
            ]
        )
    return StringValueNode(value=json.dumps(value))


def _args_for_path(
    path_prefix: str,
    spec: QuerySpec,
    parent_segments: List[str],
) -> Tuple[Optional[NameNode], List[ObjectFieldNode]]:
    full_path = ".".join(parent_segments)
    override = spec.field_overrides.get(full_path)
    if override is None:
        return None, []
    alias = NameNode(value=override.alias) if override.alias else None
    arg_nodes: List[ObjectFieldNode] = []
    if override.args:
        for arg_name, arg_value in override.args.items():
            arg_nodes.append(
                ObjectFieldNode(
                    name=NameNode(value=arg_name),
                    value=_json_to_graphql_value(arg_value),
                )
            )
    return alias, arg_nodes


def _tree_to_selection_set(
    tree: _TreeNode,
    schema: GraphQLSchema,
    current_type: GraphQLNamedType,
    path_prefix: List[str],
    spec: QuerySpec,
    type_paths: Dict[str, List[str]],
) -> SelectionSetNode:
    selections: List[Union[FieldNode, InlineFragmentNode]] = []
    for segment, child in sorted(tree.children.items()):
        child_segments = path_prefix + [segment]
        full_path = ".".join(child_segments)
        override = spec.field_overrides.get(full_path)
        graphql_field = override.field if override and override.field else segment
        field_type = _resolve_field_type(schema, current_type, graphql_field)
        if field_type is None:
            continue
        alias, arg_nodes = _args_for_path(full_path, spec, child_segments)
        if child.children:
            sub = _tree_to_selection_set(
                child,
                schema,
                field_type,
                child_segments,
                spec,
                type_paths,
            )
            field_node = FieldNode(
                alias=alias,
                name=NameNode(value=graphql_field),
                arguments=arg_nodes,
                selection_set=sub,
            )
            selections.append(field_node)
            if graphql_field == "entity":
                selections.extend(
                    _inline_fragments_for_entity(
                        schema, field_type, type_paths, spec
                    )
                )
        else:
            selections.append(
                FieldNode(
                    alias=alias,
                    name=NameNode(value=graphql_field),
                    arguments=arg_nodes,
                )
            )
    return SelectionSetNode(selections=selections)


def _inline_fragments_for_entity(
    schema: GraphQLSchema,
    entity_type: GraphQLNamedType,
    type_paths: Dict[str, List[str]],
    spec: QuerySpec,
) -> List[InlineFragmentNode]:
    fragments: List[InlineFragmentNode] = []
    for type_name, paths in sorted(type_paths.items()):
        if not _type_implements_or_equals(schema, entity_type, type_name):
            continue
        gql_type = schema.type_map.get(type_name)
        if gql_type is None or not isinstance(
            gql_type, (GraphQLObjectType, GraphQLInterfaceType)
        ):
            continue
        tree = _build_tree(paths, prefix_len=1)
        if not tree.children:
            continue
        fragments.append(
            InlineFragmentNode(
                type_condition=NamedTypeNode(name=NameNode(value=type_name)),
                selection_set=_tree_to_selection_set(
                    tree,
                    schema,
                    gql_type,
                    [type_name],
                    spec,
                    {},
                ),
            )
        )
    return fragments


def _group_type_paths(type_paths: List[str]) -> Dict[str, List[str]]:
    grouped: Dict[str, List[str]] = {}
    for path in type_paths:
        type_name = path.split(".", 1)[0]
        grouped.setdefault(type_name, []).append(path)
    return grouped


def build_operation_document(
    spec: QuerySpec,
    schema: GraphQLSchema,
) -> Tuple[str, List[str]]:
    valid_root, valid_type, omitted = _collect_valid_paths(schema, spec)
    if spec.required_paths:
        missing = [
            req
            for req in spec.required_paths
            if req not in set(valid_root) | set(valid_type)
        ]
        if missing:
            raise RequiredFieldUnsupportedError(missing)

    query_type = _unwrap_type(schema.query_type)
    if not isinstance(query_type, GraphQLObjectType):
        raise RuntimeError("Schema query type is not an object type")
    root_field_type = _resolve_field_type(schema, query_type, spec.root)
    if root_field_type is None:
        raise RuntimeError(f"Root field {spec.root!r} not found on Query type")

    root_tree = _build_tree(valid_root, prefix_len=1)
    type_grouped = _group_type_paths(valid_type)

    root_args: List[ObjectFieldNode] = []
    for arg_name, arg_value in spec.root_args.items():
        root_args.append(
            ObjectFieldNode(
                name=NameNode(value=arg_name),
                value=_json_to_graphql_value(arg_value),
            )
        )

    root_selection = _tree_to_selection_set(
        root_tree,
        schema,
        root_field_type,
        [spec.root],
        spec,
        type_grouped,
    )
    root_field = FieldNode(
        name=NameNode(value=spec.root),
        arguments=root_args,
        selection_set=root_selection,
    )

    var_defs: List[VariableDefinitionNode] = []
    for var in spec.variables:
        var_defs.append(
            VariableDefinitionNode(
                variable=VariableNode(name=NameNode(value=var.name)),
                type=parse_type(var.type),
            )
        )

    operation = OperationDefinitionNode(
        operation=OperationType.QUERY,
        name=NameNode(value=spec.operation_name),
        variable_definitions=var_defs,
        selection_set=SelectionSetNode(selections=[root_field]),
    )
    document = _minify_printed_graphql(print_ast(operation))
    return document, omitted


def parse_type(type_str: str) -> object:
    """Minimal GraphQL type reference parser for variable definitions."""
    from graphql import ListTypeNode, NamedTypeNode, NonNullTypeNode

    if type_str.endswith("!"):
        return NonNullTypeNode(type=parse_type(type_str[:-1]))
    if type_str.startswith("[") and type_str.endswith("]"):
        return ListTypeNode(type=parse_type(type_str[1:-1]))
    return NamedTypeNode(name=NameNode(value=type_str))


def build_all_operations(
    specs: Dict[str, QuerySpec],
    schema: GraphQLSchema,
) -> Dict[str, Tuple[str, List[str]]]:
    return {
        operation: build_operation_document(spec, schema)
        for operation, spec in specs.items()
    }
