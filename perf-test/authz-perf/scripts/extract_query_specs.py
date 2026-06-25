#!/usr/bin/env python3
"""Extract query_specs from legacy query_templates in benchmarks.json (one-time migration)."""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from graphql import (
    FieldNode,
    FragmentDefinitionNode,
    InlineFragmentNode,
    OperationDefinitionNode,
    parse,
)
from graphql.language.ast import ArgumentNode, DocumentNode, SelectionNode

PRIVILEGE_TYPES = frozenset({"PlatformPrivileges", "EntityPrivileges"})


def _structural_path(
    ancestors: List[object],
    current: FieldNode,
) -> str:
    last_inline_type: Optional[str] = None
    last_inline_idx = -1
    for i, ancestor in enumerate(ancestors):
        if (
            isinstance(ancestor, InlineFragmentNode)
            and ancestor.type_condition is not None
        ):
            last_inline_idx = i
            last_inline_type = ancestor.type_condition.name.value

    parts: List[str] = []
    if last_inline_type is not None:
        parts.append(last_inline_type)
    start = last_inline_idx + 1 if last_inline_idx >= 0 else 0
    for ancestor in ancestors[start:]:
        if isinstance(ancestor, FieldNode):
            alias_or_name = (
                ancestor.alias.value if ancestor.alias else ancestor.name.value
            )
            parts.append(alias_or_name)
    parts.append(current.name.value)
    return ".".join(parts)


def _root_structural_path(
    ancestors: List[object],
    current: FieldNode,
    root: str,
) -> str:
    parts: List[str] = []
    for ancestor in ancestors:
        if isinstance(ancestor, FieldNode):
            alias_or_name = (
                ancestor.alias.value if ancestor.alias else ancestor.name.value
            )
            parts.append(alias_or_name)
    parts.append(current.name.value)
    path = ".".join(parts)
    if not path.startswith(root + ".") and parts[0] != root:
        return path
    return path


def _arg_to_json(node: ArgumentNode) -> Any:
    value = node.value
    if hasattr(value, "values"):  # ListValueNode
        return [_arg_value_to_json(v) for v in value.values]
    return _arg_value_to_json(value)


def _arg_value_to_json(value: object) -> Any:
    cls = value.__class__.__name__
    if cls == "StringValueNode":
        return value.value  # type: ignore[attr-defined]
    if cls == "IntValueNode":
        return int(value.value)  # type: ignore[attr-defined]
    if cls == "BooleanValueNode":
        return value.value  # type: ignore[attr-defined]
    if cls == "VariableNode":
        return f"${value.name.value}"  # type: ignore[attr-defined]
    if cls == "ListValueNode":
        return [_arg_value_to_json(v) for v in value.values]  # type: ignore[attr-defined]
    if cls == "ObjectValueNode":
        return {
            f.name.value: _arg_value_to_json(f.value)
            for f in value.fields  # type: ignore[attr-defined]
        }
    if cls == "EnumValueNode":
        return value.value  # type: ignore[attr-defined]
    return str(value)


class _PathExtractor:
    def __init__(self, root: str) -> None:
        self.root = root
        self.root_paths: List[str] = []
        self.type_paths: List[str] = []
        self.field_overrides: Dict[str, Dict[str, Any]] = {}
        self.privilege_parents: Dict[str, str] = {}

    def visit_field(
        self,
        node: FieldNode,
        ancestors: Tuple[object, ...],
    ) -> None:
        path = _structural_path(list(ancestors), node)
        root_path = _root_structural_path(list(ancestors), node, self.root)
        parent_type = path.split(".")[0] if path else ""
        if parent_type in PRIVILEGE_TYPES and len(path.split(".")) == 2:
            parent = ".".join(root_path.split(".")[:-1])
            self.privilege_parents[parent_type] = parent
            return

        if path.split(".")[0] == self.root or (
            root_path.split(".")[0] == self.root if root_path else False
        ):
            use_path = root_path if root_path.split(".")[0] == self.root else path
            if node.selection_set is None:
                self.root_paths.append(use_path)
        elif parent_type not in PRIVILEGE_TYPES:
            if node.selection_set is None:
                self.type_paths.append(path)

        if node.arguments or node.alias:
            key = root_path if root_path.split(".")[0] == self.root else path
            override: Dict[str, Any] = {}
            if node.alias:
                override["alias"] = node.alias.value
            if node.arguments:
                override["args"] = {
                    arg.name.value: _arg_to_json(arg) for arg in node.arguments
                }
            if override:
                self.field_overrides[key] = override


def _walk_selections(
    selections: Tuple[SelectionNode, ...],
    extractor: _PathExtractor,
    ancestors: Tuple[object, ...],
) -> None:
    for sel in selections:
        if isinstance(sel, FieldNode):
            extractor.visit_field(sel, ancestors)
            if sel.selection_set:
                _walk_selections(
                    tuple(sel.selection_set.selections),
                    extractor,
                    ancestors + (sel,),
                )
        elif isinstance(sel, InlineFragmentNode) and sel.selection_set:
            _walk_selections(
                tuple(sel.selection_set.selections),
                extractor,
                ancestors + (sel,),
            )


def extract_from_template(
    operation_name: str,
    template: str,
) -> Dict[str, Any]:
    doc: DocumentNode = parse(template)
    op: Optional[OperationDefinitionNode] = None
    for defn in doc.definitions:
        if isinstance(defn, OperationDefinitionNode) and defn.name:
            if defn.name.value == operation_name:
                op = defn
                break
    if op is None or op.selection_set is None:
        raise ValueError(f"Operation {operation_name!r} not found in template")

    root_field = op.selection_set.selections[0]
    if not isinstance(root_field, FieldNode):
        raise ValueError("Expected single root field")
    root = root_field.name.value

    variables = []
    for var_def in op.variable_definitions or []:
        type_name = _type_node_to_str(var_def.type)
        variables.append({"name": var_def.variable.name.value, "type": type_name})

    root_args: Dict[str, str] = {}
    for arg in root_field.arguments:
        val = _arg_value_to_json(arg.value)
        if isinstance(val, str) and val.startswith("$"):
            root_args[arg.name.value] = val
        else:
            root_args[arg.name.value] = val  # type: ignore[assignment]

    extractor = _PathExtractor(root=root)
    _walk_selections(tuple(op.selection_set.selections), extractor, ())

    expand_types = [
        {"parent_path": parent, "type": ptype}
        for ptype, parent in sorted(extractor.privilege_parents.items())
    ]

    return {
        "operation_name": operation_name,
        "root": root,
        "variables": variables,
        "root_args": root_args,
        "paths": sorted(set(extractor.root_paths + extractor.type_paths)),
        "expand_types": expand_types,
        "field_overrides": extractor.field_overrides,
        "required_paths": ["me.corpUser.urn"] if operation_name == "getMe" else [],
    }


def _type_node_to_str(node: object) -> str:
    cls = node.__class__.__name__
    if cls == "NonNullTypeNode":
        return _type_node_to_str(node.type) + "!"  # type: ignore[attr-defined]
    if cls == "ListTypeNode":
        return "[" + _type_node_to_str(node.type) + "]"  # type: ignore[attr-defined]
    if cls == "NamedTypeNode":
        return node.name.value  # type: ignore[attr-defined]
    raise ValueError(f"Unknown type node {cls}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--benchmarks",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "fixture" / "benchmarks.json",
    )
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()

    raw = json.loads(args.benchmarks.read_text(encoding="utf-8"))
    templates = raw.get("query_templates", {})
    if not templates:
        print("No query_templates found", file=sys.stderr)
        return 1

    specs: Dict[str, Any] = {}
    for operation, template in templates.items():
        specs[operation] = extract_from_template(operation, template)
        print(f"extracted {operation}: {len(specs[operation]['paths'])} paths")

    if args.write:
        raw["schema_version"] = 3
        raw["query_specs"] = specs
        raw.pop("query_templates", None)
        args.benchmarks.write_text(json.dumps(raw, indent=2) + "\n", encoding="utf-8")
        print(f"wrote {args.benchmarks}")
    else:
        print(json.dumps({"query_specs": specs}, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
