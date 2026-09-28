from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional


@dataclass(frozen=True)
class QueryVariableSpec:
    name: str
    type: str


@dataclass(frozen=True)
class ExpandTypeSpec:
    parent_path: str
    type: str


@dataclass(frozen=True)
class FieldOverrideSpec:
    field: Optional[str] = None
    alias: Optional[str] = None
    args: Optional[Dict[str, Any]] = None


@dataclass(frozen=True)
class QuerySpec:
    operation_name: str
    root: str
    paths: List[str]
    variables: List[QueryVariableSpec]
    root_args: Dict[str, str]
    expand_types: List[ExpandTypeSpec]
    field_overrides: Dict[str, FieldOverrideSpec]
    required_paths: List[str]


def _parse_variables(raw: object) -> List[QueryVariableSpec]:
    if not raw:
        return []
    if not isinstance(raw, list):
        raise ValueError("query spec variables must be a list")
    result: List[QueryVariableSpec] = []
    for entry in raw:
        if not isinstance(entry, dict):
            raise ValueError("each variable spec must be an object")
        result.append(
            QueryVariableSpec(
                name=str(entry["name"]),
                type=str(entry["type"]),
            )
        )
    return result


def _parse_expand_types(raw: object) -> List[ExpandTypeSpec]:
    if not raw:
        return []
    if not isinstance(raw, list):
        raise ValueError("query spec expand_types must be a list")
    result: List[ExpandTypeSpec] = []
    for entry in raw:
        if not isinstance(entry, dict):
            raise ValueError("each expand_types entry must be an object")
        result.append(
            ExpandTypeSpec(
                parent_path=str(entry["parent_path"]),
                type=str(entry["type"]),
            )
        )
    return result


def _parse_field_overrides(raw: object) -> Dict[str, FieldOverrideSpec]:
    if not raw:
        return {}
    if not isinstance(raw, dict):
        raise ValueError("query spec field_overrides must be an object")
    result: Dict[str, FieldOverrideSpec] = {}
    for path, entry in raw.items():
        if not isinstance(entry, dict):
            raise ValueError(f"field_overrides[{path!r}] must be an object")
        result[str(path)] = FieldOverrideSpec(
            field=str(entry["field"]) if entry.get("field") else None,
            alias=str(entry["alias"]) if entry.get("alias") else None,
            args=entry.get("args") if isinstance(entry.get("args"), dict) else None,
        )
    return result


def parse_query_spec(operation: str, raw: Mapping[str, Any]) -> QuerySpec:
    paths_raw = raw.get("paths")
    if not isinstance(paths_raw, list) or not paths_raw:
        raise ValueError(f"query_specs[{operation!r}] must define non-empty paths")
    operation_name = str(raw.get("operation_name", operation))
    root = str(raw.get("root", ""))
    if not root:
        raise ValueError(f"query_specs[{operation!r}] must define root")
    root_args_raw = raw.get("root_args", {})
    if not isinstance(root_args_raw, dict):
        raise ValueError(f"query_specs[{operation!r}] root_args must be an object")
    return QuerySpec(
        operation_name=operation_name,
        root=root,
        paths=[str(p) for p in paths_raw],
        variables=_parse_variables(raw.get("variables")),
        root_args={str(k): str(v) for k, v in root_args_raw.items()},
        expand_types=_parse_expand_types(raw.get("expand_types")),
        field_overrides=_parse_field_overrides(raw.get("field_overrides")),
        required_paths=[str(p) for p in raw.get("required_paths", [])],
    )


def parse_query_specs(raw: Mapping[str, Any]) -> Dict[str, QuerySpec]:
    specs_raw = raw.get("query_specs")
    if not isinstance(specs_raw, dict) or not specs_raw:
        raise ValueError("benchmarks.json must define non-empty query_specs")
    return {
        str(operation): parse_query_spec(str(operation), entry)
        for operation, entry in specs_raw.items()
        if isinstance(entry, dict)
    }
