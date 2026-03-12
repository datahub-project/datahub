import re
from dataclasses import dataclass
from typing import Iterable, Set


REF_PATTERN = re.compile(r"\$\{([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\}")
PLAIN_PATTERN = re.compile(r"\b([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\b")


@dataclass(frozen=True)
class FieldRef:
    view: str
    field: str


def extract_field_refs(expression: str) -> Set[FieldRef]:
    """Extract Omni field references from a SQL/expression string."""
    refs: Set[FieldRef] = set()
    if not expression:
        return refs

    for view, field in REF_PATTERN.findall(expression):
        refs.add(FieldRef(view=view, field=field))

    for view, field in PLAIN_PATTERN.findall(expression):
        refs.add(FieldRef(view=view, field=field))

    return refs


def normalize_field_name(raw: str) -> str:
    value = raw.strip()
    if value.startswith("${") and value.endswith("}"):
        value = value[2:-1]
    return value


def parse_field_list(fields: Iterable[str]) -> Set[FieldRef]:
    parsed: Set[FieldRef] = set()
    for field in fields:
        normalized = normalize_field_name(field)
        if "." not in normalized:
            continue
        view, field_name = normalized.split(".", 1)
        parsed.add(FieldRef(view=view, field=field_name))
    return parsed
