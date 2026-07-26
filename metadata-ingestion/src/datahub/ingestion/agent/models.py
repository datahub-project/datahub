from dataclasses import dataclass, field
from typing import Dict, List, Optional

from datahub.utilities.str_enum import StrEnum


class FieldKind(StrEnum):
    SECRET = "secret"
    PATTERN = "pattern"
    NESTED = "nested"
    PLAIN = "plain"


class ProbeLeafKind(StrEnum):
    # Column is a schema field, not an entity subtype, so it is not in the subtype enums.
    COLUMN = "Column"


# A probe node's kind SHOULD be a StrEnum member from DataHub's shared subtype
# taxonomy (datahub.ingestion.source.common.subtypes) so probe output speaks the
# same vocabulary as ingestion. The type stays open (StrEnum | str) so a connector
# can name a kind that has no shared-subtype member yet without editing a central
# union — prefer an existing subtype where one fits, else a plain descriptive string.
#
# StrEnum is `class StrEnum(str, Enum)`, so StrEnum | str is exactly str to a type
# checker — this alias carries no static information beyond "a string". One real
# consequence follows from it: because ProbeLeafKind.COLUMN == "Column" is True (str
# equality, not identity), a connector that spells its kind as the bare string
# "Column" instead of the enum member hits the exact same code paths (e.g.
# ClientProbe._resolved's `kind == ProbeLeafKind.COLUMN` check) as one that imports
# and uses the enum.
ProbeNodeKind = str


@dataclass
class FieldSpec:
    name: str
    kind: FieldKind
    required: bool
    type_name: str
    default: Optional[object]
    description: Optional[str]

    def to_dict(self) -> Dict[str, object]:
        return {
            "name": self.name,
            "kind": str(self.kind),
            "required": self.required,
            "type_name": self.type_name,
            "default": self.default,
            "description": self.description,
        }


@dataclass
class SourceSpec:
    source_type: str
    fields: List[FieldSpec]
    capabilities: List[Dict[str, object]]

    def to_dict(self) -> Dict[str, object]:
        return {
            "source_type": self.source_type,
            "fields": [f.to_dict() for f in self.fields],
            "capabilities": self.capabilities,
        }


@dataclass
class ProbeNode:
    name: str
    kind: ProbeNodeKind
    fqn: str
    pattern_field: Optional[str]
    # Whether this node would be ingested given the recipe's filters plus the
    # source's built-in exclusions; excluded_by names the reason it was dropped
    # (a *_pattern field, "default_schema", or "system_object"), else None.
    #
    # One reason is different in kind: "unnamed" is NOT a prediction about
    # ingestion at all. It means the source API handed back a node with no
    # usable name (a null/blank name), so the probe could neither filter it
    # (AllowDenyPattern.allowed raises on a non-string) nor address it as a
    # --parent qualifier -- a statement about what the probe could address,
    # not about what ingestion will do. A node with excluded_by="unnamed" may
    # or may not actually be ingested; the probe genuinely doesn't know,
    # since it couldn't run the normal filter logic on it at all.
    included: bool = True
    excluded_by: Optional[str] = None

    def to_dict(self) -> Dict[str, object]:
        return {
            "name": self.name,
            "kind": str(self.kind),
            "fqn": self.fqn,
            "pattern_field": self.pattern_field,
            "included": self.included,
            "excluded_by": self.excluded_by,
        }


@dataclass
class ProbeResult:
    source_type: str
    supported: bool
    parent_path: List[str]
    nodes: List[ProbeNode] = field(default_factory=list)
    truncated: bool = False
    fallback: Optional[str] = None
    # Non-fatal problems hit while listing (see agent.probe.ProbeSoftError):
    # one endpoint/sibling level couldn't be read cleanly (a 404/403), so it
    # contributed zero nodes instead of aborting the whole call. An empty (or
    # smaller-than-expected) `nodes` alongside a non-empty `warnings` means
    # "couldn't check part of this", not "confirmed empty" -- callers should
    # treat those two situations differently.
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, object]:
        return {
            "source_type": self.source_type,
            "supported": self.supported,
            "parent_path": self.parent_path,
            "nodes": [n.to_dict() for n in self.nodes],
            "truncated": self.truncated,
            "fallback": self.fallback,
            "warnings": self.warnings,
        }


@dataclass
class ProbeShapeNode:
    """One level and the levels declared beneath it."""

    kind: ProbeNodeKind
    children: List["ProbeShapeNode"]

    def to_dict(self) -> Dict[str, object]:
        return {
            "kind": str(self.kind),
            "children": [child.to_dict() for child in self.children],
        }
