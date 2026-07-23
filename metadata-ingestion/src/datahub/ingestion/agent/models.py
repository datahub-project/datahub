from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union

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
ProbeNodeKind = Union[StrEnum, str]


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

    def to_dict(self) -> Dict[str, object]:
        return {
            "source_type": self.source_type,
            "supported": self.supported,
            "parent_path": self.parent_path,
            "nodes": [n.to_dict() for n in self.nodes],
            "truncated": self.truncated,
            "fallback": self.fallback,
        }
