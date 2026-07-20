from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union

from datahub.ingestion.source.common.subtypes import (
    BIContainerSubTypes,
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.utilities.str_enum import StrEnum


class FieldKind(StrEnum):
    SECRET = "secret"
    PATTERN = "pattern"
    NESTED = "nested"
    PLAIN = "plain"


class ProbeLeafKind(StrEnum):
    # Column is a schema field, not an entity subtype, so it is not in the subtype enums.
    COLUMN = "Column"


# A probe node's kind reuses DataHub's own subtype taxonomy so the probe speaks the same
# vocabulary as ingestion. All members are StrEnum, so they serialize to their string value.
ProbeNodeKind = Union[
    DatasetContainerSubTypes,
    DatasetSubTypes,
    BIContainerSubTypes,
    ProbeLeafKind,
]


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

    def to_dict(self) -> Dict[str, object]:
        return {
            "name": self.name,
            "kind": str(self.kind),
            "fqn": self.fqn,
            "pattern_field": self.pattern_field,
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
