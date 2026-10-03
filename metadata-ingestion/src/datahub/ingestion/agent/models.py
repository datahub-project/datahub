from dataclasses import dataclass
from typing import Dict, List, Optional

from datahub.utilities.str_enum import StrEnum


class FieldKind(StrEnum):
    SECRET = "secret"
    PATTERN = "pattern"
    NESTED = "nested"
    PLAIN = "plain"


# The subtype a probe command says its names are: a member of
# datahub.ingestion.source.common.subtypes where one exists, so probe output
# speaks ingestion's vocabulary, else any string. Marks intent at signatures.
ProbeNodeKind = str


@dataclass
class FieldSpec:
    name: str
    kind: FieldKind
    required: bool
    type_name: str
    default: Optional[object]
    description: Optional[str]
    # For an AllowDenyPattern field, the hierarchy level it filters, resolved
    # as `probe filter` resolves it (introspect._filter_kinds_by_field). None
    # for a non-pattern or a pattern gating no level (profile_pattern).
    filters: Optional[str] = None

    def to_dict(self) -> Dict[str, object]:
        return {
            "name": self.name,
            "kind": str(self.kind),
            "required": self.required,
            "type_name": self.type_name,
            "default": self.default,
            "description": self.description,
            "filters": self.filters,
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
