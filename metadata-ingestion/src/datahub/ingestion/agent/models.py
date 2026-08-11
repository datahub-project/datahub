from dataclasses import dataclass
from typing import Dict, List, Optional

from datahub.utilities.str_enum import StrEnum


class FieldKind(StrEnum):
    SECRET = "secret"
    PATTERN = "pattern"
    NESTED = "nested"
    PLAIN = "plain"


# The subtype a probe command says its names are. SHOULD be a StrEnum member from
# DataHub's shared subtype taxonomy (datahub.ingestion.source.common.subtypes) so
# probe output speaks the same vocabulary as ingestion, but stays open so a connector
# can name a kind with no shared member yet -- Hex categories are not a DataHub
# entity. Since StrEnum subclasses str, this alias carries no static information
# beyond "a string"; it marks the intent at signature sites.
ProbeNodeKind = str


@dataclass
class FieldSpec:
    name: str
    kind: FieldKind
    required: bool
    type_name: str
    default: Optional[object]
    description: Optional[str]
    # For an AllowDenyPattern field, the hierarchy level it filters, when the
    # config declares one via Filters(...). None means either "not a pattern" or
    # "a pattern that does not gate a level" -- profile_pattern and
    # user_email_pattern are real filters but not levels, and a caller walking a
    # source needs to tell those apart. Never guessed from the field name: a
    # wrong answer here would send a caller to edit the wrong line.
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
