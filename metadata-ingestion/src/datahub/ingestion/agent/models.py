from dataclasses import dataclass
from typing import Dict, List, Optional, TypedDict

from datahub.utilities.str_enum import StrEnum


class FieldKind(StrEnum):
    SECRET = "secret"
    PATTERN = "pattern"
    NESTED = "nested"
    PLAIN = "plain"


class Filtering(StrEnum):
    """What decided `probe filter`'s verdicts for a kind."""

    # A pattern field; the result's pattern_field names it.
    BY_PATTERN = "by_pattern"
    # A rule field, not a pattern, that probe_verdict_override judges.
    BY_RULE = "by_rule"
    # The source declares that nothing filters this kind.
    UNFILTERED = "unfiltered"
    # No field found and none declared absent: what a dropped annotation
    # looks like.
    UNRESOLVED = "unresolved"


class ProbeRunEnvelope(TypedDict):
    """What `probe run` prints and writes to --report-to
    (ProbeMethodResult.to_dict), and what `probe filter --from-run` reads back
    through ProbeRunEnvelopeView."""

    source_type: str
    command: str
    params: Dict[str, object]
    kind: Optional[str]
    parent_path: List[str]
    # The command's answer: for a listing, names, each bare or a record whose
    # "name" key holds it.
    result: object
    truncated: bool
    warnings: List[str]
    failures: List[str]


class ProbeRunEnvelopeView(TypedDict):
    """A ProbeRunEnvelope as read from JSON the caller supplies
    (filter_input.run_envelope_view): the same keys, None where the JSON has
    none, and every value `object` because nothing has checked it yet. A reader
    indexes it, so mypy checks the key, and narrows the value it gets.
    test_models keeps the two key sets equal."""

    source_type: object
    command: object
    params: object
    kind: object
    parent_path: object
    result: object
    truncated: object
    warnings: object
    failures: object


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
