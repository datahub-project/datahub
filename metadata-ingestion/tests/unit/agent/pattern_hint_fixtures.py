from dataclasses import dataclass, field
from typing import Any, Dict, List

from datahub.configuration.common import AllowDenyPattern, Filters
from datahub.ingestion.agent.models import ProbeNodeKind


@dataclass(frozen=True)
class _FakeFieldInfo:
    """Just enough of pydantic's FieldInfo shape for
    probe.py's `_hinted_pattern_field` to read: it only ever accesses
    `.metadata` (for a Filters instance) and `.annotation` (to confirm the
    field is an AllowDenyPattern)."""

    annotation: type
    metadata: List[Filters] = field(default_factory=list)


def config_with_hints(hinted_fields: Dict[str, ProbeNodeKind], **values: Any) -> Any:
    """Build a probe test fixture that carries the same Filters hint metadata
    a real Annotated[AllowDenyPattern, Filters(kind)] config field would.

    A plain SimpleNamespace can't do this: every SimpleNamespace instance
    shares one type, but a hint is class-level (see pattern_field_for_config's
    docstring), so a per-fixture class is needed here for any pattern field
    whose name doesn't match its kind by convention (a "name skew" field, e.g.
    Aerospike's set_pattern filtering the "Table" kind).

    hinted_fields maps a pattern-field name to the ProbeNodeKind it hints;
    every other keyword becomes a plain attribute (a lister, a client
    factory, an unhinted pattern field resolved by convention, etc.), exactly
    as a SimpleNamespace(...) fixture would set them.
    """
    model_fields = {
        name: _FakeFieldInfo(annotation=AllowDenyPattern, metadata=[Filters(kind)])
        for name, kind in hinted_fields.items()
    }
    # A fresh class per call: `_hinted_pattern_field`/`pattern_field_for_config_class`
    # are memoized on (class, kind), so two fixtures declaring different hints for
    # the same kind must not share a type or one call's cached result would leak
    # onto the other's.
    config_cls = type("_HintedProbeConfig", (), {"model_fields": model_fields})
    config = config_cls()
    for name, value in values.items():
        setattr(config, name, value)
    return config
