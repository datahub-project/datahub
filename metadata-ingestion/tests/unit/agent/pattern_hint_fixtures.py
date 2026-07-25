from typing import Annotated, Any, Dict

from pydantic import Field

from datahub.configuration.common import AllowDenyPattern, ConfigModel, Filters
from datahub.ingestion.agent.models import ProbeNodeKind


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
    # A throwaway ConfigModel exists purely to have pydantic itself build
    # real FieldInfo objects (genuine .metadata/.annotation, not a hand-rolled
    # approximation of pydantic's contract) for the hinted fields' model_fields.
    annotations = {
        name: Annotated[AllowDenyPattern, Filters(kind)]
        for name, kind in hinted_fields.items()
    }
    namespace: Dict[str, Any] = {
        "__annotations__": annotations,
        **{name: Field(default=AllowDenyPattern.allow_all()) for name in hinted_fields},
    }
    real = type("_RealHinted", (ConfigModel,), namespace)
    # type()'s 3-argument form returns plain `type` per typeshed, even though
    # the bases guarantee a ConfigModel subclass; narrow it rather than
    # `cast` to reach the real .model_fields pydantic built.
    assert issubclass(real, ConfigModel)

    # A fresh, permissive class per call: `_hinted_pattern_field`/
    # `pattern_field_for_config_class` are memoized on (class, kind), so two
    # fixtures declaring different hints for the same kind must not share a
    # type or one call's cached result would leak onto the other's. Plain
    # (not ConfigModel) so tests can still set arbitrary attributes — a real
    # ConfigModel's extra="forbid" would reject a lister or client factory.
    config_cls = type("_HintedProbeConfig", (), {"model_fields": real.model_fields})
    config = config_cls()
    for name in hinted_fields:
        setattr(config, name, AllowDenyPattern.allow_all())
    for name, value in values.items():
        setattr(config, name, value)
    return config
