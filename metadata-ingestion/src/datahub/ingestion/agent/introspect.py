import re
import typing
from functools import lru_cache
from typing import Any, Dict, List, Optional

from pydantic import SecretStr
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined

from datahub.configuration.common import AllowDenyPattern, ConfigModel, Filters
from datahub.ingestion.agent.models import (
    FieldKind,
    FieldSpec,
    ProbeNodeKind,
    SourceSpec,
)
from datahub.ingestion.source.source_registry import source_registry


def _strip_annotated(annotation: object) -> object:
    # Pydantic v2 configs often wrap secret fields as Annotated[SecretStr, PlainSerializer(...)]
    # for custom serialization; unwrap to the underlying type for classification.
    if typing.get_origin(annotation) is typing.Annotated:
        return typing.get_args(annotation)[0]
    return annotation


def _unwrap_optional(annotation: object) -> List[object]:
    # Return the non-None members of an Optional/Union annotation (or [annotation] itself).
    origin = typing.get_origin(annotation)
    if origin is typing.Union:
        return [
            _strip_annotated(a)
            for a in typing.get_args(annotation)
            if a is not type(None)
        ]
    return [_strip_annotated(annotation)]


def _kind_for(annotation: object) -> FieldKind:
    for member in _unwrap_optional(annotation):
        if isinstance(member, type):
            if issubclass(member, SecretStr):
                return FieldKind.SECRET
            if issubclass(member, AllowDenyPattern):
                return FieldKind.PATTERN
            if issubclass(member, ConfigModel):
                return FieldKind.NESTED
    return FieldKind.PLAIN


def is_pattern_field(annotation: object) -> bool:
    """True when a config field's annotation is an AllowDenyPattern.

    Shared with the probe framework's pattern resolver so both agree on what
    counts as a filter field.
    """
    return _kind_for(annotation) == FieldKind.PATTERN


# A pattern field is conventionally named after the kind it filters:
# Schema -> schema_pattern, Topic -> topic_patterns.
_PATTERN_SUFFIXES = ("_pattern", "_patterns")


def _pattern_field_candidates(kind: ProbeNodeKind) -> List[str]:
    base = re.sub(r"[^a-z0-9]+", "_", str(kind).lower()).strip("_")
    return [base + suffix for suffix in _PATTERN_SUFFIXES]


@lru_cache(maxsize=None)
def _hinted_pattern_field(config_cls: type, kind: ProbeNodeKind) -> Optional[str]:
    """The field explicitly declaring Filters(kind), or None.

    Exact by construction: unlike the name convention, a hint cannot
    accidentally match, so a wrong result here is a declaration bug and is
    raised rather than guessed around.
    """
    wanted = str(kind)
    fields = getattr(config_cls, "model_fields", {})
    matches = sorted(
        name
        for name, field in fields.items()
        if any(
            isinstance(meta, Filters) and str(meta.kind) == wanted
            for meta in field.metadata
        )
    )
    if not matches:
        return None
    if len(matches) > 1:
        raise ValueError(
            f"{config_cls.__name__} declares Filters({wanted!r}) on more than one "
            f"field ({', '.join(matches)}); a level must resolve to exactly one "
            f"AllowDenyPattern"
        )
    name = matches[0]
    if not is_pattern_field(fields[name].annotation):
        raise ValueError(
            f"{config_cls.__name__}.{name} declares Filters({wanted!r}) but is "
            f"not an AllowDenyPattern"
        )
    return name


@lru_cache(maxsize=None)
def _pattern_field_for_config_class(
    config_cls: type, kind: ProbeNodeKind
) -> Optional[str]:
    """Find the config class's AllowDenyPattern field that filters `kind`, by
    convention, from its declared pydantic fields.

    Returns None when no such field exists, or when a same-named field is not an
    AllowDenyPattern. This is the class-level fallback for when an instance has an
    Optional pattern field left as None — see pattern_field_for_config for the
    instance-aware check that runs first. Memoized: resolution is per (config
    class, kind) and never changes at runtime.
    """
    hinted = _hinted_pattern_field(config_cls, kind)
    if hinted is not None:
        return hinted

    fields = getattr(config_cls, "model_fields", {})
    for name in _pattern_field_candidates(kind):
        field = fields.get(name)
        if field is not None and is_pattern_field(field.annotation):
            return name
    return None


def pattern_field_for_config(config: Any, kind: ProbeNodeKind) -> Optional[str]:
    """Find the *live config object's* AllowDenyPattern field that filters `kind`.

    A declared hint (Filters(kind) on a field's Annotated metadata) wins over
    both the instance check below and _pattern_field_for_config_class's
    convention, since it is exact by construction. Failing that, checks the
    instance's own attributes first — what pattern_verdict() actually reads via
    getattr(config, pattern_field) — before falling back to
    _pattern_field_for_config_class's class-level introspection (which also
    catches an Optional pattern field the instance happens to hold as None).
    Deliberately not memoized: unlike _pattern_field_for_config_class's (class,
    kind) cache, many distinct config instances (e.g. every test fixture built as
    a plain SimpleNamespace) can share the same type, so caching by type would
    leak one instance's resolved field onto an unrelated instance of that same
    type.
    """
    # Narrowed via an annotated local: passing `type(config)` inline infers as
    # type[Any], which mypy's lru_cache stub rejects as Hashable (a metaclass
    # __hash__ signature mismatch) even though it is hashable at runtime.
    config_cls: type = type(config)
    hinted = _hinted_pattern_field(config_cls, kind)
    if hinted is not None:
        return hinted
    for name in _pattern_field_candidates(kind):
        if isinstance(getattr(config, name, None), AllowDenyPattern):
            return name
    return _pattern_field_for_config_class(config_cls, kind)


def _type_name(annotation: object) -> str:
    members = _unwrap_optional(annotation)
    names = [getattr(m, "__name__", str(m)) for m in members]
    return names[0] if len(names) == 1 else "Union[" + ", ".join(names) + "]"


def _is_json_safe(value: object) -> bool:
    return isinstance(value, (str, int, float, bool)) or value is None


def _classify(name: str, field_info: FieldInfo) -> FieldSpec:
    kind = _kind_for(field_info.annotation)
    required = field_info.is_required()
    default: Optional[object] = None
    # Never surface a secret's default value.
    if kind != FieldKind.SECRET and not required:
        raw_default = field_info.default
        if raw_default is not PydanticUndefined and _is_json_safe(raw_default):
            default = raw_default
    return FieldSpec(
        name=name,
        kind=kind,
        required=required,
        type_name=_type_name(field_info.annotation),
        default=default,
        description=field_info.description,
    )


def describe_source(source_type: str) -> SourceSpec:
    # source_registry.get raises KeyError/ConfigurationError on miss (never returns None).
    source_cls = source_registry.get(source_type)
    # get_config_class is injected by the @config_class decorator at runtime, so it is
    # not declared on the Source base class and mypy cannot see it statically.
    get_config_class = getattr(source_cls, "get_config_class", None)
    if get_config_class is None:
        raise TypeError(f"Source {source_type!r} does not define a config class")
    config_cls = get_config_class()
    fields = [_classify(name, info) for name, info in config_cls.model_fields.items()]
    capabilities: List[Dict[str, object]] = []
    get_caps = getattr(source_cls, "get_capabilities", None)
    if callable(get_caps):
        for setting in get_caps():
            capabilities.append(
                {
                    "capability": setting.capability.value,
                    "description": setting.description,
                    "supported": setting.supported,
                }
            )
    return SourceSpec(source_type=source_type, fields=fields, capabilities=capabilities)
