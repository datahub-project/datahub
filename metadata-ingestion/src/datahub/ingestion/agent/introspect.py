import re
import types
import typing
from functools import lru_cache
from typing import Any, Dict, List, Optional, Set, Tuple

from pydantic import SecretStr
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined

from datahub.configuration.common import (
    AllowDenyPattern,
    ConfigModel,
    Filters,
    Qualifier,
)
from datahub.ingestion.agent.models import (
    FieldKind,
    FieldSpec,
    ProbeNodeKind,
    SourceSpec,
)
from datahub.ingestion.agent.verdicts import UNFILTERED
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
    # "X | None" and "Optional[X]" mean the same thing and report different
    # origins -- types.UnionType against typing.Union. Matching only the latter
    # silently misclassifies a field written in the newer syntax: an
    # "AllowDenyPattern | None" reads as a plain field, so pattern resolution
    # reports no filter for that level.
    if origin is typing.Union or origin is types.UnionType:
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
        if field is None or not is_pattern_field(field.annotation):
            continue
        if _is_hidden_field(config_cls, name):
            # Same rule as the instance-level loop in pattern_field_for_config;
            # see the comment there. Both branches need it -- the instance
            # check only reached this field when it happened to be set, so
            # skipping it there alone left the default path resolving the
            # deprecated alias anyway.
            continue
        return name
    return None


def declared_unfiltered_kinds(config: Any) -> Set[str]:
    """Levels this source says it deliberately does not filter.

    Read duck-typed rather than off SQLCommonConfig, because the sources that
    need it are not all SQL: Mode filters spaces and reports and nothing below
    them, and its config is not a SQLCommonConfig. Same reason
    probe_container_kind is read this way.
    """
    declared = getattr(config, "probe_unfiltered_kinds", None)
    if not callable(declared):
        return set()
    try:
        return {str(kind) for kind in declared()}
    except Exception:
        # A config that cannot answer is treated as not having answered.
        return set()


def pattern_field_for_config(config: Any, kind: ProbeNodeKind) -> Optional[str]:
    """Find the *live config object's* AllowDenyPattern field that filters `kind`.

    Precedence, highest first: a kind the source declares unfiltered resolves
    to UNFILTERED before anything is looked up -- saying "nothing filters this
    level" is a statement, not a guess, and there is nothing to find. Then a
    declared hint (Filters(kind) on a field's Annotated metadata), which wins
    over both the instance check below and _pattern_field_for_config_class's
    convention because it is exact by construction. Failing that, checks the
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
    # Checked first, and deliberately: a source saying "nothing filters this
    # level" is making a statement, where the convention below is a guess. A
    # source that declares a kind unfiltered *and* has a field the guess would
    # find is contradicting itself, which a contract test refuses rather than
    # resolving silently.
    if str(kind) in declared_unfiltered_kinds(config):
        return UNFILTERED
    hinted = _hinted_pattern_field(config_cls, kind)
    if hinted is not None:
        return hinted
    for name in _pattern_field_candidates(kind):
        if not isinstance(getattr(config, name, None), AllowDenyPattern):
            continue
        if _is_hidden_field(config_cls, name):
            # The convention is a guess, and a field hidden from the docs is
            # not one a recipe is meant to set -- it is a deprecated alias
            # kept for compatibility. Resolving to one gives a confident
            # wrong verdict: on every two-tier source, `schema_pattern` is a
            # HiddenFromDocs alias that pydantic_renamed_field has already
            # emptied into `database_pattern`, so it reads allow-all. A
            # legacy mysql recipe with schema_pattern allow ['^analytics$']
            # had `probe filter --kind Schema` report other_db as included,
            # filtering "by_pattern", no warning -- while ingestion, matching
            # on database_pattern, drops it.
            #
            # Skipping it lets the resolution fall through to "no field for
            # this kind", which is what makes the "declares no kind" warning
            # fire and name the kind the source really has (Database).
            continue
        return name
    return _pattern_field_for_config_class(config_cls, kind)


def _is_hidden_field(config_cls: type, name: str) -> bool:
    """Whether this field is HiddenFromDocs, i.e. not one a recipe should set.

    HiddenFromDocs is Annotated[..., SkipJsonSchema()], so the marker is in
    the field's metadata rather than on FieldInfo itself.
    """
    from pydantic.json_schema import SkipJsonSchema

    fields = getattr(config_cls, "model_fields", None)
    if not fields or name not in fields:
        return False
    return any(isinstance(m, SkipJsonSchema) for m in fields[name].metadata)


def declared_qualifier(config: Any) -> Tuple[Optional[str], bool]:
    """The container a Qualifier-marked field names, and whether it wins.

    Read off the field rather than from a method the connector declares:
    Filters() already establishes that idiom, and a method saying "my
    container is self.project_ids" restates the field's own name in a worse
    place. Returns (value, authoritative); a list field qualifies only when
    it pins exactly one value, since several have no single answer to give
    without guessing.
    """
    fields = getattr(type(config), "model_fields", None)
    if not fields:
        return None, False
    for name, info in fields.items():
        marker = next(
            (m for m in info.metadata if isinstance(m, Qualifier)),
            None,
        )
        if marker is None:
            continue
        value = getattr(config, name, None)
        if isinstance(value, str) and value:
            return value, marker.authoritative
        if isinstance(value, (list, tuple)) and len(value) == 1:
            return str(value[0]), marker.authoritative
        return None, marker.authoritative
    return None, False


def _type_name(annotation: object) -> str:
    members = _unwrap_optional(annotation)
    names = [getattr(m, "__name__", str(m)) for m in members]
    return names[0] if len(names) == 1 else "Union[" + ", ".join(names) + "]"


def _is_json_safe(value: object) -> bool:
    return isinstance(value, (str, int, float, bool)) or value is None


def _declared_filter_kind(field_info: FieldInfo) -> Optional[str]:
    """The level this field filters, per an explicit Filters(...) annotation."""
    for meta in field_info.metadata:
        if isinstance(meta, Filters):
            return str(meta.kind)
    return None


def declared_kinds_for_class(source_type: str, config_cls: type) -> Set[str]:
    """The kinds this source names, as far as is knowable without a connection.

    probe_container_kind is a classmethod, so `containers` can be resolved from
    the class -- which is what lets `describe` answer the same question
    `probe filter` answers against a live config.
    """
    from datahub.ingestion.agent.probe_methods import list_probe_methods

    try:
        kinds = {spec.kind for spec in list_probe_methods(source_type) if spec.kind}
    except Exception:
        # A source whose provider will not import still gets described.
        kinds = set()
    container_kind = getattr(config_cls, "probe_container_kind", None)
    if callable(container_kind):
        try:
            kinds.add(str(container_kind()))
        except Exception:
            pass
    return kinds


def _filter_kinds_by_field(source_type: str, config_cls: type) -> Dict[str, str]:
    """field name -> the kind it filters, resolved as `probe filter` resolves it.

    Reading only the explicit annotation was strictness in one direction of a
    two-way mapping, which is not strictness but disagreement:
    pattern_field_for_config goes kind -> field through the annotation *and then
    the name convention*, so `describe` reported `filters: null` for a field that
    `probe filter` was actively filtering on. Teradata is the live case -- it
    redeclares database_pattern, pydantic v2 drops the inherited Filters
    metadata, and the two commands then contradict each other about the same
    field, with no way from outside to tell which is lying.

    The convention is inverted only across kinds this source actually declares,
    which is what makes it safe: `procedure_pattern` and `profile_pattern` are
    real filters that gate no hierarchy level, and a blind inversion of the name
    convention would report them as levels. No declared kind, no inversion.
    """
    resolved: Dict[str, str] = {}
    for kind in sorted(declared_kinds_for_class(source_type, config_cls)):
        field = _pattern_field_for_config_class(config_cls, kind)
        # First kind wins, and sorted() makes that deterministic rather than
        # dependent on set iteration order.
        if field is not None and field not in resolved:
            resolved[field] = kind
    return resolved


def _classify(
    name: str, field_info: FieldInfo, filter_kinds: Optional[Dict[str, str]] = None
) -> FieldSpec:
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
        filters=_declared_filter_kind(field_info) or (filter_kinds or {}).get(name),
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
    filter_kinds = _filter_kinds_by_field(source_type, config_cls)
    fields = [
        _classify(name, info, filter_kinds)
        for name, info in config_cls.model_fields.items()
    ]
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
