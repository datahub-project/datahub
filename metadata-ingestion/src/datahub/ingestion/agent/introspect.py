import logging
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

logger = logging.getLogger(__name__)


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
#
# Kept deliberately, as a net for connectors this repo cannot see. Filters(...)
# is the mechanism now -- 30 annotations, and a contract test asserting that no
# registered connector resolves by name alone -- so for everything in-tree this
# is unreachable, and review reasonably asked whether it should go.
#
# It stays because deleting it turns a right answer into a wrong one for an
# out-of-tree connector, which the source registry accepts via entry points and
# the contract test cannot reach. Measured on postgres with its annotations
# stripped: with the convention, `schema_pattern` allow ['^analytics$']
# correctly excludes other_schema; without it, resolution returns None, the
# pattern defaults to allow-all, and other_schema is reported INCLUDED --
# the opposite of what ingestion does.
#
# The "declares no kind" warning does not cover that. It is gated on
# `declared and kind not in declared`, and Schema IS among the kinds postgres
# declares, so nothing fires; only `filtering: "unresolved"` marks it. So the
# net earns its place -- but it must not be invisible, which is what
# _warn_convention is for.
_PATTERN_SUFFIXES = ("_pattern", "_patterns")


def _pattern_field_candidates(kind: ProbeNodeKind) -> List[str]:
    base = re.sub(r"[^a-z0-9]+", "_", str(kind).lower()).strip("_")
    return [base + suffix for suffix in _PATTERN_SUFFIXES]


# (config class, kind) pairs already warned about. Deduped because
# pattern_field_for_config is deliberately not memoized and `probe filter`
# resolves the field once per name it judges -- a warning per call is a
# warning nobody reads.
_CONVENTION_WARNED: Set[Tuple[str, str]] = set()


def _reset_convention_warnings() -> None:
    """Test seam: forget what has already been warned about."""
    _CONVENTION_WARNED.clear()


def _warn_convention(config_cls: type, kind: ProbeNodeKind, name: str) -> None:
    """Say out loud that a connector is leaning on the name guess.

    Without this the fallback is indistinguishable from a live path at a
    glance, which is not hypothetical: a dead `probe_schema_needs_parent`
    reader in filter_check.py survived four commits of deliberate hook removal
    for exactly that reason.
    """
    # Qualified, because two connectors can ship config classes with the same
    # __name__ and the second one's warning would be swallowed by the first's
    # -- silencing exactly the connector nobody has looked at yet.
    key = (f"{config_cls.__module__}.{config_cls.__qualname__}", str(kind))
    if key in _CONVENTION_WARNED:
        return
    _CONVENTION_WARNED.add(key)
    logger.warning(
        "%s.%s was matched to kind '%s' by name, not by declaration. The name "
        "convention is a guess kept for connectors outside this repo, and it "
        "can find the wrong field -- a deprecated alias that reads allow-all "
        "will report every object included while ingestion drops them. "
        "Annotate the field ingestion really filters on with Filters('%s').",
        config_cls.__name__,
        name,
        kind,
        kind,
    )


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
        _warn_convention(config_cls, kind, name)
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
        # Not declaring is the ordinary case and means exactly that.
        return set()
    # Deliberately unguarded. This used to be wrapped in `except Exception:
    # return set()`, described as "a config that cannot answer is treated as
    # not having answered" -- but those are the two states this hook exists
    # to keep apart. Mode's probe_unfiltered_kinds docstring makes the point:
    # declaring is how you tell "reported whole" from "the Filters annotation
    # was dropped", which is what happened to Teradata's database_pattern and
    # nothing noticed because the two look identical from outside.
    #
    # Swallowing put that back: pattern_field_for_config would fall through to
    # the name convention and answer by_pattern, contradicting the connector,
    # and there is no warn channel on this path to say so. A hook that raises
    # is a connector defect, and the caller's exit-code mapping reports it.
    return {str(kind) for kind in declared()}


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
        _warn_convention(config_cls, kind, name)
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


def declares_qualifier(config: Any) -> bool:
    """Whether any field carries Qualifier, whatever its current value.

    Distinct from declared_qualifier(), which answers "what is the container"
    and returns None both when no field is marked and when a marked field is
    empty -- BigQuery naming two projects, say. The distinction matters
    because the first is a statement about the CONNECTOR (it qualifies) and
    the second about this RECIPE (it did not say which).
    """
    fields = getattr(type(config), "model_fields", None)
    if not fields:
        return False
    return any(
        any(isinstance(m, Qualifier) for m in info.metadata) for info in fields.values()
    )


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

    A kind the source declares UNFILTERED is skipped for the same
    agree-with-the-other-command reason. pattern_field_for_config gives that
    declaration top precedence and answers UNFILTERED without looking
    anything up, so a connector that declares a kind unfiltered while keeping
    a same-named compatibility field would otherwise have `describe`
    advertise the field and `probe filter` answer UNFILTERED -- the same
    contradiction, one door along.
    """
    unfiltered = declared_unfiltered_kinds(config_cls)
    resolved: Dict[str, str] = {}
    for kind in sorted(declared_kinds_for_class(source_type, config_cls)):
        if str(kind) in unfiltered:
            continue
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
