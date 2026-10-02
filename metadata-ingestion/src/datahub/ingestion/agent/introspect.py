import logging
import re
import types
import typing
from functools import lru_cache
from typing import Dict, FrozenSet, Iterable, Iterator, List, Optional, Set, Tuple, cast

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
from datahub.ingestion.agent.probe_methods import (
    config_hook,
    declared_kind_overrides,
    declared_mapping,
    list_probe_methods,
)
from datahub.ingestion.agent.verdicts import UNFILTERED
from datahub.ingestion.source.source_registry import source_registry

logger = logging.getLogger(__name__)


def _strip_annotated(annotation: object) -> object:
    # Annotated[SecretStr, PlainSerializer(...)] and the like: classify the type.
    if typing.get_origin(annotation) is typing.Annotated:
        return typing.get_args(annotation)[0]
    return annotation


def _unwrap_optional(annotation: object) -> List[object]:
    # The non-None members of an Optional/Union (or [annotation]). "X | None"
    # reports types.UnionType where Optional[X] reports typing.Union.
    origin = typing.get_origin(annotation)
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
    """True when a config field's annotation is an AllowDenyPattern."""
    return _kind_for(annotation) == FieldKind.PATTERN


def _model_members(annotation: object) -> List[type]:
    """The ConfigModel types a field holds directly, Optional unwrapped, apart
    from AllowDenyPattern (a ConfigModel that cannot hold a Filters field)."""
    return [
        member
        for member in _unwrap_optional(annotation)
        if isinstance(member, type)
        and issubclass(member, ConfigModel)
        and not issubclass(member, AllowDenyPattern)
    ]


def iter_config_fields(
    config_cls: type,
    _prefix: str = "",
    _active: FrozenSet[type] = frozenset(),
) -> Iterator[Tuple[str, FieldInfo]]:
    """Every field on this config and its nested config blocks, as
    (dotted path, FieldInfo). Top-level fields keep their bare names."""
    fields = getattr(config_cls, "model_fields", None) or {}
    # Only classes on the current descent are excluded: a self-referencing
    # config stops, while two sibling blocks of one type are both walked.
    active = _active | {config_cls}
    for name, info in fields.items():
        path = f"{_prefix}{name}"
        yield path, info
        for member in _model_members(info.annotation):
            if member not in active:
                yield from iter_config_fields(member, f"{path}.", active)


# The name convention (Schema -> schema_pattern, Topic -> topic_patterns): a
# fallback for out-of-tree connectors, which register through entry points the
# contract test cannot reach. In-tree connectors declare Filters(...). Without
# the fallback such a connector's filter would read allow-all and report
# everything included; _warn_convention keeps its use visible.
_PATTERN_SUFFIXES = ("_pattern", "_patterns")


def _pattern_field_candidates(kind: ProbeNodeKind) -> List[str]:
    base = re.sub(r"[^a-z0-9]+", "_", str(kind).lower()).strip("_")
    return [base + suffix for suffix in _PATTERN_SUFFIXES]


# (config class, kind) pairs already warned about: `probe filter` resolves the
# field once per name it judges.
_CONVENTION_WARNED: Set[Tuple[str, str]] = set()


def _reset_convention_warnings() -> None:
    """Test seam: forget what has already been warned about."""
    _CONVENTION_WARNED.clear()


def _warn_convention(config_cls: type, kind: ProbeNodeKind, name: str) -> None:
    """Warn, once per class and kind, that a field was found by the name guess."""
    # Qualified: two connectors can ship config classes of the same __name__.
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
    """The field declaring Filters(kind), or None. Exact, so an ambiguous or
    mistyped declaration is raised as the connector's bug rather than guessed."""
    wanted = str(kind)
    fields = dict(iter_config_fields(config_cls))
    matches = sorted(
        path
        for path, field in fields.items()
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
    """The class's AllowDenyPattern field filtering `kind`: the Filters
    declaration, else the name convention; None when there is none.

    The fallback for pattern_field_for_config when an instance holds an Optional
    pattern field as None. Memoized per (class, kind).
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
            # As in pattern_field_for_config: a hidden field is a deprecated alias.
            continue
        _warn_convention(config_cls, kind, name)
        return name
    return None


def declared_unfiltered_kinds(config: object) -> Set[str]:
    """Levels this source says it deliberately does not filter
    (probe_unfiltered_kinds), read by name on any config."""
    hook = config_hook(config, "probe_unfiltered_kinds")
    return set() if hook is None else {str(k) for k in cast(Iterable[object], hook())}


def declared_rule_filtered_kinds(config: object) -> Dict[str, str]:
    """kind -> the config field whose rules (not an AllowDenyPattern) decide it
    (probe_rule_filtered_kinds, such as `path_specs`)."""
    return declared_mapping(config, "probe_rule_filtered_kinds")


def pattern_field_for_config(config: object, kind: ProbeNodeKind) -> Optional[str]:
    """The live config's AllowDenyPattern field that filters `kind`.

    Precedence: a kind declared unfiltered is UNFILTERED (a statement beats a
    guess); then a Filters(kind) declaration; then the name convention on the
    instance's attributes, which is what pattern_verdict reads; then the class
    (an Optional pattern field held as None). Not memoized: distinct instances
    (SimpleNamespace fixtures) share one type.
    """
    # Annotated local: mypy's lru_cache stub rejects an inline type[Any].
    config_cls: type = type(config)
    if str(kind) in declared_unfiltered_kinds(config):
        return UNFILTERED
    hinted = _hinted_pattern_field(config_cls, kind)
    if hinted is not None:
        return hinted
    for name in _pattern_field_candidates(kind):
        if not isinstance(getattr(config, name, None), AllowDenyPattern):
            continue
        if _is_hidden_field(config_cls, name):
            # A field hidden from the docs is a deprecated alias, already
            # renamed into its successor (two-tier `schema_pattern` reads
            # allow-all). Skipping it lets the "declares no kind" warning name
            # the kind the source really has.
            continue
        _warn_convention(config_cls, kind, name)
        return name
    return _pattern_field_for_config_class(config_cls, kind)


def _is_hidden_field(config_cls: type, name: str) -> bool:
    """Whether this field is HiddenFromDocs: a SkipJsonSchema() in its metadata."""
    # Local: at module scope mypy resolves SkipJsonSchema as a parameterized
    # generic and rejects the isinstance check, which is right at runtime.
    from pydantic.json_schema import SkipJsonSchema

    fields = getattr(config_cls, "model_fields", None)
    if not fields or name not in fields:
        return False
    return any(isinstance(m, SkipJsonSchema) for m in fields[name].metadata)


def _qualifier_fields(config: object) -> Iterator[Tuple[str, Qualifier]]:
    """Every Qualifier-marked field on this config's class, with its marker; the
    one scan both callers below share."""
    fields = getattr(type(config), "model_fields", None) or {}
    for name, info in fields.items():
        marker = next(
            (m for m in info.metadata if isinstance(m, Qualifier)),
            None,
        )
        if marker is not None:
            yield name, marker


def declares_qualifier(config: object) -> bool:
    """Whether any field carries Qualifier, whatever its value: a statement about
    the connector, where declared_qualifier() answers for one recipe."""
    return any(True for _ in _qualifier_fields(config))


def declared_qualifier(config: object) -> Tuple[Optional[str], bool]:
    """(container, authoritative) from the first Qualifier-marked field. A list
    field names a container only when it pins exactly one value."""
    for name, marker in _qualifier_fields(config):
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
    """The kinds this source names, as far as is knowable without a connection:
    its probe methods' kinds and the kinds its config declares for them
    (probe_kind_overrides), so `describe` answers what `probe filter` does.
    """
    try:
        kinds = {spec.kind for spec in list_probe_methods(source_type) if spec.kind}
    except Exception:
        # A source whose provider will not import still gets described.
        kinds = set()
    return kinds | set(declared_kind_overrides(config_cls).values())


def _filter_kinds_by_field(source_type: str, config_cls: type) -> Dict[str, str]:
    """field name -> the kind it filters, resolved as `probe filter` resolves
    it, so `describe` and `probe filter` agree.

    Only kinds the source declares are inverted: `procedure_pattern` filters no
    hierarchy level. Kinds declared unfiltered or rule-filtered are skipped, as
    `probe filter` reads no pattern for them.
    """
    unfiltered = declared_unfiltered_kinds(config_cls)
    rule_kinds = declared_rule_filtered_kinds(config_cls)
    resolved: Dict[str, str] = {}
    for kind in sorted(declared_kinds_for_class(source_type, config_cls)):
        if str(kind) in unfiltered or str(kind) in rule_kinds:
            continue
        field = _pattern_field_for_config_class(config_cls, kind)
        # First kind wins; sorted() makes it deterministic.
        if field is not None and field not in resolved:
            resolved[field] = kind
    for kind, field in sorted(rule_kinds.items()):
        resolved.setdefault(field, kind)
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
    # Raises KeyError/ConfigurationError on a miss; never returns None.
    source_cls = source_registry.get(source_type)
    # Injected by @config_class at runtime, out of mypy's view.
    get_config_class = getattr(source_cls, "get_config_class", None)
    if get_config_class is None:
        raise TypeError(f"Source {source_type!r} does not define a config class")
    config_cls = get_config_class()
    filter_kinds = _filter_kinds_by_field(source_type, config_cls)
    fields = [
        _classify(name, info, filter_kinds)
        for name, info in config_cls.model_fields.items()
    ]
    # A declared pattern in a nested block is described under its dotted path,
    # as `probe filter` reports it. Declared ones only, and scaffold() skips
    # PATTERN fields, so a dotted name never becomes a recipe key.
    fields.extend(
        _classify(path, info, filter_kinds)
        for path, info in iter_config_fields(config_cls)
        if "." in path and _declared_filter_kind(info) is not None
    )
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
