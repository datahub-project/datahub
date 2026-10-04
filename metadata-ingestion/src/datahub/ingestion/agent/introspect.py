import collections.abc
import logging
import re
import types
import typing
from functools import lru_cache
from typing import (
    Callable,
    Dict,
    FrozenSet,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
    cast,
)

from pydantic import AliasChoices, BaseModel, SecretStr
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined

from datahub.configuration.common import (
    AllowDenyPattern,
    ConfigModel,
    Enables,
    Filters,
    FiltersByRule,
    Qualifier,
)
from datahub.ingestion.agent.config_validation import validate_source_config
from datahub.ingestion.agent.models import (
    FieldKind,
    FieldSpec,
    ProbeNodeKind,
    SourceSpec,
)
from datahub.ingestion.agent.probe_methods import (
    config_class_for,
    config_hook,
    declared_kind_overrides,
    list_probe_methods,
    source_class_for,
)
from datahub.ingestion.agent.verdicts import UNFILTERED, ProbeInternalError

logger = logging.getLogger(__name__)


def _strip_annotated(annotation: object) -> object:
    # Annotated[SecretStr, PlainSerializer(...)] and the like: classify the type.
    if typing.get_origin(annotation) is typing.Annotated:
        return typing.get_args(annotation)[0]
    return annotation


def _unwrap_optional(annotation: object) -> List[object]:
    # The non-None members of an Optional/Union (or [annotation]). "X | None"
    # reports types.UnionType where Optional[X] reports typing.Union. Annotated
    # comes off the union too: pydantic lifts it off a field's own annotation
    # but not off a container's argument (Dict[str, Annotated[Union[...]]]).
    annotation = _strip_annotated(annotation)
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


# Far deeper than any registered config nests a secret (four levels): a bound,
# so a config handed in holding a value that contains itself still returns.
_MAX_SECRET_DEPTH = 16


def _recipe_keys(name: str, info: FieldInfo) -> Set[str]:
    """The keys a recipe can hold this field under. Validation reads the alias
    (or each AliasChoices string) when there is one, else the name; the name is
    read regardless, since over-collecting is the safe side for masking."""
    alias = info.validation_alias or info.alias
    if isinstance(alias, AliasChoices):
        keys = {choice for choice in alias.choices if isinstance(choice, str)}
    elif isinstance(alias, str):
        keys = {alias}
    else:
        keys = set()
    return keys | {name}


def iter_secret_field_values(
    config_cls: Type[BaseModel], config: Dict[str, object]
) -> Iterator[Tuple[str, str]]:
    """(path, value) for every non-empty string a recipe's config holds in a
    SecretStr field, at any depth: nested config blocks, and lists, tuples, sets
    and dicts of them. The path is dotted, under the key the recipe used, with
    a list index or dict key in brackets (`endpoints[0].token`), as ConfigModel
    names nested secrets. A path can repeat when a union reads it twice.

    The walk follows the recipe's values, not the class's fields, so a config
    that names itself recurses only as deep as the recipe nests it.
    """
    return _secrets_in_model(config_cls, config, "", 0)


def collect_secret_field_values(
    config_cls: Type[BaseModel], config: Dict[str, object]
) -> Set[str]:
    """The values iter_secret_field_values finds."""
    return {value for _path, value in iter_secret_field_values(config_cls, config)}


def _secrets_in_model(
    model: Type[BaseModel], config: Dict[str, object], prefix: str, depth: int
) -> Iterator[Tuple[str, str]]:
    for name, info in model.model_fields.items():
        for key in _recipe_keys(name, info):
            if key in config:
                yield from _secrets_in_value(
                    info.annotation, config[key], f"{prefix}{key}", depth + 1
                )


def _secrets_in_value(
    annotation: object, value: object, path: str, depth: int
) -> Iterator[Tuple[str, str]]:
    if depth > _MAX_SECRET_DEPTH:
        return
    for member in _unwrap_optional(annotation):
        origin = typing.get_origin(member)
        if origin is None:
            if not isinstance(member, type):
                continue
            if issubclass(member, SecretStr):
                if isinstance(value, str) and value:
                    yield path, value
            elif issubclass(member, BaseModel) and isinstance(value, dict):
                yield from _secrets_in_model(member, value, f"{path}.", depth)
            continue
        args = [a for a in typing.get_args(member) if a is not Ellipsis]
        if not isinstance(origin, type) or not args:
            continue
        if issubclass(origin, collections.abc.Mapping) and isinstance(value, dict):
            for key, item in value.items():
                yield from _secrets_in_value(
                    args[-1], item, f"{path}[{key}]", depth + 1
                )
        elif issubclass(
            origin, (collections.abc.Sequence, collections.abc.Set)
        ) and isinstance(value, (list, tuple, set, frozenset)):
            # Every item against every argument: a Tuple[A, B] is read without
            # matching positions.
            for index, item in enumerate(value):
                for arg in args:
                    yield from _secrets_in_value(
                        arg, item, f"{path}[{index}]", depth + 1
                    )


def iter_model_secret_values(model: BaseModel) -> Iterator[Tuple[str, str]]:
    """(path, value) for every non-empty SecretStr a validated config holds,
    at any depth: nested models, and lists, tuples, sets and dicts of them.
    Paths are named as iter_secret_field_values names them, by field name:
    where a validator renamed a key (pydantic_renamed_field), the new name.

    The validated config, unlike the recipe, holds a value under the field it
    was renamed into, or one a validator read in (a deploy key file).
    """
    return _secrets_in_instance(model, "", 0, frozenset())


def _secrets_in_instance(
    value: object, path: str, depth: int, active: FrozenSet[int]
) -> Iterator[Tuple[str, str]]:
    if depth > _MAX_SECRET_DEPTH or id(value) in active:
        return
    if isinstance(value, SecretStr):
        secret = value.get_secret_value()
        if secret:
            yield path, secret
        return
    children: Iterable[Tuple[str, object]]
    if isinstance(value, BaseModel):
        children = (
            (f"{path}.{name}" if path else name, getattr(value, name, None))
            for name in type(value).model_fields
        )
    elif isinstance(value, dict):
        children = ((f"{path}[{key}]", item) for key, item in value.items())
    elif isinstance(value, (list, tuple, set, frozenset)):
        children = ((f"{path}[{index}]", item) for index, item in enumerate(value))
    else:
        return
    for child_path, child in children:
        yield from _secrets_in_instance(
            child, child_path, depth + 1, active | {id(value)}
        )


def secret_field_values(source_type: str, config: Dict[str, object]) -> Set[str]:
    """Every SecretStr value of this recipe's config: the recipe's own, by
    collect_secret_field_values, and the config's as its source validates it,
    when it does (iter_model_secret_values). A recipe failing validation is
    covered by the first. Raises as describe_source does for a source type
    that does not resolve."""
    config_cls = _declared_config_class(source_type)
    found = collect_secret_field_values(config_cls, config)
    try:
        validated = validate_source_config(config_cls, source_type, config)
    except Exception:
        # The command validates the config again and reports what is wrong.
        return found
    return found | {value for _path, value in iter_model_secret_values(validated)}


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

    def declares_pattern(name: str) -> bool:
        field = fields.get(name)
        return field is not None and is_pattern_field(field.annotation)

    return _convention_field(config_cls, kind, declares_pattern)


def declared_unfiltered_kinds(config: object) -> Set[str]:
    """Levels this source says it deliberately does not filter
    (probe_unfiltered_kinds), read by name on any config."""
    hook = config_hook(config, "probe_unfiltered_kinds")
    return set() if hook is None else {str(k) for k in cast(Iterable[object], hook())}


@lru_cache(maxsize=None)
def _kind_marked_fields(
    config_cls: type, marker: Union[Type[Enables], Type[FiltersByRule]]
) -> Tuple[Tuple[str, str], ...]:
    """(kind, field) for every `marker(kind)` on the class's top-level fields:
    the one reader Enables and FiltersByRule share. A kind marked on two fields
    is the connector's defect: the probe could only guess which one ingestion
    reads."""
    found: Dict[str, str] = {}
    fields = getattr(config_cls, "model_fields", None) or {}
    for name, info in fields.items():
        for meta in info.metadata:
            if not isinstance(meta, marker):
                continue
            kind = str(meta.kind)
            if found.setdefault(kind, name) != name:
                raise ProbeInternalError(
                    f"{config_cls.__name__} declares {marker.__name__}({kind!r}) "
                    f"on both {found[kind]} and {name}; a kind must resolve to "
                    f"exactly one field"
                )
    return tuple(found.items())


def _class_of(config: object) -> type:
    return config if isinstance(config, type) else type(config)


def declared_kind_enablers(config: object) -> Dict[str, str]:
    """kind -> the bool field that enables it (Enables), on a config or its
    class."""
    return dict(_kind_marked_fields(_class_of(config), Enables))


def declared_rule_filtered_kinds(config: object) -> Dict[str, str]:
    """kind -> the field whose rules, not an AllowDenyPattern, decide it
    (FiltersByRule, such as `path_specs`), on a config or its class."""
    return dict(_kind_marked_fields(_class_of(config), FiltersByRule))


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
    found = _convention_field(
        config_cls,
        kind,
        lambda name: isinstance(getattr(config, name, None), AllowDenyPattern),
    )
    if found is not None:
        return found
    return _pattern_field_for_config_class(config_cls, kind)


def _convention_field(
    config_cls: type, kind: ProbeNodeKind, is_pattern: Callable[[str], bool]
) -> Optional[str]:
    """The first name-convention candidate for `kind` that `is_pattern`
    accepts, warned about once; None when there is none."""
    for name in _pattern_field_candidates(kind):
        if not is_pattern(name):
            continue
        if _is_hidden_field(config_cls, name):
            # A field hidden from the docs is a deprecated alias, already
            # renamed into its successor (two-tier `schema_pattern` reads
            # allow-all). Skipping it lets the "declares no kind" warning name
            # the kind the source really has.
            continue
        _warn_convention(config_cls, kind, name)
        return name
    return None


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


def _declared_config_class(source_type: str) -> Type[ConfigModel]:
    """config_class_for, refusing a source that declares no config class:
    there is nothing to describe or to collect secrets from."""
    config_cls = config_class_for(source_type)
    if config_cls is None:
        raise TypeError(f"Source {source_type!r} does not define a config class")
    return config_cls


def describe_source(source_type: str) -> SourceSpec:
    # A source type that does not resolve is a ValueError; never returns None.
    source_cls = source_class_for(source_type)
    config_cls = _declared_config_class(source_type)
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
