"""What a config declares to the probe: its field markers (`Filters`,
`Enables`, `FiltersByRule`, `Qualifier`, from configuration.common) and the
kinds it leaves unfiltered on purpose.

A misplaced marker reads as no declaration at all, and the probe then answers
with a verdict ingestion does not give: Enables on a str never switches its
kind off, a FiltersByRule inside a nested block is never seen. So every marker
reader here checks the whole class the first time it reads it, and refuses a
misdeclared one as the connector's defect (exit 1), listing every problem
(marker_problems). The contract test runs the same check over every registered
config; this is the net for connectors registered from outside this repo.
"""

from dataclasses import dataclass
from functools import lru_cache
from typing import (
    Callable,
    Dict,
    Generic,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from pydantic.fields import FieldInfo

from datahub.configuration.common import Enables, Filters, FiltersByRule, Qualifier
from datahub.ingestion.agent.config_fields import (
    is_pattern_field,
    iter_config_fields,
    unwrap_optional,
)
from datahub.ingestion.agent.probe_methods import config_hook, unknown_config_hooks
from datahub.ingestion.agent.verdicts import ProbeInternalError

_Marker = TypeVar("_Marker", Filters, Enables, FiltersByRule, Qualifier)
_KindMarker = TypeVar("_KindMarker", Filters, Enables, FiltersByRule)


@dataclass(frozen=True)
class MarkedField(Generic[_Marker]):
    """One marker on one field; `path` is dotted for a field in a nested block."""

    path: str
    marker: _Marker
    annotation: object

    @property
    def nested(self) -> bool:
        return "." in self.path


def _class_of(config: object) -> type:
    return config if isinstance(config, type) else type(config)


@lru_cache(maxsize=None)
def _fields_of(config_cls: type) -> Tuple[Tuple[str, FieldInfo], ...]:
    return tuple(iter_config_fields(config_cls))


def _marked_fields(
    config: object, marker_type: Type[_Marker]
) -> List[MarkedField[_Marker]]:
    """Every `marker_type` on a config's fields, nested blocks included, in
    field order. `config` is a config class or an instance of one."""
    return [
        MarkedField(path=path, marker=meta, annotation=info.annotation)
        for path, info in _fields_of(_class_of(config))
        for meta in info.metadata
        if isinstance(meta, marker_type)
    ]


def _kind_fields(config_cls: type, marker_type: Type[_KindMarker]) -> Dict[str, str]:
    """kind -> the field marked marker_type(kind); the last field wins, so
    read it only from a class marker_problems has passed."""
    return {str(f.marker.kind): f.path for f in _marked_fields(config_cls, marker_type)}


def _label(marker: Union[Filters, Enables, FiltersByRule, Qualifier]) -> str:
    if isinstance(marker, Qualifier):
        return "Qualifier()"
    return f"{type(marker).__name__}({str(marker.kind)!r})"


_DEFECTIVE = "the connector is defective: "


def declared_unfiltered_kinds(config: object) -> Set[str]:
    """Kinds this source says it deliberately does not filter
    (probe_unfiltered_kinds), read by name on any config."""
    hook = config_hook(config, "probe_unfiltered_kinds")
    if hook is None:
        return set()
    declared = hook()
    # A bare str is iterable too, and would read as one kind per character.
    if not isinstance(declared, (set, frozenset, list, tuple)) or not all(
        isinstance(kind, str) for kind in declared
    ):
        owner = config if isinstance(config, type) else type(config)
        raise ProbeInternalError(
            f"{_DEFECTIVE}{owner.__name__}.probe_unfiltered_kinds "
            f"returned {type(declared).__name__}, not a set of kind names"
        )
    return {str(kind) for kind in declared}


def _is_bool_field(annotation: object) -> bool:
    # Optional allowed: only False switches a kind off, so unset reads enabled.
    return unwrap_optional(annotation) == [bool]


@dataclass(frozen=True)
class _Placement:
    """Where a marker may sit. Filters may sit in a nested block, since a
    pattern can; the others are read on top-level fields only."""

    top_level_only: bool
    # The field type the marker needs, and its name for the message; None
    # accepts any type.
    field_type: Optional[Callable[[object], bool]] = None
    field_type_name: str = ""


_PLACEMENTS: Dict[type, _Placement] = {
    Filters: _Placement(
        top_level_only=False,
        field_type=is_pattern_field,
        field_type_name="an AllowDenyPattern",
    ),
    Enables: _Placement(
        top_level_only=True, field_type=_is_bool_field, field_type_name="a bool field"
    ),
    FiltersByRule: _Placement(top_level_only=True),
    Qualifier: _Placement(top_level_only=True),
}


def _placement_problems(
    config_name: str, marked: Sequence[MarkedField[_Marker]], placement: _Placement
) -> Iterator[str]:
    for field in marked:
        where = f"{config_name}.{field.path} declares {_label(field.marker)}"
        if placement.top_level_only and field.nested:
            yield f"{where} in a nested block, where the probe never reads it"
        elif placement.field_type is not None and not placement.field_type(
            field.annotation
        ):
            yield f"{where} but is not {placement.field_type_name}"


def _duplicate_problems(
    config_name: str, marked: Sequence[MarkedField[_Marker]]
) -> Iterator[str]:
    paths_by_label: Dict[str, List[str]] = {}
    for field in marked:
        paths = paths_by_label.setdefault(_label(field.marker), [])
        if field.path not in paths:
            paths.append(field.path)
    for label, paths in paths_by_label.items():
        if len(paths) > 1:
            yield (
                f"{config_name} declares {label} on more than one field "
                f"({', '.join(sorted(paths))}); the probe could only guess "
                f"which one ingestion reads"
            )


def _single_marker_problems(config_cls: type, marker_type: Type[_Marker]) -> List[str]:
    marked: List[MarkedField[_Marker]] = _marked_fields(config_cls, marker_type)
    return [
        *_placement_problems(config_cls.__name__, marked, _PLACEMENTS[marker_type]),
        *_duplicate_problems(config_cls.__name__, marked),
    ]


def _rule_kind_problems(config_cls: type) -> Iterator[str]:
    """A rule-filtered kind is judged by probe_verdict_override alone, so one
    must exist, and nothing else may claim the kind."""
    rule_kinds = sorted(_kind_fields(config_cls, FiltersByRule))
    if not rule_kinds:
        return
    name = config_cls.__name__
    if not callable(getattr(config_cls, "probe_verdict_override", None)):
        yield (
            f"{name} declares FiltersByRule for "
            f"{', '.join(repr(k) for k in rule_kinds)} but no "
            f"probe_verdict_override to judge those kinds"
        )
    pattern_fields = _kind_fields(config_cls, Filters)
    try:
        unfiltered = declared_unfiltered_kinds(config_cls)
    except ProbeInternalError as exc:
        # Listed with the class's other problems, not in place of them.
        yield str(exc).removeprefix(_DEFECTIVE)
        unfiltered = set()
    for kind in rule_kinds:
        if kind in pattern_fields:
            yield (
                f"{name} declares FiltersByRule({kind!r}) and Filters({kind!r}) "
                f"on {pattern_fields[kind]}; a kind is filtered by its rules or "
                f"by a pattern, not both"
            )
        if kind in unfiltered:
            yield (
                f"{name} declares FiltersByRule({kind!r}) and lists {kind!r} in "
                f"probe_unfiltered_kinds; a kind is filtered by its rules or not "
                f"at all"
            )


def marker_problems(config_cls: type) -> List[str]:
    """Every way `config_cls` misdeclares a probe marker; empty when it has
    none. Filters marks an AllowDenyPattern, nested or not; Enables a
    top-level bool; FiltersByRule and Qualifier a top-level field. Each kind
    is marked on one field per marker, and the container on one Qualifier
    field. A FiltersByRule kind is neither Filters-declared nor unfiltered, and
    a probe_verdict_override judges it. Every `probe_` attribute is a hook
    something reads."""
    unknown = unknown_config_hooks(config_cls)
    return [
        *(
            [
                f"{config_cls.__name__} defines {', '.join(unknown)}, which the probe never reads"
            ]
            if unknown
            else []
        ),
        *_single_marker_problems(config_cls, Filters),
        *_single_marker_problems(config_cls, Enables),
        *_single_marker_problems(config_cls, FiltersByRule),
        *_single_marker_problems(config_cls, Qualifier),
        *_rule_kind_problems(config_cls),
    ]


@dataclass(frozen=True)
class _Declarations:
    """A well-declared class's markers: kind -> field for each kind marker."""

    filters: Dict[str, str]
    enables: Dict[str, str]
    rules: Dict[str, str]
    qualifier: Optional[MarkedField[Qualifier]]


@lru_cache(maxsize=None)
def _declarations(config_cls: type) -> _Declarations:
    """The class's markers, read once it is known to declare them all as they
    are read; a misdeclared class is the connector's defect."""
    problems = marker_problems(config_cls)
    if problems:
        raise ProbeInternalError(_DEFECTIVE + "; ".join(problems))
    qualifiers = _marked_fields(config_cls, Qualifier)
    return _Declarations(
        filters=_kind_fields(config_cls, Filters),
        enables=_kind_fields(config_cls, Enables),
        rules=_kind_fields(config_cls, FiltersByRule),
        qualifier=qualifiers[0] if qualifiers else None,
    )


def filters_field(config: object, kind: str) -> Optional[str]:
    """The field declaring Filters(kind), dotted when nested; None when no
    field declares it."""
    return _declarations(_class_of(config)).filters.get(str(kind))


def declared_filter_kind(field_info: FieldInfo) -> Optional[str]:
    """The kind this one field filters, per its first Filters marker."""
    for meta in field_info.metadata:
        if isinstance(meta, Filters):
            return str(meta.kind)
    return None


def declared_kind_enablers(config: object) -> Dict[str, str]:
    """kind -> the bool field that enables it (Enables), on a config or its
    class."""
    return dict(_declarations(_class_of(config)).enables)


def declared_rule_filtered_kinds(config: object) -> Dict[str, str]:
    """kind -> the field whose rules, not an AllowDenyPattern, decide it
    (FiltersByRule, such as `path_specs`), on a config or its class."""
    return dict(_declarations(_class_of(config)).rules)


def declares_qualifier(config: object) -> bool:
    """Whether any field carries Qualifier, whatever its value: a statement about
    the connector, where declared_qualifier() answers for one recipe."""
    return _declarations(_class_of(config)).qualifier is not None


def declared_qualifier(config: object) -> Tuple[Optional[str], bool]:
    """(container, authoritative) from the Qualifier-marked field of a
    validated config. A list field names a container only when it pins exactly
    one value."""
    marked = _declarations(_class_of(config)).qualifier
    if marked is None:
        return None, False
    value = getattr(config, marked.path, None)
    if isinstance(value, str) and value:
        return value, marked.marker.authoritative
    if isinstance(value, (list, tuple)) and len(value) == 1:
        return str(value[0]), marked.marker.authoritative
    return None, marked.marker.authoritative
