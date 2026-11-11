"""A config class's fields and what their annotations hold.

Below every reader of a config: `declarations` (the probe's field markers),
`introspect` (describe, secret discovery), which imports the former, and
`redact`, whose nested walk reads a recipe against its class.
"""

import collections.abc
import types
import typing
from dataclasses import dataclass
from typing import FrozenSet, Iterator, List, Set, Tuple, Type

from pydantic import AliasChoices, BaseModel, SecretStr
from pydantic.fields import FieldInfo

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.ingestion.agent.models import FieldKind


def _strip_annotated(annotation: object) -> object:
    # Annotated[SecretStr, PlainSerializer(...)] and the like: classify the type.
    if typing.get_origin(annotation) is typing.Annotated:
        return typing.get_args(annotation)[0]
    return annotation


def unwrap_optional(annotation: object) -> List[object]:
    """The non-None members of an Optional/Union (or [annotation])."""
    # "X | None" reports types.UnionType where Optional[X] reports
    # typing.Union. Annotated comes off the union too: pydantic lifts it off a
    # field's own annotation but not off a container's argument
    # (Dict[str, Annotated[Union[...]]]).
    annotation = _strip_annotated(annotation)
    origin = typing.get_origin(annotation)
    if origin is typing.Union or origin is types.UnionType:
        return [
            _strip_annotated(a)
            for a in typing.get_args(annotation)
            if a is not type(None)
        ]
    return [_strip_annotated(annotation)]


def field_kind(annotation: object) -> FieldKind:
    for member in unwrap_optional(annotation):
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
    return field_kind(annotation) == FieldKind.PATTERN


def _model_members(annotation: object) -> List[type]:
    """The ConfigModel types a field holds directly, Optional unwrapped, apart
    from AllowDenyPattern (a ConfigModel that cannot hold a Filters field)."""
    return [
        member
        for member in unwrap_optional(annotation)
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


def recipe_keys(name: str, info: FieldInfo) -> Set[str]:
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


def _is_free_form_member(member: object) -> bool:
    cls = typing.get_origin(member) or member
    return (
        member is typing.Any
        or member is object
        or (isinstance(cls, type) and issubclass(cls, collections.abc.Mapping))
    )


@dataclass(frozen=True)
class RecipeEntry:
    """How a recipe mapping, read against the annotations that type it, holds
    one key."""

    # A config block declares the key as a field, and nothing else in the
    # union could read the mapping (no Mapping, Any or object).
    declared: bool
    # What types the key's value; empty when nothing does.
    annotations: Tuple[object, ...]

    @property
    def holds_free_form(self) -> bool:
        """Whether the key's value is free-form: nothing types it, or a
        Mapping, Any or object does (a declared Dict[str, Any] field)."""
        return not self.annotations or any(
            _is_free_form_member(m)
            for a in self.annotations
            for m in unwrap_optional(a)
        )


def recipe_entry(annotations: Tuple[object, ...], key: object) -> RecipeEntry:
    """`key` of a recipe mapping typed by `annotations` (empty: untyped)."""
    models: List[Type[BaseModel]] = []
    values: List[object] = []
    free_form = False
    for member in (m for a in annotations for m in unwrap_optional(a)):
        cls = typing.get_origin(member) or member
        if _is_free_form_member(member):
            free_form = True
            if member is not typing.Any and member is not object:
                values.extend(typing.get_args(member)[-1:])
        elif cls is member and isinstance(cls, type) and issubclass(cls, BaseModel):
            models.append(cls)
    declared = [
        info.annotation
        for model in models
        for name, info in model.model_fields.items()
        if key in recipe_keys(name, info)
    ]
    return RecipeEntry(
        declared=bool(declared) and not free_form,
        annotations=tuple(values + declared),
    )


def recipe_items(annotations: Tuple[object, ...]) -> Tuple[object, ...]:
    """What types the items of a recipe list typed by `annotations`."""
    items: List[object] = []
    for member in (m for a in annotations for m in unwrap_optional(a)):
        origin = typing.get_origin(member)
        if (
            isinstance(origin, type)
            and issubclass(origin, (collections.abc.Sequence, collections.abc.Set))
            and not issubclass(origin, (str, bytes))
        ):
            items.extend(a for a in typing.get_args(member) if a is not Ellipsis)
    return tuple(items)
