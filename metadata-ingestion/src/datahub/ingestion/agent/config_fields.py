"""A config class's fields and what their annotations hold.

Below both readers of a config: `declarations` (the probe's field markers) and
`introspect` (describe, secret discovery), which imports the former.
"""

import types
import typing
from typing import FrozenSet, Iterator, List, Tuple

from pydantic import SecretStr
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
