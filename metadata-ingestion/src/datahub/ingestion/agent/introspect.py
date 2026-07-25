import typing
from typing import Dict, List, Optional

from pydantic import SecretStr
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.ingestion.agent.models import FieldKind, FieldSpec, SourceSpec
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
