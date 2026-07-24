import inspect
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Protocol,
    Tuple,
    Union,
    get_args,
    get_origin,
    runtime_checkable,
)

_TYPE_NAMES: Dict[type, str] = {str: "str", int: "int", bool: "bool"}


@dataclass
class ProbeParam:
    name: str
    type: str
    required: bool
    default: Optional[object] = None

    def to_dict(self) -> Dict[str, object]:
        return {
            "name": self.name,
            "type": self.type,
            "required": self.required,
            "default": self.default,
        }


@dataclass
class ProbeMethodSpec:
    command: str
    params: List[ProbeParam]
    description: str

    def to_dict(self) -> Dict[str, object]:
        return {
            "command": self.command,
            "description": self.description,
            "params": [p.to_dict() for p in self.params],
        }

    @classmethod
    def from_func(cls, fn: Callable, name: Optional[str]) -> "ProbeMethodSpec":
        sig = inspect.signature(fn)
        params: List[ProbeParam] = []
        for pname, p in sig.parameters.items():
            if pname == "self":
                continue
            if p.kind in (p.VAR_POSITIONAL, p.VAR_KEYWORD):
                raise TypeError(
                    f"probe method '{fn.__name__}' may not take *args/**kwargs "
                    f"(parameter '{pname}')"
                )
            type_name, required = _resolve_annotation(fn.__name__, pname, p)
            params.append(
                ProbeParam(
                    name=pname,
                    type=type_name,
                    required=required,
                    default=None if p.default is inspect.Parameter.empty else p.default,
                )
            )
        doc = inspect.getdoc(fn)
        if not doc:
            raise ValueError(
                f"probe method '{fn.__name__}' must have a docstring — it is the "
                f"help text shown to users and to the agent"
            )
        return cls(command=name or fn.__name__, params=params, description=doc)


def _resolve_annotation(
    fn_name: str, pname: str, p: inspect.Parameter
) -> Tuple[str, bool]:
    ann = p.annotation
    required = p.default is inspect.Parameter.empty
    if get_origin(ann) is Union:
        non_none = [a for a in get_args(ann) if a is not type(None)]
        if len(non_none) == 1:
            ann = non_none[0]
            required = False  # Optional[...] => caller may omit it
    if ann not in _TYPE_NAMES:
        raise TypeError(
            f"probe method '{fn_name}' parameter '{pname}' must be annotated "
            f"str/int/bool (or Optional of those); got {p.annotation!r}"
        )
    return _TYPE_NAMES[ann], required


def probe_method(name: Optional[str] = None) -> Callable[[Callable], Callable]:
    """Mark a provider method as an agent/CLI probe command.

    Command name defaults to the method name (override via ``name``). Non-self
    parameters become CLI flags (annotate them str/int/bool, or Optional of
    those); the FULL docstring is the help text; the return value is
    JSON-serialized then redacted. Methods MUST return metadata only — names,
    types, DDL, constraints, counts — never table rows or message payloads.
    """

    def deco(fn: Callable) -> Callable:
        # setattr (not `fn.__probe_command__ = ...`) because `fn: Callable` has no
        # such attribute — this keeps mypy clean without widening the parameter type.
        setattr(fn, "__probe_command__", ProbeMethodSpec.from_func(fn, name))  # noqa: B010
        return fn

    return deco


@runtime_checkable
class ProbeProvider(Protocol):
    def __enter__(self) -> "ProbeProvider": ...

    def __exit__(self, *exc: object) -> None: ...


@dataclass
class ProbeMethodResult:
    source_type: str
    command: str
    params: Dict[str, object]
    result: object
    truncated: bool = False

    def to_dict(self) -> Dict[str, object]:
        return {
            "source_type": self.source_type,
            "command": self.command,
            "params": self.params,
            "result": self.result,
            "truncated": self.truncated,
        }


# Returns the connector's config class. Typed Any because get_config_class is
# injected by the @config_class decorator at runtime — mypy can't see it, nor the
# pydantic model API (model_validate) / probe_provider_class contract on the result.
def _config_class(source_type: str) -> Any:
    from datahub.ingestion.source.source_registry import source_registry

    try:
        # registry.get raises KeyError (unknown source_type) or ConfigurationError
        # (plugin failed to load) — neither is in the framework's ValueError/
        # TypeError/AssertionError contract, so normalize to ValueError.
        source_cls = source_registry.get(source_type)
    except Exception as exc:
        raise ValueError(
            f"unknown or unloadable source type '{source_type}': {exc}"
        ) from exc
    get_config_class = getattr(source_cls, "get_config_class", None)
    return get_config_class() if get_config_class is not None else None


def _provider_class(source_type: str) -> Optional[type]:
    getter = getattr(_config_class(source_type), "probe_provider_class", None)
    return getter() if callable(getter) else None


def _iter_specs(provider_cls: type) -> List[Tuple[str, ProbeMethodSpec]]:
    found: Dict[str, ProbeMethodSpec] = {}
    for attr in dir(provider_cls):
        spec = getattr(getattr(provider_cls, attr, None), "__probe_command__", None)
        if isinstance(spec, ProbeMethodSpec):
            found[spec.command] = spec
    return sorted(found.items())


def list_probe_methods(source_type: str) -> List[ProbeMethodSpec]:
    provider_cls = _provider_class(source_type)
    return [spec for _, spec in _iter_specs(provider_cls)] if provider_cls else []


def _coerce(param: ProbeParam, value: object) -> object:
    if param.type == "int":
        # Agent kwargs may arrive as native int/float/bool or a numeric string
        # (CLI flags); bool is an int subclass so it is covered here too. Assert
        # rather than str()-route so `int(5.0)` and `int(True)` don't break.
        assert isinstance(value, (int, float, str)), (
            f"parameter '{param.name}' expects an int-coercible value, got "
            f"{type(value).__name__}"
        )
        return int(value)
    if param.type == "bool":
        return str(value).lower() in ("1", "true", "yes", "on")
    return str(value)


def _coerce_kwargs(spec: ProbeMethodSpec, raw: Dict[str, object]) -> Dict[str, object]:
    by_name = {p.name: p for p in spec.params}
    unknown = set(raw) - set(by_name)
    if unknown:
        raise ValueError(f"unknown parameter(s): {', '.join(sorted(unknown))}")
    out: Dict[str, object] = {}
    for p in spec.params:
        if p.name in raw:
            out[p.name] = _coerce(p, raw[p.name])
        elif p.required:
            raise ValueError(f"missing required parameter '--{p.name}'")
    return out


def _bound_method(provider: object, command: str) -> Callable:
    for attr in dir(type(provider)):
        spec = getattr(getattr(type(provider), attr, None), "__probe_command__", None)
        if isinstance(spec, ProbeMethodSpec) and spec.command == command:
            return getattr(provider, attr)
    raise ValueError(f"no probe method bound for command '{command}'")


def run_probe_method(
    source_type: str,
    config_dict: Dict[str, object],
    command: str,
    kwargs: Dict[str, object],
) -> ProbeMethodResult:
    provider_cls = _provider_class(source_type)
    if provider_cls is None:
        raise ValueError(f"source '{source_type}' has no probe methods")
    specs = dict(_iter_specs(provider_cls))
    if command not in specs:
        raise ValueError(
            f"unknown probe method '{command}' for source '{source_type}'; "
            f"available: {', '.join(sorted(specs)) or '(none)'}"
        )
    call_kwargs = _coerce_kwargs(specs[command], kwargs)
    config = _config_class(source_type).model_validate(config_dict)
    with config.build_probe_provider() as provider:
        result = _bound_method(provider, command)(**call_kwargs)
    return ProbeMethodResult(
        source_type=source_type, command=command, params=call_kwargs, result=result
    )
