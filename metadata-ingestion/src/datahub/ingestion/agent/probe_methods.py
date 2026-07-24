import inspect
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Union, get_args, get_origin

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
) -> "tuple[str, bool]":
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
