import inspect
from dataclasses import dataclass, field
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
)

from datahub.configuration.env_vars import get_disable_agent_probe_raw_access

_TYPE_NAMES: Dict[type, str] = {str: "str", int: "int", bool: "bool"}

# The most items any probe command may return, whatever the caller asked for.
# Probe output is read by an agent with a finite context window, and a listing
# can legitimately run to tens of thousands (one row per column in a large
# warehouse, one topic per tenant on a shared cluster), so an unbounded limit
# floods the reader rather than informing it. Well above any listing a person
# reads through, well below a flood.
MAX_PROBE_ITEMS = 1000


def clamp_item_limit(limit: int) -> int:
    """Bound a caller's limit into a range that can actually be returned.

    A limit at or below zero is the interesting case: `items[:0]` and
    `items[:-1]` both "work" but mean nothing a caller intended -- `-1` silently
    drops the last item and then reports the result as truncated.
    """
    return max(1, min(int(limit), MAX_PROBE_ITEMS))


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
    # Names a parameter carrying raw SQL. The framework scope-checks it before
    # invoking the method, so a provider cannot reach its engine with an
    # unchecked query even though the query arrives as an ordinary parameter.
    scoped_sql_param: Optional[str] = None
    # Names a parameter carrying an API path, checked against the provider's
    # api_allowlist the same way.
    scoped_path_param: Optional[str] = None
    # Names a parameter bounding how many rows the method returns. Declared so
    # the framework can clamp it *before* invoking: a getter fetches `limit + 1`
    # rows, so a limit of 10_000_000 is a fetch the connector really performs,
    # and trimming the output afterwards would be too late to matter.
    row_limit_param: Optional[str] = None
    # The DataHub subtype the returned names are, for a command that returns a
    # listing. Declared here because the getter knows it and the caller would
    # otherwise have to guess an exact subtype string to pass to `probe filter`.
    # None for commands that return something other than one kind of name --
    # `sql` cannot declare one, since what a catalog query selects is the
    # caller's choice.
    kind: Optional[str] = None

    def to_dict(self) -> Dict[str, object]:
        return {
            "command": self.command,
            "description": self.description,
            "params": [p.to_dict() for p in self.params],
            "kind": self.kind,
        }

    @classmethod
    def from_func(
        cls,
        fn: Callable,
        name: Optional[str],
        scoped_sql_param: Optional[str] = None,
        scoped_path_param: Optional[str] = None,
        kind: Optional[str] = None,
        row_limit_param: Optional[str] = None,
    ) -> "ProbeMethodSpec":
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
        declared = {p.name for p in params}
        for label, scoped in (
            ("scoped_sql_param", scoped_sql_param),
            ("scoped_path_param", scoped_path_param),
            ("row_limit_param", row_limit_param),
        ):
            if scoped is not None and scoped not in declared:
                raise ValueError(
                    f"probe method '{fn.__name__}' declares {label}='{scoped}' "
                    f"but has no such parameter; the framework would have "
                    f"nothing to check"
                )
        return cls(
            command=name or fn.__name__,
            params=params,
            description=doc,
            scoped_sql_param=scoped_sql_param,
            scoped_path_param=scoped_path_param,
            kind=str(kind) if kind is not None else None,
            row_limit_param=row_limit_param,
        )


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


def probe_method(
    name: Optional[str] = None,
    scoped_sql_param: Optional[str] = None,
    scoped_path_param: Optional[str] = None,
    kind: Optional[Any] = None,
    row_limit_param: Optional[str] = None,
) -> Callable[[Callable], Callable]:
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
        setattr(  # noqa: B010
            fn,
            "__probe_command__",
            ProbeMethodSpec.from_func(
                fn, name, scoped_sql_param, scoped_path_param, kind, row_limit_param
            ),
        )
        return fn

    return deco


class ProbeProvider(Protocol):
    """What a connector's probe provider must be: constructible from the recipe's
    config, and a context manager so whatever it opened gets closed.

    `for_config` lives here rather than as a second hook on the config class so
    that `probe_provider_class()` is the ONLY place naming the provider. When the
    config named it twice -- once for discovery, once for construction -- the two
    could disagree, and for Snowflake and BigQuery they did: both advertised six
    SQLAlchemy getters their own provider does not have, each of which failed at
    invocation. One naming site makes that unrepresentable rather than merely
    tested for.
    """

    @classmethod
    def for_config(cls, config: Any) -> "ProbeProvider": ...

    def __enter__(self) -> "ProbeProvider": ...

    def __exit__(self, *exc: object) -> None: ...


@dataclass
class ProbeMethodResult:
    source_type: str
    command: str
    params: Dict[str, object]
    result: object
    # The subtype the returned names are, when the command declared one. Echoed
    # so a caller can pass it to `probe filter` without knowing the vocabulary.
    kind: Optional[str] = None
    # Non-fatal problems the provider hit while building `result` (see
    # agent.verdicts.ProbeSoftError): one sub-fetch couldn't be read cleanly, so
    # it degraded to an empty/partial contribution instead of failing the
    # whole command. A provider surfaces
    # these by exposing its own `warnings: List[str]` attribute, which
    # run_probe_method reads back after the call; a provider with no such
    # attribute always reports an empty list here.
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, object]:
        return {
            "source_type": self.source_type,
            "command": self.command,
            "params": self.params,
            "kind": self.kind,
            "result": self.result,
            "warnings": self.warnings,
        }


# Returns the connector's config class. Typed Any because get_config_class is
# injected by the @config_class decorator at runtime — mypy can't see it, nor the
# pydantic model API (model_validate) / probe_provider_class contract on the result.
def config_class_for(source_type: str) -> Any:
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
    getter = getattr(config_class_for(source_type), "probe_provider_class", None)
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


def _bounded_kwargs(
    spec: ProbeMethodSpec, call_kwargs: Dict[str, object]
) -> Dict[str, object]:
    """Clamp a declared row-limit parameter into the returnable range.

    Separate from _enforce_gates because it is a different kind of act: gates
    refuse, this one adjusts. An omitted limit is left out so the getter's own
    default applies rather than being overwritten with a framework guess.
    """
    if spec.row_limit_param is None or spec.row_limit_param not in call_kwargs:
        return call_kwargs
    raw = call_kwargs[spec.row_limit_param]
    # _coerce already made this an int for an int-annotated parameter.
    assert isinstance(raw, int)
    return {**call_kwargs, spec.row_limit_param: clamp_item_limit(raw)}


def _enforce_gates(
    spec: ProbeMethodSpec, provider: object, call_kwargs: Dict[str, object]
) -> None:
    """Check a scoped parameter before the provider ever sees it.

    Here rather than inside each getter on purpose: a connector cannot forget a
    check it does not perform. A getter declares which parameter carries raw SQL
    or an API path, and the framework is the only thing that gates it -- the same
    "declare, framework enforces" split as Filters(...) on a config field.

    Needs the live provider, since the dialect and the endpoint allowlist are
    properties of the connector's client, not of the declaration.
    """
    is_passthrough = (
        spec.scoped_sql_param is not None or spec.scoped_path_param is not None
    )
    if is_passthrough and get_disable_agent_probe_raw_access():
        # Withholds only the passthroughs; typed getters take no caller-supplied
        # query or path, so there is nothing in them to withhold.
        raise ValueError(
            f"probe command '{spec.command}' takes a caller-supplied query or "
            f"path, and raw probe access is switched off here "
            f"(DATAHUB_PROBE_DISABLE_RAW_ACCESS); this connector's other probe "
            f"commands still work"
        )

    if spec.scoped_sql_param is not None:
        # Lazy import: the gates pull in sqlglot, which a probe that runs no
        # queries should not pay for.
        from datahub.ingestion.agent.sql_gate import check_query_scope

        dialect = getattr(provider, "sql_dialect", None)
        if not isinstance(dialect, str) or not dialect:
            raise ValueError(
                f"probe method '{spec.command}' takes SQL but its provider "
                f"declares no sql_dialect, so the query cannot be checked"
            )
        # The scope is the connector's declaration of what its dialect's catalog
        # is; absent one, check_query_scope falls back to information_schema only.
        check_query_scope(
            str(call_kwargs[spec.scoped_sql_param]),
            platform=dialect,
            scope=getattr(provider, "catalog_scope", None),
        )

    if spec.scoped_path_param is not None:
        from datahub.ingestion.agent.api_gate import check_api_request

        allowlist = getattr(provider, "api_allowlist", None)
        if allowlist is None:
            # Distinct from an unlisted path, which is the caller's to fix. An
            # absent allowlist means no path can ever work, so reporting it as
            # "not in this connector's allowlist" would send the caller off to
            # rewrite a path when the connector is what is incomplete. Mirrors
            # the missing-sql_dialect refusal above.
            raise ValueError(
                f"probe method '{spec.command}' takes an API path but its "
                f"provider declares no api_allowlist, so no path can be permitted"
            )
        # GET-only is the rule, so the method is not the caller's to choose.
        check_api_request("GET", str(call_kwargs[spec.scoped_path_param]), allowlist)


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
    call_kwargs = _bounded_kwargs(
        specs[command], _coerce_kwargs(specs[command], kwargs)
    )
    config = config_class_for(source_type).model_validate(config_dict)
    builder = getattr(provider_cls, "for_config", None)
    if not callable(builder):
        raise ValueError(
            f"probe provider '{provider_cls.__name__}' for source "
            f"'{source_type}' has no for_config(config) classmethod, so it "
            f"cannot be built from the recipe"
        )
    # The same class discovery described, so the two cannot disagree about what
    # this source can do.
    with builder(config) as provider:
        _enforce_gates(specs[command], provider, call_kwargs)
        result = _bound_method(provider, command)(**call_kwargs)
        # Optional, source-agnostic: a provider that degrades a sub-fetch
        # instead of failing outright (see agent.verdicts.ProbeSoftError) may
        # expose its own `warnings` list to report that here. Duck-typed
        # rather than part of the ProbeProvider Protocol, since most
        # providers have nothing to report and shouldn't need to declare it.
        provider_warnings = getattr(provider, "warnings", None)
    return ProbeMethodResult(
        source_type=source_type,
        command=command,
        params=call_kwargs,
        kind=specs[command].kind,
        result=result,
        warnings=list(provider_warnings) if provider_warnings else [],
    )
