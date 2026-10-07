import functools
import inspect
from contextlib import ExitStack
from dataclasses import dataclass, field, replace
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    Callable,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Mapping,
    NoReturn,
    Optional,
    Protocol,
    Set,
    Tuple,
    Type,
    Union,
    cast,
    get_args,
    get_origin,
    runtime_checkable,
)

from datahub.configuration.env_vars import (
    get_disable_agent_probe_raw_access,
    get_probe_disabled,
)
from datahub.ingestion.agent.api_gate import READ_METHOD, check_api_request
from datahub.ingestion.agent.config_validation import validate_source_config
from datahub.ingestion.agent.error_policy import (
    NETWORK_REASON_HINTS,
    PASS_THROUGH,
    call_config_hook,
    classify_foreign,
    foreign_label,
    is_callers_sql_error,
    is_trusted,
    label_foreign_text,
    missing_module,
    name_foreign,
    network_reason,
    police_trusted,
    verbose_detail,
)
from datahub.ingestion.agent.log_guard import (
    FRAMEWORK_LOGGERS,
    quiet_reused_logs,
)
from datahub.ingestion.agent.models import ProbeRunEnvelope
from datahub.ingestion.agent.redact import scrub_text
from datahub.ingestion.agent.verdicts import (
    ProbeArgumentError,
    ProbeConnectionError,
    ProbeInternalError,
    ProbeReadFailed,
)

if TYPE_CHECKING:
    # Annotations only: configuration.common stays off this module's import
    # path (see source_class_for).
    from datahub.configuration.common import ConfigModel

_TYPE_NAMES: Dict[type, str] = {str: "str", int: "int", bool: "bool"}

# The most items any probe command returns. The reader is an agent with a finite
# context window, and a listing can run to tens of thousands of names.
MAX_PROBE_ITEMS = 1000


def clamp_item_limit(limit: int) -> int:
    """Bound a caller's limit to 1..MAX_PROBE_ITEMS: `items[:-1]` would drop the
    last item and still report the result truncated."""
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
    # The parameter carrying raw SQL; the framework scope-checks it first.
    scoped_sql_param: Optional[str] = None
    # The parameter carrying an API path; checked against api_allowlist first.
    scoped_path_param: Optional[str] = None
    # The parameter bounding the result, clamped before the call: the getter
    # really fetches what it is asked for, so trimming afterwards is too late.
    row_limit_param: Optional[str] = None
    # The command returns its own envelope and truncation flag (`sql`), so the
    # framework adds no +1 of its own. Declared, not inferred from scoped_sql_param.
    shapes_own_result: bool = False
    # The parameters naming the container the result lives under, outermost
    # first; echoed as parent_path so a caller need not restate them as --parent.
    parent_params: Tuple[str, ...] = ()
    # The DataHub subtype of the returned names, so `probe filter` needs no
    # guessed kind. None when the caller decides what comes back (`sql`).
    kind: Optional[str] = None

    def to_dict(self) -> Dict[str, object]:
        return {
            "command": self.command,
            "description": self.description,
            "params": [p.to_dict() for p in self.params],
            "parent_params": list(self.parent_params),
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
        parent_params: Tuple[str, ...] = (),
        shapes_own_result: bool = False,
    ) -> "ProbeMethodSpec":
        params = _probe_params(fn)
        doc = _required_doc(fn)
        _check_named_params(
            fn_name=fn.__name__,
            declared={p.name for p in params},
            parent_params=parent_params,
            gates={
                "scoped_sql_param": scoped_sql_param,
                "scoped_path_param": scoped_path_param,
                "row_limit_param": row_limit_param,
            },
        )
        return cls(
            command=name or fn.__name__,
            params=params,
            description=doc,
            scoped_sql_param=scoped_sql_param,
            scoped_path_param=scoped_path_param,
            kind=str(kind) if kind is not None else None,
            row_limit_param=row_limit_param,
            shapes_own_result=shapes_own_result,
            parent_params=tuple(parent_params),
        )


def _probe_params(fn: Callable) -> List[ProbeParam]:
    """The CLI flags a probe method takes: every parameter but `self`."""
    params: List[ProbeParam] = []
    for pname, p in inspect.signature(fn).parameters.items():
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
    return params


def _required_doc(fn: Callable) -> str:
    doc = inspect.getdoc(fn)
    if not doc:
        raise ValueError(
            f"probe method '{fn.__name__}' must have a docstring — it is the "
            f"help text shown to users and to the agent"
        )
    return doc


def _check_named_params(
    fn_name: str,
    declared: Set[str],
    parent_params: Tuple[str, ...],
    gates: Mapping[str, Optional[str]],
) -> None:
    """Refuse a parent or gate parameter the method does not take: the
    framework would have nothing to echo or check."""
    for parent_param in parent_params:
        if parent_param not in declared:
            raise ValueError(
                f"probe method '{fn_name}' declares parent_params "
                f"'{parent_param}' but has no such parameter"
            )
    for label, scoped in gates.items():
        if scoped is not None and scoped not in declared:
            raise ValueError(
                f"probe method '{fn_name}' declares {label}='{scoped}' "
                f"but has no such parameter; the framework would have "
                f"nothing to check"
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
            # `required` stays as the default decided it: Optional[...] says the
            # parameter accepts None, not that it may be left out.
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
    parent_params: Tuple[str, ...] = (),
    shapes_own_result: bool = False,
) -> Callable[[Callable], Callable]:
    """Mark a provider method as an agent/CLI probe command.

    Command name defaults to the method name (override via ``name``). Non-self
    parameters become CLI flags (annotate them str/int/bool, or Optional of
    those); the FULL docstring is the help text; the return value is
    JSON-serialized then redacted. Methods MUST return metadata only — names,
    types, DDL, constraints, counts — never table rows or message payloads.
    """

    def deco(fn: Callable) -> Callable:
        # setattr: `Callable` declares no such attribute for mypy.
        setattr(  # noqa: B010
            fn,
            "__probe_command__",
            ProbeMethodSpec.from_func(
                fn,
                name=name,
                scoped_sql_param=scoped_sql_param,
                scoped_path_param=scoped_path_param,
                kind=kind,
                row_limit_param=row_limit_param,
                parent_params=parent_params,
                shapes_own_result=shapes_own_result,
            ),
        )
        return fn

    return deco


@runtime_checkable
class ProbeProvider(Protocol):
    """A probe provider: built from the recipe's config, and a context manager so
    whatever it opened is closed.

    `for_config` lives here, not as a config hook, so `probe_provider_class()` is
    the only place naming the provider: discovery and execution cannot disagree.
    """

    @classmethod
    def for_config(cls, config: Any) -> "ProbeProvider": ...

    def __enter__(self) -> "ProbeProvider": ...

    def __exit__(self, *exc: object) -> None: ...


# Every hook the framework reads off a config by name: the guide's hook
# reference table, which test_probe_contract checks against this list, as it
# refuses a `probe_*` config method outside it (or outside the SQL family's
# list, on a SQLCommonConfig). Each is read through config_hook except
# probe_validation_context, which config_validation reads itself (this module
# imports it) and calls under the same policy.
CONFIG_HOOKS: FrozenSet[str] = frozenset(
    {
        # _provider_class: the provider class, for `probe methods` and `run`.
        "probe_provider_class",
        # config_validation: the pydantic context a source type validates with.
        "probe_validation_context",
        # filter_check._match_target: the string a pattern is matched against.
        "probe_match_target",
        # filter_check._override_verdict: the connector's verdict for one name
        # when no single pattern states it; see VerdictContext.
        "probe_verdict_override",
        # declarations.declared_unfiltered_kinds: kinds nothing filters, on purpose.
        "probe_unfiltered_kinds",
        # filter_check._parent_exclusion: the containers above a kind.
        "probe_ancestor_kinds",
        # declared_kind_overrides: the kind a command reports when the config
        # class, not the provider, decides it.
        "probe_kind_overrides",
    }
)

# The CONFIG_HOOKS the framework calls on the config class, where no recipe has
# been validated (`probe methods`, `describe`, validation itself), so each must
# be a classmethod or staticmethod: an instance method fails only when called.
# test_probe_contract checks every registered config against this list.
CLASS_CONFIG_HOOKS: Tuple[str, ...] = (
    # _provider_class.
    "probe_provider_class",
    # config_validation.validate_source_config.
    "probe_validation_context",
    # list_probe_methods, run_probe_method, introspect.declared_kinds_for_class.
    "probe_kind_overrides",
    # introspect._filter_kinds_by_field.
    "probe_unfiltered_kinds",
)


# The class attribute naming the hooks a config family's own code reads beyond
# CONFIG_HOOKS, set on the family's base class: SQLCommonConfig names
# SQL_FAMILY_HOOKS. Read off the config class rather than registered here, so
# the family's config module need not import the probe framework.
CONFIG_HOOK_FAMILY_ATTRIBUTE = "__probe_family_hooks__"


def _probe_named(cls: type, exempt: Callable[[str, object], bool]) -> List[str]:
    """`probe_` attributes `cls` or a base defines, minus the exempt ones."""
    return sorted(
        {
            name
            for klass in cls.__mro__
            if klass is not object
            for name, value in vars(klass).items()
            if name.startswith("probe_") and not exempt(name, value)
        }
    )


def unknown_config_hooks(config_cls: type) -> List[str]:
    """`probe_` attributes on a config class that no reader calls: a hook
    removed or renamed since the connector was written, or misspelled. Read
    by name, such a hook would silently do nothing."""
    known: Set[str] = set(CONFIG_HOOKS)
    known |= getattr(config_cls, CONFIG_HOOK_FAMILY_ATTRIBUTE, frozenset())
    fields = getattr(config_cls, "model_fields", None) or {}
    return _probe_named(
        config_cls, lambda name, _value: name in known or name in fields
    )


def unknown_provider_attributes(provider_cls: type) -> List[str]:
    """`probe_` attributes on a provider class that are neither a
    PROVIDER_ATTRIBUTES name nor a probe command."""
    return _probe_named(
        provider_cls,
        lambda name, value: (
            name in PROVIDER_ATTRIBUTES
            or isinstance(getattr(value, "__probe_command__", None), ProbeMethodSpec)
        ),
    )


def _unknown_names_error(owner: type, names: List[str], what: str) -> str:
    return (
        f"the connector is defective: {owner.__name__} defines "
        f"{', '.join(names)}, which the probe never reads; a {what} by that "
        f"name was removed or is misspelled (see "
        f"metadata-ingestion/docs/dev_guides/probe_interface.md)"
    )


@functools.lru_cache(maxsize=None)
def _refuse_unknown_config_hooks(config_cls: type) -> None:
    unknown = unknown_config_hooks(config_cls)
    if unknown:
        raise ProbeInternalError(_unknown_names_error(config_cls, unknown, "hook"))


@functools.lru_cache(maxsize=None)
def _refuse_unknown_provider_attributes(provider_cls: type) -> None:
    unknown = unknown_provider_attributes(provider_cls)
    if unknown:
        raise ProbeInternalError(
            _unknown_names_error(provider_cls, unknown, "provider attribute")
        )


def config_hook(config: object, name: str) -> Optional[Callable[..., object]]:
    """The config's `name` hook, or None where it declares none.

    Only a CONFIG_HOOKS name may be read, so that list is what the framework
    reads; any other is the framework's own defect. The hook is returned
    behind agent.error_policy.call_config_hook: one that raises is the
    connector's defect, reported rather than swallowed, since swallowing it
    would make "declared" read as "not declared".
    """
    if name not in CONFIG_HOOKS:
        raise ProbeInternalError(f"'{name}' is not a config hook the framework reads")
    if config is not None:
        # At the first hook read, so a stale hook fails loudly rather than
        # being silently skipped.
        _refuse_unknown_config_hooks(
            config if isinstance(config, type) else type(config)
        )
    hook = getattr(config, name, None)
    if not callable(hook):
        return None
    return functools.partial(call_config_hook, config, name, hook)


# The optional attributes the framework reads off a provider by name, beyond the
# ProbeProvider protocol. ProbeProviderBase declares each with a default that
# reads as absent. test_probe_contract refuses a near-miss name, which nothing
# would read.
PROVIDER_ATTRIBUTES: FrozenSet[str] = frozenset(
    {
        # _enforce_gates: a query's dialect and catalog, a path's allowlist and base.
        "sql_dialect",
        "catalog_scope",
        "api_allowlist",
        "api_base_url",
        # Read back after each command.
        "warnings",
        "failures",
        "probe_report",
        # run_probe_method: loggers dropped while the probe runs.
        "silenced_loggers",
        # error_policy.foreign_label: the vendor's code for a foreign error.
        "probe_error_code",
    }
)


@dataclass
class ProbeMethodResult:
    source_type: str
    command: str
    params: Dict[str, object]
    result: object
    # The subtype of the returned names, to pass to `probe filter`.
    kind: Optional[str] = None
    # The container the names live under, from the arguments named in
    # ProbeMethodSpec.parent_params: `probe filter` needs it to build ingestion's
    # identifier.
    parent_path: List[str] = field(default_factory=list)
    # Sub-fetches that degraded rather than failed (see ProbeSoftError).
    warnings: List[str] = field(default_factory=list)
    # The limit cut the listing short.
    truncated: bool = False
    # Reads that could not complete, from the provider or its SourceReport. Any
    # entry means the result is incomplete, and the CLI exits 3: an empty result
    # must never stand in for "could not read".
    failures: List[str] = field(default_factory=list)

    def to_dict(self) -> ProbeRunEnvelope:
        return {
            "source_type": self.source_type,
            "command": self.command,
            "params": self.params,
            "kind": self.kind,
            "parent_path": self.parent_path,
            "result": self.result,
            "truncated": self.truncated,
            "warnings": self.warnings,
            "failures": self.failures,
        }


def source_class_for(source_type: str) -> type:
    """The registered Source class for `source_type`; a name the caller got
    wrong is a ValueError (exit 2)."""
    # lazy: keeps the configuration module off this module's import path
    from datahub.configuration.common import ConfigurationError
    from datahub.ingestion.source.source_registry import source_registry

    # The ValueErrors below quote {exc}: the registry's own message, or the
    # import system's about a path the caller wrote -- never foreign text.
    try:
        return source_registry.get(source_type)
    except (KeyError, ConfigurationError) as exc:
        # An unregistered name, or a plugin whose extra is not installed: the
        # caller's to fix (exit 2). Anything else a plugin's module raises is a
        # defect and propagates (exit 1).
        raise ValueError(
            f"unknown or unloadable source type '{source_type}': {exc}"
        ) from exc
    except ImportError as exc:
        # A dotted or colon source_type is an import path the caller wrote, so a
        # typo in it is exit 2. No registered name contains either, so an
        # ImportError inside a registered plugin still propagates (exit 1).
        if "." in source_type or ":" in source_type:
            raise ValueError(
                f"unknown or unloadable source type '{source_type}': {exc}"
            ) from exc
        raise


def config_class_for(source_type: str) -> Optional[Type["ConfigModel"]]:
    """The config class `source_type` validates its recipe with, or None for a
    source that declares none. Raises as source_class_for does."""
    get_config_class = getattr(source_class_for(source_type), "get_config_class", None)
    return get_config_class() if get_config_class is not None else None


def require_config_class(source_type: str) -> Type["ConfigModel"]:
    """config_class_for, refusing a source that declares no config class. A
    TypeError, so exit 1: a registered source with nothing to validate its
    recipe against is the connector's defect, not the caller's input."""
    config_cls = config_class_for(source_type)
    if config_cls is None:
        raise TypeError(f"Source {source_type!r} does not define a config class")
    return config_cls


def _silenced_loggers(provider_cls: type) -> Tuple[str, ...]:
    """The provider's `silenced_loggers` (see log_guard.quiet_reused_logs).

    Must be a tuple or list of names: a bare string would silence one-letter
    loggers. A framework logger, one under it, or an ancestor it inherits its
    level from (root included) is refused, so the probe's own logs stay visible.
    """
    declared = _provider_attribute(provider_cls, "silenced_loggers", ())
    if not isinstance(declared, (tuple, list)) or not all(
        isinstance(name, str) and name for name in declared
    ):
        raise ProbeInternalError(
            f"{provider_cls.__name__}.silenced_loggers must be a tuple or list "
            f"of logger names, got {type(declared).__name__}; this is a defect in "
            f"the probe provider"
        )
    for name in declared:
        # logging.getLogger("root") is the root logger.
        if name == "root" or any(
            name == f or name.startswith(f + ".") or f.startswith(name + ".")
            for f in FRAMEWORK_LOGGERS
        ):
            raise ProbeInternalError(
                f"silenced_loggers cannot name '{name}': it would hide the probe "
                f"framework's own logs; this is a defect in the probe provider"
            )
    return tuple(declared)


def _provider_class(source_type: str) -> Optional[Type[ProbeProvider]]:
    getter = config_hook(config_class_for(source_type), "probe_provider_class")
    provider_cls = getter() if getter else None
    if isinstance(provider_cls, type):
        _refuse_unknown_provider_attributes(provider_cls)
    return cast(Optional[Type[ProbeProvider]], provider_cls)


def _iter_specs(provider_cls: type) -> List[Tuple[str, ProbeMethodSpec]]:
    """Every command this provider declares, as (command, spec), sorted.

    Two attributes declaring one command are refused. _enforce_gates checks one
    declaration and _bound_method invokes one method, and only one attribute per
    command guarantees they are the same: otherwise a gated method could run
    under an ungated declaration. To override an inherited command, redefine the
    same method name.
    """
    found: Dict[str, ProbeMethodSpec] = {}
    owner: Dict[str, str] = {}
    for attr in dir(provider_cls):
        spec = getattr(getattr(provider_cls, attr, None), "__probe_command__", None)
        if not isinstance(spec, ProbeMethodSpec):
            continue
        clash = owner.get(spec.command)
        if clash is not None and clash != attr:
            raise ValueError(
                f"{provider_cls.__name__} declares command '{spec.command}' on "
                f"two different methods ('{clash}' and '{attr}'). The framework "
                f"checks one declaration and invokes one method, and with two "
                f"of each it cannot guarantee they are the same one -- so a "
                f"gated command could be checked against an ungated "
                f"declaration. To override an inherited command, redefine the "
                f"same method name instead of adding a second one."
            )
        found[spec.command] = spec
        owner[spec.command] = attr
    return sorted(found.items())


def declared_kind_overrides(config: object) -> Dict[str, str]:
    """command -> kind, where the config class decides it (probe_kind_overrides).

    One provider may serve configs that disagree about a command's kind (the SQL
    family's `containers`: schemas or databases). A classmethod, so discovery
    answers without a recipe.
    """
    hook = config_hook(config, "probe_kind_overrides")
    if hook is None:
        return {}
    declared = cast(Mapping[object, object], hook())
    return {str(key): str(value) for key, value in declared.items()}


def list_probe_methods(source_type: str) -> List[ProbeMethodSpec]:
    """Every command this source offers, with the kind each one reports."""
    provider_cls = _provider_class(source_type)
    if provider_cls is None:
        return []
    overrides = declared_kind_overrides(config_class_for(source_type))
    return [
        replace(spec, kind=overrides[spec.command])
        if spec.command in overrides
        else spec
        for _, spec in _iter_specs(provider_cls)
    ]


class _BareFlag:
    """A `--flag` given with no value. The CLI parser sees tokens, not types, so
    _coerce decides: true for a bool, exit 2 for anything else."""


BARE_FLAG = _BareFlag()


def _coerce(param: ProbeParam, value: object) -> object:
    if value is BARE_FLAG:
        if param.type != "bool":
            raise ProbeArgumentError(
                f"'--{param.name}' expects a {param.type} value but was given none"
            )
        return True
    if param.type == "int":
        # Native numbers (bool included) or a numeric string. Raised, not
        # asserted: `python -O` strips asserts, and this is caller input.
        if not isinstance(value, (int, float, str)):
            raise ProbeArgumentError(
                f"parameter '{param.name}' expects an int-coercible value, got "
                f"{type(value).__name__}"
            )
        try:
            return int(value)
        except ValueError:
            raise ProbeArgumentError(
                f"parameter '{param.name}' expects an int; got {value!r}"
            ) from None
    if param.type == "bool":
        # Anything unrecognised is refused: reading it as False would silently
        # narrow the answer.
        text = str(value).lower()
        if text in ("1", "true", "yes", "on"):
            return True
        if text in ("0", "false", "no", "off"):
            return False
        raise ProbeArgumentError(
            f"parameter '{param.name}' expects a boolean "
            f"(true/false, yes/no, on/off, 1/0); got {value!r}"
        )
    return str(value)


def _coerce_kwargs(spec: ProbeMethodSpec, raw: Dict[str, object]) -> Dict[str, object]:
    by_name = {p.name: p for p in spec.params}
    unknown = set(raw) - set(by_name)
    if unknown:
        raise ProbeArgumentError(f"unknown parameter(s): {', '.join(sorted(unknown))}")
    out: Dict[str, object] = {}
    for p in spec.params:
        if p.name in raw:
            out[p.name] = _coerce(p, raw[p.name])
        elif p.required:
            raise ProbeArgumentError(f"missing required parameter '--{p.name}'")
    return out


def _bound_method(provider: object, command: str) -> Callable:
    for attr in dir(type(provider)):
        spec = getattr(getattr(type(provider), attr, None), "__probe_command__", None)
        if isinstance(spec, ProbeMethodSpec) and spec.command == command:
            return getattr(provider, attr)
    # The command was found on the provider class, so a built provider lacking
    # it means for_config returned something else: the provider's defect.
    raise ProbeInternalError(
        f"the provider built for this source has no method for command "
        f"'{command}'; this is a defect in the probe provider"
    )


def _effective_row_limit(
    spec: ProbeMethodSpec, call_kwargs: Dict[str, object]
) -> Optional[int]:
    """How many items this call may return: the caller's clamped limit, else the
    getter's declared default, so truncation is detected without a --limit."""
    if spec.row_limit_param is None:
        return None
    raw = call_kwargs.get(spec.row_limit_param)
    if raw is None:
        declared = next(
            (p.default for p in spec.params if p.name == spec.row_limit_param), None
        )
        raw = declared if isinstance(declared, int) else None
    if not isinstance(raw, int):
        return None
    return clamp_item_limit(raw)


def _bounded_kwargs(
    spec: ProbeMethodSpec, call_kwargs: Dict[str, object]
) -> Dict[str, object]:
    """Clamp a declared row-limit parameter, and ask for one item past it.

    Truncation is detected by comparing what came back against the limit, so a
    getter returning exactly `limit` items would read as complete. Done here so
    no getter can forget it.
    """
    limit = _effective_row_limit(spec, call_kwargs)
    if limit is None:
        return call_kwargs
    assert spec.row_limit_param is not None
    if spec.shapes_own_result:
        # The command adds its own +1.
        return {**call_kwargs, spec.row_limit_param: limit}
    return {**call_kwargs, spec.row_limit_param: limit + 1}


def _refuse_withheld_passthrough(spec: ProbeMethodSpec, source_type: str) -> None:
    """The operator's raw-access kill switch, checked before the provider is
    built so a slow or unreachable source cannot hide it behind a connection
    error. The refusal names the commands that still work for this connector."""
    if spec.scoped_sql_param is None and spec.scoped_path_param is None:
        return
    if not get_disable_agent_probe_raw_access():
        return
    others = sorted(
        other.command
        for other in list_probe_methods(source_type)
        if other.command != spec.command
        and other.scoped_sql_param is None
        and other.scoped_path_param is None
    )
    remaining = (
        f"this connector's other probe commands still work: {', '.join(others)}"
        if others
        else f"'{source_type}' declares no other probe command, so its probe is "
        f"fully withheld here"
    )
    raise ProbeArgumentError(
        f"probe command '{spec.command}' takes a caller-supplied query or "
        f"path, and raw probe access is switched off here "
        f"(DATAHUB_PROBE_DISABLE_RAW_ACCESS); {remaining}"
    )


def _enforce_gates(
    spec: ProbeMethodSpec, provider: object, call_kwargs: Dict[str, object]
) -> None:
    """Check a scoped parameter before the provider sees it.

    The getter declares the parameter and the framework gates it, so a connector
    cannot forget a check it does not perform. Needs the built provider: the
    dialect, catalog scope and allowlist belong to the connector's client.
    """
    if spec.scoped_sql_param is not None:
        # Lazy: sqlglot is paid for only by a probe that runs a query.
        from datahub.ingestion.agent.sql_gate import CatalogScope, check_query_scope

        dialect = _provider_attribute(provider, "sql_dialect")
        if not isinstance(dialect, str) or not dialect:
            # The provider's defect, not the caller's: no query could pass.
            raise ProbeInternalError(
                f"probe method '{spec.command}' takes SQL but its provider "
                f"declares no sql_dialect, so the query cannot be checked; "
                f"this is a defect in the probe provider"
            )
        # The connector's declared catalog; absent one, information_schema only.
        scope = _provider_attribute(provider, "catalog_scope")
        if scope is not None and not isinstance(scope, CatalogScope):
            raise ProbeInternalError(
                f"probe method '{spec.command}' takes SQL but its provider's "
                f"catalog_scope is a {type(scope).__name__}, not a CatalogScope, "
                f"so the query cannot be checked; this is a defect in the probe "
                f"provider"
            )
        check_query_scope(
            str(call_kwargs[spec.scoped_sql_param]),
            platform=dialect,
            scope=scope,
        )

    if spec.scoped_path_param is not None:
        allowlist = _provider_attribute(provider, "api_allowlist")
        if allowlist is None:
            # Unlike an unlisted path (the caller's), no path could ever pass.
            raise ProbeInternalError(
                f"probe method '{spec.command}' takes an API path but its "
                f"provider declares no api_allowlist, so no path can be "
                f"permitted; this is a defect in the probe provider"
            )
        # A str is iterable too, and would read as one entry per character.
        if isinstance(allowlist, (str, bytes)) or not isinstance(allowlist, Iterable):
            raise ProbeInternalError(
                f"probe method '{spec.command}' takes an API path but its "
                f"provider's api_allowlist is a {type(allowlist).__name__}, not "
                f"a collection of endpoints; this is a defect in the probe "
                f"provider"
            )
        endpoints = tuple(allowlist)
        if not all(isinstance(entry, str) for entry in endpoints):
            raise ProbeInternalError(
                f"probe method '{spec.command}' takes an API path but its "
                f"provider's api_allowlist holds a non-string entry; this is a "
                f"defect in the probe provider"
            )
        # GET only. The base URL lets the gate match the path the client will
        # send, not the caller's string (see api_gate._effective_path).
        base_url = _provider_attribute(provider, "api_base_url")
        if base_url is not None and not isinstance(base_url, str):
            raise ProbeInternalError(
                f"probe method '{spec.command}' takes an API path but its "
                f"provider's api_base_url is a {type(base_url).__name__}, not a "
                f"string; this is a defect in the probe provider"
            )
        check_api_request(
            READ_METHOD,
            str(call_kwargs[spec.scoped_path_param]),
            endpoints,
            base_url=base_url,
        )


def _report_entries(report: object, kind: str) -> Set[str]:
    """One list ("warnings" or "failures") off the SourceReport a provider exposes
    as `probe_report`, each StructuredLogEntry rendered "title: message"."""
    if report is None:
        return set()
    entries: Set[str] = set()
    for entry in getattr(report, kind, None) or []:
        if isinstance(entry, str):
            entries.add(entry)
            continue
        title = getattr(entry, "title", None)
        message = getattr(entry, "message", None)
        if title and message:
            entries.add(f"{title}: {message}")
        elif title or message:
            entries.add(str(title or message))
        else:
            entries.add(str(entry))
    return entries


def _raise_call_failure(
    exc: BaseException,
    provider: object,
    provider_cls: type,
    command: str,
    own_values: AbstractSet[str],
    callers_sql: bool = False,
) -> NoReturn:
    """Re-raise a provider call's failure as the CLI reports it (see
    agent.error_policy): untrusted named by label only, trusted kept.
    `callers_sql` when the command ran SQL the caller wrote, whose SQLSTATE
    class 42 is the caller's mistake."""
    recorded = _read_back(provider, "failures")
    if recorded:
        # The recorded failure explains the miss, whatever was raised after it.
        detail = (
            scrub_text(label_foreign_text(exc, provider_cls, own_values), set())
            if is_trusted(exc)
            else foreign_label(exc, provider_cls) + verbose_detail(exc)
        )
        raise ProbeReadFailed(
            f"{detail}; the connector recorded: " + "; ".join(sorted(recorded))
        ) from None
    if not is_trusted(exc):
        if callers_sql and is_callers_sql_error(exc, provider_cls):
            # The caller's own query was wrong, not the source (exit 2).
            raise ProbeArgumentError(
                f"'{command}' failed {name_foreign(exc, provider_cls)}: the "
                f"query names something this connection cannot read or is "
                f"not valid SQL here; correct the query"
            ) from None
        raise classify_foreign(exc, f"'{command}'", provider_cls) from None
    _reraise_trusted(exc, provider_cls, own_values)


def _reraise_trusted(
    exc: BaseException,
    provider_cls: type,
    own_values: AbstractSet[str] = frozenset(),
) -> NoReturn:
    """Raise a trusted exception, minus any untrusted text it quotes (see
    agent.error_policy.police_trusted). An attribute read has no argument
    values of its own, so it passes none."""
    replacement = police_trusted(exc, provider_cls, own_values)
    if replacement is not None:
        raise replacement from None
    raise exc


def _provider_attribute(owner: object, name: str, default: object = None) -> object:
    """`owner.name` for a provider or its class: how every PROVIDER_ATTRIBUTES
    entry is read. Absent reads as `default`. A trusted error keeps its type;
    anything else raised is the provider's defect (exit 1), named by class and
    attribute, never by its text.
    """
    owner_cls = owner if isinstance(owner, type) else type(owner)
    try:
        return getattr(owner, name, default)
    except PASS_THROUGH:
        raise
    except BaseException as exc:
        if is_trusted(exc):
            _reraise_trusted(exc, owner_cls)
        raise ProbeInternalError(
            f"the probe provider is defective: reading "
            f"{owner_cls.__name__}.{name} failed {name_foreign(exc, owner_cls)}"
        ) from None


def _read_back(provider: object, kind: str) -> Set[str]:
    """The provider's `kind` entries ("warnings" or "failures"), its own and its
    `probe_report`'s."""
    own = _provider_attribute(provider, kind)
    return set(cast(Iterable[str], own or [])) | _report_entries(
        _provider_attribute(provider, "probe_report"), kind
    )


@dataclass(frozen=True)
class _ProviderCall:
    builder: Callable[[Any], Any]
    config: Any
    spec: ProbeMethodSpec
    call_kwargs: Dict[str, object]
    provider_cls: type
    source_type: str

    @property
    def own_values(self) -> FrozenSet[str]:
        """The values the caller passed this call, as strings: a lookup
        error naming one quotes the caller, not foreign text."""
        return frozenset(str(v) for v in self.call_kwargs.values() if v is not None)


# The verb of the open path, the one place a network cause is named: a
# context there is the failure being handled, while elsewhere it can be an
# unrelated earlier retry.
_OPENING = "opening"


def _source_failure(exc: BaseException, call: _ProviderCall, verb: str) -> NoReturn:
    """Re-raise a failure while opening or closing the provider.

    A trusted type keeps its message. Anything else is a connection error (exit
    3) named by label: the caller's input was checked before the provider was
    built, so an untrusted failure here is the source's. On opening, a stdlib
    network exception in its chain adds a reason and a fixed hint
    (error_policy.network_reason).
    """
    if is_trusted(exc):
        _reraise_trusted(exc, call.provider_cls, call.own_values)
    module = missing_module(exc)
    if module is not None:
        # The environment's, not the source's: retrying cannot help (exit 1).
        raise ProbeInternalError(
            f"{verb} source '{call.source_type}' failed "
            f"{name_foreign(exc, call.provider_cls)}: the Python module "
            f"'{module}' is not installed; install the plugin for this source "
            f"(pip install 'acryl-datahub[{call.source_type}]') or the driver "
            f"its connection URL names"
        ) from None
    reason = network_reason(exc) if verb == _OPENING else None
    if reason is None:
        raise ProbeConnectionError(
            f"{verb} source '{call.source_type}' failed "
            f"{name_foreign(exc, call.provider_cls)}"
        ) from None
    raise ProbeConnectionError(
        f"{verb} source '{call.source_type}' failed "
        f"({foreign_label(exc, call.provider_cls)}): {reason} - "
        f"{NETWORK_REASON_HINTS[reason]}{verbose_detail(exc)}"
    ) from None


@dataclass(frozen=True)
class _CallOutcome:
    result: object
    warnings: Set[str]
    failures: Set[str]


def _open_call_close(call: _ProviderCall) -> _CallOutcome:
    """Open the provider, run the command, close it, policing every failure.

    The close (`__exit__`) runs reused code too, so it is held to the open
    path's rule, and its failure never replaces the command's own.
    """
    body_error: Optional[BaseException] = None
    try:
        with ExitStack() as stack:
            try:
                return _open_and_call(stack, call)
            except BaseException as exc:
                body_error = exc
                raise
    except BaseException as exc:
        if isinstance(exc, PASS_THROUGH):
            raise
        if body_error is None:
            _source_failure(exc, call, "closing")
        if not is_trusted(body_error) and not isinstance(body_error, PASS_THROUGH):
            # Raised outside the handlers that police their own (open, call,
            # attribute reads): a gate, or the read-back iterating a provider value.
            raise classify_foreign(
                body_error, f"'{call.spec.command}'", call.provider_cls
            ) from None
        if exc is body_error:
            raise
        # Keep the command's failure; drop the close failure from the chain.
        raise body_error from body_error.__cause__
    # The provider's __exit__ returned true and swallowed the command's failure.
    raise ProbeInternalError(
        f"the probe provider for source '{call.source_type}' suppressed the "
        f"command's failure in its __exit__; this is a defect in the provider"
    )


def _open_and_call(stack: ExitStack, call: _ProviderCall) -> _CallOutcome:
    source_type = call.source_type
    command = call.spec.command
    try:
        provider = stack.enter_context(call.builder(call.config))
    except PASS_THROUGH:
        raise
    except BaseException as exc:
        _source_failure(exc, call, _OPENING)
    _enforce_gates(call.spec, provider, call.call_kwargs)
    method = _bound_method(provider, command)
    try:
        result = method(**call.call_kwargs)
    except NotImplementedError:
        # The source lacks the concept (a dialect without a reflection method).
        # The connection was fine, so exit 2: the command was the wrong one.
        raise ProbeArgumentError(
            f"source '{source_type}' does not support the '{command}' command. "
            f"The source was reached fine -- this is a limit of the source, so "
            f"choose another command rather than retrying"
        ) from None
    except PASS_THROUGH:
        raise
    except BaseException as exc:
        # A getter that recorded a failed fetch and then raised "no such name"
        # is reporting the fetch, not a bad argument.
        _raise_call_failure(
            exc,
            provider,
            call.provider_cls,
            command,
            call.own_values,
            callers_sql=call.spec.scoped_sql_param is not None,
        )
    return _CallOutcome(
        result=result,
        warnings=_read_back(provider, "warnings"),
        failures=_read_back(provider, "failures"),
    )


@dataclass(frozen=True)
class _PreparedCall:
    """A command checked and its provider ready to build: everything that can
    refuse the call without reaching the source has run."""

    call: _ProviderCall
    config_cls: Type["ConfigModel"]
    # The caller's arguments typed, before _bounded_kwargs asks one past the
    # limit: truncation is judged against these.
    coerced_kwargs: Dict[str, object]


def _spec_for(provider_cls: type, source_type: str, command: str) -> ProbeMethodSpec:
    specs = dict(_iter_specs(provider_cls))
    if command not in specs:
        raise ProbeArgumentError(
            f"unknown probe method '{command}' for source '{source_type}'; "
            f"available: {', '.join(sorted(specs)) or '(none)'}"
        )
    return specs[command]


def _prepare_call(
    source_type: str,
    config_dict: Dict[str, object],
    command: str,
    kwargs: Dict[str, object],
) -> _PreparedCall:
    provider_cls = _provider_class(source_type)
    if provider_cls is None:
        raise ProbeArgumentError(f"source '{source_type}' has no probe methods")
    spec = _spec_for(provider_cls, source_type, command)
    coerced_kwargs = _coerce_kwargs(spec, kwargs)
    call_kwargs = _bounded_kwargs(spec, coerced_kwargs)
    # Before the config is built: the operator's switch must not depend on the
    # source being reachable.
    _refuse_withheld_passthrough(spec, source_type)
    config_cls = require_config_class(source_type)
    config = validate_source_config(config_cls, source_type, config_dict)
    builder = getattr(provider_cls, "for_config", None)
    if not callable(builder):
        raise ProbeArgumentError(
            f"probe provider '{provider_cls.__name__}' for source "
            f"'{source_type}' has no for_config(config) classmethod, so it "
            f"cannot be built from the recipe"
        )
    return _PreparedCall(
        call=_ProviderCall(
            builder=builder,
            config=config,
            spec=spec,
            call_kwargs=call_kwargs,
            provider_cls=provider_cls,
            source_type=source_type,
        ),
        config_cls=config_cls,
        coerced_kwargs=coerced_kwargs,
    )


@dataclass(frozen=True)
class _CappedResult:
    result: object
    truncated: bool
    # The arguments echoed back: the limit that applies, not the +1 asked of
    # the getter.
    params: Dict[str, object]


def _cap_result(
    spec: ProbeMethodSpec,
    result: object,
    coerced_kwargs: Dict[str, object],
    call_kwargs: Dict[str, object],
) -> _CappedResult:
    """The result cut to the limit that applies, and whether that cut it."""
    params = dict(call_kwargs)
    if spec.shapes_own_result:
        # Mirror the envelope's flag, so every command answers in one field.
        truncated = isinstance(result, dict) and bool(result.get("truncated"))
        return _CappedResult(result=result, truncated=truncated, params=params)
    limit = _effective_row_limit(spec, coerced_kwargs)
    if limit is not None and spec.row_limit_param is not None:
        params[spec.row_limit_param] = limit
        # The getter was asked for one past the limit.
        if isinstance(result, list) and len(result) > limit:
            return _CappedResult(result=result[:limit], truncated=True, params=params)
        return _CappedResult(result=result, truncated=False, params=params)
    return _capped_to_max_items(result, params)


def _capped_to_max_items(result: object, params: Dict[str, object]) -> _CappedResult:
    """A command with no row_limit_param is still capped, and says so. This
    bounds what reaches the caller, not what was fetched (only a declared limit
    reaches the fetcher). A mapping keeps its first entries."""
    if isinstance(result, dict) and len(result) > MAX_PROBE_ITEMS:
        capped = dict(list(result.items())[:MAX_PROBE_ITEMS])
        return _CappedResult(result=capped, truncated=True, params=params)
    if isinstance(result, list) and len(result) > MAX_PROBE_ITEMS:
        return _CappedResult(
            result=result[:MAX_PROBE_ITEMS], truncated=True, params=params
        )
    return _CappedResult(result=result, truncated=False, params=params)


def run_probe_method(
    source_type: str,
    config_dict: Dict[str, object],
    command: str,
    kwargs: Dict[str, object],
    *,
    guard_logs: bool = True,
) -> ProbeMethodResult:
    """Run one probe method against a source.

    The log guard (log_guard.quiet_reused_logs) keeps connector code from
    logging credentials or source text while the probe runs. It acts on every
    non-framework record in the process, other threads' included, and drops
    their tracebacks. It is on by default, so a caller gets the CLI's
    protection without doing anything. An embedder that masks its own logs
    and needs its other threads' tracebacks passes guard_logs=False, and then
    owns keeping connector log lines out of its output.
    """
    # SECURITY: every command that touches the source runs through here, so this
    # is where the whole-probe switch is enforced. It precedes the command lookup
    # so the refusal does not depend on naming a real command.
    if get_probe_disabled():
        raise ProbeArgumentError(
            "the probe is switched off here (DATAHUB_PROBE_DISABLED), so no "
            "command that connects to the source will run. The commands that "
            "need no connection still work: `recipe describe`, "
            "`recipe scaffold`, `recipe validate`, `probe methods` and "
            "`probe filter`"
        )
    prepared = _prepare_call(source_type, config_dict, command, kwargs)
    call = prepared.call
    # Read either way, so a misdeclared one is a defect with or without a guard.
    silenced = _silenced_loggers(call.provider_cls)
    with ExitStack() as stack:
        if guard_logs:
            # Outermost, so the guard also covers __exit__. No secrets here: the
            # CLI's own guard holds the recipe's, and credential shapes are
            # scrubbed regardless.
            stack.enter_context(quiet_reused_logs(set(), silenced=silenced))
        outcome = _open_call_close(call)
    capped = _cap_result(
        spec=call.spec,
        result=outcome.result,
        coerced_kwargs=prepared.coerced_kwargs,
        call_kwargs=call.call_kwargs,
    )
    return ProbeMethodResult(
        source_type=source_type,
        command=command,
        params=capped.params,
        kind=declared_kind_overrides(prepared.config_cls).get(command, call.spec.kind),
        parent_path=[
            str(call.call_kwargs[p])
            for p in call.spec.parent_params
            if p in call.call_kwargs
        ],
        result=capped.result,
        truncated=capped.truncated,
        warnings=sorted(outcome.warnings),
        failures=sorted(outcome.failures),
    )
