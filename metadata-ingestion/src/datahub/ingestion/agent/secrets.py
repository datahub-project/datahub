import copy
import os
import re
from dataclasses import dataclass, field
from typing import (
    Dict,
    Iterator,
    List,
    MutableMapping,
    Optional,
    Protocol,
    Set,
)

from expandvars import ExpandvarsException, UnboundVariable

from datahub.configuration.config_loader import EnvResolver


@dataclass
class ResolvedConfig:
    config: Dict[str, object]
    secret_values: Set[str] = field(default_factory=set)


class SecretResolver(Protocol):
    def resolve(self, ref: str) -> Optional[str]: ...


class EnvVarResolver:
    def resolve(self, ref: str) -> Optional[str]:
        return os.environ.get(ref)


class MappingResolver:
    """Resolve `${ref}` from an explicit mapping rather than the environment.

    For a caller holding resolved secrets that must stay out of `os.environ`
    (readable from /proc/<pid>/environ and inherited by children): the
    executor's stdin envelope. Placed ahead of EnvVarResolver, so it wins.
    """

    def __init__(self, values: Dict[str, str]) -> None:
        self._values = dict(values)

    def resolve(self, ref: str) -> Optional[str]:
        return self._values.get(ref)


# ~/.datahubenv nests under `gms:`; these map the env-var names a recipe spells
# onto where the file keeps them.
_DATAHUB_ENV_ALIASES = {
    "DATAHUB_GMS_URL": "gms.server",
    "DATAHUB_GMS_TOKEN": "gms.token",
}


class DatahubEnvResolver:
    def resolve(self, ref: str) -> Optional[str]:
        # Lazy: the CLI config is read only when this resolver is used.
        from datahub.cli.config_utils import DATAHUB_CONFIG_PATH

        if not os.path.exists(DATAHUB_CONFIG_PATH):
            return None
        import yaml

        try:
            with open(DATAHUB_CONFIG_PATH) as stream:
                data = yaml.safe_load(stream) or {}
        except (OSError, yaml.YAMLError):
            # Declining, not failing: the next resolver tries, and an
            # unresolvable ref fails by name.
            return None
        # Not get_url_and_token(): it raises on a missing config and warns on
        # an expired token, and this command writes JSON.
        value = _lookup_path(data, _DATAHUB_ENV_ALIASES.get(ref, ref))
        return str(value) if value is not None else None


def _lookup_path(data: object, path: str) -> Optional[object]:
    """Resolve a dotted path into the file, where the aliases above point."""
    node: object = data
    for part in path.split("."):
        if not isinstance(node, dict):
            return None
        node = node.get(part)
    return None if isinstance(node, (dict, list)) else node


def default_resolvers() -> List[SecretResolver]:
    return [EnvVarResolver(), DatahubEnvResolver()]


# The setting expandvars reads from the same environ after a miss.
_EXPANDVARS_SETTINGS = frozenset({"EXPANDVARS_RECOVER_NULL"})


class _ResolverEnviron(MutableMapping[str, str]):
    """The resolvers, in order, as the environment EnvResolver expands
    against, recording each value one supplies.

    Only the resolvers' answers are recorded: an inline `${X:-default}` is
    recipe text, not a secret. `${X:=default}` assigns, which ingestion does
    to os.environ; here it lands in an overlay for the rest of this recipe.
    Iteration lists only that overlay, since resolvers can be asked for a
    name but not enumerated; expansion never iterates.
    """

    def __init__(self, resolvers: List[SecretResolver]) -> None:
        self._resolvers = resolvers
        self._assigned: Dict[str, str] = {}
        self.supplied: Set[str] = set()
        # The last recipe name nothing resolved: the one an UnboundVariable is
        # about, recorded here rather than parsed out of expandvars' message.
        self.last_missing: Optional[str] = None

    def __getitem__(self, name: str) -> str:
        if name in self._assigned:
            return self._assigned[name]
        for resolver in self._resolvers:
            value = resolver.resolve(name)
            if value is not None:
                # Every resolved value is masked, a non-secret one (a host) too.
                if value:
                    self.supplied.add(value)
                return value
        # expandvars then reads a setting of its own, which is not the
        # reference that failed; a recipe's own EXPANDVARS_* reference is.
        if name not in _EXPANDVARS_SETTINGS:
            self.last_missing = name
        raise KeyError(name)

    def __setitem__(self, name: str, value: str) -> None:
        self._assigned[name] = value

    def __delitem__(self, name: str) -> None:
        del self._assigned[name]

    def __iter__(self) -> Iterator[str]:
        return iter(self._assigned)

    def __len__(self) -> int:
        return len(self._assigned)


_VARIABLE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")

# A reference whose expansion is part of a value rather than all of it:
# `${#X}` (its length), and `${X:` followed by an offset (`${X:0:8}`,
# `${X:8}`, `${X: -3}`) rather than `-`, `=`, `?` or `+`. Only whole values a
# resolver supplies are registered for masking, so a slice would print in
# clear; two slices rebuild the secret.
_PARTIAL_EXPANSION = re.compile(
    r"\$\{(?:#(?P<length>[A-Za-z_][A-Za-z0-9_]*)"
    r"|(?P<slice>[A-Za-z_][A-Za-z0-9_]*):(?![-=?+]))"
)


def _strings(node: object) -> Iterator[str]:
    if isinstance(node, str):
        yield node
    elif isinstance(node, dict):
        for value in node.values():
            yield from _strings(value)
    elif isinstance(node, list):
        for item in node:
            yield from _strings(item)


def _refuse_partial_expansions(config_dict: Dict[str, object]) -> None:
    """Refuse a reference that expands to part of a value. `datahub ingest`
    accepts these, but the probe's output goes to a caller, and only whole
    values can be masked. Named by variable, never by value."""
    for text in _strings(config_dict):
        match = _PARTIAL_EXPANSION.search(text)
        if match:
            name = match.group("length") or match.group("slice")
            raise ValueError(
                f"the recipe takes part of ${{{name}}} (a substring or its "
                f"length); the probe masks only whole values, so it refuses "
                f"this. Reference the whole value as ${{{name}}}"
            )


def resolve_config_collecting(
    config_dict: Dict[str, object], resolvers: List[SecretResolver]
) -> ResolvedConfig:
    """The config with its variable references resolved exactly as
    `datahub ingest` resolves them (EnvResolver: `${X}` anywhere, `$X` when it
    starts the value, `${X:-default}`), each looked up through `resolvers` in
    order, and the values they supplied. One difference: a reference that
    expands to part of a value (`${X:0:8}`, `${#X}`) is refused, since only
    whole values are masked.

    A reference nothing resolves and no default covers fails ingestion too;
    here it is a ValueError naming it, as is any other reference expandvars
    cannot parse, so it reads as the caller's input (exit 2)."""
    _refuse_partial_expansions(config_dict)
    environ = _ResolverEnviron(resolvers)
    resolver = EnvResolver(environ=environ, register_secrets=False)
    try:
        resolved = resolver.resolve(copy.deepcopy(config_dict))
    except UnboundVariable:
        # The name the lookup missed, never a value.
        name = environ.last_missing
        if name is not None and _VARIABLE_NAME.fullmatch(name):
            raise ValueError(
                f"Could not resolve secret reference ${{{name}}}"
            ) from None
        raise ValueError("Could not resolve a secret reference") from None
    except ExpandvarsException as exc:
        # Not its text: it quotes the recipe's value around the reference.
        raise ValueError(
            f"Could not resolve a variable reference in the recipe "
            f"({type(exc).__name__})"
        ) from None
    return ResolvedConfig(config=resolved, secret_values=environ.supplied)


def _names_ingest_reads(value: str) -> Optional[Set[str]]:
    """The variable names ingestion's EnvResolver looks up for this string;
    None when expandvars cannot parse it."""
    try:
        return EnvResolver.list_referenced_variables({"value": value})
    except ExpandvarsException:
        return None


def is_variable_reference(value: str) -> bool:
    """Whether ingestion reads this recipe string as a variable reference
    (resolved from the environment) rather than as literal text."""
    names = _names_ingest_reads(value)
    # Malformed (`${}`, or an unclosed `${X` leading the value): resolving it
    # fails, and that failure is reported, so it is not plaintext too.
    return names is None or bool(names)


def names_a_dotted_path(value: str) -> bool:
    """Whether a `${a.b}` or a leading `$a.b` in this string names a dotted
    path, which ingestion reads as `${a}` followed by a modifier or by the
    literal `.b`: the name it looks up stops where the written one goes on
    with a dot."""
    names = _names_ingest_reads(value) or set()
    return any(f"${{{name}." in value or f"${name}." in value for name in names)


def resolve_config(
    config_dict: Dict[str, object], resolvers: List[SecretResolver]
) -> Dict[str, object]:
    return resolve_config_collecting(config_dict, resolvers).config
