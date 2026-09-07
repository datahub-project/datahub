import copy
import os
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Protocol, Set

_REF = re.compile(r"\$\{([^}]+)\}")


@dataclass
class ResolvedConfig:
    config: Dict[str, object]
    secret_values: Set[str] = field(default_factory=set)


class SecretResolver(Protocol):
    def resolve(self, ref: str) -> Optional[str]: ...


class EnvVarResolver:
    def resolve(self, ref: str) -> Optional[str]:
        return os.environ.get(ref)


# ~/.datahubenv nests everything under `gms:` (DatahubConfig.gms), so a flat
# top-level lookup resolves nothing at all. These map the two names a recipe
# actually spells -- the same ones the CLI documents as env vars -- onto where
# the file keeps them.
_DATAHUB_ENV_ALIASES = {
    "DATAHUB_GMS_URL": "gms.server",
    "DATAHUB_GMS_TOKEN": "gms.token",
}


class DatahubEnvResolver:
    def resolve(self, ref: str) -> Optional[str]:
        # Lazy import: only touch the CLI config file when this resolver is actually used.
        from datahub.cli.config_utils import DATAHUB_CONFIG_PATH

        if not os.path.exists(DATAHUB_CONFIG_PATH):
            return None
        import yaml

        with open(DATAHUB_CONFIG_PATH) as stream:
            data = yaml.safe_load(stream) or {}
        # Read the file directly rather than through get_url_and_token(): that
        # path raises on a missing config and warns on an expired token, and
        # this command emits JSON on stdout.
        value = _lookup_path(data, _DATAHUB_ENV_ALIASES.get(ref, ref))
        return str(value) if value is not None else None


def _lookup_path(data: object, path: str) -> Optional[object]:
    """Resolve a dotted path, so a recipe may name `${gms.server}` directly."""
    node: object = data
    for part in path.split("."):
        if not isinstance(node, dict):
            return None
        node = node.get(part)
    return None if isinstance(node, (dict, list)) else node


def default_resolvers() -> List[SecretResolver]:
    return [EnvVarResolver(), DatahubEnvResolver()]


def _resolve_str(
    value: str, resolvers: List[SecretResolver], collected: Set[str]
) -> str:
    def replace(match: "re.Match[str]") -> str:
        ref = match.group(1)
        for resolver in resolvers:
            resolved = resolver.resolve(ref)
            if resolved is not None:
                # Record every ${ref}-sourced value so the redactor can mask it
                # regardless of how deeply it is nested. Over-collecting a
                # non-secret ref (e.g. a host) is acceptable defense-in-depth.
                if resolved:
                    collected.add(resolved)
                return resolved
        raise ValueError(f"Could not resolve secret reference ${{{ref}}}")

    return _REF.sub(replace, value)


def resolve_config_collecting(
    config_dict: Dict[str, object], resolvers: List[SecretResolver]
) -> ResolvedConfig:
    collected: Set[str] = set()

    def walk(node: object) -> object:
        if isinstance(node, str):
            return _resolve_str(node, resolvers, collected)
        if isinstance(node, dict):
            return {k: walk(v) for k, v in node.items()}
        if isinstance(node, list):
            return [walk(v) for v in node]
        return node

    result = walk(copy.deepcopy(config_dict))
    assert isinstance(result, dict)
    return ResolvedConfig(config=result, secret_values=collected)


def resolve_config(
    config_dict: Dict[str, object], resolvers: List[SecretResolver]
) -> Dict[str, object]:
    return resolve_config_collecting(config_dict, resolvers).config
