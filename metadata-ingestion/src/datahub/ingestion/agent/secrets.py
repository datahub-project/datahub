import copy
import os
import re
from typing import Dict, List, Optional, Protocol

_REF = re.compile(r"\$\{([^}]+)\}")


class SecretResolver(Protocol):
    def resolve(self, ref: str) -> Optional[str]: ...


class EnvVarResolver:
    def resolve(self, ref: str) -> Optional[str]:
        return os.environ.get(ref)


class DatahubEnvResolver:
    def resolve(self, ref: str) -> Optional[str]:
        # Lazy import: only touch the CLI config file when this resolver is actually used.
        from datahub.cli.config_utils import DATAHUB_CONFIG_PATH

        if not os.path.exists(DATAHUB_CONFIG_PATH):
            return None
        import yaml

        with open(DATAHUB_CONFIG_PATH) as stream:
            data = yaml.safe_load(stream) or {}
        value = data.get(ref)
        return str(value) if value is not None else None


def default_resolvers() -> List[SecretResolver]:
    return [EnvVarResolver(), DatahubEnvResolver()]


def _resolve_str(value: str, resolvers: List[SecretResolver]) -> str:
    def replace(match: "re.Match[str]") -> str:
        ref = match.group(1)
        for resolver in resolvers:
            resolved = resolver.resolve(ref)
            if resolved is not None:
                return resolved
        raise ValueError(f"Could not resolve secret reference ${{{ref}}}")

    return _REF.sub(replace, value)


def resolve_config(
    config_dict: Dict[str, object], resolvers: List[SecretResolver]
) -> Dict[str, object]:
    def walk(node: object) -> object:
        if isinstance(node, str):
            return _resolve_str(node, resolvers)
        if isinstance(node, dict):
            return {k: walk(v) for k, v in node.items()}
        if isinstance(node, list):
            return [walk(v) for v in node]
        return node

    result = walk(copy.deepcopy(config_dict))
    assert isinstance(result, dict)
    return result
