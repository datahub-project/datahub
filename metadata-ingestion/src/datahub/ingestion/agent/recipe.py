import re
from typing import Dict, List, Optional, Set

from datahub.ingestion.agent.config_validation import validate_source_config
from datahub.ingestion.agent.introspect import (
    describe_source,
    iter_secret_field_values,
)
from datahub.ingestion.agent.models import FieldKind
from datahub.ingestion.agent.redact import (
    _SENSITIVE_KEY_HINTS,
    collect_nested_credential_values,
)
from datahub.ingestion.agent.secrets import (
    SecretResolver,
    default_resolvers,
    resolve_config_collecting,
)
from datahub.ingestion.source.source_registry import source_registry

_REF = re.compile(r"\$\{[^}]+\}")


def _env_name(path: str) -> str:
    """An environment variable name for a config path (`git_info.deploy_key`
    -> GIT_INFO_DEPLOY_KEY)."""
    return re.sub(r"[^0-9A-Za-z]+", "_", path).strip("_").upper()


def scaffold(source_type: str) -> Dict[str, object]:
    """A minimal recipe for this source: its required fields, and its secrets
    as ${REF} placeholders.

    Pattern fields are absent: an allow-all would overwrite the connector's
    own deny defaults (system schemas, internal topics), and a copied default
    would stop tracking DataHub's. `describe` lists them and their defaults.
    """
    spec = describe_source(source_type)
    config: Dict[str, object] = {}
    for f in spec.fields:
        if f.kind == FieldKind.SECRET:
            config[f.name] = "${" + f.name.upper() + "}"
        elif f.kind == FieldKind.PATTERN:
            continue
        elif f.required:
            config[f.name] = ""
    return {"source": {"type": source_type, "config": config}}


def validate_recipe(
    recipe: Dict[str, object], resolvers: Optional[List[SecretResolver]] = None
) -> Dict[str, object]:
    """Whether this recipe would load, and what is wrong with it if not.

    `resolvers` defaults to the environment chain; a caller holding secrets
    elsewhere (the CLI's stdin envelope) passes its own.
    """
    errors: List[str] = []
    warnings: List[str] = []
    source = recipe.get("source")
    if not isinstance(source, dict) or "type" not in source:
        return {
            "valid": False,
            "errors": ["recipe.source.type is required"],
            "warnings": [],
        }
    source_type = str(source["type"])
    # Only YAML null ("config:") means no config; any other non-mapping is
    # refused for its shape rather than reaching the config class as {}.
    config = source.get("config", {})
    if config is None:
        config = {}
    if not isinstance(config, dict):
        return {
            "valid": False,
            "errors": ["recipe.source.config must be a mapping"],
            "warnings": [],
        }

    try:
        source_cls = source_registry.get(source_type)
        # Injected by @config_class at runtime, out of mypy's view.
        get_config_class = getattr(source_cls, "get_config_class", None)
        if get_config_class is None:
            raise TypeError(f"Source {source_type!r} does not define a config class")
        config_cls = get_config_class()
    except Exception as exc:
        # Any failure to resolve the source makes the recipe invalid.
        return {
            "valid": False,
            "errors": [f"unknown or unloadable source type '{source_type}': {exc}"],
            "warnings": [],
        }

    # Every SecretStr field the recipe writes inline, at any depth, by path: the
    # walk the redactor masks with. dict() drops a path a union read twice.
    already_named: Set[str] = set()
    for path, value in dict(iter_secret_field_values(config_cls, config)).items():
        if _REF.search(value):
            continue
        already_named.add(value)
        env = _env_name(path)
        warnings.append(
            f"'{path}' contains a plaintext secret; the agent sees this value when "
            f"editing the file. Recommend '{path}: ${{{env}}}' and "
            f"export {env}=..."
        )

    # Secrets nested in free-form dicts (`consumer_config['sasl.password']`),
    # counted without naming the value or its path, which would put it in the
    # transcript. The detecting collector judges a dotted key on its last
    # segment, so `sasl.mechanism` is not flagged.
    nested = collect_nested_credential_values(config, _SENSITIVE_KEY_HINTS)
    # Minus values the sweep above already reported.
    plaintext_nested = sorted(
        v for v in nested if not _REF.search(v) and v not in already_named
    )
    if plaintext_nested:
        warnings.append(
            f"{len(plaintext_nested)} plaintext secret value(s) sit under "
            f"sensitive-looking keys nested in this config (the same keys the "
            f"redactor masks on the way out). Replace each with a ${{REF}} "
            f"placeholder and export it where the probe runs"
        )

    # Validated resolved, as `datahub ingest` does: a raw `${VAR}` is a string
    # even in a bool field. An unresolvable reference is an error by name.
    try:
        resolved = resolve_config_collecting(
            config, default_resolvers() if resolvers is None else resolvers
        ).config
    except ValueError as exc:
        errors.append(str(exc))
        resolved = None
    if resolved is not None:
        try:
            validate_source_config(config_cls, source_type, resolved)
        except (ValueError, TypeError, AssertionError) as exc:
            errors.append(str(exc))

    return {"valid": not errors, "errors": errors, "warnings": warnings}
