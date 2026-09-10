import re
from typing import Dict, List, Set

from datahub.ingestion.agent.introspect import describe_source
from datahub.ingestion.agent.models import FieldKind
from datahub.ingestion.source.source_registry import source_registry

_REF = re.compile(r"\$\{[^}]+\}")


def _secret_field_names(source_type: str) -> Set[str]:
    spec = describe_source(source_type)
    return {f.name for f in spec.fields if f.kind == FieldKind.SECRET}


def scaffold(source_type: str) -> Dict[str, object]:
    """A minimal recipe for this source: its required fields, and its secrets
    as ${REF} placeholders.

    Pattern fields are deliberately absent. Emitting
    `{"allow": [".*"], "deny": []}` for each looked helpful and was the
    opposite: it *overwrites* the connector's own deny defaults, so a
    scaffolded recipe ingested more than the same recipe without the line.
    Snowflake stops denying ^SNOWFLAKE$ and ^SNOWFLAKE_SAMPLE_DATA$, Kafka
    stops denying ^_.* so __consumer_offsets becomes a dataset, Mode picks up
    Personal spaces, Teradata picks up all 43 of its system databases. This is
    the first command an agent runs, and it was handing back a recipe strictly
    worse than the connector's defaults with nothing saying so.

    Emitting the real default instead would freeze it: a deny list copied into
    a recipe today does not gain the entry DataHub adds tomorrow. Omitting the
    field is what keeps the connector's own default live, and `describe` is
    where an agent learns which pattern fields exist and what they default to.
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


def validate_recipe(recipe: Dict[str, object]) -> Dict[str, object]:
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
    # `or {}` would swallow every falsey value, so a "config: []" reached the
    # config class as {} and failed on whatever field happened to be required
    # -- an error naming host_port when the real problem is the config's shape.
    # A bare "config:" is YAML null and does mean "no config", so it alone
    # still defaults.
    config = source.get("config", {})
    if config is None:
        config = {}
    if not isinstance(config, dict):
        return {
            "valid": False,
            "errors": ["recipe.source.config must be a mapping"],
            "warnings": [],
        }

    # Resolve source class once, up front, and degrade gracefully on failure.
    try:
        source_cls = source_registry.get(source_type)
        # get_config_class is injected by the @config_class decorator at runtime, so it is
        # not declared on the Source base class and mypy cannot see it statically.
        get_config_class = getattr(source_cls, "get_config_class", None)
        if get_config_class is None:
            raise TypeError(f"Source {source_type!r} does not define a config class")
        config_cls = get_config_class()
    except Exception as exc:
        # Catch KeyError, ConfigurationError, TypeError, and any other exception from
        # source resolution. Degrade to invalid recipe.
        return {
            "valid": False,
            "errors": [f"unknown or unloadable source type '{source_type}': {exc}"],
            "warnings": [],
        }

    # Plaintext-secret detection reuses the same field classification the
    # introspection API exposes elsewhere (FieldKind.SECRET), so a field is
    # flagged here exactly when describe_source would report it as secret.
    for name in _secret_field_names(source_type):
        value = config.get(name)
        if isinstance(value, str) and value and not _REF.search(value):
            warnings.append(
                f"'{name}' contains a plaintext secret; the agent sees this value when "
                f"editing the file. Recommend '{name}: ${{{name.upper()}}}' and "
                f"export {name.upper()}=..."
            )

    try:
        config_cls.model_validate(config)
    except (ValueError, TypeError, AssertionError) as exc:
        errors.append(str(exc))

    return {"valid": not errors, "errors": errors, "warnings": warnings}
