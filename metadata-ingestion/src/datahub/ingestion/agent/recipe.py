import re
from typing import Dict, List, Set

from datahub.ingestion.agent.introspect import describe_source
from datahub.ingestion.agent.models import FieldKind
from datahub.ingestion.agent.redact import (
    _SENSITIVE_KEY_HINTS,
    collect_nested_secret_values,
)
from datahub.ingestion.agent.secrets import default_resolvers, resolve_config_collecting
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

    # The sweep above sees only top-level fields describe_source classifies as
    # SECRET, so a secret in a free-form nested dict was reported as a clean
    # recipe -- kafka's connection.consumer_config['sasl.password'] being the
    # case that matters, and snowflake's credential.private_key the typed one.
    # The redactor already knows those are secrets: collect_nested_secret_values
    # is what masks them on the way out. The one command whose job is to say
    # "you have a plaintext secret in this file" was the only thing not asking.
    #
    # Reported without naming the value or its path: the point is that the file
    # holds one, and echoing where would put it in the transcript this warning
    # exists to keep it out of.
    nested = collect_nested_secret_values(config, _SENSITIVE_KEY_HINTS)
    plaintext_nested = sorted(v for v in nested if not _REF.search(v))
    if plaintext_nested:
        warnings.append(
            f"{len(plaintext_nested)} plaintext secret value(s) sit under "
            f"sensitive-looking keys nested in this config (the same keys the "
            f"redactor masks on the way out). Replace each with a ${{REF}} "
            f"placeholder and export it where the probe runs"
        )

    # Validated against the RESOLVED config, not the raw one. `${VAR}` is a
    # string wherever it appears, so a recipe using one for an int or bool
    # field -- `profiling: {enabled: ${PROFILING_ENABLED}}` -- failed
    # pydantic with "Input should be a valid boolean" and was reported
    # invalid, while `datahub ingest` ran it happily. This command's own
    # warning text tells the author to use ${...} references, so it was
    # advising the thing it then rejected.
    #
    # An unresolvable reference is a real error and says so by name, which is
    # a better answer than a type complaint about the literal "${VAR}".
    try:
        resolved = resolve_config_collecting(config, default_resolvers()).config
    except ValueError as exc:
        errors.append(str(exc))
        resolved = None
    if resolved is not None:
        try:
            config_cls.model_validate(resolved)
        except (ValueError, TypeError, AssertionError) as exc:
            errors.append(str(exc))

    return {"valid": not errors, "errors": errors, "warnings": warnings}
