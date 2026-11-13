"""Recipe tools of the opt-in ``ingestion`` group: ``datahub recipe`` as MCP tools.

``list_connector_types`` lists the connectors (``datahub check plugins``),
``get_recipe_schema`` describes one and scaffolds a recipe (``datahub recipe
describe`` / ``scaffold``), and ``validate_recipe`` checks a recipe without
connecting to anything (``datahub recipe validate``).

They run in this server's process, against the ``acryl-datahub`` installed
here, which usually lacks most connectors' packages: the executor installs a
connector only when it runs one. A connector this server cannot load is
reported as such, never as an invalid recipe.

Credentials in a recipe must be secret references such as
``${SNOWFLAKE_PASSWORD}``: a literal value would be stored in the source, and
has already passed through the model that wrote it. The tools that send a
recipe refuse literal ones (``require_secret_references``).
"""

import re
from typing import Optional, TypedDict, cast

import yaml
from fastmcp.exceptions import ToolError
from loguru import logger
from pydantic import JsonValue

from ..arg_validation import OptionalJsonObject
from ..version_requirements import read_only

JsonObject = dict[str, JsonValue]

# Matched on the key name because the connector's config class, which knows its
# SecretStr fields, is usually not installed here. Keys that name a path, a
# file or a reference to a credential are not credentials themselves.
_SECRET_KEY_RE = re.compile(
    r"password|passwd|secret|token|private_key|api_key|apikey|credential",
    re.IGNORECASE,
)
_NON_SECRET_KEY_SUFFIXES = ("_path", "_file", "_url", "_uri", "_name", "_env")
_SECRET_REFERENCE_RE = re.compile(r"^\$\{[^}]+\}$")
# Validation resolves ${...} references to this, never to this server's own
# environment, which holds its credentials.
_SECRET_PLACEHOLDER = "secret-reference-placeholder"
_NEEDS_RECIPE_SUPPORT = (
    "This MCP server's acryl-datahub is too old to describe recipes (it needs "
    "1.8.0 or newer)."
)
_DOCS_URL = "https://docs.datahub.com/docs/generated/ingestion/sources/"


class _ConfigValidation(TypedDict):
    valid: bool
    errors: list[str]
    warnings: list[str]


def literal_secret_paths(value: object, path: str = "") -> list[str]:
    """Dotted paths of credential fields that hold a literal value."""
    found: list[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}" if path else str(key)
            key_text = str(key).lower()
            if (
                isinstance(child, str)
                and child
                and _SECRET_KEY_RE.search(key_text)
                and not key_text.endswith(_NON_SECRET_KEY_SUFFIXES)
                and not _SECRET_REFERENCE_RE.match(child.strip())
            ):
                found.append(child_path)
            else:
                found.extend(literal_secret_paths(child, child_path))
    elif isinstance(value, list):
        for i, child in enumerate(value):
            found.extend(literal_secret_paths(child, f"{path}[{i}]"))
    return found


def require_secret_references(value: object, what: str) -> None:
    paths = literal_secret_paths(value)
    if paths:
        raise ToolError(
            f"{what} has literal credentials at {', '.join(paths)}. Store each one "
            "as a secret in DataHub (Settings > Secrets) and refer to it as "
            "${SECRET_NAME}."
        )


def parse_recipe(recipe: str) -> JsonObject:
    """A recipe's YAML or JSON as a mapping, or a ToolError."""
    try:
        parsed = yaml.safe_load(recipe)
    except yaml.YAMLError as e:
        raise ToolError(f"The recipe is not valid YAML or JSON: {e}") from e
    if not isinstance(parsed, dict):
        raise ToolError("The recipe must be a mapping with a `source` block.")
    return cast(JsonObject, parsed)


def checked_recipe(recipe: str) -> JsonObject:
    """A recipe parsed, with every credential a secret reference."""
    parsed = parse_recipe(recipe)
    require_secret_references(parsed, "The recipe")
    return parsed


def recipe_source_type(parsed: JsonObject) -> str:
    source = parsed.get("source")
    source_type = source.get("type") if isinstance(source, dict) else None
    if not isinstance(source_type, str) or not source_type.strip():
        raise ToolError("The recipe has no `source.type`.")
    return source_type.strip()


def _connector_names() -> tuple[list[str], dict[str, str]]:
    """Every registered connector, and each alias with the connector it names."""
    from datahub.ingestion.source.source_registry import source_registry

    # The alias table has no public accessor; it maps a name to (target, warn).
    raw_aliases = getattr(source_registry, "_aliases", None) or {}
    aliases = {
        alias: target[0]
        for alias, target in raw_aliases.items()
        if isinstance(target, tuple) and target and isinstance(target[0], str)
    }
    return sorted(source_registry.mapping), aliases


def _require_known_connector(connector: str) -> None:
    names, aliases = _connector_names()
    if connector not in names and connector not in aliases:
        raise ToolError(
            f"{connector!r} is not a connector type DataHub knows. Call "
            "list_connector_types to find its name."
        )


def _not_loadable(connector: str, error: Exception) -> str:
    return (
        f"This MCP server cannot load the {connector} connector ({error}), so it "
        "cannot describe its config: the executor installs a connector's "
        "packages only when it runs it. Its docs list the config fields: "
        f"{_DOCS_URL}{connector}"
    )


@read_only
def list_connector_types(query: Optional[str] = None) -> dict:
    """List the ingestion connector types ("source.type" values) DataHub supports (`datahub check plugins`).

    Use to find the connector name for a recipe. `query` filters by name
    (case-insensitive substring). `aliases` maps older names to the connector
    they now select.
    """
    names, aliases = _connector_names()
    needle = (query or "").strip().lower()
    result: dict = {"connectors": [n for n in names if needle in n.lower()]}
    result["total"] = len(result["connectors"])
    matched_aliases = {
        alias: target
        for alias, target in sorted(aliases.items())
        if needle in alias.lower() or needle in target.lower()
    }
    if matched_aliases:
        result["aliases"] = matched_aliases
    return result


@read_only
def get_recipe_schema(
    connector: str,
    field_name: Optional[str] = None,
    partial_config: OptionalJsonObject = None,
) -> dict:
    """Get a connector's recipe config fields (`datahub recipe describe`), and optionally a recipe template (`datahub recipe scaffold`).

    Each field has its kind (plain, secret, pattern, nested), whether it is
    required, its type, default and description. Pass `field_name` (a
    top-level config field, for example "profiling") to read one field. Pass
    `partial_config` (the `source.config` fields you know) to also get
    `recipe_template`: a recipe YAML with the required fields and the ones you
    gave. Put every credential in as a secret reference such as
    "${SNOWFLAKE_PASSWORD}", never as its value.

    Describes the connector as installed on this MCP server. A connector whose
    packages are not installed here cannot be described; the error links its
    docs.
    """
    connector = connector.strip()
    if partial_config is not None:
        require_secret_references(partial_config, "partial_config")
    try:
        from datahub.ingestion.agent.introspect import describe_source
        from datahub.ingestion.agent.recipe import scaffold
    except ImportError as e:
        raise ToolError(_NEEDS_RECIPE_SUPPORT) from e
    _require_known_connector(connector)
    try:
        spec: dict = describe_source(connector).to_dict()
    except Exception as e:
        logger.info(f"Could not load the {connector} connector here: {e}")
        raise ToolError(_not_loadable(connector, e)) from e

    result = spec
    if field_name:
        fields = [f for f in spec.get("fields") or [] if isinstance(f, dict)]
        match = [f for f in fields if f.get("name") == field_name.strip()]
        if not match:
            known = ", ".join(str(f.get("name")) for f in fields)
            raise ToolError(
                f"The {connector} connector has no config field "
                f"{field_name.strip()!r}. Its fields are: {known}."
            )
        result = {"source_type": spec.get("source_type"), "field": match[0]}
    if partial_config is not None:
        template = scaffold(connector)
        source = template.get("source")
        if isinstance(source, dict) and isinstance(source.get("config"), dict):
            source["config"].update(partial_config)
        result["recipe_template"] = yaml.safe_dump(template, sort_keys=False)
    return result


class _PlaceholderResolver:
    def resolve(self, ref: str) -> Optional[str]:
        return _SECRET_PLACEHOLDER


def _validate_config(
    parsed: JsonObject, source_type: str
) -> Optional[_ConfigValidation]:
    """The connector's own validation, or None when it can't be loaded here."""
    try:
        from datahub.ingestion.agent.recipe import (
            validate_recipe as validate_source_recipe,
        )
        from datahub.ingestion.source.source_registry import source_registry
    except ImportError:
        return None
    try:
        source_registry.get(source_type)
    except Exception as e:
        logger.info(f"Could not load the {source_type} connector here: {e}")
        return None
    return cast(
        _ConfigValidation,
        validate_source_recipe(
            cast(dict[str, object], parsed), [_PlaceholderResolver()]
        ),
    )


@read_only
def validate_recipe(recipe: str) -> dict:
    """Check a recipe without connecting to anything (`datahub recipe validate`).

    Checks the YAML, that `source.type` is a connector DataHub knows, that
    every credential is a secret reference, and, when this MCP server can load
    the connector, the config's field names and types (`schema_checked`). An
    invalid recipe comes back as `valid: false` with its `errors`. Call this
    before saving or running a recipe.
    """
    try:
        parsed = parse_recipe(recipe)
    except ToolError as e:
        return {"valid": False, "errors": [str(e)], "schema_checked": False}
    errors: list[str] = []
    extra: dict = {}
    literal = literal_secret_paths(parsed)
    if literal:
        extra["literal_secrets"] = literal
        errors.append(
            f"Credentials at {', '.join(literal)} are literal values. Replace "
            "each with a ${SECRET_NAME} reference to a DataHub secret; the other "
            "ingestion tools refuse the recipe until then."
        )
    schema_checked = False
    try:
        source_type = recipe_source_type(parsed)
        _require_known_connector(source_type)
    except ToolError as e:
        errors.append(str(e))
    else:
        checked = _validate_config(parsed, source_type)
        if checked is None:
            extra["note"] = (
                f"This MCP server cannot load the {source_type} connector, so "
                "its field names and types were not checked. The executor "
                "checks them when it runs the recipe."
            )
        else:
            schema_checked = True
            errors.extend(checked["errors"])
            if checked["warnings"]:
                extra["warnings"] = checked["warnings"]
    return {
        "valid": not errors,
        "errors": errors,
        "schema_checked": schema_checked,
        **extra,
    }
