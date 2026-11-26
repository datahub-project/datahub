import re
from dataclasses import dataclass, field
from typing import Callable, Dict, Iterator, List, Optional, Set, Tuple, Type, TypedDict

from datahub.configuration.common import ConfigModel
from datahub.configuration.validate_multiline_string import escaped_newlines_to_real
from datahub.ingestion.agent.config_validation import validate_source_config
from datahub.ingestion.agent.introspect import (
    describe_source,
    iter_model_secret_values,
    iter_secret_field_values,
)
from datahub.ingestion.agent.models import FieldKind
from datahub.ingestion.agent.redact import (
    SENSITIVE_KEY_HINTS,
    collect_nested_credential_values,
)
from datahub.ingestion.agent.secrets import (
    SecretResolver,
    default_resolvers,
    is_variable_reference,
    names_a_dotted_path,
    resolve_config_collecting,
)
from datahub.ingestion.source.source_registry import source_registry


def _env_name(path: str) -> str:
    """An environment variable name for a config path (`git_info.deploy_key`
    -> GIT_INFO_DEPLOY_KEY)."""
    return re.sub(r"[^0-9A-Za-z]+", "_", path).strip("_").upper()


def _unique_env_name(path: str, taken: Set[str]) -> str:
    """_env_name(path), suffixed `_2`, `_3`, ... when an earlier path in
    `taken` spells it too (`deps[foo.bar]` and `deps[foo-bar]`): one variable
    bound to two secrets would hand one of them the wrong value."""
    base = _env_name(path)
    name, suffix = base, 1
    while name in taken:
        suffix += 1
        name = f"{base}_{suffix}"
    taken.add(name)
    return name


def _string_values(node: object, path: str = "") -> Iterator[Tuple[str, str]]:
    """(path, value) for every string a recipe's config holds, at any depth."""
    if isinstance(node, str):
        yield path, node
    elif isinstance(node, dict):
        for key, item in node.items():
            yield from _string_values(item, f"{path}.{key}" if path else str(key))
    elif isinstance(node, list):
        for index, item in enumerate(node):
            yield from _string_values(item, f"{path}[{index}]")


def _literal_strings(config: Dict[str, object]) -> Iterator[str]:
    """Every string value a recipe's config spells as text, not as a variable
    reference."""
    for _path, value in _string_values(config):
        if not is_variable_reference(value):
            yield value


def _plaintext_warning(path: str, env: str) -> str:
    return (
        f"'{path}' contains a plaintext secret; the agent sees this value when "
        f"editing the file. Recommend '{path}: ${{{env}}}' and export {env}=..."
    )


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


class RecipeValidation(TypedDict):
    valid: bool
    errors: List[str]
    warnings: List[str]


class _Invalid(Exception):
    """The recipe's one error, where validation cannot go on."""


@dataclass(frozen=True)
class _RecipeSource:
    source_type: str
    config: Dict[str, object]


def _recipe_source(recipe: Dict[str, object]) -> _RecipeSource:
    source = recipe.get("source")
    if not isinstance(source, dict) or "type" not in source:
        raise _Invalid("recipe.source.type is required")
    # Only YAML null ("config:") means no config; any other non-mapping is
    # refused for its shape rather than reaching the config class as {}.
    config = source.get("config", {})
    if config is None:
        config = {}
    if not isinstance(config, dict):
        raise _Invalid("recipe.source.config must be a mapping")
    return _RecipeSource(source_type=str(source["type"]), config=config)


def _config_class(source_type: str) -> Type[ConfigModel]:
    try:
        # The registry, not probe_methods.require_config_class:
        # source_class_for already words its failure as "unknown or
        # unloadable source type", which the error below would then repeat.
        source_cls = source_registry.get(source_type)
        # Injected by @config_class at runtime, out of mypy's view.
        get_config_class = getattr(source_cls, "get_config_class", None)
        if get_config_class is None:
            raise TypeError(f"Source {source_type!r} does not define a config class")
        return get_config_class()
    except Exception as exc:
        # Any failure to resolve the source makes the recipe invalid.
        raise _Invalid(
            f"unknown or unloadable source type '{source_type}': {exc}"
        ) from exc


@dataclass
class _PlaintextSecrets:
    """The secrets a recipe spells in plaintext, one warning each."""

    warnings: List[str] = field(default_factory=list)
    # The values warned about, so no later sweep reports one twice.
    values: Set[str] = field(default_factory=set)
    # Every path judged, a ${REF} one too: a later sweep naming a path the
    # recipe holds the same way needs no second verdict.
    paths: Set[str] = field(default_factory=set)
    _env_names: Set[str] = field(default_factory=set)

    def judged(self, path: str, value: str) -> bool:
        return path in self.paths or value in self.values

    def note(self, path: str, value: str) -> None:
        self.paths.add(path)
        self.values.add(value)
        env = _unique_env_name(path, self._env_names)
        self.warnings.append(_plaintext_warning(path, env))


def _recipe_secrets(
    config_cls: Type[ConfigModel],
    config: Dict[str, object],
    plaintext: _PlaintextSecrets,
) -> None:
    """Every SecretStr field the recipe writes inline, at any depth, by path:
    the walk the redactor masks with. dict() drops a path a union read twice."""
    for path, value in dict(iter_secret_field_values(config_cls, config)).items():
        if is_variable_reference(value):
            plaintext.paths.add(path)
        else:
            plaintext.note(path, value)


def _validated_config(
    config_cls: Type[ConfigModel],
    source: _RecipeSource,
    resolvers: Optional[List[SecretResolver]],
) -> ConfigModel:
    """The config validated resolved, as `datahub ingest` does: a raw `${VAR}`
    is a string even in a bool field. An unresolvable reference is an error by
    name."""
    try:
        resolved = resolve_config_collecting(
            source.config, default_resolvers() if resolvers is None else resolvers
        )
    except ValueError as exc:
        raise _Invalid(str(exc)) from exc
    try:
        return validate_source_config(config_cls, source.source_type, resolved.config)
    except (ValueError, TypeError, AssertionError) as exc:
        raise _Invalid(str(exc)) from exc


# What field validators make of a recipe's string before a SecretStr field
# holds it: a value the recipe spells in either form is in the file.
_VALIDATOR_REWRITES: Tuple[Callable[[str], str], ...] = (escaped_newlines_to_real,)


def _validated_secrets(
    validated: ConfigModel, config: Dict[str, object], plaintext: _PlaintextSecrets
) -> None:
    """What only the validated config holds under a SecretStr field: a value
    under a key validation renamed (github_info -> git_info). Its path is not
    the recipe's, so it is plaintext only if the recipe spells the value, as
    written or as a field validator rewrites it. Not one a ${REF} supplied, nor
    one a validator read in (deploy_key_file): those are masked, but there is
    nothing in the file to move into a ${REF}."""
    literals = set(_literal_strings(config))
    spelled = literals.union(
        *({rewrite(v) for v in literals} for rewrite in _VALIDATOR_REWRITES)
    )
    for path, value in iter_model_secret_values(validated):
        if not plaintext.judged(path, value) and value in spelled:
            plaintext.note(path, value)


def _nested_secret_warnings(
    config_cls: Type[ConfigModel],
    config: Dict[str, object],
    already_warned: Set[str],
) -> List[str]:
    """Secrets nested in free-form dicts (`consumer_config['sasl.password']`),
    counted without naming the value or its path, which would put it in the
    transcript. The detecting collector judges a dotted key on its last
    segment, so `sasl.mechanism` is not flagged, and a config block's fields by
    their own names."""
    nested = collect_nested_credential_values(
        config, SENSITIVE_KEY_HINTS, config_cls=config_cls
    )
    plaintext_nested = sorted(
        v for v in nested if not is_variable_reference(v) and v not in already_warned
    )
    if not plaintext_nested:
        return []
    return [
        f"{len(plaintext_nested)} plaintext secret value(s) sit under "
        f"sensitive-looking keys nested in this config (the same keys the "
        f"redactor masks on the way out). Replace each with a ${{REF}} "
        f"placeholder and export it where the probe runs"
    ]


def _dotted_reference_warnings(config: Dict[str, object]) -> List[str]:
    """A `${a.b}` (or a leading `$a.b`) reads as a dotted path, ~/.datahubenv
    style, but ingestion looks up `a`: braced, it substitutes an empty string
    unless `a` is set; unbraced, it expands `$a` and keeps the literal `.b`.
    Named by path only: the value may hold more than the reference."""
    return [
        f"'{path}' holds a variable reference whose name contains a dot. "
        f"`datahub ingest` reads `${{a.b}}` as `${{a}}` followed by a modifier, "
        f"which is an empty string unless `a` itself is set, and `$a.b` as "
        f"`$a` followed by the literal text `.b`; the probe does the same. "
        f"For the DataHub server and token use "
        f"${{DATAHUB_GMS_URL}} and ${{DATAHUB_GMS_TOKEN}}"
        for path, value in _string_values(config)
        if names_a_dotted_path(value)
    ]


def validate_recipe(
    recipe: Dict[str, object], resolvers: Optional[List[SecretResolver]] = None
) -> RecipeValidation:
    """Whether this recipe would load, and what is wrong with it if not.

    `resolvers` defaults to the environment chain; a caller holding secrets
    elsewhere (the CLI's stdin envelope) passes its own.
    """
    try:
        source = _recipe_source(recipe)
        config_cls = _config_class(source.source_type)
    except _Invalid as exc:
        return RecipeValidation(valid=False, errors=[str(exc)], warnings=[])
    plaintext = _PlaintextSecrets()
    _recipe_secrets(config_cls, source.config, plaintext)
    errors: List[str] = []
    try:
        validated = _validated_config(config_cls, source, resolvers)
    except _Invalid as exc:
        errors.append(str(exc))
    else:
        _validated_secrets(validated, source.config, plaintext)
    warnings = [
        *plaintext.warnings,
        *_nested_secret_warnings(config_cls, source.config, plaintext.values),
        *_dotted_reference_warnings(source.config),
    ]
    return RecipeValidation(valid=not errors, errors=errors, warnings=warnings)
