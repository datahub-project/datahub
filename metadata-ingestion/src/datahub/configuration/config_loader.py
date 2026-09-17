import io
import json
import os
import pathlib
import re
import sys
import tempfile
import unittest.mock
import urllib.parse
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Set, Union

import requests
from expandvars import UnboundVariable, expand

from datahub.configuration.common import ConfigurationError, ConfigurationMechanism
from datahub.configuration.json_loader import JsonConfigurationMechanism
from datahub.configuration.toml import TomlConfigurationMechanism
from datahub.configuration.yaml import YamlConfigurationMechanism
from datahub.masking.secret_registry import SecretRegistry, is_masking_enabled

Environ = Mapping[str, str]


def _extract_env_var_names(text: str) -> Set[str]:
    """Extract environment variable names from ${VAR} or $VAR patterns."""
    var_names = set()

    # Match ${VAR} and bash parameter expansion patterns
    # Pattern breakdown:
    #   ([A-Za-z_][A-Za-z0-9_]*)  - capture variable name
    #   (?::[+\-=?][^}]*)?        - optional bash operators with content
    for match in re.finditer(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::[+\-=?][^}]*)?\}", text):
        var_names.add(match.group(1))

    # Match $VAR patterns (without braces)
    for match in re.finditer(r"\$([A-Za-z_][A-Za-z0-9_]*)", text):
        var_name = match.group(1)
        # Only add if not already captured by ${} pattern
        if var_name not in var_names:
            var_names.add(var_name)

    return var_names


def resolve_env_variables(config: dict, environ: Environ) -> dict:
    # TODO: This is kept around for backwards compatibility.
    return EnvResolver(environ).resolve(config)


def list_referenced_env_variables(config: dict) -> Set[str]:
    # TODO: This is kept around for backwards compatibility.
    return EnvResolver(environ=os.environ).list_referenced_variables(config)


class EnvResolver:
    """Resolves environment variable references in configuration dictionaries."""

    def __init__(
        self,
        environ: Environ,
        strict_env_syntax: bool = False,
        register_secrets: bool = True,
    ):
        """
        Initialize the environment variable resolver.

        Args:
            environ: Environment variable mapping (os.environ, custom dict, or external secrets)
            strict_env_syntax: If True, only match ${VAR} syntax (not $VAR)
            register_secrets: If True, register resolved values with masking registry
                            (only if masking is globally enabled)
        """
        self.environ = environ
        self.strict_env_syntax = strict_env_syntax
        self.register_secrets = register_secrets

    def resolve(self, config: dict) -> dict:
        return self._resolve_dict(config)

    @classmethod
    def list_referenced_variables(
        cls,
        config: dict,
        strict_env_syntax: bool = False,
    ) -> Set[str]:
        # This is a bit of a hack, but expandvars does a bunch of escaping
        # and other logic that we don't want to duplicate here.

        vars = set()

        def mock_get_env(key: str, default: Optional[str] = None) -> str:
            vars.add(key)
            if default is not None:
                return default
            return "mocked_value"

        mock = unittest.mock.MagicMock()
        mock.get.side_effect = mock_get_env

        resolver = EnvResolver(
            environ=mock, strict_env_syntax=strict_env_syntax, register_secrets=False
        )
        resolver._resolve_dict(config)

        return vars

    def _register_env_vars_from_element(self, element: str) -> None:
        """Register environment variables found in element for secret masking."""
        if not self.register_secrets or not is_masking_enabled():
            return

        # Extract variable names from the pattern
        var_names = _extract_env_var_names(element)

        # Collect secrets for batch registration
        # We register all ${VAR} references from recipe as secrets
        secrets = {}
        for var_name in var_names:
            value = self.environ.get(var_name)
            if value:
                secrets[var_name] = value

        # Batch register all secrets from this element
        if secrets:
            SecretRegistry.get_instance().register_secrets_batch(secrets)

    def _resolve_element(self, element: str) -> str:
        if re.search(r"(\$\{).+(\})", element):
            # Register secrets before expansion
            self._register_env_vars_from_element(element)
            return expand(element, nounset=True, environ=self.environ)
        elif not self.strict_env_syntax and element.startswith("$"):
            try:
                # Register secrets before expansion
                self._register_env_vars_from_element(element)
                return expand(element, nounset=True, environ=self.environ)
            except UnboundVariable:
                # TODO: This fallback is kept around for backwards compatibility, but
                # doesn't make a ton of sense from first principles.
                return element
        else:
            return element

    def _resolve_list(self, ele_list: list) -> list:
        new_v: list = []
        for ele in ele_list:
            if isinstance(ele, str):
                new_v.append(self._resolve_element(ele))
            elif isinstance(ele, list):
                new_v.append(self._resolve_list(ele))
            elif isinstance(ele, dict):
                new_v.append(self._resolve_dict(ele))
            else:
                new_v.append(ele)
        return new_v

    def _resolve_dict(self, config: dict) -> dict:
        new_dict: Dict[Any, Any] = {}
        for k, v in config.items():
            if isinstance(v, dict):
                new_dict[k] = self._resolve_dict(v)
            elif isinstance(v, list):
                new_dict[k] = self._resolve_list(v)
            elif isinstance(v, str):
                new_dict[k] = self._resolve_element(v)
            else:
                new_dict[k] = v
        return new_dict


WRITE_TO_FILE_DIRECTIVE_PREFIX = "__DATAHUB_TO_FILE_"


def _process_directives(config: dict) -> dict:
    def _process(obj: Any) -> Any:
        if isinstance(obj, dict):
            new_obj = {}
            for k, v in obj.items():
                if isinstance(k, str) and k.startswith(WRITE_TO_FILE_DIRECTIVE_PREFIX):
                    # This writes the value to a temporary file and replaces the value with the path to the file.
                    config_option = k[len(WRITE_TO_FILE_DIRECTIVE_PREFIX) :]

                    with tempfile.NamedTemporaryFile("w", delete=False) as f:
                        filepath = f.name
                        f.write(v)

                    new_obj[config_option] = filepath
                else:
                    new_obj[k] = _process(v)

            return new_obj
        else:
            return obj

    return _process(config)


class MalformedRecipeEnvelope(ConfigurationError):
    """A well-formed JSON envelope whose `__recipe_yaml__` is not a string.

    Carries the envelope's secrets so a caller can still register them for
    masking before it reports the failure -- the error path is where
    unmasked values do the most damage.
    """

    def __init__(self, message: str, secrets: Dict[str, str]) -> None:
        super().__init__(message)
        self.secrets = secrets


@dataclass(frozen=True)
class RecipeEnvelope:
    """The JSON envelope `--recipe -` and `ingest -c -` both accept.

    `{"__recipe_yaml__": "<yaml>", "__secrets__": {"NAME": "value"}}` --
    secrets travel beside the recipe so a caller can hand over resolved
    credentials without putting them in the environment, where they are
    readable from /proc/<pid>/environ and inherited by every child.
    """

    recipe_yaml: str
    secrets: Dict[str, str]


def parse_recipe_envelope(raw: str) -> Optional[RecipeEnvelope]:
    """The envelope in `raw`, or None when `raw` is a plain recipe.

    One parser, because there were two and they had already drifted: only
    one validated that `__recipe_yaml__` is a string, so the same malformed
    envelope produced a named error on one path and
    `TypeError: initial_value must be str or None, not dict` on the other.

    Returning None rather than raising for a non-envelope is what keeps the
    plain YAML/JSON form working: every caller treats None as "this is the
    recipe itself".

    Secrets are filtered to strings, and that is not tidying. str(v) would
    turn a JSON null into the literal "None", so a secret the caller failed
    to resolve becomes a password of "None" and the probe reports whatever
    the server says about it rather than naming the reference it could not
    resolve -- and the registry will not mask that value either, since
    "none" is on its unmaskable-literals list. Dropping the entry lets
    resolution fail by name. An empty string is KEPT: it is a value the
    caller chose, and dropping it leaves nothing for the mapping resolver,
    so the ambient variable of the same name is read instead -- the
    fall-through the envelope exists to prevent.
    """
    try:
        envelope = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(envelope, dict) or "__recipe_yaml__" not in envelope:
        return None

    # Secrets FIRST, before anything can raise.
    #
    # recipe_cli registers envelope secrets before parsing the YAML on
    # purpose: a failure during loading is exactly when the error text is
    # least controlled, so the values have to be maskable by then. Raising
    # on a bad __recipe_yaml__ before reading __secrets__ reopened that
    # window -- the caller never saw the secrets it was about to need.
    #
    # They travel on the exception so the caller can register them and
    # still fail.
    raw_secrets = envelope.get("__secrets__") or {}
    secrets = (
        {str(k): v for k, v in raw_secrets.items() if isinstance(v, str)}
        if isinstance(raw_secrets, dict)
        else {}
    )

    recipe_yaml = envelope["__recipe_yaml__"]
    if not isinstance(recipe_yaml, str):
        raise MalformedRecipeEnvelope(
            "__recipe_yaml__ must be a string holding the recipe YAML; got "
            f"{type(recipe_yaml).__name__}",
            secrets=secrets,
        )
    return RecipeEnvelope(recipe_yaml=recipe_yaml, secrets=secrets)


def load_config_file(
    config_file: Union[str, pathlib.Path],
    squirrel_original_config: bool = False,
    squirrel_field: str = "__orig_config",
    allow_stdin: bool = False,
    allow_remote: bool = True,  # TODO: Change the default to False.
    resolve_env_vars: bool = True,  # TODO: Change the default to False.
    process_directives: bool = False,
    extra_env_vars: Optional[Dict[str, str]] = None,
) -> dict:
    config_mech: ConfigurationMechanism
    if allow_stdin and config_file == "-":
        # Reading from stdin. Supports two formats:
        # 1. Plain YAML/JSON recipe (backward compatible)
        # 2. JSON envelope: {"__recipe_yaml__": "...", "__secrets__": {...}}
        #    Secrets are merged into the env resolver without touching os.environ.
        config_mech = YamlConfigurationMechanism()
        raw_stdin = sys.stdin.read()

        envelope = parse_recipe_envelope(raw_stdin)
        if envelope is None:
            # Plain YAML or plain JSON (which is valid YAML) — the recipe itself.
            raw_config_file = raw_stdin
        else:
            raw_config_file = envelope.recipe_yaml
            if envelope.secrets:
                extra_env_vars = {**(extra_env_vars or {}), **envelope.secrets}
                # Envelope secrets are secrets by declaration: maskable even
                # when the recipe does not reference them (e.g. it arrived
                # with values already substituted).
                SecretRegistry.get_instance().register_secrets_batch(envelope.secrets)
    else:
        config_file_path = pathlib.Path(config_file)
        if config_file_path.suffix in {".yaml", ".yml"}:
            config_mech = YamlConfigurationMechanism()
        elif config_file_path.suffix == ".json":
            config_mech = JsonConfigurationMechanism()
        elif config_file_path.suffix == ".toml":
            config_mech = TomlConfigurationMechanism()
        else:
            raise ConfigurationError(
                f"Only .toml, .yml, and .json are supported. Cannot process file type {config_file_path.suffix}"
            )

        url_parsed = urllib.parse.urlparse(str(config_file))
        if allow_remote and url_parsed.scheme in (
            "http",
            "https",
        ):  # URLs will return http/https
            # If the URL is remote, we need to fetch it.
            try:
                response = requests.get(
                    str(config_file),
                    auth=(
                        urllib.parse.unquote(url_parsed.username or ""),
                        urllib.parse.unquote(url_parsed.password or ""),
                    )
                    if url_parsed.username or url_parsed.password
                    else None,
                )
                response.raise_for_status()
                raw_config_file = response.text
            except Exception as e:
                raise ConfigurationError(
                    f"Cannot read remote file {config_file_path}: {e}"
                ) from e
        else:
            if not config_file_path.is_file():
                raise ConfigurationError(
                    f"Cannot open config file {config_file_path.resolve()}"
                )
            raw_config_file = config_file_path.read_text()

    config_fp = io.StringIO(raw_config_file)
    raw_config = config_mech.load_config(config_fp)

    config = raw_config.copy()
    if resolve_env_vars:
        environ: Environ = os.environ
        if extra_env_vars:
            environ = {**os.environ, **extra_env_vars}
        config = EnvResolver(environ=environ).resolve(config)
    if process_directives:
        config = _process_directives(config)

    if squirrel_original_config:
        config[squirrel_field] = raw_config
    return config
