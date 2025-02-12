import io
import os
import pathlib
import re
import sys
import tempfile
import unittest.mock
from typing import Any, Dict, Mapping, Optional, Set, Union
from urllib import parse

import requests
from expandvars import UnboundVariable, expand

from datahub.configuration.common import ConfigurationError, ConfigurationMechanism
from datahub.configuration.json_loader import JsonConfigurationMechanism
from datahub.configuration.toml import TomlConfigurationMechanism
from datahub.configuration.yaml import YamlConfigurationMechanism

Environ = Mapping[str, str]


def resolve_env_variables(config: dict, environ: Environ) -> dict:
    # TODO: This is kept around for backwards compatibility.
    return EnvResolver(environ).resolve(config)


def list_referenced_env_variables(config: dict) -> Set[str]:
    # TODO: This is kept around for backwards compatibility.
    return EnvResolver(environ=os.environ).list_referenced_variables(config)


class EnvResolver:
    def __init__(self, environ: Environ, strict_env_syntax: bool = False):
        self.environ = environ
        self.strict_env_syntax = strict_env_syntax

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

        resolver = EnvResolver(environ=mock, strict_env_syntax=strict_env_syntax)
        resolver._resolve_dict(config)

        return vars

    def _resolve_element(self, element: str) -> str:
        if re.search(r"(\$\{).+(\})", element):
            return expand(element, nounset=True, environ=self.environ)
        elif not self.strict_env_syntax and element.startswith("$"):
            try:
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


def load_config_file(
    config_file: Union[str, pathlib.Path],
    squirrel_original_config: bool = False,
    squirrel_field: str = "__orig_config",
    allow_stdin: bool = False,
    allow_remote: bool = True,  # TODO: Change the default to False.
    resolve_env_vars: bool = True,  # TODO: Change the default to False.
    process_directives: bool = False,
) -> dict:
    config_mech: ConfigurationMechanism
    if allow_stdin and config_file == "-":
        # If we're reading from stdin, we assume that the input is a YAML file.
        # Note that YAML is a superset of JSON, so this will also read JSON files.
        config_mech = YamlConfigurationMechanism()
        raw_config_file = sys.stdin.read()
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

        url_parsed = parse.urlparse(str(config_file))
        if allow_remote and url_parsed.scheme in (
            "http",
            "https",
        ):  # URLs will return http/https
            # If the URL is remote, we need to fetch it.
            try:
                response = requests.get(str(config_file))
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
        config = EnvResolver(environ=os.environ).resolve(config)
    if process_directives:
        config = _process_directives(config)

    if squirrel_original_config:
        config[squirrel_field] = raw_config
    return config
