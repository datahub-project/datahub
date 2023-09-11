import io
import pathlib
import re
import sys
import tempfile
import unittest.mock
from typing import Any, Dict, Set, Union
from urllib import parse

import requests
from expandvars import UnboundVariable, expandvars

from datahub.configuration.common import ConfigurationError, ConfigurationMechanism
from datahub.configuration.toml import TomlConfigurationMechanism
from datahub.configuration.yaml import YamlConfigurationMechanism


def _resolve_element(element: str) -> str:
    if re.search(r"(\$\{).+(\})", element):
        return expandvars(element, nounset=True)
    elif element.startswith("$"):
        try:
            return expandvars(element, nounset=True)
        except UnboundVariable:
            return element
    else:
        return element


def _resolve_list(ele_list: list) -> list:
    new_v: list = []
    for ele in ele_list:
        if isinstance(ele, str):
            new_v.append(_resolve_element(ele))
        elif isinstance(ele, list):
            new_v.append(_resolve_list(ele))
        elif isinstance(ele, dict):
            new_v.append(resolve_env_variables(ele))
        else:
            new_v.append(ele)
    return new_v


def resolve_env_variables(config: dict) -> dict:
    new_dict: Dict[Any, Any] = {}
    for k, v in config.items():
        if isinstance(v, dict):
            new_dict[k] = resolve_env_variables(v)
        elif isinstance(v, list):
            new_dict[k] = _resolve_list(v)
        elif isinstance(v, str):
            new_dict[k] = _resolve_element(v)
        else:
            new_dict[k] = v
    return new_dict


def list_referenced_env_variables(config: dict) -> Set[str]:
    # This is a bit of a hack, but expandvars does a bunch of escaping
    # and other logic that we don't want to duplicate here.

    with unittest.mock.patch("expandvars.getenv") as mock_getenv:
        mock_getenv.return_value = "mocked_value"

        resolve_env_variables(config)

    calls = mock_getenv.mock_calls
    return set([call[1][0] for call in calls])


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
    resolve_env_vars: bool = True,
    process_directives: bool = True,
) -> dict:
    config_mech: ConfigurationMechanism
    if allow_stdin and config_file == "-":
        # If we're reading from stdin, we assume that the input is a YAML file.
        config_mech = YamlConfigurationMechanism()
        raw_config_file = sys.stdin.read()
    else:
        config_file_path = pathlib.Path(config_file)
        if config_file_path.suffix in {".yaml", ".yml"}:
            config_mech = YamlConfigurationMechanism()
        elif config_file_path.suffix == ".toml":
            config_mech = TomlConfigurationMechanism()
        else:
            raise ConfigurationError(
                f"Only .toml and .yml are supported. Cannot process file type {config_file_path.suffix}"
            )
        url_parsed = parse.urlparse(str(config_file))
        if url_parsed.scheme in ("http", "https"):  # URLs will return http/https
            try:
                response = requests.get(str(config_file))
                raw_config_file = response.text
            except Exception as e:
                raise ConfigurationError(
                    f"Cannot read remote file {config_file_path}, error:{e}"
                )
        else:
            if not config_file_path.is_file():
                raise ConfigurationError(f"Cannot open config file {config_file_path}")
            raw_config_file = config_file_path.read_text()

    config_fp = io.StringIO(raw_config_file)
    raw_config = config_mech.load_config(config_fp)

    config = raw_config.copy()
    if resolve_env_vars:
        config = resolve_env_variables(config)
    if process_directives:
        config = _process_directives(config)

    if squirrel_original_config:
        config[squirrel_field] = raw_config
    return config
