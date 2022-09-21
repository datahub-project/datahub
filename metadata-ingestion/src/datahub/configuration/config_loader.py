import io
import pathlib
import re
import sys
from typing import Any, Dict, Union

from expandvars import UnboundVariable, expandvars

from datahub.configuration.common import ConfigurationError, ConfigurationMechanism
from datahub.configuration.toml import TomlConfigurationMechanism
from datahub.configuration.yaml import YamlConfigurationMechanism


def resolve_element(element: str) -> str:
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
            new_v.append(resolve_element(ele))
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
            new_dict[k] = resolve_element(v)
        else:
            new_dict[k] = v
    return new_dict


def load_config_file(
    config_file: Union[pathlib.Path, str],
    squirrel_original_config: bool = False,
    squirrel_field: str = "__orig_config",
    allow_stdin: bool = False,
) -> dict:
    config_mech: ConfigurationMechanism
    if allow_stdin and config_file == "-":
        # If we're reading from stdin, we assume that the input is a YAML file.
        config_mech = YamlConfigurationMechanism()
        raw_config_file = sys.stdin.read()
    else:
        if isinstance(config_file, str):
            config_file = pathlib.Path(config_file)
        if not config_file.is_file():
            raise ConfigurationError(f"Cannot open config file {config_file}")

        if config_file.suffix in {".yaml", ".yml"}:
            config_mech = YamlConfigurationMechanism()
        elif config_file.suffix == ".toml":
            config_mech = TomlConfigurationMechanism()
        else:
            raise ConfigurationError(
                "Only .toml and .yml are supported. Cannot process file type {}".format(
                    config_file.suffix
                )
            )

        raw_config_file = config_file.read_text()

    config_fp = io.StringIO(raw_config_file)
    raw_config = config_mech.load_config(config_fp)
    config = resolve_env_variables(raw_config)
    if squirrel_original_config:
        config[squirrel_field] = raw_config
    return config
