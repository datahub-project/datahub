import io
import pathlib
from typing import Union

from expandvars import UnboundVariable, expandvars

from datahub.configuration.common import ConfigurationError, ConfigurationMechanism
from datahub.configuration.toml import TomlConfigurationMechanism
from datahub.configuration.yaml import YamlConfigurationMechanism


def resolve_element(element: str) -> str:
    if element.startswith("${") & element.endswith("}"):
        return expandvars(element, nounset=True)
    elif element.startswith("$"):
        try:
            return expandvars(element, nounset=True)
        except UnboundVariable:
            return element
    else:
        return element


def resolve_list(ele_list: list) -> list:
    new_v = []
    for ele in ele_list:
        if isinstance(ele, str):
            new_v.append(resolve_element(ele))  # type:ignore
        elif isinstance(ele, list):
            new_v.append(resolve_list(ele))  # type:ignore
        else:
            new_v = ele_list
    return new_v


def resolve_env_variables(config: dict) -> None:
    for k, v in config.items():
        if isinstance(v, dict):
            resolve_env_variables(v)
        elif isinstance(v, list):
            config[k] = resolve_list(v)
        elif isinstance(v, str):
            config[k] = resolve_element(v)
    return None


def load_config_file(config_file: Union[pathlib.Path, str]) -> dict:
    if isinstance(config_file, str):
        config_file = pathlib.Path(config_file)
    if not config_file.is_file():
        raise ConfigurationError(f"Cannot open config file {config_file}")

    config_mech: ConfigurationMechanism
    if config_file.suffix in [".yaml", ".yml"]:
        config_mech = YamlConfigurationMechanism()
    elif config_file.suffix == ".toml":
        config_mech = TomlConfigurationMechanism()
    else:
        raise ConfigurationError(
            "Only .toml and .yml are supported. Cannot process file type {}".format(
                config_file.suffix
            )
        )

    with config_file.open() as raw_config_fp:
        raw_config_file = raw_config_fp.read()
    config_fp = io.StringIO(raw_config_file)
    config = config_mech.load_config(config_fp)
    resolve_env_variables(config)
    return config
