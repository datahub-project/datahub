import pathlib
from typing import Union

from expandvars import expandvars, UnboundVariable

from datahub.configuration.common import ConfigurationError, ConfigurationMechanism
from datahub.configuration.toml import TomlConfigurationMechanism
from datahub.configuration.yaml import YamlConfigurationMechanism


def resolve_env_variables(config: dict):
    for k, v in config.items():
        if isinstance(v, dict):
            resolve_env_variables(v)
        else:
            v = v.strip()
            if v.startswith("${") & v.endswith("}"):
                config[k] = expandvars(v, nounset=True)
            elif v.startswith("$"):
                try:
                    config[k] = expandvars(v, nounset=True)
                except UnboundVariable:
                    pass


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

    config = config_mech.load_config(raw_config_file)
    resolve_env_variables(config)
    return config