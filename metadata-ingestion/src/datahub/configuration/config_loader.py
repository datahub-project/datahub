import io
import pathlib
from typing import Union

from expandvars import expandvars

from datahub.configuration.common import ConfigurationError, ConfigurationMechanism
from datahub.configuration.toml import TomlConfigurationMechanism
from datahub.configuration.yaml import YamlConfigurationMechanism


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

    expanded_config_file = expandvars(raw_config_file, nounset=True)
    config_fp = io.StringIO(expanded_config_file)
    config = config_mech.load_config(config_fp)

    return config
