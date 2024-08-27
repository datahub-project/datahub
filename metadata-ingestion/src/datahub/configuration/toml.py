from typing import IO, cast

import toml

from datahub.configuration.common import ConfigurationMechanism


class TomlConfigurationMechanism(ConfigurationMechanism):
    """Ability to load configuration from toml files"""

    def load_config(self, config_fp: IO) -> dict:
        config = toml.load(config_fp)
        return cast(dict, config)  # converts MutableMapping -> dict
