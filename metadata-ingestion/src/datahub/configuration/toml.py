from typing import IO

import toml

from .common import ConfigurationMechanism


class TomlConfigurationMechanism(ConfigurationMechanism):
    """Ability to load configuration from toml files"""

    def load_config(self, config_fp: IO):
        config = toml.load(config_fp)
        return config
