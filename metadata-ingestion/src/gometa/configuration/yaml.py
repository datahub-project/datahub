from typing import IO
import yaml

from .common import ConfigModel, ConfigurationMechanism


class YamlConfigurationMechanism(ConfigurationMechanism):
    """Ability to load configuration from yaml files"""

    def load_config(self, config_fp: IO):
        config = yaml.safe_load(config_fp)
        return config
