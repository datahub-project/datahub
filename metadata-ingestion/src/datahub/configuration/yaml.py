from typing import IO

import yaml

from datahub.configuration.common import ConfigurationMechanism


class YamlConfigurationMechanism(ConfigurationMechanism):
    """Ability to load configuration from yaml files"""

    def load_config(self, config_fp: IO) -> dict:
        return yaml.safe_load(config_fp)
