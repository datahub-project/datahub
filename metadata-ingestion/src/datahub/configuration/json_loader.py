import json
from typing import IO

from datahub.configuration import ConfigurationMechanism


class JsonConfigurationMechanism(ConfigurationMechanism):
    """Ability to load configuration from json files"""

    def load_config(self, config_fp: IO) -> dict:
        return json.load(config_fp)
