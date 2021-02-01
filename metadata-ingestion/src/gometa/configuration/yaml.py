from typing import Type, TypeVar
from pathlib import Path
import yaml


from .common import ConfigModel, ConfigurationMechanism, generic_load_file


T = TypeVar("T", bound=ConfigModel)


class YamlConfigurationMechanism(ConfigurationMechanism):
    """Ability to load configuration from yaml files"""

    def load_config(self, cls: Type[T], config_file: Path) -> T:
        config = generic_load_file(cls, config_file, yaml.safe_load)
        return config
