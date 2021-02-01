from typing import Type, TypeVar
from pathlib import Path
import toml
from .common import ConfigModel, ConfigurationMechanism, generic_load_file

T = TypeVar("T", bound=ConfigModel)


class TomlConfigurationMechanism(ConfigurationMechanism):
    """Ability to load configuration from toml files"""

    def load_config(self, cls: Type[T], config_file: Path) -> T:
        config = generic_load_file(cls, config_file, toml.load)
        return config
