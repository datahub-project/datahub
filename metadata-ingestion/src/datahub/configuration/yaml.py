from typing import IO

import yaml, os

from datahub.configuration import ConfigurationMechanism

class IncludeLoader(yaml.SafeLoader):
    """YAML Loader with `!include` constructor."""

    def __init__(self, stream: IO) -> None:
        """Initialise Loader."""

        try:
            self._root = os.path.split(stream.name)[0]
        except AttributeError:
            self._root = os.path.curdir

        super().__init__(stream)


def construct_include(loader: IncludeLoader, node: yaml.Node) -> dict:
    """Include file referenced at node."""

    filename = os.path.abspath(os.path.join(loader._root, loader.construct_scalar(node)))

    with open(filename, 'r') as f:
        return yaml.load(f, IncludeLoader)


yaml.add_constructor('!include', construct_include, IncludeLoader)


class YamlConfigurationMechanism(ConfigurationMechanism):
    """Ability to load configuration from yaml files"""

    def load_config(self, config_fp: IO) -> dict:
        return yaml.load(config_fp, Loader=IncludeLoader)