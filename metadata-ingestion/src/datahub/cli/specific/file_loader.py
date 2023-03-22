import io
from pathlib import Path
from typing import Union

from datahub.configuration.common import ConfigurationError
from datahub.configuration.yaml import YamlConfigurationMechanism


def load_file(config_file: Path) -> Union[dict, list]:
    """
    A variant of datahub.configuration.common.config_loader.load_config_file
    that does not:
    - resolve env variables
    - pretend to return a dictionary

    Required for other use-cases of loading pydantic based models and will probably
    evolve to becoming a standard function that all the specific. cli variants will use
    to load up the models from external files
    """
    if not isinstance(config_file, Path):
        config_file = Path(config_file)
    if not config_file.is_file():
        raise ConfigurationError(f"Cannot open config file {config_file}")

    if config_file.suffix in {".yaml", ".yml"}:
        config_mech: YamlConfigurationMechanism = YamlConfigurationMechanism()
    else:
        raise ConfigurationError(
            f"Only .yaml and .yml are supported. Cannot process file type {config_file.suffix}"
        )

    raw_config_file = config_file.read_text()
    config_fp = io.StringIO(raw_config_file)
    raw_config = config_mech.load_config(config_fp)
    return raw_config
