from pathlib import Path
from typing import Union

from datahub.configuration.config_loader import load_config_file


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

    res = load_config_file(
        config_file,
        squirrel_original_config=False,
        resolve_env_vars=False,
        allow_stdin=False,
    )
    return res
