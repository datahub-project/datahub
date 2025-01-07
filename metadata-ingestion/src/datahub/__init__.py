import logging
import sys
import warnings

# Published at https://pypi.org/project/acryl-datahub/.
__package_name__ = "acryl-datahub"
__version__ = "1!0.0.0.dev0"

from typing import Any

from datahub.cli.env_utils import get_boolean_env_variable


def is_dev_mode() -> bool:
    return __version__.endswith("dev0")


def nice_version_name() -> str:
    if is_dev_mode():
        return "unavailable (installed in develop mode)"
    return __version__


if sys.version_info < (3, 8):
    warnings.warn(
        "DataHub requires Python 3.8 or newer. "
        "Please upgrade your Python version to continue using DataHub.",
        FutureWarning,
        stacklevel=2,
    )


class CustomLogger(logging.Logger):
    def trace(self, message: str, *args: Any, **kwargs: Any) -> None:
        if not self.isEnabledFor(logging.DEBUG):
            # we trace only if we are in debug phase
            return
        env = f'TRACE_{self.name.replace(".", "__").upper()}'
        if get_boolean_env_variable(env, False):
            self._log(logging.DEBUG, message, args, **kwargs)


# we have to set this class before getLogger is called
logging.setLoggerClass(CustomLogger)
