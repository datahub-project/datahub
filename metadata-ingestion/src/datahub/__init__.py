import sys
import warnings

# Published at https://pypi.org/project/acryl-datahub/.
__package_name__ = "acryl-datahub"
__version__ = "0.0.0.dev0"


def is_dev_mode() -> bool:
    return __version__.endswith("dev0")


def nice_version_name() -> str:
    if is_dev_mode():
        return "unavailable (installed in develop mode)"
    return __version__


if sys.version_info < (3, 7):
    warnings.warn(
        "DataHub requires Python 3.7 or newer. "
        "Please upgrade your Python version to continue using DataHub.",
        FutureWarning,
    )
