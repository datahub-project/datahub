import os
import re
import sys
import warnings
from typing import Optional

# Published at https://pypi.org/project/acryl-datahub/.
__package_name__ = "acryl-datahub"
__version__ = "0.0.0.dev0"


def is_dev_mode() -> bool:
    return __version__.endswith("dev0")


def nice_version_name() -> str:
    if is_dev_mode():
        return "unavailable (installed in develop mode)"
    return __version__


def git_tag(version: Optional[str] = None) -> str:
    """
    Attempt to extract the expected github tag based on environment or version. Fallback to `master`
    :return: github tag
    """
    if version not in ("master", "head"):
        match = re.match(
            "^v?([0-9]+[.][0-9]+([.][0-9]+){1,2})",
            os.getenv("DATAHUB_VERSION", version if version else __version__),
        )
        if match and match.group(1) != "0.0.0":
            return "v" + match.group(1)

    return "master"


def docker_tag(version: Optional[str] = None) -> str:
    """
    Attempt to extract the expected docker tag based on environment or version. Fallback to `head`
    :return: docker tag
    """
    if version not in ("master", "head"):
        match = re.match(
            "^v?([0-9]+[.][0-9]([.][0-9]+){1,2}(-.+)?)",
            os.getenv("DATAHUB_VERSION", version if version else __version__),
        )
        if match and match.group(1) != "0.0.0":
            return "v" + match.group(1)

    return "head"


if sys.version_info < (3, 7):
    warnings.warn(
        "DataHub requires Python 3.7 or newer. "
        "Please upgrade your Python version to continue using DataHub.",
        FutureWarning,
    )
