import os
import pathlib
import tempfile
from dataclasses import dataclass
from typing import Optional


@dataclass
class AnalyticsConfig:
    """
    A class to represent the configuration of the analytics module
    Currently written as a simple facade over the environment variables
    """

    cache_dir: pathlib.Path = pathlib.Path(
        os.environ.get("ANALYTICS_CACHE_DIR") or tempfile.gettempdir()
    )

    reporting_prefix: Optional[str] = os.environ.get("ANALYTICS_REPORTING_PREFIX")

    @classmethod
    def get_instance(cls) -> "AnalyticsConfig":
        """
        Get the instance of the analytics config
        """
        return _instance


_instance = AnalyticsConfig()
