from enum import Enum
from typing import List, Optional

from datahub_monitors.common.config import PermissiveBaseModel


class MonitorFetcherMode(Enum):
    """Mode of execution for a monitor fetcher"""

    DEFAULT = "DEFAULT"
    REMOTE = "REMOTE"


class MonitorFetcherConfig(PermissiveBaseModel):
    """Config required to create a monitor fetcher"""

    mode: MonitorFetcherMode = MonitorFetcherMode.DEFAULT

    executor_ids: Optional[List[str]] = None
