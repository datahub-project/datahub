from enum import Enum
from typing import List, Optional

from datahub_monitors.common.config import PermissiveBaseModel


class FetcherMode(Enum):
    """Mode of execution for a fetcher"""

    DEFAULT = "DEFAULT"
    REMOTE = "REMOTE"


class FetcherConfig(PermissiveBaseModel):
    """Config required to create a fetcher"""

    id: str

    enabled: bool = True

    mode: FetcherMode = FetcherMode.DEFAULT

    executor_ids: Optional[List[str]] = None

    refresh_interval: int
