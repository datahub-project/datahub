from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datahub.sdk.main_client import DataHubClient


class LineageClient:
    def __init__(self, client: DataHubClient):
        self._client = client
