import logging
import sys
from typing import Any, List

import requests

from datahub.ingestion.source.airbyte.config import AirbyteSourceConfig
from datahub.ingestion.source.airbyte.rest_api_wrapper.data_classes import Workspace
from datahub.ingestion.source.airbyte.rest_api_wrapper.data_resolver import (
    CloudAPIResolver,
    DataResolverBase,
    OssAPIResolver,
)

# Logger instance
logger = logging.getLogger(__name__)


class AirbyteAPI:
    def __init__(self, config: AirbyteSourceConfig) -> None:
        self.__config: AirbyteSourceConfig = config
        self.__api_resolver: DataResolverBase = (
            CloudAPIResolver(
                api_url=self.__config.api_url, api_key=self.__config.api_key
            )
            if self.__config.cloud_deploy
            else OssAPIResolver(
                api_url=self.__config.api_url,
                username=self.__config.username,
                password=self.__config.password,
            )
        )
        self.__api_resolver.test_connection()

    def log_http_error(self, message: str) -> Any:
        logger.warning(message)
        _, e, _ = sys.exc_info()
        if isinstance(e, requests.exceptions.HTTPError):
            logger.warning(f"HTTP status-code = {e.response.status_code}")
        logger.debug(msg=message, exc_info=e)
        return e

    def get_workspaces(self) -> List[Workspace]:
        workspaces: List[Workspace] = []
        try:
            workspaces = self.__api_resolver.get_workspaces()
        except Exception:  # It will catch all type of exception
            self.log_http_error(message="Unable to fetch workspaces.")
        return workspaces
