import logging
from abc import ABC, abstractmethod

from datahub.configuration.source_common import PlatformDetail
from datahub.ingestion.source.common.m_query.config import PowerBIPlatformDetail
from datahub.ingestion.source.common.m_query.interfaces import MQueryLineageConfig

logger = logging.getLogger(__name__)


class AbstractDataPlatformInstanceResolver(ABC):
    @abstractmethod
    def get_platform_instance(
        self, data_platform_detail: PowerBIPlatformDetail
    ) -> PlatformDetail:
        pass


class ServerToPlatformInstanceResolver(AbstractDataPlatformInstanceResolver):
    # Resolves the DataHub platform instance/env for an upstream table from the
    # server named in the M-Query, using the connector's
    # ``server_to_platform_instance`` mapping.
    config: MQueryLineageConfig

    def __init__(self, config: MQueryLineageConfig) -> None:
        self.config = config

    def get_platform_instance(
        self, data_platform_detail: PowerBIPlatformDetail
    ) -> PlatformDetail:
        server = data_platform_detail.data_platform_server
        if not server:
            return PlatformDetail.model_validate({})

        mapping = self.config.server_to_platform_instance
        if server in mapping:
            return mapping[server]

        # Oracle TNS aliases are case-insensitive in the source system but recipe
        # keys are case-sensitive strings; fall back to case-insensitive match.
        server_lower = server.lower()
        for key, value in mapping.items():
            if key.lower() == server_lower:
                return value

        return PlatformDetail.model_validate({})
