import logging
from abc import ABC, abstractmethod

from datahub.configuration.source_common import PlatformDetail
from datahub.ingestion.source.powerbi.config import (
    PowerBiDashboardSourceConfig,
    PowerBIPlatformDetail,
)

logger = logging.getLogger(__name__)


class AbstractDataPlatformInstanceResolver(ABC):
    @abstractmethod
    def get_platform_instance(
        self, data_platform_detail: PowerBIPlatformDetail
    ) -> PlatformDetail:
        pass


class BaseAbstractDataPlatformInstanceResolver(
    AbstractDataPlatformInstanceResolver, ABC
):
    config: PowerBiDashboardSourceConfig

    def __init__(self, config):
        self.config = config


class ResolvePlatformInstanceFromDatasetTypeMapping(
    BaseAbstractDataPlatformInstanceResolver
):
    def get_platform_instance(
        self, data_platform_detail: PowerBIPlatformDetail
    ) -> PlatformDetail:
        powerbi_platform_name = (
            data_platform_detail.data_platform_pair.powerbi_data_platform_name
        )

        platform = self.config.get_from_dataset_type_mapping(powerbi_platform_name)

        if platform is None:
            logger.debug(
                f"Platform '{powerbi_platform_name}' not found in dataset_type_mapping. "
                "Returning empty PlatformDetail."
            )
            return PlatformDetail.model_validate({})

        if isinstance(platform, PlatformDetail):
            return platform

        return PlatformDetail.model_validate({})


class ResolvePlatformInstanceFromServerToPlatformInstance(
    BaseAbstractDataPlatformInstanceResolver
):
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


def create_dataplatform_instance_resolver(
    config: PowerBiDashboardSourceConfig,
) -> AbstractDataPlatformInstanceResolver:
    if config.server_to_platform_instance:
        logger.debug(
            "Creating resolver to resolve platform instance from server_to_platform_instance"
        )
        return ResolvePlatformInstanceFromServerToPlatformInstance(config)

    logger.debug(
        "Creating resolver to resolve platform instance from dataset_type_mapping"
    )
    return ResolvePlatformInstanceFromDatasetTypeMapping(config)
