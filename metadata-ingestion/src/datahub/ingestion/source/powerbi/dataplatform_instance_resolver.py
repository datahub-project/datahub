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
        return (
            self.config.server_to_platform_instance[
                data_platform_detail.data_platform_server
            ]
            if data_platform_detail.data_platform_server
            in self.config.server_to_platform_instance
            else PlatformDetail.model_validate({})
        )


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
