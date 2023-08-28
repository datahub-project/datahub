import logging
from abc import ABC, abstractmethod
from typing import Union

from datahub.ingestion.source.powerbi.config import (
    PlatformDetail,
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
        platform: Union[str, PlatformDetail] = self.config.dataset_type_mapping[
            data_platform_detail.data_platform_pair.powerbi_data_platform_name
        ]

        if isinstance(platform, PlatformDetail):
            return platform

        return PlatformDetail.parse_obj({})


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
            else PlatformDetail.parse_obj({})
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
