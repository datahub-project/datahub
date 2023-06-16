import logging
from abc import ABC, abstractmethod
from typing import Union

from datahub.ingestion.source.powerbi.config import (
    PlatformDetail,
    PowerBiDashboardSourceConfig,
)
from datahub.ingestion.source.powerbi.m_query.resolver import DataPlatformTable

logger = logging.getLogger(__name__)


class AbstractDataPlatformInstanceResolver(ABC):
    @abstractmethod
    def get_platform_instance(
        self, dataplatform_table: DataPlatformTable
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
        self, dataplatform_table: DataPlatformTable
    ) -> PlatformDetail:
        platform: Union[str, PlatformDetail] = self.config.dataset_type_mapping[
            dataplatform_table.data_platform_pair.powerbi_data_platform_name
        ]

        if isinstance(platform, PlatformDetail):
            return platform

        return PlatformDetail.parse_obj({})


class ResolvePlatformInstanceFromServerToPlatformInstance(
    BaseAbstractDataPlatformInstanceResolver
):
    def get_platform_instance(
        self, dataplatform_table: DataPlatformTable
    ) -> PlatformDetail:
        return (
            self.config.server_to_platform_instance[
                dataplatform_table.datasource_server
            ]
            if dataplatform_table.datasource_server
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
