import logging
from abc import ABC, abstractmethod
from typing import Optional, Union

from datahub.configuration.source_common import PlatformDetail
from datahub.ingestion.source.powerbi.config import (
    PowerBiDashboardSourceConfig,
    PowerBIPlatformDetail,
)

logger = logging.getLogger(__name__)


def _normalize_platform_name_for_lookup(platform_name: str) -> str:
    """
    Normalize platform name for lookup in dataset_type_mapping.

    Removes spaces to handle mismatches like:
    - "Amazon Redshift" (from ODBC) -> "AmazonRedshift" (in SupportedDataPlatform)
    - "Amazon Athena" -> "AmazonAthena"
    - "Google BigQuery" -> "GoogleBigQuery"

    Args:
        platform_name: The platform name to normalize

    Returns:
        Normalized platform name with spaces removed
    """
    return platform_name.replace(" ", "")


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

        # Try exact match first
        platform: Optional[Union[str, PlatformDetail]] = (
            self.config.dataset_type_mapping.get(powerbi_platform_name)
        )

        # If not found, try normalized version (removes spaces)
        # This handles cases like "Amazon Redshift" -> "AmazonRedshift"
        if platform is None:
            normalized_name = _normalize_platform_name_for_lookup(powerbi_platform_name)
            if normalized_name != powerbi_platform_name:
                platform = self.config.dataset_type_mapping.get(normalized_name)
                if platform is not None:
                    logger.debug(
                        f"Found platform '{powerbi_platform_name}' in dataset_type_mapping "
                        f"using normalized key '{normalized_name}'"
                    )

        if platform is None:
            logger.debug(
                f"Platform '{powerbi_platform_name}' (normalized: "
                f"'{_normalize_platform_name_for_lookup(powerbi_platform_name)}') "
                "not found in dataset_type_mapping. Returning empty PlatformDetail."
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
