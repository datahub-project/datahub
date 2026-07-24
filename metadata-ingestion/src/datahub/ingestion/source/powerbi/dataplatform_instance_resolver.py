import logging

from datahub.configuration.source_common import PlatformDetail
from datahub.ingestion.source.common.m_query.config import PowerBIPlatformDetail
from datahub.ingestion.source.common.m_query.instance_resolver import (
    AbstractDataPlatformInstanceResolver,
    ServerToPlatformInstanceResolver,
)
from datahub.ingestion.source.powerbi.config import PowerBiDashboardSourceConfig

logger = logging.getLogger(__name__)

# Re-exported so existing importers keep working after the resolver moved into
# the shared M-Query engine.
__all__ = [
    "AbstractDataPlatformInstanceResolver",
    "ServerToPlatformInstanceResolver",
    "ResolvePlatformInstanceFromServerToPlatformInstance",
    "ResolvePlatformInstanceFromDatasetTypeMapping",
    "create_dataplatform_instance_resolver",
]

# The server-based resolver moved into the shared M-Query engine and was renamed;
# keep the original name importable from here for backward compatibility.
ResolvePlatformInstanceFromServerToPlatformInstance = ServerToPlatformInstanceResolver


class ResolvePlatformInstanceFromDatasetTypeMapping(
    AbstractDataPlatformInstanceResolver
):
    # Legacy PowerBI resolver backed by the deprecated ``dataset_type_mapping``
    # recipe field. Retained only for backward compatibility; new recipes should
    # use ``server_to_platform_instance``.
    def __init__(self, config: PowerBiDashboardSourceConfig) -> None:
        self.config = config

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


def create_dataplatform_instance_resolver(
    config: PowerBiDashboardSourceConfig,
) -> AbstractDataPlatformInstanceResolver:
    if config.server_to_platform_instance:
        logger.debug(
            "Creating resolver to resolve platform instance from server_to_platform_instance"
        )
        return ServerToPlatformInstanceResolver(config)

    logger.debug(
        "Creating resolver to resolve platform instance from dataset_type_mapping"
    )
    return ResolvePlatformInstanceFromDatasetTypeMapping(config)
