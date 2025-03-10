import logging
from abc import ABC, abstractmethod
from typing import List, Optional

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.fivetran.config import (
    FivetranSourceConfig,
    FivetranSourceReport,
)
from datahub.ingestion.source.fivetran.data_classes import Connector
from datahub.ingestion.source.fivetran.fivetran_constants import FivetranMode

logger = logging.getLogger(__name__)


class FivetranAccessInterface(ABC):
    """Abstract interface for accessing Fivetran data."""

    @property
    @abstractmethod
    def fivetran_log_database(self) -> Optional[str]:
        """Get the Fivetran log database name."""
        pass

    @abstractmethod
    def get_user_email(self, user_id: str) -> Optional[str]:
        """Get a user's email from their user ID."""
        pass

    @abstractmethod
    def get_allowed_connectors_list(
        self,
        connector_patterns: AllowDenyPattern,
        destination_patterns: AllowDenyPattern,
        report: FivetranSourceReport,
        syncs_interval: int,
    ) -> List[Connector]:
        """Get a list of connectors filtered by the provided patterns."""
        pass


def create_fivetran_access(config: FivetranSourceConfig) -> FivetranAccessInterface:
    """
    Create the appropriate Fivetran access implementation based on the configuration.

    Args:
        config: The Fivetran source config

    Returns:
        An implementation of FivetranAccessInterface
    """
    from datahub.ingestion.source.fivetran.fivetran_api_client import FivetranAPIClient
    from datahub.ingestion.source.fivetran.fivetran_log_api import FivetranLogAPI
    from datahub.ingestion.source.fivetran.fivetran_standard_api import (
        FivetranStandardAPI,
    )

    mode = getattr(config, "fivetran_mode", FivetranMode.AUTO)

    # Explicit enterprise mode selection
    if mode == FivetranMode.ENTERPRISE:
        if (
            not hasattr(config, "fivetran_log_config")
            or config.fivetran_log_config is None
        ):
            raise ValueError("Enterprise mode requires fivetran_log_config")
        logger.info("Using enterprise mode with log tables")
        return FivetranLogAPI(config.fivetran_log_config)

    # Explicit standard mode selection
    if mode == FivetranMode.STANDARD:
        if not hasattr(config, "api_config") or config.api_config is None:
            raise ValueError("Standard mode requires api_config")
        logger.info("Using standard mode with REST API")
        return FivetranStandardAPI(FivetranAPIClient(config.api_config))

    # Auto-detect mode
    if (
        hasattr(config, "fivetran_log_config")
        and config.fivetran_log_config is not None
    ):
        logger.info("Auto-detected enterprise mode based on provided log config")
        return FivetranLogAPI(config.fivetran_log_config)

    if hasattr(config, "api_config") and config.api_config is not None:
        logger.info("Auto-detected standard mode based on provided API config")
        return FivetranStandardAPI(FivetranAPIClient(config.api_config))

    raise ValueError(
        "Cannot determine Fivetran mode from configuration. "
        "Please provide either fivetran_log_config (for enterprise) "
        "or api_config (for standard)."
    )
