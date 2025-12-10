"""
Power BI Configuration Module

This module serves as a re-export module for backward compatibility.
All model definitions have been moved to models.py.
"""

import logging

from datahub.ingestion.source.powerbi.models import (
    Constant,
    DataBricksPlatformDetail,
    DataPlatformPair,
    OwnershipMapping,
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
    PowerBIPlatformDetail,
    PowerBiProfilingConfig,
    SupportedDataPlatform,
)
from datahub.ingestion.source.powerbi.utils import default_for_dataset_type_mapping

logger = logging.getLogger(__name__)

# Re-export for backward compatibility
__all__ = [
    "Constant",
    "DataPlatformPair",
    "PowerBIPlatformDetail",
    "SupportedDataPlatform",
    "PowerBiDashboardSourceReport",
    "DataBricksPlatformDetail",
    "OwnershipMapping",
    "PowerBiProfilingConfig",
    "PowerBiDashboardSourceConfig",
    "default_for_dataset_type_mapping",
]
