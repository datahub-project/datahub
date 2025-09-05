"""
DataHub Cloud Router - Multi-tenant Teams integration router.
"""

from .core import MultiTenantRouter
from .db.interface import DatabaseFactory
from .db.models import DatabaseConfig, DataHubInstance, DeploymentType, TenantMapping
from .server import DataHubMultiTenantRouter

__version__ = "0.1.0"
__all__ = [
    "MultiTenantRouter",
    "DataHubMultiTenantRouter",
    "DatabaseFactory",
    "DatabaseConfig",
    "DataHubInstance",
    "DeploymentType",
    "TenantMapping",
]
