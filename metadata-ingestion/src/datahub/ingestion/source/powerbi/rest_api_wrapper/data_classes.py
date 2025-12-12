"""
Power BI REST API Wrapper Data Classes

This module now serves as a re-export for backward compatibility.
All data classes have been moved to models.py.
"""

from datahub.ingestion.source.powerbi.models import (
    FIELD_TYPE_MAPPING,
    App,
    AppDashboard,
    AppReport,
    Column,
    Dashboard,
    DatasetKey,
    DataSource,
    Measure,
    MeasureProfile,
    Page,
    PowerBIDataset,
    Report,
    ReportKey,
    ReportType,
    Table,
    Tile,
    User,
    Workspace,
    WorkspaceKey,
)
from datahub.ingestion.source.powerbi.utils import create_powerbi_dataset

__all__ = [
    "FIELD_TYPE_MAPPING",
    "WorkspaceKey",
    "ReportKey",
    "DatasetKey",
    "AppDashboard",
    "AppReport",
    "App",
    "Workspace",
    "DataSource",
    "MeasureProfile",
    "Column",
    "Measure",
    "Table",
    "PowerBIDataset",
    "Page",
    "User",
    "ReportType",
    "Report",
    "Tile",
    "Dashboard",
    "create_powerbi_dataset",
]
