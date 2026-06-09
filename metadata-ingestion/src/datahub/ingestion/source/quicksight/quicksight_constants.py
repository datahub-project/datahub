"""Static lookup tables for the QuickSight connector.

These maps are intentionally data-only so they can be unit-tested in isolation
and extended (via PR) as customers surface additional QuickSight data source or
visual types.
"""

from typing import Dict, Optional, Type

from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    ChartTypeClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    StringTypeClass,
)

# --- DataHub subtypes for QuickSight entities ---
SUBTYPE_NAMESPACE = "Namespace"
# Synthetic root that mirrors QuickSight's "Shared folders" left-nav section
# (a UI grouping of all SHARED-type folders, not a real folder/ARN). Only emitted
# when `add_shared_folders_container` is enabled.
SUBTYPE_SHARED_FOLDERS = "Shared Folders"
SUBTYPE_FOLDER = "Folder"
SUBTYPE_DATA_SOURCE = "Data Source"
# QuickSight natively calls these assets "Datasets", so the subtype label is the
# plain "Dataset" (registered in the shared DatasetSubTypes enum for consistency).
SUBTYPE_DATASET = DatasetSubTypes.QUICKSIGHT_DATASET
SUBTYPE_ANALYSIS = "Analysis"
SUBTYPE_DASHBOARD = "Dashboard"


# --- QuickSight DataSource.Type -> DataHub platform ---
# Maps the QuickSight ``DataSource.Type`` to the DataHub data platform used when
# constructing upstream Dataset URNs for lineage stitching. Types without a
# resolvable upstream platform (FILE uploads, most SaaS connectors) are omitted;
# the lineage extractor treats a missing entry as "no upstream stitch".
DATA_SOURCE_TYPE_TO_PLATFORM: Dict[str, str] = {
    "ATHENA": "athena",
    "REDSHIFT": "redshift",
    "SNOWFLAKE": "snowflake",
    "AURORA": "mysql",  # Aurora MySQL
    "AURORA_POSTGRESQL": "postgres",
    "POSTGRESQL": "postgres",
    "MYSQL": "mysql",
    "ORACLE": "oracle",
    "SQLSERVER": "mssql",
    "MARIADB": "mariadb",
    "TERADATA": "teradata",
    "DATABRICKS": "databricks",
    "PRESTO": "presto",
    "STARBURST": "trino",
    "TRINO": "trino",
    "BIGQUERY": "bigquery",
    "S3": "s3",
    "TIMESTREAM": "timestream",
    "AMAZON_ELASTICSEARCH": "elasticsearch",
    "AMAZON_OPENSEARCH": "elasticsearch",
}


# --- QuickSight DataSource.Type -> sqlglot dialect ---
# Used only for CustomSql column-level lineage parsing. ``None`` means the type
# has no SQL surface to parse (S3, FILE, search/time-series engines).
DATA_SOURCE_TYPE_TO_DIALECT: Dict[str, Optional[str]] = {
    "ATHENA": "athena",
    "REDSHIFT": "redshift",
    "SNOWFLAKE": "snowflake",
    "AURORA": "mysql",
    "AURORA_POSTGRESQL": "postgres",
    "POSTGRESQL": "postgres",
    "MYSQL": "mysql",
    "ORACLE": "oracle",
    "SQLSERVER": "tsql",
    "MARIADB": "mysql",
    "TERADATA": "teradata",
    "DATABRICKS": "databricks",
    "PRESTO": "presto",
    "STARBURST": "trino",
    "TRINO": "trino",
    "BIGQUERY": "bigquery",
    "S3": None,
    "TIMESTREAM": None,
    "AMAZON_ELASTICSEARCH": None,
    "AMAZON_OPENSEARCH": None,
}


# --- QuickSight visual type -> DataHub ChartType ---
# QuickSight's visual definitions are keyed by a ``*Visual`` field name. DataHub's
# ``ChartTypeClass`` has no HEATMAP/TREEMAP/MAP members, so grid-shaped visuals
# fall back to TABLE and hierarchical/flow visuals to AREA. Unknown keys fall back
# to ``DEFAULT_CHART_TYPE`` with the raw key preserved in customProperties.
VISUAL_TYPE_TO_CHART_TYPE: Dict[str, str] = {
    "BarChartVisual": ChartTypeClass.BAR,
    "LineChartVisual": ChartTypeClass.LINE,
    "PieChartVisual": ChartTypeClass.PIE,
    "ComboChartVisual": ChartTypeClass.BAR,
    "FunnelChartVisual": ChartTypeClass.BAR,
    "WaterfallVisual": ChartTypeClass.BAR,
    "HistogramVisual": ChartTypeClass.HISTOGRAM,
    "ScatterPlotVisual": ChartTypeClass.SCATTER,
    "BoxPlotVisual": ChartTypeClass.BOX_PLOT,
    "RadarChartVisual": ChartTypeClass.LINE,
    "WordCloudVisual": ChartTypeClass.WORD_CLOUD,
    "TableVisual": ChartTypeClass.TABLE,
    "PivotTableVisual": ChartTypeClass.TABLE,
    "KPIVisual": ChartTypeClass.TABLE,
    "GaugeChartVisual": ChartTypeClass.TABLE,
    "HeatMapVisual": ChartTypeClass.TABLE,
    "GeospatialMapVisual": ChartTypeClass.TABLE,
    "FilledMapVisual": ChartTypeClass.TABLE,
    "TreeMapVisual": ChartTypeClass.AREA,
    "SankeyDiagramVisual": ChartTypeClass.AREA,
    "InsightVisual": ChartTypeClass.TEXT,
    "CustomContentVisual": ChartTypeClass.TEXT,
}

# Visuals that are layout placeholders and should not emit a Chart entity.
SKIP_VISUAL_TYPES = frozenset({"EmptyVisual"})

# Fallback chart type for unrecognized QuickSight visual keys.
DEFAULT_CHART_TYPE = ChartTypeClass.BAR


# --- QuickSight OutputColumn.Type -> DataHub schema field type ---
# QuickSight's ColumnDataType enum is small; unknown/new values fall back to
# NullType (rendered as "unknown" in DataHub) rather than failing ingestion.
QUICKSIGHT_COLUMN_TYPE_TO_FIELD_TYPE: Dict[str, Type] = {
    "STRING": StringTypeClass,
    "INTEGER": NumberTypeClass,
    "DECIMAL": NumberTypeClass,
    "DATETIME": DateTypeClass,
    "BIT": BooleanTypeClass,
    "BOOLEAN": BooleanTypeClass,
    "JSON": RecordTypeClass,
}

DEFAULT_COLUMN_FIELD_TYPE: Type = NullTypeClass
