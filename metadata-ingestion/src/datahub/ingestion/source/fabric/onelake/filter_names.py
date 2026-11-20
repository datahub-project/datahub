"""The string each Fabric OneLake pattern is matched against.

One definition shared by ingestion (source.py) and the probe's verdict hooks
(config.py), so `probe filter` cannot drift from what a run actually filters.
"""

from datahub.ingestion.source.fabric.onelake.constants import (
    FABRIC_SQL_DEFAULT_SCHEMA,
)


def effective_schema_name(schema_name: str) -> str:
    """Schemas-disabled lakehouses list tables with no schema; the SQL layer
    and ingestion both treat them as dbo."""
    return schema_name or FABRIC_SQL_DEFAULT_SCHEMA


def qualified_filter_name(schema_name: str, name: str) -> str:
    """What table_pattern and view_pattern are matched against."""
    return f"{effective_schema_name(schema_name)}.{name}"
