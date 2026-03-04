"""Data classes representing dlt pipeline metadata."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

# dlt system column names — these are injected by dlt and should not be
# emitted as DataHub schema fields.
DLT_SYSTEM_COLUMNS = frozenset(
    {
        "_dlt_id",
        "_dlt_load_id",
        "_dlt_parent_id",
        "_dlt_list_idx",
        "_dlt_root_id",
    }
)


@dataclass
class DltColumnInfo:
    """A single column in a dlt-managed table."""

    name: str
    data_type: str  # dlt type: "text", "bigint", "bool", "timestamp", "double", etc.
    nullable: bool
    primary_key: bool
    is_dlt_system_column: bool  # True for _dlt_id, _dlt_load_id, etc.


@dataclass
class DltTableInfo:
    """Metadata for a single dlt destination table (top-level or nested child)."""

    table_name: str
    write_disposition: str  # "append", "replace", or "merge"
    parent_table: Optional[
        str
    ]  # Non-None for auto-unnested child tables (e.g., orders__items)
    columns: List[DltColumnInfo]
    resource_name: Optional[str]  # The @dlt.resource name that produced this table


@dataclass
class DltSchemaInfo:
    """All tables in a single dlt schema (one pipeline may have multiple schemas)."""

    schema_name: str
    version: int
    version_hash: str
    tables: List[DltTableInfo]


@dataclass
class DltLoadInfo:
    """A single completed load from the _dlt_loads destination table."""

    load_id: str  # Unique float-timestamp string
    schema_name: str
    status: int  # 0 = complete; anything else = in-progress/failed
    inserted_at: datetime
    schema_version_hash: str


@dataclass
class DltPipelineInfo:
    """All metadata for a single dlt pipeline, gathered from state files and/or the SDK."""

    pipeline_name: str
    destination: str  # e.g. "postgres", "bigquery", "snowflake", "duckdb"
    dataset_name: str  # The destination dataset/schema name
    working_dir: str  # Absolute path to local state directory
    pipelines_dir: str  # Parent directory (may contain multiple pipelines)
    schemas: List[DltSchemaInfo]
    last_load_info: Optional[DltLoadInfo]
    run_history: List[DltLoadInfo] = field(default_factory=list)
