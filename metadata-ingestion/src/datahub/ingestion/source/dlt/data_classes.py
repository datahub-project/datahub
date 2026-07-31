"""Data classes representing dlt pipeline metadata."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import IntEnum
from typing import List, Literal, Optional

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


# dlt write_disposition values per dlt's resource API. dlt defaults to "append"
# when a resource does not specify one.
DltWriteDisposition = Literal["append", "replace", "merge"]


class DltLoadStatus(IntEnum):
    """Status code from dlt's _dlt_loads table.

    Per dlt's load package contract, a row is committed to _dlt_loads only when
    a load completes; status=0 indicates a successfully loaded package. Other
    values are version-dependent, undocumented across dlt minor versions, and
    treated as UNKNOWN here so the connector keeps a typed contract while
    surfacing them as failed runs to DataHub.
    """

    LOADED = 0
    # Sentinel for any status code that is not a recognized dlt LOADED value.
    UNKNOWN = -1

    @classmethod
    def _missing_(cls, value: object) -> "DltLoadStatus":
        # IntEnum invokes _missing_ when the integer does not match any member.
        # Box unrecognized statuses into UNKNOWN so callers always receive a
        # DltLoadStatus rather than a raw int.
        return cls.UNKNOWN


@dataclass
class DltColumnInfo:
    """A single column in a dlt-managed table."""

    name: str
    # Post-mapped DataHub-friendly type name (see DLT_TYPE_MAP in dlt_client),
    # e.g. "string", "long", "double", "timestamp", "boolean".
    data_type: str
    nullable: bool
    primary_key: bool
    is_dlt_system_column: bool  # True for _dlt_id, _dlt_load_id, etc.


@dataclass
class DltTableInfo:
    """Metadata for a single dlt destination table (top-level or nested child)."""

    table_name: str
    write_disposition: DltWriteDisposition
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
    status: DltLoadStatus
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
