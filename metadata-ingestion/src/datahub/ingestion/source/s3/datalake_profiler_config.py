# The data-lake profiler config now lives in the shared ``data_lake_common``
# layer (the DuckDB profiler there consumes it, and all data-lake sources share
# it). Re-exported here to preserve the historical ``s3`` import path.
from datahub.ingestion.source.data_lake_common.datalake_profiler_config import (
    DUCKDB_EXTENSION_DIRECTORY_ENV,
    DataLakeProfilerConfig,
)

__all__ = ["DUCKDB_EXTENSION_DIRECTORY_ENV", "DataLakeProfilerConfig"]
