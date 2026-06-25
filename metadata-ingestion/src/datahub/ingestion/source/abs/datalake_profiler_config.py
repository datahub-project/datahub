# ABS shares the same data-lake profiler config as the other data-lake sources.
# Re-exported from the shared ``data_lake_common`` layer rather than hand-rolling
# a divergent class, so the ABS config stays consistent with s3/gcs/delta-lake.
# (ABS profiling is currently rejected at config-validation time — see
# ``abs/config.py`` ``reject_unsupported_profiling`` — but the config surface must
# still match its siblings for recipe portability and UI form generation.)
from datahub.ingestion.source.data_lake_common.datalake_profiler_config import (
    DataLakeProfilerConfig,
)

__all__ = ["DataLakeProfilerConfig"]
