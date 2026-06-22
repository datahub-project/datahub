import os
from typing import Optional

from pydantic import field_validator
from pydantic.fields import Field

from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig

# Env var used to point DuckDB profiling at a pre-staged extension directory.
# The datahub-ingestion image sets this to a build-time-baked location so
# profiling works offline out of the box; operators can also set it themselves.
DUCKDB_EXTENSION_DIRECTORY_ENV = "DATAHUB_DUCKDB_EXTENSION_DIRECTORY"


class DataLakeProfilerConfig(GEProfilingConfig):
    """Profiling config for data-lake sources (S3, GCS, abs, delta-lake).

    Inherits the shared profiler config: the `method` switch (defaulting to
    "sqlalchemy"), sampling, row/size limits, and the query combiner. Data-lake
    profiling historically enabled quantiles, histograms, and distinct-value
    frequencies by default, whereas GEProfilingConfig defaults them off — we
    restore the historical defaults here so existing recipes produce the same
    output after the migration from Spark/Deequ to DuckDB.
    """

    include_field_quantiles: bool = Field(
        default=True,
        description="Whether to profile for the quantiles of numeric columns.",
    )
    include_field_distinct_value_frequencies: bool = Field(
        default=True, description="Whether to profile for distinct value frequencies."
    )
    include_field_histogram: bool = Field(
        default=True,
        description="Whether to profile for the histogram for numeric fields.",
    )

    duckdb_extension_directory: Optional[str] = Field(
        default_factory=lambda: os.environ.get(DUCKDB_EXTENSION_DIRECTORY_ENV) or None,
        description=(
            "Directory DuckDB loads extensions from. DuckDB profiling uses the "
            "`httpfs` extension for remote (s3/gcs/abs) reads, `avro` for Avro "
            "files, and `aws` for instance-profile/role-based S3 credentials; by "
            "default these are downloaded on first use, which fails in air-gapped "
            "environments. Pre-stage the `.duckdb_extension` binaries in this "
            "directory (DuckDB stores them version/platform-keyed under "
            "`<dir>/v<version>/<platform>/`); a correctly staged binary then loads "
            "with no network access. This is not a hard offline switch: if a "
            "required binary is missing or does not match this DuckDB "
            "version/platform, DuckDB still attempts to download it (and the table "
            "is skipped with a warning if that download fails). Defaults to the "
            f"`{DUCKDB_EXTENSION_DIRECTORY_ENV}` environment variable, which the "
            "datahub-ingestion image points at a build-time-baked location."
        ),
    )

    @field_validator("duckdb_extension_directory")
    @classmethod
    def _blank_extension_directory_is_none(cls, v: Optional[str]) -> Optional[str]:
        # Treat an explicit blank/whitespace value (e.g. a set-but-empty env var
        # or `duckdb_extension_directory: ""`) the same as unset, so it is not
        # passed to DuckDB's `SET extension_directory = ''` (which would point at
        # the current working directory rather than meaning "use the default").
        if v is not None and not v.strip():
            return None
        return v
