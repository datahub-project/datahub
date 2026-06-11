from pydantic.fields import Field

from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig


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
