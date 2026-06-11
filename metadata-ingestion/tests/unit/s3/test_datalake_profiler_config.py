from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
from datahub.ingestion.source.s3.datalake_profiler_config import DataLakeProfilerConfig


def test_is_ge_profiling_config_subclass():
    assert issubclass(DataLakeProfilerConfig, GEProfilingConfig)


def test_preserves_data_lake_defaults():
    cfg = DataLakeProfilerConfig()
    assert cfg.include_field_quantiles is True
    assert cfg.include_field_distinct_value_frequencies is True
    assert cfg.include_field_histogram is True


def test_default_method_is_sqlalchemy():
    assert DataLakeProfilerConfig().method == "sqlalchemy"


def test_inherits_shared_fields():
    cfg = DataLakeProfilerConfig()
    # sampling / limit knobs inherited from GEProfilingConfig
    assert hasattr(cfg, "use_sampling")
    assert hasattr(cfg, "profile_table_row_limit")
    assert hasattr(cfg, "query_combiner_enabled")


def test_table_level_only_does_not_raise_with_default_metrics():
    # Regression: defaults set quantiles/histogram/freq True, but the GE before-validator
    # only raises on EXPLICIT include_field_*=True. profile_table_level_only alone must be fine.
    cfg = DataLakeProfilerConfig(enabled=True, profile_table_level_only=True)
    assert cfg.include_field_quantiles is False  # validator forces field metrics off
    assert cfg.include_field_histogram is False
