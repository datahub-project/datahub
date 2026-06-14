import warnings

from datahub.ingestion.source.s3.config import DataLakeSourceConfig


def _minimal(**extra: object) -> DataLakeSourceConfig:
    return DataLakeSourceConfig.parse_obj(
        {"path_specs": [{"include": "s3://bucket/*.parquet"}], **extra}
    )


def test_legacy_spark_driver_memory_does_not_crash() -> None:
    with warnings.catch_warnings(record=True):
        cfg = _minimal(spark_driver_memory="8g")
    assert cfg is not None
    assert not hasattr(cfg, "spark_driver_memory")


def test_legacy_spark_config_does_not_crash() -> None:
    with warnings.catch_warnings(record=True):
        cfg = _minimal(spark_config={"spark.executor.memory": "2g"})
    assert cfg is not None
    assert not hasattr(cfg, "spark_config")


def test_config_without_spark_knobs_still_valid() -> None:
    assert _minimal() is not None
