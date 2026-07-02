"""Parallel-SQL-parsing config wiring for the unity catalog source."""


def test_unity_catalog_config_parallel_defaults() -> None:
    from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig

    cfg = UnityCatalogSourceConfig(
        workspace_url="https://adb-123.azuredatabricks.net",
        token="dapi-test-token",
    )
    assert cfg.use_parallel_sql_parsing is False
    assert cfg.sql_parsing_workers is None


def test_unity_catalog_config_parallel_values() -> None:
    from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig

    cfg = UnityCatalogSourceConfig(
        workspace_url="https://adb-123.azuredatabricks.net",
        token="dapi-test-token",
        use_parallel_sql_parsing=True,
        sql_parsing_workers=4,
    )
    assert cfg.use_parallel_sql_parsing is True
    assert cfg.sql_parsing_workers == 4
