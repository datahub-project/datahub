"""Parallel-SQL-parsing config wiring for the snowflake source."""


def test_snowflake_queries_extractor_config_parallel_defaults() -> None:
    from datahub.ingestion.source.snowflake.snowflake_queries import (
        SnowflakeQueriesExtractorConfig,
    )

    cfg = SnowflakeQueriesExtractorConfig()
    assert cfg.use_parallel_sql_parsing is False
    assert cfg.sql_parsing_workers is None


def test_snowflake_queries_extractor_config_parallel_values() -> None:
    from datahub.ingestion.source.snowflake.snowflake_queries import (
        SnowflakeQueriesExtractorConfig,
    )

    cfg = SnowflakeQueriesExtractorConfig(
        use_parallel_sql_parsing=True, sql_parsing_workers=4
    )
    assert cfg.use_parallel_sql_parsing is True
    assert cfg.sql_parsing_workers == 4


def test_snowflake_v2_config_parallel_values() -> None:
    from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config

    cfg = SnowflakeV2Config.model_validate(
        {
            "account_id": "test-account",
            "username": "user",
            "password": "pass",
            "use_parallel_sql_parsing": True,
            "sql_parsing_workers": 4,
        }
    )
    assert cfg.use_parallel_sql_parsing is True
    assert cfg.sql_parsing_workers == 4


def test_snowflake_v2_config_forwarding() -> None:
    from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
    from datahub.ingestion.source.snowflake.snowflake_queries import (
        SnowflakeQueriesExtractorConfig,
    )

    main_cfg = SnowflakeV2Config.model_validate(
        {
            "account_id": "test-account",
            "username": "user",
            "password": "pass",
            "use_parallel_sql_parsing": True,
            "sql_parsing_workers": 8,
        }
    )
    extractor_cfg = SnowflakeQueriesExtractorConfig(
        use_parallel_sql_parsing=main_cfg.use_parallel_sql_parsing,
        sql_parsing_workers=main_cfg.sql_parsing_workers,
    )
    assert extractor_cfg.use_parallel_sql_parsing is True
    assert extractor_cfg.sql_parsing_workers == 8
