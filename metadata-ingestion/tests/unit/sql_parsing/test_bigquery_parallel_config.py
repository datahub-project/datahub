"""Parallel-SQL-parsing config wiring for the bigquery source."""


def test_bigquery_queries_extractor_config_parallel_defaults() -> None:
    from datahub.ingestion.source.bigquery_v2.queries_extractor import (
        BigQueryQueriesExtractorConfig,
    )

    cfg = BigQueryQueriesExtractorConfig()
    assert cfg.use_parallel_sql_parsing is False
    assert cfg.sql_parsing_workers is None


def test_bigquery_queries_extractor_config_parallel_values() -> None:
    from datahub.ingestion.source.bigquery_v2.queries_extractor import (
        BigQueryQueriesExtractorConfig,
    )

    cfg = BigQueryQueriesExtractorConfig(
        use_parallel_sql_parsing=True, sql_parsing_workers=4
    )
    assert cfg.use_parallel_sql_parsing is True
    assert cfg.sql_parsing_workers == 4


def test_bigquery_v2_config_parallel_values() -> None:
    from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config

    cfg = BigQueryV2Config.model_validate(
        {"use_parallel_sql_parsing": True, "sql_parsing_workers": 4}
    )
    assert cfg.use_parallel_sql_parsing is True
    assert cfg.sql_parsing_workers == 4


def test_bigquery_v2_config_forwarding() -> None:
    from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
    from datahub.ingestion.source.bigquery_v2.queries_extractor import (
        BigQueryQueriesExtractorConfig,
    )

    main_cfg = BigQueryV2Config.model_validate(
        {"use_parallel_sql_parsing": True, "sql_parsing_workers": 6}
    )
    extractor_cfg = BigQueryQueriesExtractorConfig(
        use_parallel_sql_parsing=main_cfg.use_parallel_sql_parsing,
        sql_parsing_workers=main_cfg.sql_parsing_workers,
    )
    assert extractor_cfg.use_parallel_sql_parsing is True
    assert extractor_cfg.sql_parsing_workers == 6
