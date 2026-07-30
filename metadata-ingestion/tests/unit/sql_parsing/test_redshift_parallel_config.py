"""Parallel-SQL-parsing config wiring for the redshift source."""


def test_redshift_config_parallel_defaults() -> None:
    from datahub.ingestion.source.redshift.config import RedshiftConfig

    cfg = RedshiftConfig(host_port="localhost:5439", database="test")
    assert cfg.use_parallel_sql_parsing is False
    assert cfg.sql_parsing_workers is None


def test_redshift_config_parallel_values() -> None:
    from datahub.ingestion.source.redshift.config import RedshiftConfig

    cfg = RedshiftConfig(
        host_port="localhost:5439",
        database="test",
        use_parallel_sql_parsing=True,
        sql_parsing_workers=4,
    )
    assert cfg.use_parallel_sql_parsing is True
    assert cfg.sql_parsing_workers == 4
