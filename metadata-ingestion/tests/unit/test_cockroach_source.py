from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.cockroachdb import (
    CockroachDBConfig,
    CockroachDBSource,
)
from datahub.ingestion.source.sql.postgres import PostgresConfig, PostgresSource


def _base_config():
    return {"username": "user", "password": "password", "host_port": "host:1521"}


def test_platform_correctly_set_cockroachdb():
    source = CockroachDBSource(
        ctx=PipelineContext(run_id="cockroachdb-source-test"),
        config=CockroachDBConfig.parse_obj(_base_config()),
    )
    assert source.platform == "cockroachdb"


def test_platform_correctly_set_postgres():
    source = PostgresSource(
        ctx=PipelineContext(run_id="postgres-source-test"),
        config=PostgresConfig.parse_obj(_base_config()),
    )
    assert source.platform == "postgres"
