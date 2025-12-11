# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
        config=CockroachDBConfig.model_validate(_base_config()),
    )
    assert source.platform == "cockroachdb"


def test_platform_correctly_set_postgres():
    source = PostgresSource(
        ctx=PipelineContext(run_id="postgres-source-test"),
        config=PostgresConfig.model_validate(_base_config()),
    )
    assert source.platform == "postgres"
