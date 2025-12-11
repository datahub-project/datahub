# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.mariadb import MariaDBSource
from datahub.ingestion.source.sql.mysql import MySQLConfig, MySQLSource


def test_platform_correctly_set_mariadb():
    source = MariaDBSource(
        ctx=PipelineContext(run_id="mariadb-source-test"),
        config=MySQLConfig(),
    )
    assert source.platform == "mariadb"


def test_platform_correctly_set_mysql():
    source = MySQLSource(
        ctx=PipelineContext(run_id="mysql-source-test"),
        config=MySQLConfig(),
    )
    assert source.platform == "mysql"
