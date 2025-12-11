# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.ingestion.source.sql.druid import DruidConfig


def test_druid_uri():
    config = DruidConfig.model_validate({"host_port": "localhost:8082"})

    assert config.get_sql_alchemy_url() == "druid://localhost:8082/druid/v2/sql/"


def test_druid_get_identifier():
    config = DruidConfig.model_validate({"host_port": "localhost:8082"})

    assert config.get_identifier("schema", "table") == "table"
