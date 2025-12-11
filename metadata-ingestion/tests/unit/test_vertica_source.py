# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.ingestion.source.sql.vertica import VerticaConfig


def test_vertica_uri_https():
    config = VerticaConfig.model_validate(
        {
            "username": "user",
            "password": "password",
            "host_port": "host:5433",
            "database": "db",
        }
    )
    assert (
        config.get_sql_alchemy_url()
        == "vertica+vertica_python://user:password@host:5433/db"
    )
