# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import unittest.mock

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.oracle import OracleConfig, OracleSource


def test_oracle_config():
    base_config = {
        "username": "user",
        "password": "password",
        "host_port": "host:1521",
    }

    config = OracleConfig.model_validate(
        {
            **base_config,
            "service_name": "svc01",
        }
    )
    assert (
        config.get_sql_alchemy_url()
        == "oracle://user:password@host:1521/?service_name=svc01"
    )

    with pytest.raises(ValueError):
        config = OracleConfig.model_validate(
            {
                **base_config,
                "database": "db",
                "service_name": "svc01",
            }
        )

    with unittest.mock.patch(
        "datahub.ingestion.source.sql.sql_common.SQLAlchemySource.get_workunits"
    ):
        OracleSource.create(
            {
                **base_config,
                "service_name": "svc01",
            },
            PipelineContext("test-oracle-config"),
        ).get_workunits()
