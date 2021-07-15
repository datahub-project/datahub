import pytest

from datahub.ingestion.source.oracle import OracleConfig


def test_oracle_config():
    base_config = {
        "username": "user",
        "password": "password",
        "host_port": "host:1521",
    }

    config = OracleConfig.parse_obj(
        {
            **base_config,
            "service_name": "svc01",
        }
    )
    assert (
        config.get_sql_alchemy_url()
        == "oracle+cx_oracle://user:password@host:1521/?service_name=svc01"
    )

    with pytest.raises(ValueError):
        config = OracleConfig.parse_obj(
            {
                **base_config,
                "database": "db",
                "service_name": "svc01",
            }
        )
