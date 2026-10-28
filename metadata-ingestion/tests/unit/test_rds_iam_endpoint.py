"""Where the IAM token is signed for when the recipe's URL cannot say."""

from typing import Any

from datahub.ingestion.source.sql.postgres.source import PostgresConfig


def _postgres(**kw: Any) -> PostgresConfig:
    return PostgresConfig.model_validate(
        {"host_port": "db.rds.amazonaws.com:5432", "username": "u", **kw}
    )


def test_an_unparseable_uri_falls_back_to_host_port_and_the_config_user() -> None:
    config = _postgres(sqlalchemy_uri="not a url")
    assert config.rds_iam_endpoint() == ("db.rds.amazonaws.com", 5432)
    assert config.rds_iam_username() == "u"


def test_a_uri_without_a_host_falls_back_to_host_port() -> None:
    config = _postgres(
        host_port="db.rds.amazonaws.com", sqlalchemy_uri="postgresql:///db"
    )
    # The dialect default port fills in when host_port carries none.
    assert config.rds_iam_endpoint() == ("db.rds.amazonaws.com", 5432)
