import pytest
from pydantic import ValidationError

from datahub.pgqueue.config import PgQueueAuthMode, PgQueueConnectionConfig


def test_password_mode_requires_secret() -> None:
    with pytest.raises(ValidationError):
        PgQueueConnectionConfig(
            host_port="localhost:5432",
            database="db",
            username="u",
            auth_mode=PgQueueAuthMode.PASSWORD,
        )


def test_iam_requires_colon_port() -> None:
    with pytest.raises(ValidationError):
        PgQueueConnectionConfig(
            host_port="localhost",
            database="db",
            username="u",
            auth_mode=PgQueueAuthMode.AWS_IAM,
        )


def test_password_mode_ok() -> None:
    PgQueueConnectionConfig(
        host_port="localhost:5432",
        database="db",
        username="u",
        password="secret",
        auth_mode=PgQueueAuthMode.PASSWORD,
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("queue_schema", "queue;drop"),
        ("queue_schema", ""),
        ("queue_schema", "has space"),
        ("table_prefix", "prefix--bad"),
        ("table_prefix", "1starts_with_digit"),
    ],
)
def test_rejects_unsafe_sql_identifiers(field: str, value: str) -> None:
    kwargs = {
        "host_port": "localhost:5432",
        "database": "db",
        "username": "u",
        "password": "secret",
        "auth_mode": PgQueueAuthMode.PASSWORD,
        field: value,
    }
    with pytest.raises(ValidationError):
        PgQueueConnectionConfig(**kwargs)
