"""Unit tests for :func:`datahub.pgqueue.connection.build_psycopg2_connect_kwargs`."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from datahub.pgqueue.config import PgQueueAuthMode, PgQueueConnectionConfig
from datahub.pgqueue.connection import build_psycopg2_connect_kwargs


def _make_config(**overrides: object) -> PgQueueConnectionConfig:
    defaults: dict[str, object] = dict(
        host_port="pg.example.com:5432",
        database="datahub",
        username="dh_user",
        password="s3cret",
    )
    defaults.update(overrides)
    return PgQueueConnectionConfig(**defaults)


class TestPasswordMode:
    def test_basic_kwargs(self) -> None:
        cfg = _make_config()
        kwargs = build_psycopg2_connect_kwargs(cfg)

        assert kwargs["host"] == "pg.example.com"
        assert kwargs["port"] == 5432
        assert kwargs["dbname"] == "datahub"
        assert kwargs["user"] == "dh_user"
        assert kwargs["password"] == "s3cret"
        assert kwargs["sslmode"] == "prefer"
        assert kwargs["connect_timeout"] == 60

    def test_sslmode_propagated(self) -> None:
        cfg = _make_config(sslmode="require")
        kwargs = build_psycopg2_connect_kwargs(cfg)
        assert kwargs["sslmode"] == "require"

    def test_custom_connect_timeout(self) -> None:
        cfg = _make_config(connect_timeout_seconds=10)
        kwargs = build_psycopg2_connect_kwargs(cfg)
        assert kwargs["connect_timeout"] == 10


class TestPortParsing:
    def test_missing_port_uses_default(self) -> None:
        cfg = _make_config(host_port="pg.example.com")
        kwargs = build_psycopg2_connect_kwargs(cfg)
        assert kwargs["host"] == "pg.example.com"
        assert kwargs["port"] == 5432

    def test_non_numeric_port_rejected_by_config(self) -> None:
        with pytest.raises(Exception, match="port"):
            _make_config(host_port="pg.example.com:abc")


class TestAwsIamMode:
    @patch("datahub.ingestion.source.aws.aws_common.RDSIAMTokenManager")
    def test_builds_iam_token_and_requires_ssl(self, token_mgr_cls: MagicMock) -> None:
        token_mgr_cls.return_value.get_token.return_value = "iam-token-xyz"
        cfg = PgQueueConnectionConfig(
            host_port="mydb.us-east-1.rds.amazonaws.com:5432",
            database="datahub",
            username="dh_user",
            auth_mode=PgQueueAuthMode.AWS_IAM,
            aws_config={"aws_region": "us-east-1"},
        )

        kwargs = build_psycopg2_connect_kwargs(cfg)

        assert kwargs["password"] == "iam-token-xyz"
        assert kwargs["sslmode"] == "require"
        token_mgr_cls.assert_called_once()
