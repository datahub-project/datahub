from unittest.mock import patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.postgres import (
    PostgresAuthMode,
    PostgresConfig,
    PostgresSource,
)


class TestPostgresRDSIAMConfig:
    def test_config_without_rds_iam(self):
        config_dict = {
            "host_port": "localhost:5432",
            "username": "testuser",
            "password": "testpass",
            "database": "testdb",
        }
        config = PostgresConfig.parse_obj(config_dict)

        assert config.auth_mode == PostgresAuthMode.PASSWORD
        assert config.aws_config is not None  # aws_config always has default value
        assert config.aws_config.aws_region is None  # but region is not set

    def test_config_with_rds_iam_valid(self):
        config_dict = {
            "host_port": "test.rds.amazonaws.com:5432",
            "username": "testuser",
            "database": "testdb",
            "auth_mode": "IAM",
            "aws_config": {"aws_region": "us-west-2"},
        }
        config = PostgresConfig.parse_obj(config_dict)

        assert config.auth_mode == PostgresAuthMode.IAM
        assert config.aws_config is not None
        assert config.aws_config.aws_region == "us-west-2"

    def test_config_with_rds_iam_without_explicit_aws_config(self):
        """Test that RDS IAM works with default aws_config (boto3 will infer region from env)."""
        config_dict = {
            "host_port": "test.rds.amazonaws.com:5432",
            "username": "testuser",
            "database": "testdb",
            "auth_mode": "IAM",
        }
        config = PostgresConfig.parse_obj(config_dict)

        assert config.auth_mode == PostgresAuthMode.IAM
        assert config.aws_config is not None
        assert config.aws_config.aws_region is None  # Will be inferred by boto3


class TestPostgresSourceRDSIAM:
    @patch("datahub.ingestion.source.sql.postgres.RDSIAMTokenManager")
    def test_init_without_rds_iam(self, mock_token_manager):
        config_dict = {
            "host_port": "localhost:5432",
            "username": "testuser",
            "password": "testpass",
            "database": "testdb",
        }
        config = PostgresConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")

        source = PostgresSource(config, ctx)

        assert source._rds_iam_token_manager is None
        mock_token_manager.assert_not_called()

    @patch("datahub.ingestion.source.sql.postgres.RDSIAMTokenManager")
    def test_init_with_rds_iam(self, mock_token_manager):
        config_dict = {
            "host_port": "test.rds.amazonaws.com:5432",
            "username": "testuser",
            "database": "testdb",
            "auth_mode": "IAM",
            "aws_config": {"aws_region": "us-west-2"},
        }
        config = PostgresConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")

        source = PostgresSource(config, ctx)

        assert source._rds_iam_token_manager is not None
        mock_token_manager.assert_called_once_with(
            endpoint="test.rds.amazonaws.com",
            username="testuser",
            port=5432,
            aws_config=config.aws_config,
        )

    @patch("datahub.ingestion.source.sql.postgres.RDSIAMTokenManager")
    def test_init_with_rds_iam_custom_port(self, mock_token_manager):
        config_dict = {
            "host_port": "test.rds.amazonaws.com:5433",
            "username": "testuser",
            "database": "testdb",
            "auth_mode": "IAM",
            "aws_config": {"aws_region": "us-west-2"},
        }
        config = PostgresConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")

        PostgresSource(config, ctx)

        mock_token_manager.assert_called_once_with(
            endpoint="test.rds.amazonaws.com",
            username="testuser",
            port=5433,
            aws_config=config.aws_config,
        )

    @patch("datahub.ingestion.source.sql.postgres.RDSIAMTokenManager")
    def test_init_with_rds_iam_no_username(self, mock_token_manager):
        config_dict = {
            "host_port": "test.rds.amazonaws.com:5432",
            "database": "testdb",
            "auth_mode": "IAM",
            "aws_config": {"aws_region": "us-west-2"},
        }
        config = PostgresConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")

        with pytest.raises(ValueError, match="username is required"):
            PostgresSource(config, ctx)

    @patch("datahub.ingestion.source.sql.postgres.RDSIAMTokenManager")
    def test_init_with_rds_iam_invalid_port(self, mock_token_manager):
        config_dict = {
            "host_port": "test.rds.amazonaws.com:invalid",
            "username": "testuser",
            "database": "testdb",
            "auth_mode": "IAM",
            "aws_config": {"aws_region": "us-west-2"},
        }
        config = PostgresConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")

        PostgresSource(config, ctx)

        # Should use default port 5432 (silently, like make_sqlalchemy_uri)
        mock_token_manager.assert_called_once_with(
            endpoint="test.rds.amazonaws.com",
            username="testuser",
            port=5432,
            aws_config=config.aws_config,
        )

    @patch("datahub.ingestion.source.sql.postgres.RDSIAMTokenManager")
    def test_init_with_rds_iam_stores_hostname_and_port(self, mock_token_manager):
        """Test that hostname and port are stored as instance variables."""
        config_dict = {
            "host_port": "test.rds.amazonaws.com:5433",
            "username": "testuser",
            "database": "testdb",
            "auth_mode": "IAM",
            "aws_config": {"aws_region": "us-west-2"},
        }
        config = PostgresConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")

        source = PostgresSource(config, ctx)

        # Verify hostname and port are stored
        assert source._rds_iam_hostname == "test.rds.amazonaws.com"
        assert source._rds_iam_port == 5433
        assert source._rds_iam_token_manager is not None
