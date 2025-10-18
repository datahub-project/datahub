from unittest.mock import patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.mysql import MySQLAuthMode, MySQLConfig, MySQLSource


class TestMySQLRDSIAMConfig:
    def test_config_without_rds_iam(self):
        config_dict = {
            "host_port": "localhost:3306",
            "username": "testuser",
            "password": "testpass",
            "database": "testdb",
        }
        config = MySQLConfig.parse_obj(config_dict)

        assert config.auth_mode == MySQLAuthMode.PASSWORD
        assert config.aws_config is not None  # aws_config always has default value
        assert config.aws_config.aws_region is None  # but region is not set

    def test_config_with_rds_iam_valid(self):
        config_dict = {
            "host_port": "test.rds.amazonaws.com:3306",
            "username": "testuser",
            "database": "testdb",
            "auth_mode": "AWS_IAM",
            "aws_config": {"aws_region": "us-west-2"},
        }
        config = MySQLConfig.parse_obj(config_dict)

        assert config.auth_mode == MySQLAuthMode.AWS_IAM
        assert config.aws_config is not None
        assert config.aws_config.aws_region == "us-west-2"

    def test_config_with_rds_iam_without_explicit_aws_config(self):
        """Test that RDS IAM works with default aws_config (boto3 will infer region from env)."""
        config_dict = {
            "host_port": "test.rds.amazonaws.com:3306",
            "username": "testuser",
            "database": "testdb",
            "auth_mode": "AWS_IAM",
        }
        config = MySQLConfig.parse_obj(config_dict)

        assert config.auth_mode == MySQLAuthMode.AWS_IAM
        assert config.aws_config is not None
        assert config.aws_config.aws_region is None  # Will be inferred by boto3


class TestMySQLSourceRDSIAM:
    @patch("datahub.ingestion.source.sql.mysql.RDSIAMTokenManager")
    def test_init_without_rds_iam(self, mock_token_manager):
        config_dict = {
            "host_port": "localhost:3306",
            "username": "testuser",
            "password": "testpass",
            "database": "testdb",
        }
        config = MySQLConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")

        source = MySQLSource(config, ctx)

        assert source._rds_iam_token_manager is None
        mock_token_manager.assert_not_called()

    @patch("datahub.ingestion.source.sql.mysql.RDSIAMTokenManager")
    def test_init_with_rds_iam(self, mock_token_manager):
        config_dict = {
            "host_port": "test.rds.amazonaws.com:3306",
            "username": "testuser",
            "database": "testdb",
            "auth_mode": "AWS_IAM",
            "aws_config": {"aws_region": "us-west-2"},
        }
        config = MySQLConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")

        source = MySQLSource(config, ctx)

        assert source._rds_iam_token_manager is not None
        mock_token_manager.assert_called_once_with(
            endpoint="test.rds.amazonaws.com",
            username="testuser",
            port=3306,
            aws_config=config.aws_config,
        )

    @patch("datahub.ingestion.source.sql.mysql.RDSIAMTokenManager")
    def test_init_with_rds_iam_custom_port(self, mock_token_manager):
        config_dict = {
            "host_port": "test.rds.amazonaws.com:3307",
            "username": "testuser",
            "database": "testdb",
            "auth_mode": "AWS_IAM",
            "aws_config": {"aws_region": "us-west-2"},
        }
        config = MySQLConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")

        MySQLSource(config, ctx)

        mock_token_manager.assert_called_once_with(
            endpoint="test.rds.amazonaws.com",
            username="testuser",
            port=3307,
            aws_config=config.aws_config,
        )

    @patch("datahub.ingestion.source.sql.mysql.RDSIAMTokenManager")
    def test_init_with_rds_iam_no_username(self, mock_token_manager):
        config_dict = {
            "host_port": "test.rds.amazonaws.com:3306",
            "database": "testdb",
            "auth_mode": "AWS_IAM",
            "aws_config": {"aws_region": "us-west-2"},
        }
        config = MySQLConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")

        with pytest.raises(ValueError, match="username is required"):
            MySQLSource(config, ctx)

    @patch("datahub.ingestion.source.sql.mysql.RDSIAMTokenManager")
    def test_init_with_rds_iam_invalid_port(self, mock_token_manager):
        config_dict = {
            "host_port": "test.rds.amazonaws.com:invalid",
            "username": "testuser",
            "database": "testdb",
            "auth_mode": "AWS_IAM",
            "aws_config": {"aws_region": "us-west-2"},
        }
        config = MySQLConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")

        MySQLSource(config, ctx)

        # Should use default port 3306 (silently, like make_sqlalchemy_uri)
        mock_token_manager.assert_called_once_with(
            endpoint="test.rds.amazonaws.com",
            username="testuser",
            port=3306,
            aws_config=config.aws_config,
        )
