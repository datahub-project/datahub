import json
import os
from unittest.mock import MagicMock, patch

import boto3
import pytest
from moto import mock_iam, mock_lambda, mock_sts

from datahub.ingestion.source.aws.aws_common import (
    AwsConnectionConfig,
    AwsEnvironment,
    detect_aws_environment,
    get_current_identity,
    get_instance_metadata_token,
    get_instance_role_arn,
    is_running_on_ec2,
)


@pytest.fixture
def mock_disable_ec2_metadata():
    """Disable EC2 metadata detection"""
    with patch("requests.put") as mock_put:
        mock_put.return_value.status_code = 404
        yield mock_put


@pytest.fixture
def mock_aws_config():
    return AwsConnectionConfig(
        aws_access_key_id="test-key",
        aws_secret_access_key="test-secret",
        aws_region="us-east-1",
    )


class TestAwsCommon:
    def test_environment_detection_no_environment(self, mock_disable_ec2_metadata):
        """Test environment detection when no AWS environment is present"""
        with patch.dict(os.environ, {}, clear=True):
            assert detect_aws_environment() == AwsEnvironment.UNKNOWN

    def test_environment_detection_lambda(self, mock_disable_ec2_metadata):
        """Test Lambda environment detection"""
        with patch.dict(os.environ, {"AWS_LAMBDA_FUNCTION_NAME": "test-function"}):
            assert detect_aws_environment() == AwsEnvironment.LAMBDA

    def test_environment_detection_lambda_cloudformation(
        self, mock_disable_ec2_metadata
    ):
        """Test CloudFormation Lambda environment detection"""
        with patch.dict(
            os.environ,
            {
                "AWS_LAMBDA_FUNCTION_NAME": "test-function",
                "AWS_EXECUTION_ENV": "CloudFormation.xxx",
            },
        ):
            assert detect_aws_environment() == AwsEnvironment.CLOUD_FORMATION

    def test_environment_detection_eks(self, mock_disable_ec2_metadata):
        """Test EKS environment detection"""
        with patch.dict(
            os.environ,
            {
                "AWS_WEB_IDENTITY_TOKEN_FILE": "/var/run/secrets/token",
                "AWS_ROLE_ARN": "arn:aws:iam::123456789012:role/test-role",
            },
        ):
            assert detect_aws_environment() == AwsEnvironment.EKS

    def test_environment_detection_app_runner(self, mock_disable_ec2_metadata):
        """Test App Runner environment detection"""
        with patch.dict(os.environ, {"AWS_APP_RUNNER_SERVICE_ID": "service-id"}):
            assert detect_aws_environment() == AwsEnvironment.APP_RUNNER

    def test_environment_detection_ecs(self, mock_disable_ec2_metadata):
        """Test ECS environment detection"""
        with patch.dict(
            os.environ, {"ECS_CONTAINER_METADATA_URI_V4": "http://169.254.170.2/v4"}
        ):
            assert detect_aws_environment() == AwsEnvironment.ECS

    def test_environment_detection_beanstalk(self, mock_disable_ec2_metadata):
        """Test Elastic Beanstalk environment detection"""
        with patch.dict(os.environ, {"ELASTIC_BEANSTALK_ENVIRONMENT_NAME": "my-env"}):
            assert detect_aws_environment() == AwsEnvironment.BEANSTALK

    @patch("requests.put")
    def test_ec2_metadata_token(self, mock_put):
        """Test EC2 metadata token retrieval"""
        mock_put.return_value.status_code = 200
        mock_put.return_value.text = "token123"

        token = get_instance_metadata_token()
        assert token == "token123"

        mock_put.assert_called_once_with(
            "http://169.254.169.254/latest/api/token",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
            timeout=1,
        )

    @patch("requests.put")
    def test_ec2_metadata_token_failure(self, mock_put):
        """Test EC2 metadata token failure case"""
        mock_put.return_value.status_code = 404

        token = get_instance_metadata_token()
        assert token is None

    @patch("requests.get")
    @patch("requests.put")
    def test_is_running_on_ec2(self, mock_put, mock_get):
        """Test EC2 instance detection with IMDSv2"""
        # Explicitly mock EC2 metadata responses
        mock_put.return_value.status_code = 200
        mock_put.return_value.text = "token123"
        mock_get.return_value.status_code = 200

        assert is_running_on_ec2() is True

        mock_put.assert_called_once_with(
            "http://169.254.169.254/latest/api/token",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
            timeout=1,
        )
        mock_get.assert_called_once_with(
            "http://169.254.169.254/latest/meta-data/instance-id",
            headers={"X-aws-ec2-metadata-token": "token123"},
            timeout=1,
        )

    @patch("requests.get")
    @patch("requests.put")
    def test_is_running_on_ec2_failure(self, mock_put, mock_get):
        """Test EC2 instance detection failure"""
        mock_put.return_value.status_code = 404
        assert is_running_on_ec2() is False

        mock_put.return_value.status_code = 200
        mock_put.return_value.text = "token123"
        mock_get.return_value.status_code = 404
        assert is_running_on_ec2() is False

    @mock_sts
    @mock_lambda
    @mock_iam
    def test_get_current_identity_lambda(self):
        """Test getting identity in Lambda environment"""
        with patch.dict(
            os.environ,
            {
                "AWS_LAMBDA_FUNCTION_NAME": "test-function",
                "AWS_DEFAULT_REGION": "us-east-1",
            },
        ):
            # Create IAM role first with proper trust policy
            iam_client = boto3.client("iam", region_name="us-east-1")
            trust_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "lambda.amazonaws.com"},
                        "Action": "sts:AssumeRole",
                    }
                ],
            }
            iam_client.create_role(
                RoleName="test-role", AssumeRolePolicyDocument=json.dumps(trust_policy)
            )

            lambda_client = boto3.client("lambda", region_name="us-east-1")
            lambda_client.create_function(
                FunctionName="test-function",
                Runtime="python3.8",
                Role="arn:aws:iam::123456789012:role/test-role",
                Handler="index.handler",
                Code={"ZipFile": b"def handler(event, context): pass"},
            )

            role_arn, source = get_current_identity()
            assert source == "lambda.amazonaws.com"
            assert role_arn == "arn:aws:iam::123456789012:role/test-role"

    @patch("requests.get")
    @patch("requests.put")
    @mock_sts
    def test_get_instance_role_arn_success(self, mock_put, mock_get):
        """Test getting EC2 instance role ARN"""
        mock_put.return_value.status_code = 200
        mock_put.return_value.text = "token123"
        mock_get.return_value.status_code = 200
        mock_get.return_value.text = "test-role"

        with patch("boto3.client") as mock_boto:
            mock_sts = MagicMock()
            mock_sts.get_caller_identity.return_value = {
                "Arn": "arn:aws:sts::123456789012:assumed-role/test-role/instance"
            }
            mock_boto.return_value = mock_sts

            role_arn = get_instance_role_arn()
            assert (
                role_arn == "arn:aws:sts::123456789012:assumed-role/test-role/instance"
            )

    @mock_sts
    def test_aws_connection_config_basic(self, mock_aws_config):
        """Test basic AWS connection configuration"""
        session = mock_aws_config.get_session()
        creds = session.get_credentials()
        assert creds.access_key == "test-key"
        assert creds.secret_key == "test-secret"

    @mock_sts
    def test_aws_connection_config_with_session_token(self):
        """Test AWS connection with session token"""
        config = AwsConnectionConfig(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            aws_session_token="test-token",
            aws_region="us-east-1",
        )

        session = config.get_session()
        creds = session.get_credentials()
        assert creds is not None
        assert creds.token == "test-token"

    @mock_sts
    def test_aws_connection_config_role_assumption(self):
        """Test AWS connection with role assumption"""
        config = AwsConnectionConfig(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            aws_region="us-east-1",
            aws_role="arn:aws:iam::123456789012:role/test-role",
        )

        with patch(
            "datahub.ingestion.source.aws.aws_common.get_current_identity"
        ) as mock_identity:
            mock_identity.return_value = (None, None)
            session = config.get_session()
            creds = session.get_credentials()
            assert creds is not None

    @mock_sts
    def test_aws_connection_config_skip_role_assumption(self):
        """Test AWS connection skipping role assumption when already in role"""
        config = AwsConnectionConfig(
            aws_region="us-east-1",
            aws_role="arn:aws:iam::123456789012:role/current-role",
        )

        with patch(
            "datahub.ingestion.source.aws.aws_common.get_current_identity"
        ) as mock_identity:
            mock_identity.return_value = (
                "arn:aws:iam::123456789012:role/current-role",
                "ec2.amazonaws.com",
            )
            session = config.get_session()
            assert session is not None

    @mock_sts
    def test_aws_connection_config_multiple_roles(self):
        """Test AWS connection with multiple role assumption"""
        config = AwsConnectionConfig(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            aws_region="us-east-1",
            aws_role=[
                "arn:aws:iam::123456789012:role/role1",
                "arn:aws:iam::123456789012:role/role2",
            ],
        )

        with patch(
            "datahub.ingestion.source.aws.aws_common.get_current_identity"
        ) as mock_identity:
            mock_identity.return_value = (None, None)
            session = config.get_session()
            assert session is not None

    def test_aws_connection_config_validation_error(self):
        """Test AWS connection validation"""
        with patch.dict(
            "os.environ",
            {
                "AWS_ACCESS_KEY_ID": "test-key",
                # Deliberately missing AWS_SECRET_ACCESS_KEY
                "AWS_DEFAULT_REGION": "us-east-1",
            },
            clear=True,
        ):
            config = AwsConnectionConfig()  # Let it pick up from environment
            session = config.get_session()
            with pytest.raises(
                Exception,
                match="Partial credentials found in env, missing: AWS_SECRET_ACCESS_KEY",
            ):
                session.get_credentials()

    @pytest.mark.parametrize(
        "env_vars,expected_environment",
        [
            ({}, AwsEnvironment.UNKNOWN),
            ({"AWS_LAMBDA_FUNCTION_NAME": "test"}, AwsEnvironment.LAMBDA),
            (
                {
                    "AWS_LAMBDA_FUNCTION_NAME": "test",
                    "AWS_EXECUTION_ENV": "CloudFormation",
                },
                AwsEnvironment.CLOUD_FORMATION,
            ),
            (
                {
                    "AWS_WEB_IDENTITY_TOKEN_FILE": "/token",
                    "AWS_ROLE_ARN": "arn:aws:iam::123:role/test",
                },
                AwsEnvironment.EKS,
            ),
            ({"AWS_APP_RUNNER_SERVICE_ID": "service-123"}, AwsEnvironment.APP_RUNNER),
            (
                {"ECS_CONTAINER_METADATA_URI_V4": "http://169.254.170.2"},
                AwsEnvironment.ECS,
            ),
            (
                {"ELASTIC_BEANSTALK_ENVIRONMENT_NAME": "my-env"},
                AwsEnvironment.BEANSTALK,
            ),
        ],
    )
    def test_environment_detection_parametrized(
        self, mock_disable_ec2_metadata, env_vars, expected_environment
    ):
        """Parametrized test for environment detection with different configurations"""
        with patch.dict(os.environ, env_vars, clear=True):
            assert detect_aws_environment() == expected_environment

    @mock_sts
    def test_role_assumption_credentials_cached_across_sessions(self):
        """
        Test that assumed role credentials are cached and reused across multiple
        get_session() calls within the same AwsConnectionConfig instance.

        This is the main test for the bug fix where the second/third get_session()
        calls were using base credentials instead of assumed role credentials.
        """
        config = AwsConnectionConfig(
            aws_region="us-east-1",
            aws_role="arn:aws:iam::339713033063:role/dh-executor-s3-access-role",
        )

        with (
            patch(
                "datahub.ingestion.source.aws.aws_common.get_current_identity"
            ) as mock_identity,
            patch(
                "datahub.ingestion.source.aws.aws_common.assume_role"
            ) as mock_assume_role,
            patch(
                "datahub.ingestion.source.aws.aws_common.detect_aws_environment"
            ) as mock_detect_env,
        ):
            # Setup mocks
            mock_identity.return_value = (
                "arn:aws:sts::024848452848:assumed-role/dh-remote-executor/session",
                "ecs.amazonaws.com",
            )
            mock_detect_env.return_value = AwsEnvironment.ECS

            # Mock assume_role to return credentials with expiration
            from datetime import datetime, timedelta, timezone

            expiration = datetime.now(timezone.utc) + timedelta(hours=1)
            mock_assume_role.return_value = {
                "AccessKeyId": "ASSUMED_KEY_ID",
                "SecretAccessKey": "ASSUMED_SECRET_KEY",
                "SessionToken": "ASSUMED_SESSION_TOKEN",
                "Expiration": expiration,
            }

            # First call - should assume role
            session1 = config.get_session()
            creds1 = session1.get_credentials()
            assert creds1 is not None
            assert creds1.access_key == "ASSUMED_KEY_ID"
            assert creds1.secret_key == "ASSUMED_SECRET_KEY"
            assert creds1.token == "ASSUMED_SESSION_TOKEN"
            assert mock_assume_role.call_count == 1

            # Second call - should use cached credentials, NOT assume role again
            session2 = config.get_session()
            creds2 = session2.get_credentials()
            assert creds2 is not None
            assert creds2.access_key == "ASSUMED_KEY_ID"
            assert creds2.secret_key == "ASSUMED_SECRET_KEY"
            assert creds2.token == "ASSUMED_SESSION_TOKEN"
            # assume_role should still only be called once
            assert mock_assume_role.call_count == 1

            # Third call - should still use cached credentials
            session3 = config.get_session()
            creds3 = session3.get_credentials()
            assert creds3 is not None
            assert creds3.access_key == "ASSUMED_KEY_ID"
            assert creds3.secret_key == "ASSUMED_SECRET_KEY"
            assert creds3.token == "ASSUMED_SESSION_TOKEN"
            # assume_role should still only be called once
            assert mock_assume_role.call_count == 1

    @mock_sts
    def test_role_assumption_refreshes_expired_credentials(self):
        """
        Test that expired credentials trigger a new role assumption.
        """
        config = AwsConnectionConfig(
            aws_region="us-east-1",
            aws_role="arn:aws:iam::123456789012:role/test-role",
        )

        with (
            patch(
                "datahub.ingestion.source.aws.aws_common.get_current_identity"
            ) as mock_identity,
            patch(
                "datahub.ingestion.source.aws.aws_common.assume_role"
            ) as mock_assume_role,
            patch(
                "datahub.ingestion.source.aws.aws_common.detect_aws_environment"
            ) as mock_detect_env,
        ):
            mock_identity.return_value = (None, None)
            mock_detect_env.return_value = AwsEnvironment.ECS

            from datetime import datetime, timedelta, timezone

            # First call - credentials that will expire soon (4 minutes remaining)
            expiration1 = datetime.now(timezone.utc) + timedelta(minutes=4)
            mock_assume_role.return_value = {
                "AccessKeyId": "KEY_1",
                "SecretAccessKey": "SECRET_1",
                "SessionToken": "TOKEN_1",
                "Expiration": expiration1,
            }

            session1 = config.get_session()
            creds1 = session1.get_credentials()
            assert creds1 is not None
            assert creds1.access_key == "KEY_1"
            assert mock_assume_role.call_count == 1

            # Second call - credentials are about to expire (< 5 min threshold)
            # Should trigger refresh
            expiration2 = datetime.now(timezone.utc) + timedelta(hours=1)
            mock_assume_role.return_value = {
                "AccessKeyId": "KEY_2",
                "SecretAccessKey": "SECRET_2",
                "SessionToken": "TOKEN_2",
                "Expiration": expiration2,
            }

            session2 = config.get_session()
            creds2 = session2.get_credentials()
            assert creds2 is not None
            assert creds2.access_key == "KEY_2"
            # assume_role should be called again due to expiration
            assert mock_assume_role.call_count == 2

    @mock_sts
    def test_multiple_clients_use_same_cached_credentials(self):
        """
        Test that multiple AWS clients (glue, s3, lakeformation) created from
        the same config instance all use the same assumed role credentials.

        This simulates the real-world scenario where GlueSource.__init__
        creates multiple clients in quick succession.
        """
        config = AwsConnectionConfig(
            aws_region="us-east-1",
            aws_role="arn:aws:iam::339713033063:role/cross-account-role",
        )

        with (
            patch(
                "datahub.ingestion.source.aws.aws_common.get_current_identity"
            ) as mock_identity,
            patch(
                "datahub.ingestion.source.aws.aws_common.assume_role"
            ) as mock_assume_role,
            patch(
                "datahub.ingestion.source.aws.aws_common.detect_aws_environment"
            ) as mock_detect_env,
        ):
            mock_identity.return_value = (
                "arn:aws:sts::024848452848:assumed-role/base-role/session",
                "ecs.amazonaws.com",
            )
            mock_detect_env.return_value = AwsEnvironment.ECS

            from datetime import datetime, timedelta, timezone

            expiration = datetime.now(timezone.utc) + timedelta(hours=1)
            mock_assume_role.return_value = {
                "AccessKeyId": "CROSS_ACCOUNT_KEY",
                "SecretAccessKey": "CROSS_ACCOUNT_SECRET",
                "SessionToken": "CROSS_ACCOUNT_TOKEN",
                "Expiration": expiration,
            }

            # Create multiple clients simulating GlueSource.__init__
            config.get_glue_client()
            config.get_s3_client()
            config.get_lakeformation_client()

            # Verify all clients use the same assumed credentials
            # assume_role should only be called once
            assert mock_assume_role.call_count == 1

    @mock_sts
    def test_role_assumption_without_caching_before_fix(self):
        """
        This test demonstrates the bug that existed before the fix.
        Without credential caching, the second get_session() call would
        skip role assumption and return base credentials.

        This is a regression test to ensure the bug doesn't come back.
        """
        config = AwsConnectionConfig(
            aws_region="us-east-1",
            aws_role="arn:aws:iam::123456789012:role/test-role",
        )

        # Simulate the old buggy behavior by clearing cached credentials
        # after first call
        with (
            patch(
                "datahub.ingestion.source.aws.aws_common.get_current_identity"
            ) as mock_identity,
            patch(
                "datahub.ingestion.source.aws.aws_common.assume_role"
            ) as mock_assume_role,
            patch(
                "datahub.ingestion.source.aws.aws_common.detect_aws_environment"
            ) as mock_detect_env,
        ):
            mock_identity.return_value = (None, None)
            mock_detect_env.return_value = AwsEnvironment.ECS

            from datetime import datetime, timedelta, timezone

            expiration = datetime.now(timezone.utc) + timedelta(hours=1)
            mock_assume_role.return_value = {
                "AccessKeyId": "ASSUMED_KEY",
                "SecretAccessKey": "ASSUMED_SECRET",
                "SessionToken": "ASSUMED_TOKEN",
                "Expiration": expiration,
            }

            # First call
            session1 = config.get_session()
            creds1 = session1.get_credentials()
            assert creds1 is not None
            assert creds1.access_key == "ASSUMED_KEY"

            # Verify that second call also gets assumed credentials
            # (not base credentials as in the bug)
            session2 = config.get_session()
            creds2 = session2.get_credentials()
            assert creds2 is not None
            assert creds2.access_key == "ASSUMED_KEY"
            assert creds2.secret_key == "ASSUMED_SECRET"
            assert creds2.token == "ASSUMED_TOKEN"

    @mock_sts
    def test_role_assumption_with_explicit_credentials(self):
        """
        Test that explicit credentials (aws_access_key_id/aws_secret_access_key)
        take precedence and role assumption works with them.
        """
        config = AwsConnectionConfig(
            aws_access_key_id="EXPLICIT_KEY",
            aws_secret_access_key="EXPLICIT_SECRET",
            aws_region="us-east-1",
            aws_role="arn:aws:iam::123456789012:role/test-role",
        )

        # When explicit credentials are provided, get_session() doesn't
        # go through the role assumption logic, so this is just a sanity check
        session = config.get_session()
        creds = session.get_credentials()
        assert creds is not None
        assert creds.access_key == "EXPLICIT_KEY"
        assert creds.secret_key == "EXPLICIT_SECRET"

    @mock_sts
    def test_role_assumption_chain(self):
        """
        Test assuming multiple roles in a chain.
        """
        config = AwsConnectionConfig(
            aws_region="us-east-1",
            aws_role=[
                "arn:aws:iam::111111111111:role/role1",
                "arn:aws:iam::222222222222:role/role2",
            ],
        )

        with (
            patch(
                "datahub.ingestion.source.aws.aws_common.get_current_identity"
            ) as mock_identity,
            patch(
                "datahub.ingestion.source.aws.aws_common.assume_role"
            ) as mock_assume_role,
            patch(
                "datahub.ingestion.source.aws.aws_common.detect_aws_environment"
            ) as mock_detect_env,
        ):
            mock_identity.return_value = (None, None)
            mock_detect_env.return_value = AwsEnvironment.ECS

            from datetime import datetime, timedelta, timezone

            expiration = datetime.now(timezone.utc) + timedelta(hours=1)

            # First role assumption
            mock_assume_role.side_effect = [
                {
                    "AccessKeyId": "KEY_1",
                    "SecretAccessKey": "SECRET_1",
                    "SessionToken": "TOKEN_1",
                    "Expiration": expiration,
                },
                {
                    "AccessKeyId": "KEY_2",
                    "SecretAccessKey": "SECRET_2",
                    "SessionToken": "TOKEN_2",
                    "Expiration": expiration,
                },
            ]

            session = config.get_session()
            creds = session.get_credentials()

            # Should use credentials from second role in chain
            assert creds is not None
            assert creds.access_key == "KEY_2"
            assert creds.secret_key == "SECRET_2"
            assert creds.token == "TOKEN_2"

            # Both roles should have been assumed
            assert mock_assume_role.call_count == 2
