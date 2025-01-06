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
