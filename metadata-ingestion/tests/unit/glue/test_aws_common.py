import os
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from unittest.mock import Mock, patch

import requests

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.aws.aws_common import (
    AwsAssumeRoleConfig,
    AwsEnvironment,
    AwsServicePrincipal,
    AwsSourceConfig,
    assume_role,
    detect_aws_environment,
    get_current_identity,
    get_instance_metadata_token,
    is_running_on_ec2,
)


class TestAwsEnvironment:
    """Tests for AwsEnvironment enum."""

    def test_enum_values(self) -> None:
        """Test that all enum values are correctly defined."""
        assert AwsEnvironment.EC2.value == "EC2"
        assert AwsEnvironment.ECS.value == "ECS"
        assert AwsEnvironment.EKS.value == "EKS"
        assert AwsEnvironment.LAMBDA.value == "LAMBDA"
        assert AwsEnvironment.APP_RUNNER.value == "APP_RUNNER"
        assert AwsEnvironment.BEANSTALK.value == "ELASTIC_BEANSTALK"
        assert AwsEnvironment.CLOUD_FORMATION.value == "CLOUD_FORMATION"
        assert AwsEnvironment.UNKNOWN.value == "UNKNOWN"


class TestAwsServicePrincipal:
    """Tests for AwsServicePrincipal enum."""

    def test_enum_values(self) -> None:
        """Test that all enum values are correctly defined."""
        assert AwsServicePrincipal.LAMBDA.value == "lambda.amazonaws.com"
        assert AwsServicePrincipal.EKS.value == "eks.amazonaws.com"
        assert AwsServicePrincipal.APP_RUNNER.value == "apprunner.amazonaws.com"
        assert AwsServicePrincipal.ECS.value == "ecs.amazonaws.com"
        assert (
            AwsServicePrincipal.ELASTIC_BEANSTALK.value
            == "elasticbeanstalk.amazonaws.com"
        )
        assert AwsServicePrincipal.EC2.value == "ec2.amazonaws.com"


class TestAwsAssumeRoleConfig:
    """Tests for AwsAssumeRoleConfig class."""

    def test_init_required_fields(self) -> None:
        """Test initialization with required fields only."""
        config = AwsAssumeRoleConfig(RoleArn="arn:aws:iam::123456789012:role/TestRole")
        assert config.RoleArn == "arn:aws:iam::123456789012:role/TestRole"
        assert config.ExternalId is None

    def test_init_all_fields(self) -> None:
        """Test initialization with all fields."""
        config = AwsAssumeRoleConfig(
            RoleArn="arn:aws:iam::123456789012:role/TestRole",
            ExternalId="external-id-123",
        )
        assert config.RoleArn == "arn:aws:iam::123456789012:role/TestRole"
        assert config.ExternalId == "external-id-123"

    def test_dict_method(self) -> None:
        """Test dict() method returns correct values."""
        config = AwsAssumeRoleConfig(
            RoleArn="arn:aws:iam::123456789012:role/TestRole",
            ExternalId="external-id-123",
        )
        config_dict = config.model_dump()
        assert config_dict["RoleArn"] == "arn:aws:iam::123456789012:role/TestRole"
        assert config_dict["ExternalId"] == "external-id-123"


class TestMetadataFunctions:
    """Tests for EC2 metadata functions."""

    @patch("requests.put")
    def test_get_instance_metadata_token_success(self, mock_put: Mock) -> None:
        """Test successful metadata token retrieval."""
        mock_response = Mock()
        mock_response.status_code = HTTPStatus.OK
        mock_response.text = "test-token-123"
        mock_put.return_value = mock_response

        token = get_instance_metadata_token()

        assert token == "test-token-123"
        mock_put.assert_called_once_with(
            "http://169.254.169.254/latest/api/token",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
            timeout=1,
        )

    @patch("requests.put")
    def test_get_instance_metadata_token_failure(self, mock_put: Mock) -> None:
        """Test metadata token retrieval failure."""
        mock_put.side_effect = requests.exceptions.RequestException("Network error")

        token = get_instance_metadata_token()

        assert token is None

    @patch("requests.put")
    def test_get_instance_metadata_token_bad_status(self, mock_put: Mock) -> None:
        """Test metadata token retrieval with bad status code."""
        mock_response = Mock()
        mock_response.status_code = HTTPStatus.NOT_FOUND
        mock_put.return_value = mock_response

        token = get_instance_metadata_token()

        assert token is None

    @patch("datahub.ingestion.source.aws.aws_common.get_instance_metadata_token")
    @patch("requests.get")
    def test_is_running_on_ec2_success(self, mock_get: Mock, mock_token: Mock) -> None:
        """Test successful EC2 detection."""
        mock_token.return_value = "test-token"
        mock_response = Mock()
        mock_response.status_code = HTTPStatus.OK
        mock_get.return_value = mock_response

        result = is_running_on_ec2()

        assert result is True
        mock_get.assert_called_once_with(
            "http://169.254.169.254/latest/meta-data/instance-id",
            headers={"X-aws-ec2-metadata-token": "test-token"},
            timeout=1,
        )

    @patch("datahub.ingestion.source.aws.aws_common.get_instance_metadata_token")
    def test_is_running_on_ec2_no_token(self, mock_token: Mock) -> None:
        """Test EC2 detection when no token is available."""
        mock_token.return_value = None

        result = is_running_on_ec2()

        assert result is False

    @patch("datahub.ingestion.source.aws.aws_common.get_instance_metadata_token")
    @patch("requests.get")
    def test_is_running_on_ec2_request_failure(
        self, mock_get: Mock, mock_token: Mock
    ) -> None:
        """Test EC2 detection when request fails."""
        mock_token.return_value = "test-token"
        mock_get.side_effect = requests.exceptions.RequestException("Network error")

        result = is_running_on_ec2()

        assert result is False


class TestDetectAwsEnvironment:
    """Tests for detect_aws_environment function."""

    def test_detect_lambda_environment(self) -> None:
        """Test Lambda environment detection."""
        with patch.dict(
            os.environ, {"AWS_LAMBDA_FUNCTION_NAME": "test-function"}, clear=True
        ):
            result = detect_aws_environment()
            assert result == AwsEnvironment.LAMBDA

    def test_detect_cloud_formation_environment(self) -> None:
        """Test CloudFormation environment detection."""
        with patch.dict(
            os.environ,
            {
                "AWS_LAMBDA_FUNCTION_NAME": "test-function",
                "AWS_EXECUTION_ENV": "CloudFormation-custom-resource",
            },
            clear=True,
        ):
            result = detect_aws_environment()
            assert result == AwsEnvironment.CLOUD_FORMATION

    def test_detect_eks_environment(self) -> None:
        """Test EKS (IRSA) environment detection."""
        with patch.dict(
            os.environ,
            {
                "AWS_WEB_IDENTITY_TOKEN_FILE": "/var/run/secrets/eks.amazonaws.com/serviceaccount/token",
                "AWS_ROLE_ARN": "arn:aws:iam::123456789012:role/eks-role",
            },
            clear=True,
        ):
            result = detect_aws_environment()
            assert result == AwsEnvironment.EKS

    def test_detect_app_runner_environment(self) -> None:
        """Test App Runner environment detection."""
        with patch.dict(
            os.environ, {"AWS_APP_RUNNER_SERVICE_ID": "service-123"}, clear=True
        ):
            result = detect_aws_environment()
            assert result == AwsEnvironment.APP_RUNNER

    def test_detect_ecs_environment_v4(self) -> None:
        """Test ECS environment detection with metadata URI v4."""
        with patch.dict(
            os.environ,
            {"ECS_CONTAINER_METADATA_URI_V4": "http://169.254.170.2/v4/metadata"},
            clear=True,
        ):
            result = detect_aws_environment()
            assert result == AwsEnvironment.ECS

    def test_detect_ecs_environment_v3(self) -> None:
        """Test ECS environment detection with metadata URI v3."""
        with patch.dict(
            os.environ,
            {"ECS_CONTAINER_METADATA_URI": "http://169.254.170.2/v3/metadata"},
            clear=True,
        ):
            result = detect_aws_environment()
            assert result == AwsEnvironment.ECS

    def test_detect_beanstalk_environment(self) -> None:
        """Test Elastic Beanstalk environment detection."""
        with patch.dict(
            os.environ, {"ELASTIC_BEANSTALK_ENVIRONMENT_NAME": "test-env"}, clear=True
        ):
            result = detect_aws_environment()
            assert result == AwsEnvironment.BEANSTALK

    @patch("datahub.ingestion.source.aws.aws_common.is_running_on_ec2")
    def test_detect_ec2_environment(self, mock_is_ec2: Mock) -> None:
        """Test EC2 environment detection."""
        mock_is_ec2.return_value = True

        with patch.dict(os.environ, {}, clear=True):
            result = detect_aws_environment()
            assert result == AwsEnvironment.EC2

    @patch("datahub.ingestion.source.aws.aws_common.is_running_on_ec2")
    def test_detect_unknown_environment(self, mock_is_ec2: Mock) -> None:
        """Test unknown environment detection."""
        mock_is_ec2.return_value = False

        with patch.dict(os.environ, {}, clear=True):
            result = detect_aws_environment()
            assert result == AwsEnvironment.UNKNOWN


class TestGetCurrentIdentity:
    """Tests for get_current_identity function."""

    @patch("datahub.ingestion.source.aws.aws_common.detect_aws_environment")
    @patch("datahub.ingestion.source.aws.aws_common.get_lambda_role_arn")
    def test_get_lambda_identity(
        self, mock_lambda_role: Mock, mock_detect: Mock
    ) -> None:
        """Test getting Lambda identity."""
        mock_detect.return_value = AwsEnvironment.LAMBDA
        mock_lambda_role.return_value = "arn:aws:iam::123456789012:role/lambda-role"

        role_arn, source = get_current_identity()

        assert role_arn == "arn:aws:iam::123456789012:role/lambda-role"
        assert source == AwsServicePrincipal.LAMBDA.value

    @patch("datahub.ingestion.source.aws.aws_common.detect_aws_environment")
    def test_get_eks_identity(self, mock_detect: Mock) -> None:
        """Test getting EKS identity."""
        mock_detect.return_value = AwsEnvironment.EKS

        with patch.dict(
            os.environ, {"AWS_ROLE_ARN": "arn:aws:iam::123456789012:role/eks-role"}
        ):
            role_arn, source = get_current_identity()

            assert role_arn == "arn:aws:iam::123456789012:role/eks-role"
            assert source == AwsServicePrincipal.EKS.value

    @patch("datahub.ingestion.source.aws.aws_common.detect_aws_environment")
    @patch("boto3.client")
    def test_get_app_runner_identity(
        self, mock_boto_client: Mock, mock_detect: Mock
    ) -> None:
        """Test getting App Runner identity."""
        mock_detect.return_value = AwsEnvironment.APP_RUNNER
        mock_sts = Mock()
        mock_sts.get_caller_identity.return_value = {
            "Arn": "arn:aws:sts::123456789012:assumed-role/app-runner-role"
        }
        mock_boto_client.return_value = mock_sts

        role_arn, source = get_current_identity()

        assert role_arn == "arn:aws:sts::123456789012:assumed-role/app-runner-role"
        assert source == AwsServicePrincipal.APP_RUNNER.value

    @patch("datahub.ingestion.source.aws.aws_common.detect_aws_environment")
    @patch("requests.get")
    def test_get_ecs_identity(self, mock_get: Mock, mock_detect: Mock) -> None:
        """Test getting ECS identity."""
        mock_detect.return_value = AwsEnvironment.ECS
        mock_response = Mock()
        mock_response.status_code = HTTPStatus.OK
        mock_response.json.return_value = {
            "TaskARN": "arn:aws:ecs:us-east-1:123456789012:task/task-id"
        }
        mock_get.return_value = mock_response

        with patch.dict(
            os.environ,
            {"ECS_CONTAINER_METADATA_URI_V4": "http://169.254.170.2/v4/metadata"},
        ):
            role_arn, source = get_current_identity()

            assert role_arn == "arn:aws:ecs:us-east-1:123456789012:task/task-id"
            assert source == AwsServicePrincipal.ECS.value

    @patch("datahub.ingestion.source.aws.aws_common.detect_aws_environment")
    def test_get_unknown_identity(self, mock_detect: Mock) -> None:
        """Test getting identity for unknown environment."""
        mock_detect.return_value = AwsEnvironment.UNKNOWN

        role_arn, source = get_current_identity()

        assert role_arn is None
        assert source is None


class TestAssumeRole:
    """Tests for assume_role function."""

    @patch("boto3.client")
    def test_assume_role_success(self, mock_boto_client: Mock) -> None:
        """Test successful role assumption."""
        mock_sts = Mock()
        mock_sts.assume_role.return_value = {
            "Credentials": {
                "AccessKeyId": "AKIA123456789",
                "SecretAccessKey": "secret123",
                "SessionToken": "token123",
                "Expiration": datetime.now(timezone.utc) + timedelta(hours=1),
            }
        }
        mock_boto_client.return_value = mock_sts

        role_config = AwsAssumeRoleConfig(
            RoleArn="arn:aws:iam::123456789012:role/test-role"
        )

        result = assume_role(role_config, "us-east-1")

        assert result["AccessKeyId"] == "AKIA123456789"
        assert result["SecretAccessKey"] == "secret123"
        assert result["SessionToken"] == "token123"

        mock_sts.assume_role.assert_called_once()
        call_args = mock_sts.assume_role.call_args[1]
        assert call_args["RoleArn"] == "arn:aws:iam::123456789012:role/test-role"
        assert call_args["RoleSessionName"] == "DatahubIngestionSource"

    @patch("boto3.client")
    def test_assume_role_with_external_id(self, mock_boto_client: Mock) -> None:
        """Test role assumption with external ID."""
        mock_sts = Mock()
        mock_sts.assume_role.return_value = {
            "Credentials": {
                "AccessKeyId": "AKIA123456789",
                "SecretAccessKey": "secret123",
                "SessionToken": "token123",
                "Expiration": datetime.now(timezone.utc) + timedelta(hours=1),
            }
        }
        mock_boto_client.return_value = mock_sts

        role_config = AwsAssumeRoleConfig(
            RoleArn="arn:aws:iam::123456789012:role/test-role",
            ExternalId="external-123",
        )

        assume_role(role_config, "us-east-1")

        call_args = mock_sts.assume_role.call_args[1]
        assert call_args["ExternalId"] == "external-123"

    @patch("boto3.client")
    def test_assume_role_with_existing_credentials(
        self, mock_boto_client: Mock
    ) -> None:
        """Test role assumption with existing credentials."""
        mock_sts = Mock()
        mock_sts.assume_role.return_value = {
            "Credentials": {
                "AccessKeyId": "AKIA123456789",
                "SecretAccessKey": "secret123",
                "SessionToken": "token123",
                "Expiration": datetime.now(timezone.utc) + timedelta(hours=1),
            }
        }
        mock_boto_client.return_value = mock_sts

        role_config = AwsAssumeRoleConfig(
            RoleArn="arn:aws:iam::123456789012:role/test-role"
        )
        existing_creds = {
            "AccessKeyId": "EXISTING123",
            "SecretAccessKey": "existingsecret",
            "SessionToken": "existingtoken",
        }

        assume_role(role_config, "us-east-1", existing_creds)

        mock_boto_client.assert_called_once_with(
            "sts",
            region_name="us-east-1",
            aws_access_key_id="EXISTING123",
            aws_secret_access_key="existingsecret",
            aws_session_token="existingtoken",
        )


class TestAwsSourceConfig:
    """Tests for AwsSourceConfig class."""

    def test_init_with_defaults(self) -> None:
        """Test initialization with default values."""
        config = AwsSourceConfig()

        # Test inherited AwsConnectionConfig fields
        assert config.aws_access_key_id is None
        assert config.aws_region is None

        # Test new fields
        assert config.database_pattern == AllowDenyPattern.allow_all()
        assert config.table_pattern == AllowDenyPattern.allow_all()

    def test_init_with_custom_patterns(self) -> None:
        """Test initialization with custom patterns."""
        db_pattern = AllowDenyPattern(allow=["test_*"], deny=["temp_*"])
        table_pattern = AllowDenyPattern(allow=["prod_*"])

        config = AwsSourceConfig(
            aws_region="us-east-1",
            database_pattern=db_pattern,
            table_pattern=table_pattern,
        )

        assert config.aws_region == "us-east-1"
        assert config.database_pattern == db_pattern
        assert config.table_pattern == table_pattern

    def test_inheritance_from_aws_connection_config(self) -> None:
        """Test that AwsSourceConfig inherits from AwsConnectionConfig properly."""
        config = AwsSourceConfig(
            aws_access_key_id="AKIA123456789", aws_secret_access_key="secret123"
        )

        # Should be able to use AwsConnectionConfig methods
        assert hasattr(config, "get_session")
        assert hasattr(config, "get_s3_client")
        assert hasattr(config, "get_glue_client")

        # Should have access to connection config fields
        assert config.aws_access_key_id == "AKIA123456789"
        assert config.aws_secret_access_key is not None
        assert config.aws_secret_access_key.get_secret_value() == "secret123"
