import logging
from datetime import datetime, timedelta, timezone
from enum import Enum
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, Tuple, Union
from urllib.parse import parse_qs, urlparse

import boto3
import requests
from boto3.session import Session
from botocore.config import DEFAULT_TIMEOUT, Config
from botocore.exceptions import ClientError, NoCredentialsError
from botocore.utils import fix_s3_host
from pydantic.fields import Field

from datahub.configuration.common import (
    AllowDenyPattern,
    ConfigModel,
    PermissiveConfigModel,
)
from datahub.configuration.env_vars import (
    get_aws_app_runner_service_id,
    get_aws_execution_env,
    get_aws_lambda_function_name,
    get_aws_role_arn,
    get_aws_web_identity_token_file,
    get_ecs_container_metadata_uri,
    get_ecs_container_metadata_uri_v4,
    get_elastic_beanstalk_environment_name,
)
from datahub.configuration.source_common import EnvConfigMixin

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient
    from mypy_boto3_glue import GlueClient
    from mypy_boto3_lakeformation import LakeFormationClient
    from mypy_boto3_s3 import S3Client, S3ServiceResource
    from mypy_boto3_sagemaker import SageMakerClient
    from mypy_boto3_sts import STSClient


class AwsEnvironment(Enum):
    EC2 = "EC2"
    ECS = "ECS"
    EKS = "EKS"
    LAMBDA = "LAMBDA"
    APP_RUNNER = "APP_RUNNER"
    BEANSTALK = "ELASTIC_BEANSTALK"
    CLOUD_FORMATION = "CLOUD_FORMATION"
    UNKNOWN = "UNKNOWN"


class AwsServicePrincipal(Enum):
    LAMBDA = "lambda.amazonaws.com"
    EKS = "eks.amazonaws.com"
    APP_RUNNER = "apprunner.amazonaws.com"
    ECS = "ecs.amazonaws.com"
    ELASTIC_BEANSTALK = "elasticbeanstalk.amazonaws.com"
    EC2 = "ec2.amazonaws.com"


class AwsAssumeRoleConfig(PermissiveConfigModel):
    # Using the PermissiveConfigModel to allow the user to pass additional arguments.

    RoleArn: str = Field(
        description="ARN of the role to assume.",
    )
    ExternalId: Optional[str] = Field(
        None,
        description="External ID to use when assuming the role.",
    )


def get_instance_metadata_token() -> Optional[str]:
    """Get IMDSv2 token"""
    try:
        response = requests.put(
            "http://169.254.169.254/latest/api/token",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
            timeout=1,
        )
        if response.status_code == HTTPStatus.OK:
            return response.text
    except requests.exceptions.RequestException:
        logger.debug("Failed to get IMDSv2 token")
    return None


def is_running_on_ec2() -> bool:
    """Check if code is running on EC2 using IMDSv2"""
    token = get_instance_metadata_token()
    if not token:
        return False

    try:
        response = requests.get(
            "http://169.254.169.254/latest/meta-data/instance-id",
            headers={"X-aws-ec2-metadata-token": token},
            timeout=1,
        )
        return response.status_code == HTTPStatus.OK
    except requests.exceptions.RequestException:
        return False


def detect_aws_environment() -> AwsEnvironment:
    """
    Detect the AWS environment we're running in.
    Order matters as some environments may have multiple indicators.
    """
    # Check Lambda first as it's most specific
    if get_aws_lambda_function_name():
        if (get_aws_execution_env() or "").startswith("CloudFormation"):
            return AwsEnvironment.CLOUD_FORMATION
        return AwsEnvironment.LAMBDA

    # Check EKS (IRSA)
    if get_aws_web_identity_token_file() and get_aws_role_arn():
        return AwsEnvironment.EKS

    # Check App Runner
    if get_aws_app_runner_service_id():
        return AwsEnvironment.APP_RUNNER

    # Check ECS
    if get_ecs_container_metadata_uri_v4() or get_ecs_container_metadata_uri():
        return AwsEnvironment.ECS

    # Check Elastic Beanstalk
    if get_elastic_beanstalk_environment_name():
        return AwsEnvironment.BEANSTALK

    if is_running_on_ec2():
        return AwsEnvironment.EC2

    return AwsEnvironment.UNKNOWN


def get_instance_role_arn() -> Optional[str]:
    """Get role ARN from EC2 instance metadata using IMDSv2"""
    token = get_instance_metadata_token()
    if not token:
        return None

    try:
        response = requests.get(
            "http://169.254.169.254/latest/meta-data/iam/security-credentials/",
            headers={"X-aws-ec2-metadata-token": token},
            timeout=1,
        )
        if response.status_code == 200:
            role_name = response.text.strip()
            if role_name:
                sts = boto3.client("sts")
                identity = sts.get_caller_identity()
                return identity.get("Arn")
    except Exception as e:
        logger.debug(f"Failed to get instance role ARN: {e}")
    return None


def get_lambda_role_arn() -> Optional[str]:
    """Get the Lambda function's role ARN"""
    try:
        function_name = get_aws_lambda_function_name()
        if not function_name:
            return None

        lambda_client = boto3.client("lambda")
        function_config = lambda_client.get_function_configuration(
            FunctionName=function_name
        )
        return function_config.get("Role")
    except Exception as e:
        logger.debug(f"Failed to get Lambda role ARN: {e}")
        return None


def get_current_identity() -> Tuple[Optional[str], Optional[str]]:
    """
    Get the current role ARN and source type based on the runtime environment.
    Returns (role_arn, credential_source)
    """
    env = detect_aws_environment()

    if env == AwsEnvironment.LAMBDA:
        role_arn = get_lambda_role_arn()
        return role_arn, AwsServicePrincipal.LAMBDA.value

    elif env == AwsEnvironment.EKS:
        role_arn = get_aws_role_arn()
        return role_arn, AwsServicePrincipal.EKS.value

    elif env == AwsEnvironment.APP_RUNNER:
        try:
            sts = boto3.client("sts")
            identity = sts.get_caller_identity()
            return identity.get("Arn"), AwsServicePrincipal.APP_RUNNER.value
        except Exception as e:
            logger.debug(f"Failed to get App Runner role: {e}")

    elif env == AwsEnvironment.ECS:
        try:
            metadata_uri = (
                get_ecs_container_metadata_uri_v4() or get_ecs_container_metadata_uri()
            )
            if metadata_uri:
                response = requests.get(f"{metadata_uri}/task", timeout=1)
                if response.status_code == HTTPStatus.OK:
                    task_metadata = response.json()
                    if "TaskARN" in task_metadata:
                        return (
                            task_metadata.get("TaskARN"),
                            AwsServicePrincipal.ECS.value,
                        )
        except Exception as e:
            logger.debug(f"Failed to get ECS task role: {e}")

    elif env == AwsEnvironment.BEANSTALK:
        # Beanstalk uses EC2 instance metadata
        return get_instance_role_arn(), AwsServicePrincipal.ELASTIC_BEANSTALK.value

    elif env == AwsEnvironment.EC2:
        return get_instance_role_arn(), AwsServicePrincipal.EC2.value

    return None, None


def assume_role(
    role: AwsAssumeRoleConfig,
    aws_region: Optional[str],
    credentials: Optional[dict] = None,
) -> dict:
    credentials = credentials or {}
    sts_client: "STSClient" = boto3.client(
        "sts",
        region_name=aws_region,
        aws_access_key_id=credentials.get("AccessKeyId"),
        aws_secret_access_key=credentials.get("SecretAccessKey"),
        aws_session_token=credentials.get("SessionToken"),
    )

    assume_role_args: dict = {
        **dict(
            RoleSessionName="DatahubIngestionSource",
        ),
        **{k: v for k, v in role.model_dump().items() if v is not None},
    }

    assumed_role_object = sts_client.assume_role(
        **assume_role_args,
    )
    return dict(assumed_role_object["Credentials"])


AUTODETECT_CREDENTIALS_DOC_LINK = "Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details."


class AwsConnectionConfig(ConfigModel):
    """
    Common AWS credentials config.

    Currently used by:
        - Glue source
        - SageMaker source
        - dbt source
    """

    _credentials_expiration: Optional[datetime] = None
    _cached_credentials: Optional[dict] = None

    aws_access_key_id: Optional[str] = Field(
        default=None,
        description=f"AWS access key ID. {AUTODETECT_CREDENTIALS_DOC_LINK}",
    )
    aws_secret_access_key: Optional[str] = Field(
        default=None,
        description=f"AWS secret access key. {AUTODETECT_CREDENTIALS_DOC_LINK}",
    )
    aws_session_token: Optional[str] = Field(
        default=None,
        description=f"AWS session token. {AUTODETECT_CREDENTIALS_DOC_LINK}",
    )
    aws_role: Optional[Union[str, List[Union[str, AwsAssumeRoleConfig]]]] = Field(
        default=None,
        description="AWS roles to assume. If using the string format, the role ARN can be specified directly. "
        "If using the object format, the role can be specified in the RoleArn field and additional available arguments are the same as [boto3's STS.Client.assume_role](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts.html?highlight=assume_role#STS.Client.assume_role).",
    )
    aws_profile: Optional[str] = Field(
        default=None,
        description="The [named profile](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html) to use from AWS credentials. Falls back to default profile if not specified and no access keys provided. Profiles are configured in ~/.aws/credentials or ~/.aws/config.",
    )
    aws_region: Optional[str] = Field(None, description="AWS region code.")

    aws_endpoint_url: Optional[str] = Field(
        default=None,
        description="The AWS service endpoint. This is normally [constructed automatically](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html), but can be overridden here.",
    )
    aws_proxy: Optional[Dict[str, str]] = Field(
        default=None,
        description="A set of proxy configs to use with AWS. See the [botocore.config](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html) docs for details.",
    )
    aws_retry_num: int = Field(
        default=5,
        description="Number of times to retry failed AWS requests. See the [botocore.retry](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html) docs for details.",
    )
    aws_retry_mode: Literal["legacy", "standard", "adaptive"] = Field(
        default="standard",
        description="Retry mode to use for failed AWS requests. See the [botocore.retry](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html) docs for details.",
    )

    read_timeout: float = Field(
        default=DEFAULT_TIMEOUT,
        description="The timeout for reading from the connection (in seconds).",
    )

    aws_advanced_config: Dict[str, Any] = Field(
        default_factory=dict,
        description="Advanced AWS configuration options. These are passed directly to [botocore.config.Config](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html).",
    )

    def allowed_cred_refresh(self) -> bool:
        if self._normalized_aws_roles():
            return True
        return False

    def _normalized_aws_roles(self) -> List[AwsAssumeRoleConfig]:
        if not self.aws_role:
            return []
        elif isinstance(self.aws_role, str):
            return [AwsAssumeRoleConfig(RoleArn=self.aws_role)]
        else:
            assert isinstance(self.aws_role, list)
            return [
                AwsAssumeRoleConfig(RoleArn=role) if isinstance(role, str) else role
                for role in self.aws_role
            ]

    def get_session(self) -> Session:
        if self.aws_access_key_id and self.aws_secret_access_key:
            # Explicit credentials take precedence
            session = Session(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                aws_session_token=self.aws_session_token,
                region_name=self.aws_region,
            )
        elif self.aws_profile:
            # Named profile is second priority
            session = Session(
                region_name=self.aws_region, profile_name=self.aws_profile
            )
        else:
            # Use boto3's credential autodetection
            target_roles = self._normalized_aws_roles()
            if target_roles:
                # If we have cached credentials that are still valid, use them
                if (
                    self._cached_credentials is not None
                    and not self._should_refresh_credentials()
                ):
                    logger.debug("Using cached assumed role credentials")
                    return Session(
                        aws_access_key_id=self._cached_credentials["AccessKeyId"],
                        aws_secret_access_key=self._cached_credentials[
                            "SecretAccessKey"
                        ],
                        aws_session_token=self._cached_credentials["SessionToken"],
                        region_name=self.aws_region,
                    )

                # Need to assume role (either first time or credentials expired)
                session = Session(region_name=self.aws_region)
                current_role_arn, credential_source = get_current_identity()

                # Only assume role if:
                # 1. We're not in a known AWS environment with a role, or
                # 2. We need to assume a different role than our current one
                should_assume_role = current_role_arn is None or any(
                    role.RoleArn != current_role_arn for role in target_roles
                )

                if should_assume_role:
                    env = detect_aws_environment()
                    role_arns = [role.RoleArn for role in target_roles]
                    logger.debug(
                        f"Assuming {role_arns} role(s) from {env.value} environment"
                    )

                    current_credentials = session.get_credentials()
                    if current_credentials is None:
                        raise ValueError("No credentials available for role assumption")

                    credentials = {
                        "AccessKeyId": current_credentials.access_key,
                        "SecretAccessKey": current_credentials.secret_key,
                        "SessionToken": current_credentials.token,
                    }

                    for role in target_roles:
                        credentials = assume_role(
                            role=role,
                            aws_region=self.aws_region,
                            credentials=credentials,
                        )

                    if isinstance(credentials["Expiration"], datetime):
                        self._credentials_expiration = credentials["Expiration"]
                    self._cached_credentials = credentials

                    session = Session(
                        aws_access_key_id=credentials["AccessKeyId"],
                        aws_secret_access_key=credentials["SecretAccessKey"],
                        aws_session_token=credentials["SessionToken"],
                        region_name=self.aws_region,
                    )
                else:
                    logger.debug(f"Using existing role from {credential_source}")
            else:
                session = Session(region_name=self.aws_region)

        return session

    def _should_refresh_credentials(self) -> bool:
        if self._credentials_expiration is None:
            return True
        # Refresh credentials when less than 5 minutes remain before expiration.
        # This buffer helps avoid race conditions where credentials expire between
        # the cache check and actual AWS API usage.
        remaining_time = self._credentials_expiration - datetime.now(timezone.utc)
        return remaining_time < timedelta(minutes=5)

    def get_credentials(self) -> Dict[str, Optional[str]]:
        credentials = self.get_session().get_credentials()
        if credentials is not None:
            return {
                "aws_access_key_id": credentials.access_key,
                "aws_secret_access_key": credentials.secret_key,
                "aws_session_token": credentials.token,
            }
        return {}

    def _aws_config(self) -> Config:
        return Config(
            proxies=self.aws_proxy,
            read_timeout=self.read_timeout,
            retries={
                "max_attempts": self.aws_retry_num,
                "mode": self.aws_retry_mode,
            },
            **self.aws_advanced_config,
        )

    def get_s3_client(
        self, verify_ssl: Optional[Union[bool, str]] = None
    ) -> "S3Client":
        return self.get_session().client(
            "s3",
            endpoint_url=self.aws_endpoint_url,
            config=self._aws_config(),
            verify=verify_ssl,
        )

    def get_s3_resource(
        self, verify_ssl: Optional[Union[bool, str]] = None
    ) -> "S3ServiceResource":
        resource = self.get_session().resource(
            "s3",
            endpoint_url=self.aws_endpoint_url,
            config=self._aws_config(),
            verify=verify_ssl,
        )
        # according to: https://stackoverflow.com/questions/32618216/override-s3-endpoint-using-boto3-configuration-file
        # boto3 only reads the signature version for s3 from that config file. boto3 automatically changes the endpoint to
        # your_bucket_name.s3.amazonaws.com when it sees fit. If you'll be working with both your own host and s3, you may wish
        # to override the functionality rather than removing it altogether.
        if self.aws_endpoint_url is not None and self.aws_proxy is not None:
            resource.meta.client.meta.events.unregister("before-sign.s3", fix_s3_host)
        return resource

    def get_glue_client(self) -> "GlueClient":
        return self.get_session().client("glue", config=self._aws_config())

    def get_dynamodb_client(self) -> "DynamoDBClient":
        return self.get_session().client("dynamodb", config=self._aws_config())

    def get_sagemaker_client(self) -> "SageMakerClient":
        return self.get_session().client("sagemaker", config=self._aws_config())

    def get_lakeformation_client(self) -> "LakeFormationClient":
        return self.get_session().client("lakeformation", config=self._aws_config())

    def get_rds_client(self):
        """Get an RDS client for generating IAM auth tokens."""
        return self.get_session().client("rds", config=self._aws_config())


def generate_rds_iam_token(
    endpoint: str,
    username: str,
    port: int,
    aws_config: AwsConnectionConfig,
) -> str:
    """
    Generate an AWS RDS IAM authentication token.

    boto3's generate_db_auth_token() returns a presigned URL in the format:
    "hostname:port/?Action=connect&DBUser=username&X-Amz-Date=...&X-Amz-Expires=..."

    This token should be used as-is by pymysql/psycopg2 drivers.

    Args:
        endpoint: RDS endpoint hostname
        username: Database username for IAM authentication
        port: Database port (5432 for PostgreSQL, 3306 for MySQL)
        aws_config: AwsConnectionConfig for session management and credentials

    Returns:
        Authentication token (presigned URL format)

    Raises:
        ValueError: If AWS credentials are not found or token generation fails

    """
    try:
        client = aws_config.get_rds_client()
        token = client.generate_db_auth_token(
            DBHostname=endpoint, Port=port, DBUsername=username
        )
        logger.debug(f"Generated RDS IAM token for {username}@{endpoint}:{port}")
        return token
    except NoCredentialsError as e:
        raise ValueError("AWS credentials not found") from e
    except ClientError as e:
        raise ValueError(f"Failed to generate RDS IAM token: {e}") from e


class RDSIAMTokenManager:
    """
    Manages RDS IAM token lifecycle with automatic refresh.

    RDS IAM tokens include expiration information in the URL parameters.
    This manager parses the token expiry and refreshes before expiration
    to ensure uninterrupted database access.
    """

    def __init__(
        self,
        endpoint: str,
        username: str,
        port: int,
        aws_config: AwsConnectionConfig,
        refresh_threshold_minutes: int = 5,
    ):
        """
        Initialize the token manager.

        Args:
            endpoint: RDS endpoint hostname
            username: Database username for IAM authentication
            port: Database port
            aws_config: AwsConnectionConfig for session management and credentials
            refresh_threshold_minutes: Refresh token when this many minutes remain before expiry
        """
        self.endpoint = endpoint
        self.username = username
        self.port = port
        self.aws_config = aws_config
        self.refresh_threshold = timedelta(minutes=refresh_threshold_minutes)

        self._current_token: Optional[str] = None
        self._token_expires_at: Optional[datetime] = None

    def get_token(self) -> str:
        """
        Get current token, refreshing if necessary.

        Returns:
            Valid authentication token

        Raises:
            RuntimeError: If token generation or refresh fails
        """
        if self._needs_refresh():
            self._refresh_token()

        assert self._current_token is not None
        return self._current_token

    def _needs_refresh(self) -> bool:
        """Check if token needs to be refreshed."""
        if self._current_token is None or self._token_expires_at is None:
            return True

        time_until_expiry = self._token_expires_at - datetime.now(timezone.utc)
        return time_until_expiry <= self.refresh_threshold

    def _parse_token_expiry(self, token: str) -> datetime:
        """
        Parse token expiry from X-Amz-Date and X-Amz-Expires URL parameters.

        Args:
            token: RDS IAM authentication token (presigned URL)

        Returns:
            Expiration datetime in UTC

        Raises:
            ValueError: If token URL format is invalid or missing required parameters
        """
        try:
            parsed_url = urlparse(token)
            query_params = parse_qs(parsed_url.query)

            # Extract X-Amz-Date (ISO 8601 format: YYYYMMDDTHHMMSSZ)
            amz_date_list = query_params.get("X-Amz-Date")
            if not amz_date_list:
                raise ValueError("Missing X-Amz-Date parameter in RDS IAM token")
            amz_date_str = amz_date_list[0]

            # Extract X-Amz-Expires (duration in seconds)
            amz_expires_list = query_params.get("X-Amz-Expires")
            if not amz_expires_list:
                raise ValueError("Missing X-Amz-Expires parameter in RDS IAM token")
            amz_expires_seconds = int(amz_expires_list[0])

            # Parse X-Amz-Date to datetime
            token_issued_at = datetime.strptime(amz_date_str, "%Y%m%dT%H%M%SZ").replace(
                tzinfo=timezone.utc
            )

            # Calculate expiration
            return token_issued_at + timedelta(seconds=amz_expires_seconds)

        except (ValueError, KeyError, IndexError) as e:
            raise ValueError(
                f"Failed to parse RDS IAM token expiry: {e}. Token format may be invalid."
            ) from e

    def _refresh_token(self) -> None:
        """Generate and store a new token with parsed expiry."""
        logger.info("Refreshing RDS IAM authentication token")
        self._current_token = generate_rds_iam_token(
            endpoint=self.endpoint,
            username=self.username,
            port=self.port,
            aws_config=self.aws_config,
        )
        self._token_expires_at = self._parse_token_expiry(self._current_token)
        logger.debug(f"Token will expire at {self._token_expires_at}")


class AwsSourceConfig(EnvConfigMixin, AwsConnectionConfig):
    """
    Common AWS credentials config.

    Currently used by:
        - Glue source
        - DynamoDB source
        - SageMaker source
    """

    database_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="regex patterns for databases to filter in ingestion.",
    )
    table_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="regex patterns for tables to filter in ingestion.",
    )
