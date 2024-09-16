from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import boto3
from boto3.session import Session
from botocore.config import DEFAULT_TIMEOUT, Config
from botocore.utils import fix_s3_host
from pydantic.fields import Field

from datahub.configuration.common import (
    AllowDenyPattern,
    ConfigModel,
    PermissiveConfigModel,
)
from datahub.configuration.source_common import EnvConfigMixin

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient
    from mypy_boto3_glue import GlueClient
    from mypy_boto3_s3 import S3Client, S3ServiceResource
    from mypy_boto3_sagemaker import SageMakerClient
    from mypy_boto3_sts import STSClient


class AwsAssumeRoleConfig(PermissiveConfigModel):
    # Using the PermissiveConfigModel to allow the user to pass additional arguments.

    RoleArn: str = Field(
        description="ARN of the role to assume.",
    )
    ExternalId: Optional[str] = Field(
        None,
        description="External ID to use when assuming the role.",
    )


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
        **{k: v for k, v in role.dict().items() if v is not None},
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
        description="Named AWS profile to use. Only used if access key / secret are unset. If not set the default will be used",
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
            session = Session(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                aws_session_token=self.aws_session_token,
                region_name=self.aws_region,
            )
        elif self.aws_profile:
            session = Session(
                region_name=self.aws_region, profile_name=self.aws_profile
            )
        else:
            # Use boto3's credential autodetection.
            session = Session(region_name=self.aws_region)

        if self._normalized_aws_roles():
            # Use existing session credentials to start the chain of role assumption.
            current_credentials = session.get_credentials()
            credentials = {
                "AccessKeyId": current_credentials.access_key,
                "SecretAccessKey": current_credentials.secret_key,
                "SessionToken": current_credentials.token,
            }

            for role in self._normalized_aws_roles():
                if self._should_refresh_credentials():
                    credentials = assume_role(
                        role,
                        self.aws_region,
                        credentials=credentials,
                    )
                    if isinstance(credentials["Expiration"], datetime):
                        self._credentials_expiration = credentials["Expiration"]

            session = Session(
                aws_access_key_id=credentials["AccessKeyId"],
                aws_secret_access_key=credentials["SecretAccessKey"],
                aws_session_token=credentials["SessionToken"],
                region_name=self.aws_region,
            )

        return session

    def _should_refresh_credentials(self) -> bool:
        if self._credentials_expiration is None:
            return True
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
