from functools import reduce
from typing import TYPE_CHECKING, List, Optional, Union

import boto3
from boto3.session import Session

from datahub.configuration import ConfigModel
from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import DEFAULT_ENV

if TYPE_CHECKING:

    from mypy_boto3_glue import GlueClient
    from mypy_boto3_s3 import S3Client, S3ServiceResource
    from mypy_boto3_sagemaker import SageMakerClient


def assume_role(
    role_arn: str, aws_region: str, credentials: Optional[dict] = None
) -> dict:
    credentials = credentials or {}
    sts_client = boto3.client(
        "sts",
        region_name=aws_region,
        aws_access_key_id=credentials.get("AccessKeyId"),
        aws_secret_access_key=credentials.get("SecretAccessKey"),
        aws_session_token=credentials.get("SessionToken"),
    )

    assumed_role_object = sts_client.assume_role(
        RoleArn=role_arn, RoleSessionName="DatahubIngestionSource"
    )
    return assumed_role_object["Credentials"]


class AwsSourceConfig(ConfigModel):
    """
    Common AWS credentials config.

    Currently used by:
        - Glue source
        - SageMaker source
    """

    env: str = DEFAULT_ENV

    database_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    table_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()

    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    aws_role: Optional[Union[str, List[str]]] = None
    aws_profile: Optional[str] = None
    aws_region: str

    def get_session(self) -> Session:
        if (
            self.aws_access_key_id
            and self.aws_secret_access_key
            and self.aws_session_token
        ):
            return Session(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                aws_session_token=self.aws_session_token,
                region_name=self.aws_region,
            )
        elif self.aws_access_key_id and self.aws_secret_access_key:
            return Session(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.aws_region,
            )
        elif self.aws_role:
            if isinstance(self.aws_role, str):
                credentials = assume_role(self.aws_role, self.aws_region)
            else:
                credentials = reduce(
                    lambda new_credentials, role_arn: assume_role(
                        role_arn, self.aws_region, new_credentials
                    ),
                    self.aws_role,
                    {},
                )
            return Session(
                aws_access_key_id=credentials["AccessKeyId"],
                aws_secret_access_key=credentials["SecretAccessKey"],
                aws_session_token=credentials["SessionToken"],
                region_name=self.aws_region,
            )
        else:
            return Session(region_name=self.aws_region, profile_name=self.aws_profile)

    def get_s3_client(self) -> "S3Client":
        return self.get_session().client("s3")

    def get_s3_resource(self) -> "S3ServiceResource":
        return self.get_session().resource("s3")

    def get_glue_client(self) -> "GlueClient":
        return self.get_session().client("glue")

    def get_sagemaker_client(self) -> "SageMakerClient":
        return self.get_session().client("sagemaker")
