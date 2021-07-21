from functools import reduce
from typing import List, Optional, Union

import boto3

from datahub.configuration import ConfigModel
from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mce_builder import DEFAULT_ENV


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
    aws_region: str

    def get_client(self, service: str) -> boto3.client:
        if (
            self.aws_access_key_id
            and self.aws_secret_access_key
            and self.aws_session_token
        ):
            return boto3.client(
                service,
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                aws_session_token=self.aws_session_token,
                region_name=self.aws_region,
            )
        elif self.aws_access_key_id and self.aws_secret_access_key:
            return boto3.client(
                service,
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
            return boto3.client(
                service,
                aws_access_key_id=credentials["AccessKeyId"],
                aws_secret_access_key=credentials["SecretAccessKey"],
                aws_session_token=credentials["SessionToken"],
                region_name=self.aws_region,
            )
        else:
            return boto3.client(service, region_name=self.aws_region)


def make_s3_urn(s3_uri: str, env: str, suffix: Optional[str] = None) -> str:

    if not s3_uri.startswith("s3://"):
        raise ValueError("S3 URIs should begin with 's3://'")
    # remove S3 prefix (s3://)
    s3_name = s3_uri[5:]

    if s3_name.endswith("/"):
        s3_name = s3_name[:-1]

    if suffix is not None:
        return f"urn:li:dataset:(urn:li:dataPlatform:s3,{s3_name}_{suffix},{env})"

    return f"urn:li:dataset:(urn:li:dataPlatform:s3,{s3_name},{env})"
