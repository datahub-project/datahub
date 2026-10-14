import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.aws.s3_boto_utils import (
    get_bucket_tag_set,
    get_object_tag_set,
)


def _aws() -> AwsConnectionConfig:
    return AwsConnectionConfig(
        aws_access_key_id="id", aws_secret_access_key="secret", aws_region="us-east-1"
    )


@mock_aws
def test_tag_sets_are_what_ingestion_reads() -> None:
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="my-bucket")
    client.put_bucket_tagging(
        Bucket="my-bucket", Tagging={"TagSet": [{"Key": "team", "Value": "data"}]}
    )
    client.put_object(Bucket="my-bucket", Key="raw/a.csv", Body=b"a\n")
    client.put_object_tagging(
        Bucket="my-bucket",
        Key="raw/a.csv",
        Tagging={"TagSet": [{"Key": "tier", "Value": "gold"}]},
    )
    aws = _aws()
    assert get_bucket_tag_set("my-bucket", aws) == [{"Key": "team", "Value": "data"}]
    assert get_object_tag_set("my-bucket", "raw/a.csv", aws) == [
        {"Key": "tier", "Value": "gold"}
    ]


@mock_aws
def test_an_untagged_bucket_raises_like_the_resource_does() -> None:
    boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="my-bucket")
    with pytest.raises(ClientError):
        get_bucket_tag_set("my-bucket", _aws())
