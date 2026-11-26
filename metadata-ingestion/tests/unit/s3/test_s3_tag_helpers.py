from unittest.mock import patch

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.aws.s3_boto_utils import (
    get_bucket_tag_set,
    get_object_tag_set,
    get_s3_tags,
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


def test_object_tagging_not_implemented_is_skipped() -> None:
    not_implemented = ClientError(
        {"Error": {"Code": "NotImplemented", "Message": "not supported"}},
        "GetObjectTagging",
    )
    with patch(
        "datahub.ingestion.source.aws.s3_boto_utils.get_object_tag_set",
        side_effect=not_implemented,
    ):
        tags = get_s3_tags(
            "my-bucket",
            "raw/a.csv",
            "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/raw,PROD)",
            _aws(),
            PipelineContext(run_id="test"),
            use_s3_object_tags=True,
        )
    assert tags is None
