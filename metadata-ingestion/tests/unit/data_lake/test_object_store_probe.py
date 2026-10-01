from typing import Iterator
from unittest import mock

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.data_lake_common import object_store_probe
from datahub.ingestion.source.data_lake_common.object_store_probe import (
    S3CompatibleMetadataProbe,
)
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec

KEYS = [
    "raw/a.csv",
    "raw/b.csv",
    "raw/c.csv",
    "data/events/year=2024/part-0.csv",
    "data/users/year=2024/part-0.csv",
]


def _aws() -> AwsConnectionConfig:
    return AwsConnectionConfig(
        aws_access_key_id="id",
        aws_secret_access_key="secret",
        aws_region="us-east-1",
    )


def _probe(*includes: str) -> S3CompatibleMetadataProbe:
    specs = [PathSpec(include=i) for i in (includes or ("s3://my-bucket/raw/*.csv",))]
    return S3CompatibleMetadataProbe(_aws(), specs)


def _client_error(
    code: str, status: int, operation: str = "ListObjectsV2", message: str = ""
) -> ClientError:
    return ClientError(
        {
            "Error": {"Code": code, "Message": message},
            "ResponseMetadata": {"HTTPStatusCode": status},
        },
        operation,
    )


@pytest.fixture
def bucket() -> Iterator[None]:
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="my-bucket")
        for key in KEYS:
            client.put_object(Bucket="my-bucket", Key=key, Body=b"a,b\n1,2\n")
        yield


def test_objects_are_bounded_by_limit(bucket: None) -> None:
    assert len(_probe().objects(bucket="my-bucket", prefix="raw", limit=2)) == 2


def test_templated_datasets_are_the_table_folders(bucket: None) -> None:
    names = {
        d["name"]
        for d in _probe("s3://my-bucket/data/{table}/*/*.csv").datasets(limit=10)
    }
    assert names == {"s3://my-bucket/data/events", "s3://my-bucket/data/users"}


def test_a_missing_bucket_is_a_caller_error(bucket: None) -> None:
    with pytest.raises(ValueError, match="no such bucket"):
        _probe().objects(bucket="no-such-bucket", limit=10)


def test_a_denied_whole_listing_is_a_failure_not_an_empty_result() -> None:
    probe = _probe()
    with mock.patch.object(
        object_store_probe,
        "list_objects_recursive_path",
        side_effect=_client_error("AccessDenied", 403),
    ):
        assert probe.objects(bucket="my-bucket", limit=10) == []
    assert probe.failures and not probe.warnings


def test_a_denied_prefix_during_table_resolution_is_a_warning(bucket: None) -> None:
    # resolve_templated_folders (s3.source) is not patched; only the per-prefix
    # table-folder listing the base issues is denied.
    probe = _probe("s3://my-bucket/data/{table}/*/*.csv")
    with mock.patch.object(
        object_store_probe,
        "list_folders_path",
        side_effect=_client_error("AccessDenied", 403),
    ):
        assert probe.datasets(limit=10) == []
    assert probe.warnings and not probe.failures


def test_prefix_budget_stops_and_warns(
    bucket: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(object_store_probe, "MAX_RESOLVED_PREFIXES", 1)
    probe = _probe("s3://my-bucket/*/{table}/*.csv")
    probe.datasets(limit=10)
    assert any("narrow" in w for w in probe.warnings)


def test_a_wrong_kind_of_spec_is_empty_with_a_reason(bucket: None) -> None:
    folders_only = S3CompatibleMetadataProbe(
        _aws(), [PathSpec(include="s3://my-bucket/data/*/", emit_folders_only=True)]
    )
    assert folders_only.datasets(limit=10) == []
    assert any("path_spec_folders" in w for w in folders_only.warnings)
    assert folders_only.path_spec_folders(limit=10) == [
        "s3://my-bucket/data/events",
        "s3://my-bucket/data/users",
    ]
    datasets = _probe()
    assert datasets.path_spec_folders(limit=10) == []
    assert any("datasets" in w for w in datasets.warnings)


def test_an_unknown_path_spec_index_is_a_caller_error() -> None:
    with pytest.raises(ValueError, match="path_spec 3"):
        _probe().datasets(path_spec=3, limit=10)


def test_exit_closes_the_cached_clients() -> None:
    probe = _probe()
    # On the class: pydantic refuses to set a non-field attribute on an instance.
    with mock.patch.object(AwsConnectionConfig, "close_cached_s3_clients") as close:
        with probe:
            pass
    close.assert_called_once()


def test_close_cached_s3_clients_forgets_the_client(bucket: None) -> None:
    aws = _aws()
    first = aws.get_s3_client()
    aws.close_cached_s3_clients()
    assert aws.get_s3_client() is not first
