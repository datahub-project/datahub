import pathlib
from typing import Any, Dict, Iterator, List
from unittest import mock

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from datahub.ingestion.agent.probe_methods import list_probe_methods, run_probe_method
from datahub.ingestion.source.s3.config import DataLakeSourceConfig
from datahub.ingestion.source.s3.s3_probe import S3MetadataProbe

AWS: Dict[str, object] = {
    "aws_access_key_id": "id",
    "aws_secret_access_key": "secret",
    "aws_region": "us-east-1",
}
KEYS = [
    "data/events/year=2024/part-0.csv",
    "data/users/year=2024/part-0.csv",
    "raw/a.csv",
]


def _recipe(*includes: str, **extra: object) -> Dict[str, object]:
    return {
        "path_specs": [{"include": i} for i in includes],
        "aws_config": AWS,
        **extra,
    }


@pytest.fixture
def buckets() -> Iterator[None]:
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        for name in ("my-bucket", "my-bucket-2"):
            client.create_bucket(Bucket=name)
            for key in KEYS:
                client.put_object(Bucket=name, Key=key, Body=b"a,b\n1,2\n")
        yield


def _recording(seen: List[str]) -> Any:
    real_client = boto3.session.Session.client

    def recording_client(self: boto3.session.Session, *args: Any, **kwargs: Any) -> Any:
        client = real_client(self, *args, **kwargs)
        client.meta.events.register(
            "before-call.s3", lambda model, **_: seen.append(model.name)
        )
        return client

    return mock.patch.object(boto3.session.Session, "client", recording_client)


def test_methods_advertise_the_s3_kinds() -> None:
    kinds = {
        s.command: s.kind
        for s in list_probe_methods("s3", _recipe("s3://my-bucket/raw/*.csv"))
    }
    assert kinds == {
        "buckets": "S3 bucket",
        "folders": None,
        "objects": None,
        "datasets": "Table",
        "path_spec_folders": "Folder",
        "tags": None,
    }


def test_buckets_lists_the_account(buckets: None) -> None:
    result = run_probe_method("s3", _recipe("s3://my-bucket/raw/*.csv"), "buckets", {})
    assert result.kind == "S3 bucket"
    assert isinstance(result.result, list)
    assert set(result.result) == {"my-bucket", "my-bucket-2"}


def test_bucket_wildcard_datasets_span_buckets(buckets: None) -> None:
    result = run_probe_method(
        "s3", _recipe("s3://*/data/{table}/*/*.csv"), "datasets", {}
    )
    assert result.kind == "Table"
    assert isinstance(result.result, list)
    assert {d["name"] for d in result.result} == {
        f"s3://{b}/data/{t}"
        for b in ("my-bucket", "my-bucket-2")
        for t in ("events", "users")
    }


def test_a_bucket_wildcard_without_list_buckets_is_a_failure(buckets: None) -> None:
    denied = ClientError(
        {
            "Error": {"Code": "AccessDenied"},
            "ResponseMetadata": {"HTTPStatusCode": 403},
        },
        "ListBuckets",
    )
    with mock.patch(
        "datahub.ingestion.source.aws.s3_boto_utils.list_buckets", side_effect=denied
    ):
        result = run_probe_method(
            "s3", _recipe("s3://*/data/{table}/*/*.csv"), "datasets", {}
        )
    assert result.result == [] and result.failures


def test_a_local_recipe_lists_nothing(tmp_path: pathlib.Path) -> None:
    (tmp_path / "a.csv").write_text("a\n1\n")
    recipe: Dict[str, object] = {"path_specs": [{"include": f"{tmp_path}/*.csv"}]}
    with mock.patch("os.walk") as walk, pytest.raises(ValueError, match="s3://"):
        run_probe_method("s3", recipe, "datasets", {})
    walk.assert_not_called()


def test_a_recipe_without_aws_config_is_a_caller_error() -> None:
    with pytest.raises(ValueError, match="aws_config"):
        run_probe_method(
            "s3",
            {"path_specs": [{"include": "s3://my-bucket/raw/*.csv"}]},
            "buckets",
            {},
        )


def test_for_config_assumes_no_role_until_the_first_listing() -> None:
    config = DataLakeSourceConfig.model_validate(
        {
            "path_specs": [{"include": "s3://my-bucket/raw/*.csv"}],
            "aws_config": {"aws_role": "arn:aws:iam::123456789012:role/probe-role"},
        }
    )
    with (
        mock.patch("datahub.ingestion.source.aws.aws_common.assume_role") as assume,
        mock.patch(
            "datahub.ingestion.source.aws.aws_common.get_current_identity"
        ) as identity,
    ):
        with S3MetadataProbe.for_config(config):
            pass
    assume.assert_not_called()
    identity.assert_not_called()


def test_legacy_bucket_names_are_accepted() -> None:
    probe = S3MetadataProbe.for_config(
        DataLakeSourceConfig.model_validate(_recipe("s3://my-bucket/raw/*.csv"))
    )
    assert probe._bucket_uri("Legacy_Bucket", "") == "s3://Legacy_Bucket/"
    with pytest.raises(ValueError, match="bucket name"):
        probe._bucket_uri("my-bucket/data", "")


def test_only_list_operations_reach_s3(buckets: None) -> None:
    """Metadata only, enforced by the code (and, for S3, by an IAM policy too)."""
    seen: List[str] = []
    recipe = _recipe("s3://my-bucket/data/{table}/*/*.csv")
    with _recording(seen):
        for command, kwargs in [
            ("buckets", {}),
            ("folders", {"bucket": "my-bucket"}),
            ("objects", {"bucket": "my-bucket"}),
            ("datasets", {}),
        ]:
            run_probe_method("s3", recipe, command, dict(kwargs))
    assert seen and set(seen) <= {"ListBuckets", "ListObjectsV2"}


@pytest.fixture
def tagged(buckets: None) -> Iterator[None]:
    client = boto3.client("s3", region_name="us-east-1")
    client.put_bucket_tagging(
        Bucket="my-bucket", Tagging={"TagSet": [{"Key": "team", "Value": "data"}]}
    )
    client.put_object_tagging(
        Bucket="my-bucket",
        Key="raw/a.csv",
        Tagging={"TagSet": [{"Key": "tier", "Value": "gold"}]},
    )
    yield


def _tag_recipe(**flags: bool) -> Dict[str, object]:
    return _recipe("s3://my-bucket/raw/*.csv", **flags)


def test_tags_reads_what_the_recipe_enables(tagged: None) -> None:
    result = run_probe_method(
        "s3",
        _tag_recipe(use_s3_bucket_tags=True, use_s3_object_tags=True),
        "tags",
        {"bucket": "my-bucket", "key": "raw/a.csv"},
    )
    assert result.result == {"bucket": ["team:data"], "object": ["tier:gold"]}


def test_tags_refuses_when_ingestion_would_read_none(tagged: None) -> None:
    with pytest.raises(ValueError, match="use_s3_bucket_tags"):
        run_probe_method("s3", _tag_recipe(), "tags", {"bucket": "my-bucket"})


def test_object_tags_need_the_object_flag(tagged: None) -> None:
    with pytest.raises(ValueError, match="use_s3_object_tags"):
        run_probe_method(
            "s3",
            _tag_recipe(use_s3_bucket_tags=True),
            "tags",
            {"bucket": "my-bucket", "key": "raw/a.csv"},
        )


def test_an_untagged_bucket_has_no_tags(buckets: None) -> None:
    result = run_probe_method(
        "s3", _tag_recipe(use_s3_bucket_tags=True), "tags", {"bucket": "my-bucket-2"}
    )
    assert result.result == {"bucket": [], "object": []}


def test_a_missing_object_is_a_caller_error(buckets: None) -> None:
    with pytest.raises(ValueError, match="no such object"):
        run_probe_method(
            "s3",
            _tag_recipe(use_s3_object_tags=True),
            "tags",
            {"bucket": "my-bucket", "key": "raw/missing.csv"},
        )


def test_a_denied_object_tag_read_is_a_failure(tagged: None) -> None:
    # Ingestion does not catch this one: a denied GetObjectTagging fails the run.
    denied = ClientError(
        {
            "Error": {"Code": "AccessDenied"},
            "ResponseMetadata": {"HTTPStatusCode": 403},
        },
        "GetObjectTagging",
    )
    with mock.patch(
        "datahub.ingestion.source.s3.s3_probe.get_object_tag_set", side_effect=denied
    ):
        result = run_probe_method(
            "s3",
            _tag_recipe(use_s3_object_tags=True),
            "tags",
            {"bucket": "my-bucket", "key": "raw/a.csv"},
        )
    assert result.failures


def test_tags_issues_only_tagging_reads(tagged: None) -> None:
    seen: List[str] = []
    with _recording(seen):
        run_probe_method(
            "s3",
            _tag_recipe(use_s3_bucket_tags=True, use_s3_object_tags=True),
            "tags",
            {"bucket": "my-bucket", "key": "raw/a.csv"},
        )
    assert seen and set(seen) <= {"GetBucketTagging", "GetObjectTagging"}


def test_object_tags_alone_need_a_key(tagged: None) -> None:
    # Without it there is nothing to read, and an empty answer would look like
    # "this bucket has no tags".
    with pytest.raises(ValueError, match="key"):
        run_probe_method(
            "s3", _tag_recipe(use_s3_object_tags=True), "tags", {"bucket": "my-bucket"}
        )
