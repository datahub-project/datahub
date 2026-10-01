import re
from typing import Any, Iterator, List
from unittest import mock

import boto3
import pytest
from botocore.exceptions import ClientError, EndpointConnectionError, ProfileNotFound
from moto import mock_aws

from datahub.ingestion.agent.verdicts import ProbeConnectionError
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
            "ResponseMetadata": {
                "RequestId": "",
                "HostId": "",
                "HTTPStatusCode": status,
                "HTTPHeaders": {},
                "RetryAttempts": 0,
            },
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
    # table-folder listing the base issues is denied. The wildcard before
    # {table} makes that one prefix among several.
    probe = _probe("s3://my-bucket/*/{table}/*/*.csv")
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


_ARN_MESSAGE = (
    "User: arn:aws:iam::123456789012:user/probe-user is not authorized to "
    "perform: s3:ListBucket"
)


def _objects_raising(exc: Exception) -> S3CompatibleMetadataProbe:
    probe = _probe()
    with mock.patch.object(
        object_store_probe, "list_objects_recursive_path", side_effect=exc
    ):
        probe.objects(bucket="my-bucket", limit=10)
    return probe


def test_an_sts_refusal_is_a_connection_error_not_a_denied_listing() -> None:
    # aws_role is assumed inside get_session on the first listing, so STS's
    # AccessDenied arrives where a listing 403 would.
    with pytest.raises(ProbeConnectionError, match="credential rejected"):
        _objects_raising(_client_error("AccessDenied", 403, operation="AssumeRole"))


def test_aws_error_text_never_reaches_the_output() -> None:
    with pytest.raises(ProbeConnectionError) as raised:
        _objects_raising(_client_error("InternalError", 500, message=_ARN_MESSAGE))
    assert "123456789012" not in str(raised.value)
    assert "InternalError" in str(raised.value)
    probe = _objects_raising(_client_error("AccessDenied", 403, message=_ARN_MESSAGE))
    assert probe.failures
    assert not any("123456789012" in f for f in probe.failures + probe.warnings)


def test_client_side_failures_name_the_class_not_the_endpoint_or_profile() -> None:
    with pytest.raises(ProbeConnectionError) as raised:
        _objects_raising(
            EndpointConnectionError(endpoint_url="https://private-host.example:9000")
        )
    assert "private-host" not in str(raised.value)
    assert "EndpointConnectionError" in str(raised.value)
    with pytest.raises(ProbeConnectionError) as raised:
        _objects_raising(ProfileNotFound(profile="my-secret-profile"))
    assert "my-secret-profile" not in str(raised.value)


def test_a_region_redirect_is_recorded_as_could_not_look() -> None:
    probe = _objects_raising(_client_error("PermanentRedirect", 301))
    assert any("aws_region" in f for f in probe.failures)


def test_a_missing_key_is_a_caller_error() -> None:
    with pytest.raises(ValueError, match="no such object"):
        _objects_raising(_client_error("NoSuchKey", 404))


def test_a_refusal_stops_every_command_before_any_request() -> None:
    probe = S3CompatibleMetadataProbe(
        _aws(), [PathSpec(include="s3://my-bucket/raw/*.csv")], refusal="not here"
    )
    with mock.patch.object(object_store_probe, "list_objects_recursive_path") as lister:
        with pytest.raises(ValueError, match="not here"):
            probe.objects(bucket="my-bucket", limit=1)
        with pytest.raises(ValueError, match="not here"):
            probe.datasets(limit=1)
        with pytest.raises(ValueError, match="not here"):
            probe.path_spec_folders(limit=1)
    lister.assert_not_called()


def test_a_subclass_can_widen_the_bucket_name_rule() -> None:
    class _Legacy(S3CompatibleMetadataProbe):
        bucket_name_pattern = re.compile(
            r"^[A-Za-z0-9][A-Za-z0-9._-]{1,253}[A-Za-z0-9]$"
        )

    with pytest.raises(ValueError, match="bucket name"):
        _probe()._bucket_uri("Legacy_Bucket", "")
    specs = [PathSpec(include="s3://my-bucket/raw/*.csv")]
    assert (
        _Legacy(_aws(), specs)._bucket_uri("Legacy_Bucket", "") == "s3://Legacy_Bucket/"
    )


def test_wildcard_resolution_stops_listing_dead_ends(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # `*/*/{table}` over many top-level folders with nothing below them: each
    # resolves to no prefix, so a budget on resolved prefixes alone never trips
    # while one listing per folder goes on.
    monkeypatch.setattr(object_store_probe, "MAX_RESOLVED_PREFIXES", 5)
    seen: List[str] = []
    real_client = boto3.session.Session.client

    def recording_client(self: boto3.session.Session, *args: Any, **kwargs: Any) -> Any:
        client = real_client(self, *args, **kwargs)
        client.meta.events.register(
            "before-call.s3", lambda model, **_: seen.append(model.name)
        )
        return client

    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="my-bucket")
        for i in range(40):
            client.put_object(Bucket="my-bucket", Key=f"top{i:02d}/x.csv", Body=b"a\n")
        probe = _probe("s3://my-bucket/*/*/{table}/*.csv")
        with mock.patch.object(boto3.session.Session, "client", recording_client):
            assert probe.datasets(limit=10) == []
    assert len(seen) <= 6
    assert any("narrow" in w for w in probe.warnings)


def test_a_denied_single_table_prefix_is_the_whole_answer(bucket: None) -> None:
    # No wildcard before {table}: the one listing is the answer, as in `objects`.
    probe = _probe("s3://my-bucket/data/{table}/*/*.csv")
    with mock.patch.object(
        object_store_probe,
        "list_folders_path",
        side_effect=_client_error("AccessDenied", 403),
    ):
        assert probe.datasets(limit=10) == []
    assert probe.failures and not probe.warnings


def _deny_listing_under(prefix: str) -> Any:
    """Deny resolve_templated_folders' listings of `prefix`, pass the rest."""
    from datahub.ingestion.source.s3 import source as s3_source

    real = s3_source.list_folders_path

    def listing(uri: str, *args: Any, **kwargs: Any) -> Any:
        if uri.startswith(prefix):
            raise _client_error("AccessDenied", 403)
        return real(uri, *args, **kwargs)

    return mock.patch.object(s3_source, "list_folders_path", side_effect=listing)


@pytest.mark.parametrize(
    "command", ["datasets", "path_spec_folders"], ids=["tables", "folders_only"]
)
def test_a_denied_listing_while_resolving_wildcards_is_a_warning(
    bucket: None, command: str
) -> None:
    # The first listing (the bucket) succeeds, so the answer is partial.
    spec = PathSpec(
        include="s3://my-bucket/*/*/{table}/*.csv"
        if command == "datasets"
        else "s3://my-bucket/*/*/*/",
        emit_folders_only=command == "path_spec_folders",
    )
    probe = S3CompatibleMetadataProbe(_aws(), [spec])
    with _deny_listing_under("s3://my-bucket/data/"):
        getattr(probe, command)(limit=10)
    assert probe.warnings and not probe.failures


def test_a_denied_first_listing_while_resolving_wildcards_is_a_failure(
    bucket: None,
) -> None:
    probe = _probe("s3://my-bucket/*/*/{table}/*.csv")
    with _deny_listing_under("s3://my-bucket/"):
        assert probe.datasets(limit=10) == []
    assert probe.failures and not probe.warnings
