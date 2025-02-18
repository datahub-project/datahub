import os
from typing import Any, Dict
from unittest import mock

import click
import pytest
from botocore.exceptions import ClientError

from datahub.cli import cli_utils
from datahub.cli.cli_utils import S3Path
from datahub.cli.config_utils import _get_config_from_env


def create_client_error(code: str = "404") -> ClientError:
    error_response: Dict[str, Any] = {
        "Error": {"Code": code, "Message": "Test error"},
        "ResponseMetadata": {
            "RequestId": "1234567890ABCDEF",
            "HostId": "",
            "HTTPStatusCode": 404 if code == "404" else 403,
            "HTTPHeaders": {"x-amz-request-id": "1234567890ABCDEF"},
            "RetryAttempts": 0,
        },
    }
    return ClientError(error_response, "TestOperation")  # type: ignore


@pytest.fixture
def s3_path():
    return S3Path()


@pytest.fixture
def mock_ctx():
    return mock.Mock()


@pytest.fixture
def mock_param():
    return mock.Mock()


def test_first_non_null():
    assert cli_utils.first_non_null([]) is None
    assert cli_utils.first_non_null([None]) is None
    assert cli_utils.first_non_null([None, "1"]) == "1"
    assert cli_utils.first_non_null([None, "1", "2"]) == "1"
    assert cli_utils.first_non_null(["3", "1", "2"]) == "3"
    assert cli_utils.first_non_null(["", "1", "2"]) == "1"
    assert cli_utils.first_non_null([" ", "1", "2"]) == "1"


@mock.patch.dict(os.environ, {"DATAHUB_GMS_HOST": "http://localhost:9092"})
def test_correct_url_when_gms_host_in_old_format():
    assert _get_config_from_env() == ("http://localhost:9092", None)


@mock.patch.dict(
    os.environ, {"DATAHUB_GMS_HOST": "localhost", "DATAHUB_GMS_PORT": "8080"}
)
def test_correct_url_when_gms_host_and_port_set():
    assert _get_config_from_env() == ("http://localhost:8080", None)


@mock.patch.dict(
    os.environ,
    {
        "DATAHUB_GMS_URL": "https://example.com",
        "DATAHUB_GMS_HOST": "localhost",
        "DATAHUB_GMS_PORT": "8080",
    },
)
def test_correct_url_when_gms_host_port_url_set():
    assert _get_config_from_env() == ("http://localhost:8080", None)


@mock.patch.dict(
    os.environ,
    {
        "DATAHUB_GMS_URL": "https://example.com",
        "DATAHUB_GMS_HOST": "localhost",
        "DATAHUB_GMS_PORT": "8080",
        "DATAHUB_GMS_PROTOCOL": "https",
    },
)
def test_correct_url_when_gms_host_port_url_protocol_set():
    assert _get_config_from_env() == ("https://localhost:8080", None)


@mock.patch.dict(
    os.environ,
    {
        "DATAHUB_GMS_URL": "https://example.com",
    },
)
def test_correct_url_when_url_set():
    assert _get_config_from_env() == ("https://example.com", None)


def test_fixup_gms_url():
    assert cli_utils.fixup_gms_url("http://localhost:8080") == "http://localhost:8080"
    assert cli_utils.fixup_gms_url("http://localhost:8080/") == "http://localhost:8080"
    assert cli_utils.fixup_gms_url("http://abc.acryl.io") == "https://abc.acryl.io/gms"
    assert (
        cli_utils.fixup_gms_url("http://abc.acryl.io/api/gms")
        == "https://abc.acryl.io/gms"
    )
    assert (
        cli_utils.fixup_gms_url("http://abcd.acryl.io:8080")
        == "https://abcd.acryl.io/gms"
    )


def test_guess_frontend_url_from_gms_url():
    assert (
        cli_utils.guess_frontend_url_from_gms_url("http://localhost:8080")
        == "http://localhost:9002"
    )
    assert (
        cli_utils.guess_frontend_url_from_gms_url("http://localhost:8080/")
        == "http://localhost:9002"
    )
    assert (
        cli_utils.guess_frontend_url_from_gms_url("https://abc.acryl.io/gms")
        == "https://abc.acryl.io"
    )


@mock.patch("boto3.client")
def test_existing_s3_object(mock_boto3, s3_path, mock_param, mock_ctx):
    mock_s3 = mock.Mock()
    mock_boto3.return_value = mock_s3
    mock_s3.head_object.return_value = {}

    result = s3_path.convert("s3://mybucket/mykey", mock_param, mock_ctx)
    assert result == "s3://mybucket/mykey"
    mock_s3.head_object.assert_called_once_with(Bucket="mybucket", Key="mykey")


# Mock successful prefix check
@mock.patch("boto3.client")
def test_existing_s3_prefix(mock_boto3, s3_path, mock_param, mock_ctx):
    mock_s3 = mock.Mock()
    mock_boto3.return_value = mock_s3
    mock_s3.head_object.side_effect = create_client_error()
    mock_s3.list_objects_v2.return_value = {"Contents": [{"Key": "mykey/file1.txt"}]}

    result = s3_path.convert("s3://mybucket/mykey/", mock_param, mock_ctx)
    assert result == "s3://mybucket/mykey/"
    mock_s3.list_objects_v2.assert_called_once_with(
        Bucket="mybucket", Prefix="mykey/", MaxKeys=1
    )


# Mock nonexistent path
@mock.patch("boto3.client")
def test_nonexistent_s3_path(mock_boto3, s3_path, mock_param, mock_ctx):
    mock_s3 = mock.Mock()
    mock_boto3.return_value = mock_s3
    mock_s3.head_object.side_effect = create_client_error()
    mock_s3.list_objects_v2.return_value = {}

    with pytest.raises(click.BadParameter) as exc_info:
        s3_path.convert("s3://mybucket/nonexistent", mock_param, mock_ctx)
    assert "Neither S3 object nor prefix exists" in str(exc_info.value)


# Mock access denied
@mock.patch("boto3.client")
def test_s3_access_denied(mock_boto3, s3_path, mock_param, mock_ctx):
    mock_s3 = mock.Mock()
    mock_boto3.return_value = mock_s3
    mock_s3.head_object.side_effect = create_client_error("403")

    with pytest.raises(click.BadParameter) as exc_info:
        s3_path.convert("s3://mybucket/mykey", mock_param, mock_ctx)
    assert "Error checking S3 path" in str(exc_info.value)


# Mock connection error
@mock.patch("boto3.client")
def test_s3_connection_error(mock_boto3, s3_path, mock_param, mock_ctx):
    mock_boto3.side_effect = Exception("Connection failed")

    with pytest.raises(click.BadParameter) as exc_info:
        s3_path.convert("s3://mybucket/mykey", mock_param, mock_ctx)
    assert "Error accessing S3" in str(exc_info.value)


# Test local path handling
@mock.patch("click.Path.convert")
def test_local_path(mock_convert, s3_path, mock_param, mock_ctx):
    mock_convert.return_value = "/local/path"
    result = s3_path.convert("/local/path", mock_param, mock_ctx)
    assert result == "/local/path"
    mock_convert.assert_called_once_with("/local/path", mock_param, mock_ctx)


# Test various S3 path formats
@pytest.mark.parametrize(
    "s3_path_str",
    [
        "s3://bucket/key",
        "s3://bucket/key/",
        "s3://bucket/path/to/key",
        "s3://bucket/path/to/key/",
    ],
)
@mock.patch("boto3.client")
def test_various_s3_path_formats(
    mock_boto3, s3_path, mock_param, mock_ctx, s3_path_str
):
    mock_s3 = mock.Mock()
    mock_boto3.return_value = mock_s3
    mock_s3.head_object.return_value = {}

    result = s3_path.convert(s3_path_str, mock_param, mock_ctx)
    assert result == s3_path_str
