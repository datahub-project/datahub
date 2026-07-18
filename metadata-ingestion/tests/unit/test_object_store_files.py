from unittest import mock

import pytest

from datahub.ingestion.source.common.object_store_files import (
    expand_local_glob,
    expand_object_store_glob,
    has_glob_characters,
    is_http_uri,
    read_file_as_bytes,
)


def _mock_connection_with_pages(pages):
    connection = mock.MagicMock()
    s3_client = mock.MagicMock()
    connection.get_s3_client.return_value = s3_client
    paginator = mock.MagicMock()
    s3_client.get_paginator.return_value = paginator
    paginator.paginate.return_value = pages
    return connection, s3_client, paginator


def test_has_glob_characters():
    assert has_glob_characters("s3://bucket/results/*/run_results.json")
    assert has_glob_characters("s3://bucket/results/?/run_results.json")
    assert has_glob_characters("/local/path/[abc]/file.json")
    assert not has_glob_characters("s3://bucket/results/run_results.json")
    assert not has_glob_characters("/simple/path/file.json")


def test_is_http_uri():
    assert is_http_uri("http://example.com/contract.yaml")
    assert is_http_uri("https://example.com/contract.yaml")
    assert not is_http_uri("s3://bucket/contract.yaml")
    assert not is_http_uri("/local/contract.yaml")


def test_expand_local_glob(tmp_path):
    (tmp_path / "a.json").write_text("{}")
    (tmp_path / "b.json").write_text("{}")
    (tmp_path / "c.txt").write_text("nope")

    result = expand_local_glob(str(tmp_path / "*.json"))
    assert result == [str(tmp_path / "a.json"), str(tmp_path / "b.json")]


def test_expand_s3_glob():
    s3_objects = [
        {"Key": "results/model_a/run_results.json"},
        {"Key": "results/model_b/run_results.json"},
        {"Key": "results/model_c/run_results.json"},
        {"Key": "results/model_a/manifest.json"},
        {"Key": "results/other_file.json"},
    ]
    connection, s3_client, paginator = _mock_connection_with_pages(
        [{"Contents": s3_objects}]
    )

    result = expand_object_store_glob(
        "s3://my-bucket/results/*/run_results.json", connection, "s3"
    )

    assert result == [
        "s3://my-bucket/results/model_a/run_results.json",
        "s3://my-bucket/results/model_b/run_results.json",
        "s3://my-bucket/results/model_c/run_results.json",
    ]
    s3_client.get_paginator.assert_called_once_with("list_objects_v2")
    paginator.paginate.assert_called_once_with(Bucket="my-bucket", Prefix="results/")


def test_expand_s3_glob_no_matches():
    connection, _, _ = _mock_connection_with_pages([{"Contents": []}])
    result = expand_object_store_glob(
        "s3://my-bucket/nonexistent/*/run_results.json", connection, "s3"
    )
    assert result == []


def test_expand_s3_glob_prefix_calculation():
    connection, _, paginator = _mock_connection_with_pages([{"Contents": []}])
    expand_object_store_glob(
        "s3://bucket/a/b/c/*/d/*/run_results.json", connection, "s3"
    )
    paginator.paginate.assert_called_with(Bucket="bucket", Prefix="a/b/c/")


def test_expand_s3_glob_multiple_pages():
    connection, _, _ = _mock_connection_with_pages(
        [
            {"Contents": [{"Key": "results/model_a/run_results.json"}]},
            {"Contents": [{"Key": "results/model_b/run_results.json"}]},
            {"Contents": [{"Key": "results/model_c/run_results.json"}]},
        ]
    )
    result = expand_object_store_glob(
        "s3://bucket/results/*/run_results.json", connection, "s3"
    )
    assert result == [
        "s3://bucket/results/model_a/run_results.json",
        "s3://bucket/results/model_b/run_results.json",
        "s3://bucket/results/model_c/run_results.json",
    ]


def test_expand_s3_glob_wildcard_at_root():
    connection, _, paginator = _mock_connection_with_pages(
        [
            {
                "Contents": [
                    {"Key": "run_results_a.json"},
                    {"Key": "run_results_b.json"},
                    {"Key": "other.txt"},
                ]
            }
        ]
    )
    result = expand_object_store_glob(
        "s3://bucket/run_results_*.json", connection, "s3"
    )
    paginator.paginate.assert_called_with(Bucket="bucket", Prefix="")
    assert result == [
        "s3://bucket/run_results_a.json",
        "s3://bucket/run_results_b.json",
    ]


def test_expand_s3_glob_client_error():
    from botocore.exceptions import ClientError

    connection, _, paginator = _mock_connection_with_pages([])
    paginator.paginate.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}},
        "ListObjectsV2",
    )
    with pytest.raises(ClientError, match="Access Denied"):
        expand_object_store_glob(
            "s3://bucket/results/*/run_results.json", connection, "s3"
        )


def test_expand_s3_glob_no_cross_slash_matching():
    connection, _, _ = _mock_connection_with_pages(
        [
            {
                "Contents": [
                    {"Key": "results/a/run_results.json"},
                    {"Key": "results/a/b/run_results.json"},
                    {"Key": "results/a/b/c/run_results.json"},
                ]
            }
        ]
    )
    result = expand_object_store_glob(
        "s3://bucket/results/*/run_results.json", connection, "s3"
    )
    assert result == ["s3://bucket/results/a/run_results.json"]


def test_expand_object_store_glob_gcs():
    gcs_objects = [
        {"Key": "dbt/model_a/run_results.json"},
        {"Key": "dbt/model_b/run_results.json"},
        {"Key": "dbt/model_b/manifest.json"},
    ]
    connection, s3_client, paginator = _mock_connection_with_pages(
        [{"Contents": gcs_objects}]
    )

    result = expand_object_store_glob(
        "gs://my-gcs-bucket/dbt/*/run_results.json", connection, "gs"
    )

    assert result == [
        "gs://my-gcs-bucket/dbt/model_a/run_results.json",
        "gs://my-gcs-bucket/dbt/model_b/run_results.json",
    ]
    s3_client.get_paginator.assert_called_once_with("list_objects_v2")
    paginator.paginate.assert_called_once_with(Bucket="my-gcs-bucket", Prefix="dbt/")


def test_read_file_as_bytes_local(tmp_path):
    path = tmp_path / "contract.yaml"
    path.write_text("apiVersion: v3")
    assert read_file_as_bytes(str(path)) == b"apiVersion: v3"


def test_read_file_as_bytes_http():
    with mock.patch(
        "datahub.ingestion.source.common.object_store_files.requests.get"
    ) as mock_get:
        mock_get.return_value = mock.MagicMock(content=b"payload")
        assert read_file_as_bytes("https://example.com/contract.yaml") == b"payload"
        mock_get.return_value.raise_for_status.assert_called_once()


def test_read_file_as_bytes_s3():
    connection = mock.MagicMock()
    s3_client = mock.MagicMock()
    connection.get_s3_client.return_value = s3_client
    s3_client.get_object.return_value = {
        "Body": mock.MagicMock(read=mock.MagicMock(return_value=b'{"key": "value"}'))
    }
    result = read_file_as_bytes(
        "s3://my-bucket/path/to/manifest.json", aws_connection=connection
    )
    assert result == b'{"key": "value"}'
    s3_client.get_object.assert_called_once_with(
        Bucket="my-bucket", Key="path/to/manifest.json"
    )


def test_read_file_as_bytes_s3_missing_connection():
    with pytest.raises(ValueError, match="AWS connection required"):
        read_file_as_bytes("s3://my-bucket/manifest.json")


def test_read_file_as_bytes_gcs_missing_connection():
    with pytest.raises(ValueError, match="GCS connection required"):
        read_file_as_bytes("gs://my-bucket/manifest.json")
