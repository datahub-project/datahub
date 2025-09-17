import logging
from datetime import datetime, timezone
from typing import List, Tuple
from unittest.mock import Mock, call

import pytest
from boto3.session import Session
from freezegun import freeze_time
from moto import mock_s3

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.data_lake_common.data_lake_utils import ContainerWUCreator
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec
from datahub.ingestion.source.s3.source import (
    Folder,
    S3Source,
    partitioned_folder_comparator,
)

logging.getLogger("boto3").setLevel(logging.INFO)
logging.getLogger("botocore").setLevel(logging.INFO)
logging.getLogger("s3transfer").setLevel(logging.INFO)


@pytest.fixture(autouse=True)
def s3():
    with mock_s3():
        conn = Session(
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
        )
        yield conn


@pytest.fixture(autouse=True)
def s3_client(s3):
    yield s3.client("s3")


@pytest.fixture(autouse=True)
def s3_resource(s3):
    yield s3.resource("s3")


def _get_s3_source(path_spec_: PathSpec) -> S3Source:
    return S3Source.create(
        config_dict={
            "path_spec": {
                "include": path_spec_.include,
                "table_name": path_spec_.table_name,
            },
            "aws_config": {
                "aws_access_key_id": "test",
                "aws_secret_access_key": "test",
            },
        },
        ctx=PipelineContext(run_id="test-s3"),
    )


def test_partition_comparator_numeric_folder_name():
    folder1 = "3"
    folder2 = "12"
    assert partitioned_folder_comparator(folder1, folder2) == -1


def test_partition_multi_level_key():
    folder1 = "backup/metadata_aspect_v2/year=2023/month=01"
    folder2 = "backup/metadata_aspect_v2/year=2023/month=2"
    assert partitioned_folder_comparator(folder1, folder2) == -1


def test_partition_comparator_numeric_folder_name2():
    folder1 = "12"
    folder2 = "3"
    assert partitioned_folder_comparator(folder1, folder2) == 1


def test_partition_comparator_string_folder():
    folder1 = "myfolderB"
    folder2 = "myFolderA"
    assert partitioned_folder_comparator(folder1, folder2) == 1


def test_partition_comparator_string_same_folder():
    folder1 = "myFolderA"
    folder2 = "myFolderA"
    assert partitioned_folder_comparator(folder1, folder2) == 0


def test_partition_comparator_with_numeric_partition():
    folder1 = "year=3"
    folder2 = "year=12"
    assert partitioned_folder_comparator(folder1, folder2) == -1


def test_partition_comparator_with_padded_numeric_partition():
    folder1 = "year=03"
    folder2 = "year=12"
    assert partitioned_folder_comparator(folder1, folder2) == -1


def test_partition_comparator_with_equal_sign_in_name():
    folder1 = "month=12"
    folder2 = "year=0"
    assert partitioned_folder_comparator(folder1, folder2) == -1


def test_partition_comparator_with_string_partition():
    folder1 = "year=year2020"
    folder2 = "year=year2021"
    assert partitioned_folder_comparator(folder1, folder2) == -1


def test_path_spec():
    path_spec = PathSpec(
        include="s3://my-bucket/my-folder/year=*/month=*/day=*/*.csv",
        default_extension="csv",
    )
    path = "s3://my-bucket/my-folder/year=2022/month=10/day=11/my_csv.csv"
    assert path_spec.allowed(path)


def test_path_spec_with_double_star_ending():
    path_spec = PathSpec(
        include="s3://my-bucket/{table}/**",
        default_extension="csv",
        allow_double_stars=True,
    )
    path = "s3://my-bucket/my-folder/year=2022/month=10/day=11/my_csv.csv"
    assert path_spec.allowed(path)
    vars = path_spec.get_named_vars(path)
    assert vars
    assert vars["table"] == "my-folder"


@pytest.mark.parametrize(
    "path_spec,path, expected",
    [
        pytest.param(
            "s3://my-bucket/{table}/**",
            "s3://my-bucket/my-folder/year=2022/month=10/day=11/my_csv",
            [("year", "2022"), ("month", "10"), ("day", "11")],
            id="autodetect_partitions",
        ),
        pytest.param(
            "s3://my-bucket/{table}/{partition_key[0]}={partition_value[0]}/{partition_key[1]}={partition_value[1]}/{partition_key[2]}={partition_value[2]}/*.csv",
            "s3://my-bucket/my-folder/year=2022/month=10/day=11/my_csv.csv",
            [("year", "2022"), ("month", "10"), ("day", "11")],
            id="partition_key and value set",
        ),
        pytest.param(
            "s3://my-bucket/{table}/{partition_key[0]}={partition[0]}/{partition_key[1]}={partition[1]}/{partition_key[2]}={partition[2]}/*.csv",
            "s3://my-bucket/my-folder/year=2022/month=10/day=11/my_csv.csv",
            [("year", "2022"), ("month", "10"), ("day", "11")],
            id="partition_key and partition set",
        ),
        pytest.param(
            "s3://my-bucket/{table}/{year}/{month}/{day}/*.csv",
            "s3://my-bucket/my-folder/2022/10/11/my_csv.csv",
            [("year", "2022"), ("month", "10"), ("day", "11")],
            id="named partition keys",
        ),
        pytest.param(
            "s3://my-bucket/{table}/{part[0]}/{part[1]}/{part[2]}/*.csv",
            "s3://my-bucket/my-folder/2022/10/11/my_csv.csv",
            [("part_0", "2022"), ("part_1", "10"), ("part_2", "11")],
            id="indexed partition keys",
        ),
        pytest.param(
            "s3://my-bucket/{table}/**",
            "s3://my-bucket/my-folder/2022/10/11/my_csv.csv",
            [("partition_0", "2022"), ("partition_1", "10"), ("partition_2", "11")],
            id="partition autodetect with partition values only",
        ),
        pytest.param(
            "s3://my-bucket/{table}/**",
            "s3://my-bucket/my-folder/my_csv.csv",
            None,
            id="partition autodetect with non partitioned path",
        ),
    ],
)
def test_path_spec_partition_detection(
    path_spec: str, path: str, expected: List[Tuple[str, str]]
) -> None:
    ps = PathSpec(include=path_spec, default_extension="csv", allow_double_stars=True)
    assert ps.allowed(path)
    partitions = ps.get_partition_from_path(path)
    assert partitions == expected


def test_path_spec_dir_allowed():
    path_spec = PathSpec(
        include="s3://my-bucket/my-folder/year=*/month=*/day=*/*.csv",
        exclude=[
            "s3://my-bucket/my-folder/year=2022/month=12/day=11",
            "s3://my-bucket/my-folder/year=2022/month=10/**",
        ],
        default_extension="csv",
    )
    path = "s3://my-bucket/my-folder/year=2022/"
    assert path_spec.dir_allowed(path) is True, f"{path} should be allowed"

    path = "s3://my-bucket/my-folder/year=2022/month=12/"
    assert path_spec.dir_allowed(path) is True, f"{path} should be allowed"

    path = "s3://my-bucket/my-folder/year=2022/month=12/day=11/my_csv.csv"
    assert path_spec.dir_allowed(path) is False, f"{path} should be denied"

    path = "s3://my-bucket/my-folder/year=2022/month=12/day=10/"
    assert path_spec.dir_allowed(path) is True, f"{path} should be allowed"

    path = "s3://my-bucket/my-folder/year=2022/month=12/day=10/_temporary/"
    assert path_spec.dir_allowed(path) is False, f"{path} should be denied"

    path = "s3://my-bucket/my-folder/year=2022/month=10/day=10/"
    assert path_spec.dir_allowed(path) is False, f"{path} should be denied"


def test_container_generation_without_folders():
    cwu = ContainerWUCreator("s3", None, "PROD")
    mcps = cwu.create_container_hierarchy(
        "s3://my-bucket/my-file.json.gz", "urn:li:dataset:123"
    )

    def container_properties_filter(x: MetadataWorkUnit) -> bool:
        assert isinstance(x.metadata, MetadataChangeProposalWrapper)
        return x.metadata.aspectName == "containerProperties"

    container_properties: List = list(filter(container_properties_filter, mcps))
    assert len(container_properties) == 1
    assert container_properties[0].metadata.aspect.customProperties == {
        "bucket_name": "my-bucket",
        "env": "PROD",
        "platform": "s3",
    }


def test_container_generation_with_folder():
    cwu = ContainerWUCreator("s3", None, "PROD")
    mcps = cwu.create_container_hierarchy(
        "s3://my-bucket/my-dir/my-file.json.gz", "urn:li:dataset:123"
    )

    def container_properties_filter(x: MetadataWorkUnit) -> bool:
        assert isinstance(x.metadata, MetadataChangeProposalWrapper)
        return x.metadata.aspectName == "containerProperties"

    container_properties: List = list(filter(container_properties_filter, mcps))
    assert len(container_properties) == 2
    assert container_properties[0].metadata.aspect.customProperties == {
        "bucket_name": "my-bucket",
        "env": "PROD",
        "platform": "s3",
    }
    assert container_properties[1].metadata.aspect.customProperties == {
        "env": "PROD",
        "folder_abs_path": "my-bucket/my-dir",
        "platform": "s3",
    }


def test_container_generation_with_multiple_folders():
    cwu = ContainerWUCreator("s3", None, "PROD")
    mcps = cwu.create_container_hierarchy(
        "s3://my-bucket/my-dir/my-dir2/my-file.json.gz", "urn:li:dataset:123"
    )

    def container_properties_filter(x: MetadataWorkUnit) -> bool:
        assert isinstance(x.metadata, MetadataChangeProposalWrapper)
        return x.metadata.aspectName == "containerProperties"

    container_properties: List = list(filter(container_properties_filter, mcps))

    assert len(container_properties) == 3
    assert container_properties[0].metadata.aspect.customProperties == {
        "bucket_name": "my-bucket",
        "env": "PROD",
        "platform": "s3",
    }
    assert container_properties[1].metadata.aspect.customProperties == {
        "env": "PROD",
        "folder_abs_path": "my-bucket/my-dir",
        "platform": "s3",
    }
    assert container_properties[2].metadata.aspect.customProperties == {
        "env": "PROD",
        "folder_abs_path": "my-bucket/my-dir/my-dir2",
        "platform": "s3",
    }


def test_get_folder_info_returns_latest_file_in_each_folder(s3_resource):
    """
    Test S3Source.get_folder_info returns the latest file in each folder
    """
    # arrange
    path_spec = PathSpec(
        include="s3://my-bucket/{table}/{partition0}/*.csv",
        table_name="{table}",
    )

    bucket = s3_resource.Bucket("my-bucket")
    bucket.create()
    with freeze_time("2025-01-01 01:00:00"):
        bucket.put_object(Key="my-folder/dir1/0001.csv")
    with freeze_time("2025-01-01 02:00:00"):
        bucket.put_object(Key="my-folder/dir1/0002.csv")
        bucket.put_object(Key="my-folder/dir2/0001.csv")

    # act
    res = _get_s3_source(path_spec).get_folder_info(
        path_spec,
        "s3://my-bucket/my-folder",
    )
    res = list(res)

    # assert
    assert len(res) == 2
    assert res[0].sample_file == "s3://my-bucket/my-folder/dir1/0002.csv"
    assert res[1].sample_file == "s3://my-bucket/my-folder/dir2/0001.csv"


def test_get_folder_info_ignores_disallowed_path(s3_resource, caplog):
    """
    Test S3Source.get_folder_info skips disallowed files and logs a message
    """
    # arrange
    path_spec = Mock(
        spec=PathSpec,
        include="s3://my-bucket/{table}/{partition0}/*.csv",
        table_name="{table}",
    )
    path_spec.allowed = Mock(return_value=False)

    bucket = s3_resource.Bucket("my-bucket")
    bucket.create()
    bucket.put_object(Key="my-folder/ignore/this/path/0001.csv")

    s3_source = _get_s3_source(path_spec)

    # act
    res = s3_source.get_folder_info(path_spec, "s3://my-bucket/my-folder")
    res = list(res)

    # assert
    expected_called_s3_uri = "s3://my-bucket/my-folder/ignore/this/path/0001.csv"

    assert path_spec.allowed.call_args_list == [call(expected_called_s3_uri)], (
        "File should be checked if it's allowed"
    )
    assert f"File {expected_called_s3_uri} not allowed and skipping" in caplog.text, (
        "Dropped file should be logged"
    )
    assert list(s3_source.get_report().filtered) == [expected_called_s3_uri], (
        "Dropped file should be in the report.filtered"
    )
    assert res == [], "Dropped file should not be in the result"


def test_get_folder_info_returns_expected_folder(s3_resource):
    # arrange
    path_spec = PathSpec(
        include="s3://my-bucket/{table}/{partition0}/*.csv",
        table_name="{table}",
    )

    bucket = s3_resource.Bucket("my-bucket")
    bucket.create()
    with freeze_time("2025-01-01 01:00:00"):
        bucket.put_object(Key="my-folder/dir1/0001.csv")
    with freeze_time("2025-01-01 02:00:00"):
        bucket.put_object(Key="my-folder/dir1/0002.csv", Body=" " * 150)

    # act
    res = _get_s3_source(path_spec).get_folder_info(
        path_spec,
        "s3://my-bucket/my-folder",
    )
    res = list(res)

    # assert
    assert len(res) == 1
    assert res[0] == Folder(
        partition_id=[("partition0", "dir1")],
        is_partition=True,
        creation_time=datetime(2025, 1, 1, 1, tzinfo=timezone.utc),
        modification_time=datetime(2025, 1, 1, 2, tzinfo=timezone.utc),
        size=150,
        sample_file="s3://my-bucket/my-folder/dir1/0002.csv",
    )


def test_s3_region_in_external_url():
    """Test that AWS region is properly used in external URLs."""
    source = S3Source.create(
        config_dict={
            "path_specs": [
                {
                    "include": "s3://my-bucket/my-folder/*.csv",
                }
            ],
            "aws_config": {
                "aws_region": "eu-west-1",  # Use a non-default region
                "aws_access_key_id": "test-key",
                "aws_secret_access_key": "test-secret",
            },
        },
        ctx=PipelineContext(run_id="test-s3"),
    )

    # Create a mock table data
    table_data = Mock()
    table_data.table_path = "s3://production-bucket/folder/my_data.csv"
    table_data.full_path = "s3://production-bucket/folder/my_data.csv"

    # Get the external URL
    external_url = source.get_external_url(table_data)

    # Verify that the correct region is used (not default us-east-1)
    assert "https://eu-west-1.console.aws.amazon.com" in external_url
    assert "production-bucket" in external_url
    assert "folder" in external_url


class TestResolveTemplatedFolders:
    """Test suite for the resolve_templated_folders method."""

    def test_resolve_templated_folders_no_wildcard(self):
        """Test that paths without wildcards are returned as-is."""
        # arrange
        path_spec = PathSpec(include="s3://my-bucket/data/files/*.csv")
        s3_source = _get_s3_source(path_spec)
        bucket_name = "my-bucket"
        prefix = "data/files/"

        # act
        result = list(
            s3_source.resolve_templated_folders(f"s3://{bucket_name}/{prefix}")
        )

        # assert
        assert result == ["s3://my-bucket/data/files/"]

    def test_resolve_templated_folders_single_wildcard(self, s3_client):
        """Test resolution of a single wildcard in the path."""
        # arrange
        path_spec = PathSpec(include="s3://my-bucket/data/*/files/*.csv")
        s3_source = _get_s3_source(path_spec)
        bucket_name = "my-bucket"
        prefix = "data/*/files/"

        s3_client.create_bucket(Bucket="my-bucket")
        s3_client.put_object(Bucket="my-bucket", Key="data/2023/files/test.csv")
        s3_client.put_object(Bucket="my-bucket", Key="data/2024/files/test.csv")

        # act
        result = list(
            s3_source.resolve_templated_folders(f"s3://{bucket_name}/{prefix}")
        )

        # assert
        expected = [
            "s3://my-bucket/data/2023/files/",
            "s3://my-bucket/data/2024/files/",
        ]
        assert result == expected

    def test_resolve_templated_folders_nested_wildcards(self, s3_client):
        """Test resolution of nested wildcards in the path."""
        # arrange
        path_spec = PathSpec(include="s3://my-bucket/data/*/year=*/files/*.csv")
        s3_source = _get_s3_source(path_spec)
        bucket_name = "my-bucket"
        prefix = "data/*/year=*/files/"

        s3_client.create_bucket(Bucket="my-bucket")
        s3_client.put_object(
            Bucket="my-bucket", Key="data/logs/year=2023/files/test.csv"
        )
        s3_client.put_object(
            Bucket="my-bucket", Key="data/logs/year=2024/files/test.csv"
        )
        s3_client.put_object(
            Bucket="my-bucket", Key="data/metrics/year=2023/files/test.csv"
        )
        s3_client.put_object(
            Bucket="my-bucket", Key="data/metrics/year=2024/files/test.csv"
        )

        # act
        result = list(
            s3_source.resolve_templated_folders(f"s3://{bucket_name}/{prefix}")
        )

        # assert
        expected = [
            "s3://my-bucket/data/logs/year=2023/files/",
            "s3://my-bucket/data/logs/year=2024/files/",
            "s3://my-bucket/data/metrics/year=2023/files/",
            "s3://my-bucket/data/metrics/year=2024/files/",
        ]
        assert result == expected

    def test_resolve_templated_folders_empty_folders(self, s3_client):
        """Test handling when list_folders returns empty results."""
        # arrange
        path_spec = PathSpec(include="s3://my-bucket/data/*/files/*.csv")
        s3_source = _get_s3_source(path_spec)
        bucket_name = "my-bucket"
        prefix = "data/*/files/"

        s3_client.create_bucket(Bucket="my-bucket")

        # act
        result = list(
            s3_source.resolve_templated_folders(f"s3://{bucket_name}/{prefix}")
        )

        # assert
        assert result == []

    def test_resolve_templated_folders_path_normalization(self, s3_client):
        """Test that path normalization handles slashes correctly."""
        # arrange
        path_spec = PathSpec(include="s3://my-bucket/data/*/files/*.csv")
        s3_source = _get_s3_source(path_spec)
        bucket_name = "my-bucket"
        prefix = "data/*/files/"

        s3_client.create_bucket(Bucket="my-bucket")
        s3_client.put_object(Bucket="my-bucket", Key="data/folder1/files/test.csv")
        s3_client.put_object(Bucket="my-bucket", Key="data/folder2/files/test.csv")

        # act
        result = list(
            s3_source.resolve_templated_folders(f"s3://{bucket_name}/{prefix}")
        )

        # assert
        expected = [
            "s3://my-bucket/data/folder1/files/",
            "s3://my-bucket/data/folder2/files/",
        ]
        assert result == expected

    def test_resolve_templated_folders_remaining_pattern_with_leading_slash(
        self, s3_client
    ):
        """Test handling when remaining pattern starts with a slash."""
        # arrange
        path_spec = PathSpec(include="s3://my-bucket/data/*")
        s3_source = _get_s3_source(path_spec)
        bucket_name = "my-bucket"
        prefix = "data/*/subdir/"

        s3_client.create_bucket(Bucket="my-bucket")
        s3_client.put_object(Bucket="my-bucket", Key="data/2023/subdir/test.csv")
        s3_client.put_object(Bucket="my-bucket", Key="data/2024/subdir/test.csv")

        # act
        result = list(
            s3_source.resolve_templated_folders(f"s3://{bucket_name}/{prefix}")
        )

        # assert
        expected = [
            "s3://my-bucket/data/2023/subdir/",
            "s3://my-bucket/data/2024/subdir/",
        ]
        assert result == expected

    def test_resolve_templated_folders_complex_nested_pattern(self, s3_client):
        """Test complex nested pattern with multiple wildcards."""
        # arrange
        path_spec = PathSpec(include="s3://my-bucket/logs/*/region=*/day=*/*.json")
        s3_source = _get_s3_source(path_spec)
        bucket_name = "my-bucket"
        prefix = "logs/*/region=*/day=*/"

        s3_client.create_bucket(Bucket="my-bucket")
        s3_client.put_object(
            Bucket="my-bucket",
            Key="logs/app1/region=eu-west-1/day=2024-01-01/test.json",
        )
        s3_client.put_object(
            Bucket="my-bucket",
            Key="logs/app1/region=us-east-1/day=2024-01-01/test.json",
        )
        s3_client.put_object(
            Bucket="my-bucket",
            Key="logs/app2/region=us-east-1/day=2024-01-01/test.json",
        )

        # act
        result = list(
            s3_source.resolve_templated_folders(f"s3://{bucket_name}/{prefix}")
        )

        # assert
        expected = [
            "s3://my-bucket/logs/app1/region=eu-west-1/day=2024-01-01/",
            "s3://my-bucket/logs/app1/region=us-east-1/day=2024-01-01/",
            "s3://my-bucket/logs/app2/region=us-east-1/day=2024-01-01/",
        ]
        assert result == expected

    def test_resolve_templated_folders_wildcard_at_end(self, s3_client):
        """Test wildcard at the end of the path."""
        # arrange
        path_spec = PathSpec(include="s3://my-bucket/data/*")
        s3_source = _get_s3_source(path_spec)
        bucket_name = "my-bucket"
        prefix = "data/*"

        s3_client.create_bucket(Bucket="my-bucket")
        s3_client.put_object(Bucket="my-bucket", Key="data/folder1/test.csv")
        s3_client.put_object(Bucket="my-bucket", Key="data/folder2/test.csv")

        # act
        result = list(
            s3_source.resolve_templated_folders(f"s3://{bucket_name}/{prefix}")
        )

        # assert
        expected = ["s3://my-bucket/data/folder1/", "s3://my-bucket/data/folder2/"]
        assert result == expected

    def test_resolve_templated_buckets_single_wildcard(self, s3_client):
        """Test resolution of a single wildcard in the bucket path."""
        # arrange
        path_spec = PathSpec(include="s3://*/data/")
        s3_source = _get_s3_source(path_spec)

        s3_client.create_bucket(Bucket="my-bucket")
        s3_client.create_bucket(Bucket="my-bucket-1")

        # act
        result = list(s3_source.resolve_templated_folders("s3://*/data/"))

        # assert
        expected = ["s3://my-bucket/data/", "s3://my-bucket-1/data/"]
        assert result == expected

    def test_resolve_templated_buckets_wildcard_at_end(self, s3_client):
        """Test wildcard at the end of the bucket path."""
        # arrange
        path_spec = PathSpec(include="s3://my-*/data/")
        s3_source = _get_s3_source(path_spec)

        s3_client.create_bucket(Bucket="my-bucket")
        s3_client.create_bucket(Bucket="my-bucket-1")

        # act
        result = list(s3_source.resolve_templated_folders("s3://my-*/data/"))

        # assert
        expected = ["s3://my-bucket/data/", "s3://my-bucket-1/data/"]
        assert result == expected
