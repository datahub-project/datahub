from datetime import datetime
from typing import List, Tuple
from unittest.mock import Mock, call

import pytest

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


def _get_s3_source(path_spec_: PathSpec) -> S3Source:
    return S3Source.create(
        config_dict={
            "path_spec": {
                "include": path_spec_.include,
                "table_name": path_spec_.table_name,
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


def test_get_folder_info_returns_latest_file_in_each_folder() -> None:
    """
    Test S3Source.get_folder_info returns the latest file in each folder
    """
    # arrange
    path_spec = PathSpec(
        include="s3://my-bucket/{table}/{partition0}/*.csv",
        table_name="{table}",
    )

    bucket = Mock()
    bucket.objects.filter().page_size = Mock(
        return_value=[
            Mock(
                bucket_name="my-bucket",
                key="my-folder/dir1/0001.csv",
                creation_time=datetime(2025, 1, 1, 1),
                last_modified=datetime(2025, 1, 1, 1),
                size=100,
            ),
            Mock(
                bucket_name="my-bucket",
                key="my-folder/dir2/0001.csv",
                creation_time=datetime(2025, 1, 1, 2),
                last_modified=datetime(2025, 1, 1, 2),
                size=100,
            ),
            Mock(
                bucket_name="my-bucket",
                key="my-folder/dir1/0002.csv",
                creation_time=datetime(2025, 1, 1, 2),
                last_modified=datetime(2025, 1, 1, 2),
                size=100,
            ),
        ]
    )

    # act
    res = _get_s3_source(path_spec).get_folder_info(
        path_spec, bucket, prefix="/my-folder"
    )
    res = list(res)

    # assert
    assert len(res) == 2
    assert res[0].sample_file == "s3://my-bucket/my-folder/dir1/0002.csv"
    assert res[1].sample_file == "s3://my-bucket/my-folder/dir2/0001.csv"


def test_get_folder_info_ignores_disallowed_path(
    caplog: pytest.LogCaptureFixture,
) -> None:
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

    bucket = Mock()
    bucket.objects.filter().page_size = Mock(
        return_value=[
            Mock(
                bucket_name="my-bucket",
                key="my-folder/ignore/this/path/0001.csv",
                creation_time=datetime(2025, 1, 1, 1),
                last_modified=datetime(2025, 1, 1, 1),
                size=100,
            ),
        ]
    )

    s3_source = _get_s3_source(path_spec)

    # act
    res = s3_source.get_folder_info(path_spec, bucket, prefix="/my-folder")
    res = list(res)

    # assert
    expected_called_s3_uri = "s3://my-bucket/my-folder/ignore/this/path/0001.csv"

    assert path_spec.allowed.call_args_list == [call(expected_called_s3_uri)], (
        "File should be checked if it's allowed"
    )
    assert f"File {expected_called_s3_uri} not allowed and skipping" in caplog.text, (
        "Dropped file should be logged"
    )
    assert s3_source.get_report().filtered == [expected_called_s3_uri], (
        "Dropped file should be in the report.filtered"
    )
    assert res == [], "Dropped file should not be in the result"


def test_get_folder_info_returns_expected_folder() -> None:
    # arrange
    path_spec = PathSpec(
        include="s3://my-bucket/{table}/{partition0}/*.csv",
        table_name="{table}",
    )

    bucket = Mock()
    bucket.objects.filter().page_size = Mock(
        return_value=[
            Mock(
                bucket_name="my-bucket",
                key="my-folder/dir1/0001.csv",
                creation_time=datetime(2025, 1, 1, 1),
                last_modified=datetime(2025, 1, 1, 1),
                size=100,
            ),
            Mock(
                bucket_name="my-bucket",
                key="my-folder/dir1/0002.csv",
                creation_time=datetime(2025, 1, 1, 2),
                last_modified=datetime(2025, 1, 1, 2),
                size=50,
            ),
        ]
    )

    # act
    res = _get_s3_source(path_spec).get_folder_info(
        path_spec, bucket, prefix="/my-folder"
    )
    res = list(res)

    # assert
    assert len(res) == 1
    assert res[0] == Folder(
        partition_id=[("partition0", "dir1")],
        is_partition=True,
        creation_time=datetime(2025, 1, 1, 1),
        modification_time=datetime(2025, 1, 1, 2),
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
