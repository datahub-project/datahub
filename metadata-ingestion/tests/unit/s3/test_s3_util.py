from unittest.mock import Mock

from datahub.ingestion.source.aws.s3_util import group_s3_objects_by_dirname


def test_group_s3_objects_by_dirname():
    s3_objects = [
        Mock(key="/dir1/file1.txt"),
        Mock(key="/dir2/file2.txt"),
        Mock(key="/dir1/file3.txt"),
    ]

    grouped_objects = group_s3_objects_by_dirname(s3_objects)

    assert len(grouped_objects) == 2
    assert grouped_objects["/dir1"] == [s3_objects[0], s3_objects[2]]
    assert grouped_objects["/dir2"] == [s3_objects[1]]


def test_group_s3_objects_by_dirname_files_in_root_directory():
    s3_objects = [
        Mock(key="file1.txt"),
        Mock(key="file2.txt"),
    ]

    grouped_objects = group_s3_objects_by_dirname(s3_objects)

    assert len(grouped_objects) == 1
    assert grouped_objects["/"] == s3_objects
