import pytest
from datahub.ingestion.fs.fs_base import get_path_schema


@pytest.mark.parametrize(
    "path,expected_schema",
    [
        (r"C:\Users\foo\bar.json", "file"),
        ("C:/Users/foo/bar.json", "file"),
        (r"D:\data\table.csv", "file"),
        ("/tmp/foo/bar.json", "file"),
        ("s3://my-bucket/key.json", "s3"),
        ("https://example.com/file.json", "https"),
        # Boundary case: no path separator after the colon, so this must
        # NOT be treated as a Windows drive letter — falls through to the
        # existing urlparse behavior instead.
        ("c:relative", "c"),
    ],
)
def test_get_path_schema(path: str, expected_schema: str) -> None:
    assert get_path_schema(path) == expected_schema
