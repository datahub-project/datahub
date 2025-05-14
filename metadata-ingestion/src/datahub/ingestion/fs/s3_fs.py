from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, Iterable
from urllib.parse import urlparse

import boto3
import smart_open

from datahub.ingestion.fs import s3_fs
from datahub.ingestion.fs.fs_base import FileInfo, FileSystem


def parse_s3_path(path: str) -> "S3Path":
    parsed = urlparse(path)
    return S3Path(parsed.netloc, parsed.path.lstrip("/"))


def assert_ok_status(s3_response):
    is_ok = s3_response["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert is_ok, (
        f"Failed to fetch S3 object, error message: {s3_response['Error']['Message']}"
    )


@dataclass
class S3Path:
    bucket: str
    key: str

    def __str__(self):
        return f"S3Path({self.bucket}, {self.key})"


class S3ListIterator(Iterator):
    MAX_KEYS = 1000

    def __init__(
        self, s3_client: Any, bucket: str, prefix: str, max_keys: int = MAX_KEYS
    ) -> None:
        self._s3 = s3_client
        self._bucket = bucket
        self._prefix = prefix
        self._max_keys = max_keys
        self._file_statuses: Iterator = iter([])
        self._token = ""
        self.fetch()

    def __next__(self) -> FileInfo:
        try:
            return next(self._file_statuses)
        except StopIteration as e:
            if self._token:
                self.fetch()
                return next(self._file_statuses)
            else:
                raise e

    def fetch(self):
        params = dict(Bucket=self._bucket, Prefix=self._prefix, MaxKeys=self._max_keys)
        if self._token:
            params.update(ContinuationToken=self._token)

        response = self._s3.list_objects_v2(**params)

        s3_fs.assert_ok_status(response)

        self._file_statuses = iter(
            [
                FileInfo(f"s3://{response['Name']}/{x['Key']}", x["Size"], is_file=True)
                for x in response.get("Contents", [])
            ]
        )
        self._token = response.get("NextContinuationToken")


class S3FileSystem(FileSystem):
    def __init__(self, **kwargs):
        self.s3 = boto3.client("s3", **kwargs)

    @classmethod
    def create(cls, **kwargs):
        return S3FileSystem(**kwargs)

    def open(self, path: str, **kwargs: Any) -> Any:
        transport_params = kwargs.update({"client": self.s3})
        return smart_open.open(path, mode="rb", transport_params=transport_params)

    def file_status(self, path: str) -> FileInfo:
        s3_path = parse_s3_path(path)
        try:
            response = self.s3.get_object_attributes(
                Bucket=s3_path.bucket, Key=s3_path.key, ObjectAttributes=["ObjectSize"]
            )
            assert_ok_status(response)
            return FileInfo(path, response["ObjectSize"], is_file=True)
        except Exception as e:
            if (
                hasattr(e, "response")
                and e.response["ResponseMetadata"]["HTTPStatusCode"] == 404
            ):
                return FileInfo(path, 0, is_file=False)
            else:
                raise e

    def list(self, path: str) -> Iterable[FileInfo]:
        s3_path = parse_s3_path(path)
        return S3ListIterator(self.s3, s3_path.bucket, s3_path.key)
