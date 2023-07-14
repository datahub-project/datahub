from datahub.ingestion.source.fs import s3_fs
from collections.abc import Iterator
from datahub.ingestion.source.fs.fs_base import FileStatus


class S3ListIterator(Iterator):

    MAX_KEYS = 1000

    def __init__(self, s3_client, bucket: str, prefix: str, max_keys=MAX_KEYS):
        self._s3 = s3_client
        self._bucket = bucket
        self._prefix = prefix
        self._max_keys = max_keys
        self._file_statuses = iter([])
        self._token = ''
        self.fetch()

    def __next__(self) -> FileStatus:
        try:
            return next(self._file_statuses)
        except StopIteration:
            if self._token:
                self.fetch()
                return next(self._file_statuses)
            else:
                raise StopIteration()

    def fetch(self):
        params = dict(Bucket=self._bucket, Prefix=self._prefix, MaxKeys=self._max_keys)
        if self._token:
            params.update(ContinuationToken=self._token)

        response = self._s3.list_objects_v2(**params)

        s3_fs.assert_ok_status(response)

        self._file_statuses = iter([
            FileStatus(f"s3://{response['Name']}/{x['Key']}", x['Size'], is_file=True)
            for x in response.get('Contents', [])
        ])
        self._token = response.get('NextContinuationToken')
