from urllib import parse
from datahub.ingestion.source.fs.local_fs import LocalFileSystem
from datahub.ingestion.source.fs.s3_fs import S3FileSystem
from datahub.ingestion.source.fs.http_fs import HttpFileSystem

FS_MAP = {
    'file': LocalFileSystem,
    's3': S3FileSystem,
    'http': HttpFileSystem
}


def get_fs(path: str):
    scheme = parse.urlparse(path).scheme
    if scheme == "":
        scheme = "file"
    return FS_MAP[scheme]()
