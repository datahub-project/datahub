import requests
import smart_open
from typing import Iterable
from datahub.ingestion.source.fs.fs_base import FileSystem, FileStatus


class HttpFileSystem(FileSystem):

    def open(self, path: str, **kwargs):
        return smart_open.open(path, mode='rb', transport_params=kwargs)

    def file_status(self, path: str) -> FileStatus:
        head = requests.head(path)
        if head.ok:
            return FileStatus(path, int(head.headers['Content-length']), is_file=True)
        elif head.status_code == 404:
            raise Exception(f"Requested path {path} does not exists.")
        else:
            raise Exception(f"Cannot get file status for the requested path {path}.")

    def list(self, path: str) -> Iterable[FileStatus]:
        status = self.file_status(path)
        return [status]
