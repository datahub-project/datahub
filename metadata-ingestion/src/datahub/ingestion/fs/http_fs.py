from typing import Any, Iterable

import requests
import smart_open

from datahub.ingestion.fs.fs_base import FileInfo, FileSystem


class HttpFileSystem(FileSystem):
    @classmethod
    def create(cls, **kwargs):
        return HttpFileSystem()

    def open(self, path: str, **kwargs: Any) -> Any:
        return smart_open.open(path, mode="rb", transport_params=kwargs)

    def file_status(self, path: str) -> FileInfo:
        head = requests.head(path)
        if head.ok:
            return FileInfo(path, int(head.headers["Content-length"]), is_file=True)
        elif head.status_code == 404:
            raise FileNotFoundError(f"Requested path {path} does not exist.")
        else:
            raise IOError(f"Cannot get file status for the requested path {path}.")

    def list(self, path: str) -> Iterable[FileInfo]:
        status = self.file_status(path)
        return [status]

    def write(self, path: str, content: str, **kwargs: Any) -> None:
        """HTTP file system does not support writing."""
        raise NotImplementedError("HTTP file system does not support write operations")

    def exists(self, path: str) -> bool:
        """Check if an HTTP resource exists."""
        try:
            head = requests.head(path)
            return head.ok
        except Exception:
            return False
