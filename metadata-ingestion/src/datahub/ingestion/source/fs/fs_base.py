from dataclasses import dataclass
from typing import Iterable


@dataclass
class FileStatus:
    path: str
    size: int
    is_file: bool

    def __str__(self):
        return f"FileStatus({self.path}, {self.size}, {self.is_file})"


class FileSystem:

    @classmethod
    def get(cls, path: str):
        from datahub.ingestion.source.fs import fs_factory
        return fs_factory.get_fs(path)

    def open(self, path: str, **kwargs):
        raise NotImplementedError("open method must be implemented in a subclass")

    def file_status(self, path: str) -> FileStatus:
        raise NotImplementedError("file_status method must be implemented in a subclass")

    def list(self, path: str) -> Iterable[FileStatus]:
        raise NotImplementedError("list method must be implemented in a subclass")
