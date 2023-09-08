from dataclasses import dataclass
from typing import Iterable
from urllib import parse
from abc import ABCMeta, abstractmethod


@dataclass
class FileStatus:
    path: str
    size: int
    is_file: bool

    def __str__(self):
        return f"FileStatus({self.path}, {self.size}, {self.is_file})"


class FileSystem(metaclass=ABCMeta):

    @classmethod
    def create_fs(cls) -> "FileSystem":
        raise NotImplementedError('File system implementations must implement "create_fs"')

    @abstractmethod
    def open(self, path: str, **kwargs):
        pass

    @abstractmethod
    def file_status(self, path: str) -> FileStatus:
        pass

    @abstractmethod
    def list(self, path: str) -> Iterable[FileStatus]:
        pass


def get_path_schema(path: str):
    scheme = parse.urlparse(path).scheme
    if scheme == "":
        scheme = "file"
    return scheme