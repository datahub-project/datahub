from dataclasses import dataclass
from typing import Iterable
from urllib import parse
from abc import ABCMeta, abstractmethod


@dataclass
class FileInfo:
    path: str
    size: int
    is_file: bool

    def __str__(self):
        return f"FileInfo({self.path}, {self.size}, {self.is_file})"


class FileSystem(metaclass=ABCMeta):

    @classmethod
    def create(cls, **kwargs) -> "FileSystem":
        raise NotImplementedError('File system implementations must implement "create"')

    @abstractmethod
    def open(self, path: str, **kwargs):
        pass

    @abstractmethod
    def file_status(self, path: str) -> FileInfo:
        pass

    @abstractmethod
    def list(self, path: str) -> Iterable[FileInfo]:
        pass


def get_path_schema(path: str):
    scheme = parse.urlparse(path).scheme
    if scheme == "":
        scheme = "file"
    return scheme