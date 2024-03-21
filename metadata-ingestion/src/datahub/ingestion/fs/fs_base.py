from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import Any, Iterable
from urllib import parse


@dataclass
class FileInfo:
    path: str
    size: int
    is_file: bool

    def __str__(self):
        return f"FileInfo({self.path}, {self.size}, {self.is_file})"


class FileSystem(metaclass=ABCMeta):
    @classmethod
    def create(cls, **kwargs: Any) -> "FileSystem":
        raise NotImplementedError('File system implementations must implement "create"')

    @abstractmethod
    def open(self, path: str, **kwargs: Any) -> Any:
        pass

    @abstractmethod
    def file_status(self, path: str) -> FileInfo:
        pass

    @abstractmethod
    def list(self, path: str) -> Iterable[FileInfo]:
        pass


def get_path_schema(path: str) -> str:
    scheme = parse.urlparse(path).scheme
    if scheme == "":
        # This makes the default schema "file" for local paths.
        scheme = "file"
    return scheme
