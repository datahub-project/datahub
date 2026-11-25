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

    @abstractmethod
    def write(self, path: str, content: str, **kwargs: Any) -> None:
        """Write content to a file at the given path."""
        pass

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Check if a file exists at the given path."""
        pass


def get_path_schema(path: str) -> str:
    scheme = parse.urlparse(path).scheme
    # No scheme means a local path. A one-letter scheme is a Windows drive
    # letter (C:\data\x.csv parses with scheme "c"), so it is local too.
    if len(scheme) <= 1:
        scheme = "file"
    return scheme
