from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import Any, Iterable
from urllib import parse
import re

# Matches Windows absolute paths like "C:\..." or "C:/...". urlparse()
# misreads the drive letter as a URL scheme (e.g. "c"), so these must be
# special-cased to "file" before falling through to urlparse.
_WINDOWS_DRIVE_PATH_RE = re.compile(r"^[a-zA-Z]:[\\/]")


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
    if _WINDOWS_DRIVE_PATH_RE.match(path):
        return "file"
    scheme = parse.urlparse(path).scheme
    if scheme == "":
        # This makes the default schema "file" for local paths.
        scheme = "file"
    return scheme
