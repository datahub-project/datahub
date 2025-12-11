# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
