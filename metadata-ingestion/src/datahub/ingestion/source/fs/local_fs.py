from datahub.ingestion.source.fs.fs_base import FileSystem, FileStatus
from typing import Iterable
import os
import pathlib


class LocalFileSystem(FileSystem):

    def open(self, path: str):
        return open(path, "rb")

    def list(self, path: str) -> Iterable[FileStatus]:
        p = pathlib.Path(path)
        if p.is_file():
            return [self.file_status(path)]
        elif p.is_dir():
            return iter([self.file_status(str(x)) for x in p.iterdir()])
        else:
            raise Exception(f"Failed to process {path}")

    def file_status(self, path: str) -> FileStatus:
        if os.path.isfile(path):
            return FileStatus(path, os.path.getsize(path), is_file=True)
        else:
            return FileStatus(path, 0, is_file=False)
