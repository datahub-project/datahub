# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
