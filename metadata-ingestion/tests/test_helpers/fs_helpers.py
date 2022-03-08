import contextlib
import os
import pathlib
from typing import Iterator


@contextlib.contextmanager
def isolated_filesystem(temp_dir: pathlib.Path) -> Iterator[None]:
    cwd = os.getcwd()

    os.chdir(temp_dir)
    try:
        yield
    finally:
        os.chdir(cwd)
