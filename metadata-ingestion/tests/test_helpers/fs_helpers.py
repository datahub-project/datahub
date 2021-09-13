import contextlib
import os


@contextlib.contextmanager
def isolated_filesystem(temp_dir):
    cwd = os.getcwd()

    os.chdir(temp_dir)
    try:
        yield
    finally:
        os.chdir(cwd)
