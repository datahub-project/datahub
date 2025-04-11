"""
Configure logging and stdout for the CLI. Our goal is to have the following behavior:

1. By default, show INFO level logs from datahub and WARNINGs from everything else.
2. If the user passes --debug, show DEBUG level logs from datahub and INFOs from everything else.
3. If the user passes --log-file, write all logs and stdout to the specified file.
   This should contain debug logs regardless of the user's CLI args.
4. Maintain an in-memory buffer of the latest logs for reporting purposes.
5. When outputting to a TTY, colorize the logs.

This code path should not be executed if we're being used as a library.
"""

import collections
import contextlib
import itertools
import logging
import os
import pathlib
import sys
from typing import Deque, Iterator, Optional

import click

from datahub.utilities.tee_io import TeeIO

BASE_LOGGING_FORMAT = (
    "[%(asctime)s] %(levelname)-8s {%(name)s:%(lineno)d} - %(message)s"
)
DATAHUB_PACKAGES = [
    "datahub",
    "datahub_provider",
    "datahub_classify",
    "datahub_actions",
    "acryl_datahub_cloud",
]
IN_MEMORY_LOG_BUFFER_SIZE = 2000  # lines
IN_MEMORY_LOG_BUFFER_MAX_LINE_LENGTH = 2000  # characters


NO_COLOR = os.environ.get("NO_COLOR", False)


def extract_name_from_filename(filename: str, fallback_name: str) -> str:
    """Guess the module path from the filename.

    Because the logger name may not be the same as the package path (e.g. when using stacklevel),
    we do a best-effort attempt to extract the module name from the filename.

    >>> extract_name_from_filename("/datahub-ingestion/.local/lib/python3.10/site-packages/datahub/configuration/common.py", "bad")
    'datahub.configuration.common'

    >>> extract_name_from_filename("/home/user/datahub/metadata-ingestion/src/datahub/telemetry/telemetry.py", "bad")
    'datahub.telemetry.telemetry'

    >>> extract_name_from_filename("/home/user/datahub/metadata-ingestion-modules/airflow-plugin/src/datahub_airflow_plugin/datahub_listener.py", "bad")
    'datahub_airflow_plugin.datahub_listener'

    >>> extract_name_from_filename("/this/is/not/a/normal/path.py", "fallback.package")
    'fallback.package'

    Args:
        filename: The filename of the module.
        fallback_name: The name to use if we can't guess the module.

    Returns:
        The guessed module name.
    """

    with contextlib.suppress(Exception):
        # Split the path into components
        path_parts = list(pathlib.Path(filename).parts)

        # Remove the .py extension from the last part
        if path_parts[-1].endswith(".py"):
            path_parts[-1] = path_parts[-1][:-3]

        # If we're in a site-packages directory, we want to use the package name as the top-level module.
        if "site-packages" in path_parts:
            # Find the index of 'site-packages' in the path
            site_packages_index = path_parts.index("site-packages")
            # Join the parts from 'site-packages' onwards with '.'
            return ".".join(path_parts[site_packages_index + 1 :])

        # We're probably in a development environment, so take everything after 'src' as the module.
        src_dir_index = next(
            itertools.chain(
                (
                    i + 2
                    for i, part in enumerate(path_parts)
                    if "metadata-ingestion-modules" in part
                ),
                (
                    i + 1
                    for i, part in enumerate(path_parts)
                    if "metadata-ingestion" in part
                ),
                [None],
            )
        )
        if src_dir_index is not None:
            # Join the parts after 'src' with '.'
            return ".".join(path_parts[src_dir_index + 1 :])

    return fallback_name


class _ColorLogFormatter(logging.Formatter):
    # Adapted from https://stackoverflow.com/a/56944256/3638629.

    MESSAGE_COLORS = {
        "DEBUG": "blue",
        "INFO": None,  # print with default color
        "WARNING": "yellow",
        "ERROR": "red",
        "CRITICAL": "red",
    }

    def __init__(self) -> None:
        super().__init__(BASE_LOGGING_FORMAT)

    def formatMessage(self, record: logging.LogRecord) -> str:
        record.name = extract_name_from_filename(record.pathname, record.name)
        if not NO_COLOR and sys.stderr.isatty():
            return self._formatMessageColor(record)
        else:
            return super().formatMessage(record)

    def _formatMessageColor(self, record: logging.LogRecord) -> str:
        # Mimic our default format, but with color.
        message_fg = self.MESSAGE_COLORS.get(record.levelname)
        return (
            f"{click.style(f'[{self.formatTime(record, self.datefmt)}]', fg='green', dim=True)} "
            f"{click.style(f'{record.levelname:8}', fg=message_fg)} "
            f"{click.style(f'{{{record.name}:{record.lineno}}}', fg='blue', dim=True)} - "
            f"{click.style(record.getMessage(), fg=message_fg)}"
        )


class _DatahubLogFilter(logging.Filter):
    def __init__(self, debug: bool) -> None:
        self.debug = debug

    def filter(self, record: logging.LogRecord) -> bool:
        top_module = record.name.split(".")[0]

        if top_module in DATAHUB_PACKAGES:
            if self.debug:
                return record.levelno >= logging.DEBUG
            else:
                return record.levelno >= logging.INFO
        else:
            if self.debug:
                return record.levelno >= logging.INFO
            else:
                return record.levelno >= logging.WARNING


class _LogBuffer:
    def __init__(self, maxlen: Optional[int] = None) -> None:
        self._buffer: Deque[str] = collections.deque(maxlen=maxlen)

    def write(self, line: str) -> None:
        if len(line) > IN_MEMORY_LOG_BUFFER_MAX_LINE_LENGTH:
            line = line[:IN_MEMORY_LOG_BUFFER_MAX_LINE_LENGTH] + "[truncated]"

        self._buffer.append(line)

    def clear(self) -> None:
        self._buffer.clear()

    def format_lines(self) -> str:
        text = "\n".join(self._buffer)
        if len(self._buffer) == 0:
            text = "[no logs]"
        elif len(self._buffer) == self._buffer.maxlen:
            text = f"[earlier logs truncated...]\n{text}"
        return text


class _BufferLogHandler(logging.Handler):
    def __init__(self, storage: _LogBuffer) -> None:
        super().__init__()
        self._storage = storage

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = self.format(record)
        except TypeError as e:
            message = f"Error formatting log message: {e}\nMessage: {record.msg}, Args: {record.args}"
        self._storage.write(message)


def _remove_all_handlers(logger: logging.Logger) -> None:
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        handler.close()


_log_buffer = _LogBuffer(maxlen=IN_MEMORY_LOG_BUFFER_SIZE)


def get_log_buffer() -> _LogBuffer:
    return _log_buffer


_stream_formatter = _ColorLogFormatter()
_default_formatter = logging.Formatter(BASE_LOGGING_FORMAT)


@contextlib.contextmanager
def configure_logging(debug: bool, log_file: Optional[str] = None) -> Iterator[None]:
    _log_buffer.clear()

    if os.environ.get("DATAHUB_SUPPRESS_LOGGING_MANAGER") == "1":
        # If we're running in pytest, we don't want to configure logging.
        yield
        return

    with contextlib.ExitStack() as stack:
        # Create stdout handler.
        stream_handler = logging.StreamHandler()
        stream_handler.addFilter(_DatahubLogFilter(debug=debug))
        stream_handler.setFormatter(_stream_formatter)

        # Create file handler.
        file_handler: logging.Handler
        if log_file:
            file = stack.enter_context(open(log_file, "w"))
            tee = TeeIO(sys.stdout, file)
            stack.enter_context(contextlib.redirect_stdout(tee))  # type: ignore

            file_handler = logging.StreamHandler(file)
            file_handler.addFilter(_DatahubLogFilter(debug=True))
            file_handler.setFormatter(_default_formatter)
        else:
            file_handler = logging.NullHandler()

        # Create the in-memory buffer handler.
        buffer_handler = _BufferLogHandler(_log_buffer)
        buffer_handler.addFilter(_DatahubLogFilter(debug=debug))
        buffer_handler.setFormatter(_default_formatter)

        handlers = [
            stream_handler,
            file_handler,
            buffer_handler,
        ]

        # Configure the loggers.
        root_logger = logging.getLogger()
        _remove_all_handlers(root_logger)
        root_logger.setLevel(logging.INFO)
        for handler in handlers:
            root_logger.addHandler(handler)

        for lib in DATAHUB_PACKAGES:
            # Using a separate handler from the root logger allows us to control the log level
            # of the datahub libs independently of the root logger.
            # It also insulates us from rogue libraries that might call `logging.basicConfig`
            # or otherwise mess with the logging configuration.
            lib_logger = logging.getLogger(lib)
            _remove_all_handlers(lib_logger)
            lib_logger.setLevel(logging.DEBUG)
            lib_logger.propagate = False
            for handler in handlers:
                lib_logger.addHandler(handler)

        yield

        # Cleanup.
        for handler in handlers:
            root_logger.removeHandler(handler)
            for lib in DATAHUB_PACKAGES:
                lib_logger = logging.getLogger(lib)
                lib_logger.removeHandler(handler)
                lib_logger.propagate = True


# Reduce logging from some particularly chatty libraries.
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("urllib3.util.retry").setLevel(logging.WARNING)
logging.getLogger("snowflake").setLevel(level=logging.WARNING)
# logging.getLogger("botocore").setLevel(logging.INFO)
# logging.getLogger("google").setLevel(logging.INFO)
logging.getLogger("pyodata").setLevel(logging.WARNING)
