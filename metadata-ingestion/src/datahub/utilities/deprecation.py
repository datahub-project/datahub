"""Deprecation warnings for public APIs: attribution to the first caller outside datahub, and
the caller check for APIs that accept a MetadataChangeEvent."""

import functools
import logging
import os
import sys
import warnings
from types import FrameType
from typing import Optional

from datahub.errors import DataHubDeprecationWarning

logger = logging.getLogger(__name__)

# This module lives in datahub/utilities/, so two levels up is the datahub package.
_PACKAGE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + os.sep


# Cached: the pipeline asks once per work unit and transformer, over a handful of file names.
@functools.lru_cache(maxsize=None)
def _in_datahub_file(filename: str) -> bool:
    return os.path.abspath(filename).startswith(_PACKAGE_DIR)


def _in_datahub(frame: FrameType) -> bool:
    return _in_datahub_file(frame.f_code.co_filename)


# ABCMeta.__new__ and typing.Generic.__init_subclass__ run between a class statement and a
# datahub __init_subclass__; BaseModel.__init__ and model_validate run between a config's
# construction and a datahub validator.
_SKIPPED_MODULES = frozenset({"abc", "typing", "pydantic.main"})


def _skipped(frame: FrameType) -> bool:
    name = frame.f_globals.get("__name__", "")
    # A custom models package generates its schema classes outside the datahub package.
    return (
        _in_datahub(frame)
        or name in _SKIPPED_MODULES
        or name.endswith(".metadata._internal_schema_classes")
    )


# Like pandas' find_stack_level; warnings.warn(skip_file_prefixes=) needs Python 3.12.
def find_stack_level() -> int:
    """Return the stacklevel of the first frame outside datahub, for the function that calls warnings.warn."""
    frame: Optional[FrameType] = sys._getframe(1)
    level = 1
    while frame is not None and _skipped(frame):
        frame = frame.f_back
        level += 1
    return level


# An API that accepts a MetadataChangeEvent warns only for callers outside datahub: when datahub
# calls it, the MCE was already reported where it entered datahub. The gated function must not
# be wrapped by a datahub decorator, or every call would look internal.
def called_from_datahub() -> bool:
    """Whether the function calling this was itself called from inside the datahub package."""
    caller = sys._getframe(1).f_back
    # A frame with no Python caller (atexit, finalizers) is treated as outside datahub.
    return caller is not None and _in_datahub(caller)


def deprecation_message(api: object) -> str:
    """Return the message that typing_extensions.deprecated stored on api."""
    # The decorator takes the message as a literal, because type checkers ignore a constant.
    return str(api.__deprecated__)  # type: ignore[attr-defined]


@functools.lru_cache(maxsize=None)
def log_deprecation_once(message: str) -> None:
    logger.warning(message)


def warn_deprecated(message: str) -> None:
    warnings.warn(message, DataHubDeprecationWarning, stacklevel=find_stack_level())
    log_deprecation_once(message)
