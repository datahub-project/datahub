"""Text partitioner for converting markdown text to unstructured elements."""

import contextlib
import logging
import signal
import sys
import threading
from types import FrameType, TracebackType
from typing import Any, Callable, ClassVar, ContextManager, Optional, Union

logger = logging.getLogger(__name__)

# Mirrors the default extensions in unstructured/partition/md.py. Passing extensions to
# partition_md replaces that default, so these must be repeated alongside our own.
_DEFAULT_MARKDOWN_EXTENSIONS = ("tables", "fenced_code")

# Shared by the source config, TextPartitioner and embed_document so they can't drift.
DEFAULT_PARTITION_TIMEOUT_SECONDS = 30.0

# After the first alarm, keep re-raising at this interval: code under the limit can
# catch the alarm in a broad `except` (langdetect has a bare one) and carry on.
_REARM_INTERVAL_SECONDS = 0.5

# What signal.signal() accepts and returns (signal.Handlers is an int enum).
_SignalHandler = Union[Callable[[int, Optional[FrameType]], Any], int, None]


class PartitionTimeoutError(BaseException):
    """Raised when partitioning one document exceeds the configured time limit.

    A BaseException so that the `except Exception` handlers in the libraries being
    timed (e.g. a per-item loop in Python-Markdown) can't catch it on every pass and
    run past the limit. Callers must catch it by name.
    """


def _disarm(previous_handler: _SignalHandler) -> None:
    # The re-arming alarm can fire during cleanup; retry, or the timer would stay on
    # and keep raising in whatever code runs after the limited block.
    while True:
        try:
            signal.setitimer(signal.ITIMER_VIRTUAL, 0)
            signal.signal(signal.SIGVTALRM, previous_handler)
            return
        except PartitionTimeoutError:
            continue


class _TimeLimit:
    """Raise PartitionTimeoutError if the block runs longer than ``seconds``.

    Counts the process's user CPU time (ITIMER_VIRTUAL / SIGVTALRM), not wall-clock
    time, so a throttled or busy machine doesn't time out documents that would finish
    on a free one. CPU time from the process's other threads counts too, which can only
    make the limit fire earlier. The signal also interrupts compiled code that returns
    to the interpreter between operations (e.g. spaCy's tokenizer). SIGVTALRM rather
    than SIGPROF, which sampling profilers commonly use. Only usable on the main thread
    of a POSIX platform; callers must check that first. Must be used directly in a
    `with` statement: __enter__ records its caller's frame as the block to limit, so
    entering it any other way (e.g. ExitStack.enter_context) records a frame that has
    already returned and silently disables the limit. A plain class rather than
    @contextmanager, so cleanup runs directly in __exit__ and never depends on a
    generator being garbage-collected.
    """

    def __init__(self, seconds: float) -> None:
        self._seconds = seconds
        self._previous_handler: _SignalHandler = None
        # The frame of the `with` statement; the limit applies only while it is running.
        self._owner_frame: Optional[FrameType] = None

    def _owner_is_running(self, frame: Optional[FrameType]) -> bool:
        while frame is not None:
            if frame is self._owner_frame:
                return True
            frame = frame.f_back
        return False

    def _on_alarm(self, signum: int, frame: Optional[FrameType]) -> None:
        if self._owner_is_running(frame):
            raise PartitionTimeoutError(
                f"Partitioning exceeded {self._seconds:g}s of CPU time"
            )
        # The limited block was left without disarming: an alarm interrupted __exit__
        # before _disarm ran. Disarm now instead of raising into unrelated code, where
        # this BaseException would end the whole run rather than one document.
        self._owner_frame = None
        _disarm(self._previous_handler)

    def __enter__(self) -> None:
        self._owner_frame = sys._getframe(1)
        self._previous_handler = signal.signal(signal.SIGVTALRM, self._on_alarm)
        try:
            signal.setitimer(
                signal.ITIMER_VIRTUAL, self._seconds, _REARM_INTERVAL_SECONDS
            )
        except BaseException:
            _disarm(self._previous_handler)
            raise

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        _disarm(self._previous_handler)
        self._owner_frame = None


class TextPartitioner:
    """Partitions markdown text into unstructured.io elements."""

    # Process-wide, like the langdetect factory it reports on: warn once per process.
    _language_profiles_load_failed: ClassVar[bool] = False

    def __init__(
        self, timeout_seconds: float = DEFAULT_PARTITION_TIMEOUT_SECONDS
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self._logged_no_time_limit = False

    @classmethod
    def _preload_language_profiles(cls) -> None:
        # langdetect loads its profiles into a process-wide factory on first use, under
        # a bare `except:` that unstructured then logs and ignores. An alarm landing
        # there would be swallowed and leave the factory half-loaded for the rest of
        # the run, so load it before any time limit starts. init_factory() is a no-op
        # once loaded.
        try:
            from langdetect.detector_factory import init_factory
        except ModuleNotFoundError:
            return
        try:
            init_factory()
        except Exception as e:
            # unstructured logs and continues on the same error; do likewise rather
            # than failing every document, and warn only once.
            if not cls._language_profiles_load_failed:
                logger.warning(f"Could not load langdetect language profiles: {e}")
                cls._language_profiles_load_failed = True

    def _can_enforce_time_limit(self) -> bool:
        if self.timeout_seconds <= 0:
            return False
        reason: Optional[str] = None
        if not (
            hasattr(signal, "SIGVTALRM")
            and threading.current_thread() is threading.main_thread()
        ):
            reason = "SIGVTALRM is only available on the main thread of POSIX platforms"
        elif signal.getitimer(signal.ITIMER_VIRTUAL)[0] > 0:
            # The timer is process-wide; arming ours would overwrite and then clear a
            # timer some other code in the process (e.g. a profiler) relies on.
            reason = "another ITIMER_VIRTUAL timer is already armed in this process"
        if reason is None:
            return True
        if not self._logged_no_time_limit:
            logger.debug(f"Partition time limit not enforced: {reason}")
            self._logged_no_time_limit = True
        return False

    def partition_text(self, text: str) -> list[dict[str, Any]]:
        """Partition markdown text into structured elements.

        Uses unstructured.partition.md for markdown-aware partitioning,
        which preserves structure (headers, paragraphs, lists, etc.)

        Args:
            text: Markdown text content from Document.text field

        Returns:
            List of element dictionaries compatible with chunking pipeline

        Raises:
            ImportError: If unstructured library is not installed
            PartitionTimeoutError: If partitioning exceeds ``timeout_seconds``
        """
        try:
            from unstructured.partition.md import partition_md
        except ModuleNotFoundError as e:
            raise ModuleNotFoundError(
                "unstructured is required for text partitioning. "
                "Install with: pip install 'acryl-datahub[unstructured]'"
            ) from e

        # Handle empty text - return empty list
        # This avoids potential issues with empty input in various unstructured versions
        if not text or text.strip() == "":
            logger.debug("Empty text provided, returning empty element list")
            return []

        if self._can_enforce_time_limit():
            self._preload_language_profiles()
            time_limit: ContextManager[None] = _TimeLimit(self.timeout_seconds)
        else:
            time_limit = contextlib.nullcontext()
        # Imported here, not at module level: it imports markdown, which is only
        # installed with the unstructured extra.
        from datahub.ingestion.source.datahub_documents.markdown_link_guard import (
            NoInlineLinksExtension,
        )

        with time_limit:
            # Link and image syntax is kept as literal text (CVE-2019-25781 workaround;
            # see markdown_link_guard).
            elements = partition_md(
                text=text,
                extensions=[*_DEFAULT_MARKDOWN_EXTENSIONS, NoInlineLinksExtension()],
            )

        # Convert to dict format expected by chunking
        element_dicts = [elem.to_dict() for elem in elements]

        # unstructured.partition_md yields zero elements for minimal inputs
        # (e.g. a single character like "x"), which would otherwise make the
        # document look empty and get silently dropped downstream. Fall back to
        # one text element built from the raw content so any non-empty document
        # stays embeddable.
        if not element_dicts:
            from unstructured.documents.elements import Text

            element_dicts = [Text(text=text.strip()).to_dict()]

        logger.debug(f"Partitioned text into {len(element_dicts)} elements")
        return element_dicts
