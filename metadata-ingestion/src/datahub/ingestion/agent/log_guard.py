import logging
import threading
import warnings
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Callable, Dict, Iterator, Optional, Sequence, Set, Tuple

from datahub.configuration.env_vars import get_probe_verbose_logs
from datahub.ingestion.agent.redact import scrub_text

# Loggers whose text the framework writes itself, and so may show as logged,
# tracebacks included. Every other record is reused code -- a source, a
# driver, an SDK, or a datahub module a provider calls into. Default deny: a
# library nobody thought to list is still covered.
FRAMEWORK_LOGGERS: Tuple[str, ...] = (
    "datahub.ingestion.agent",
    "datahub.cli",
    "datahub.masking",
    "datahub.entrypoints",
    "datahub.telemetry",
)

# Above CRITICAL, so a logger at this level makes no record at all.
_SILENT = logging.CRITICAL + 1


def _under(name: str, prefixes: Tuple[str, ...]) -> bool:
    return any(name == p or name.startswith(p + ".") for p in prefixes)


@dataclass(eq=False)
class _Guard:
    # The caller's set, not a copy, so a value it adds while the guard is open
    # is scrubbed too.
    secrets: Set[str]
    silenced: Tuple[str, ...]


class _Scrubbing:
    """The record factory while any guard is open, a pass-through otherwise.
    Each install wraps the factory it found, so one left under a third
    party's wrapper is never reached through itself."""

    def __init__(self, below: Callable[..., logging.LogRecord]) -> None:
        self.below = below

    def __call__(self, *args: object, **kwargs: object) -> logging.LogRecord:
        record = self.below(*args, **kwargs)
        guards = _ACTIVE.guards
        # makeLogRecord makes its record with no name and fills it in after.
        name = record.name if isinstance(record.name, str) else ""
        if guards and not _under(name, FRAMEWORK_LOGGERS):
            secrets: Set[str] = set().union(*(g.secrets for g in guards))
            record.msg = scrub_text(_message(record), secrets)
            record.args = None
            record.exc_info = None
            record.exc_text = None
            record.stack_info = None
        return record


def _message(record: logging.LogRecord) -> str:
    try:
        return record.getMessage()
    except Exception:
        # Mismatched args: logging would report it later, raw args included.
        try:
            return str(record.msg)
        except Exception:
            # Raising here would escape into the caller's logging call.
            return "<unprintable log message>"


class _Active:
    """The open guards, process-wide, so guards may close in any order.
    `guards` is replaced, never mutated, so the factory reads it unlocked."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.guards: Tuple[_Guard, ...] = ()
        self.turned_on_capture = False
        # The showwarning put aside while logging's own is put back in front.
        self.bypass: Optional[Callable[..., None]] = None
        # Each silenced logger's level before the first open guard naming it.
        self.saved_levels: Dict[str, int] = {}

    def open(self, guard: _Guard) -> None:
        with self.lock:
            self.guards = (*self.guards, guard)
            current = logging.getLogRecordFactory()
            if not isinstance(current, _Scrubbing):
                logging.setLogRecordFactory(_Scrubbing(current))
            self._capture_warnings()
            for name in guard.silenced:
                logger = logging.getLogger(name)
                if name not in self.saved_levels and logger.level < _SILENT:
                    self.saved_levels[name] = logger.level
                    logger.setLevel(_SILENT)

    def close(self, guard: _Guard) -> None:
        with self.lock:
            self.guards = tuple(g for g in self.guards if g is not guard)
            still_silenced = {n for g in self.guards for n in g.silenced}
            for name in set(guard.silenced) - still_silenced:
                logger = logging.getLogger(name)
                # A level someone else set meanwhile is theirs to keep.
                if name in self.saved_levels and logger.level == _SILENT:
                    logger.setLevel(self.saved_levels[name])
                self.saved_levels.pop(name, None)
            if self.guards:
                return
            # Left under a third party's wrapper, it stays and passes through.
            current = logging.getLogRecordFactory()
            if isinstance(current, _Scrubbing):
                logging.setLogRecordFactory(current.below)
            if self.turned_on_capture:
                logging.captureWarnings(False)
                self.turned_on_capture = False
            if self.bypass is not None:
                if warnings.showwarning is getattr(logging, "_showwarning", None):
                    warnings.showwarning = self.bypass
                self.bypass = None

    def _capture_warnings(self) -> None:
        # Judged by what warnings calls, not by logging's flag alone: the flag
        # stays set after a catch_warnings block that turned capture on exits,
        # or after a library puts its own showwarning in, and then
        # captureWarnings(True) does nothing while warnings print raw.
        to_logging = getattr(logging, "_showwarning", None)
        if warnings.showwarning is to_logging:
            return
        if getattr(logging, "_warnings_showwarning", None) is None:
            logging.captureWarnings(True)
            self.turned_on_capture = True
        elif to_logging is not None:
            self.bypass = warnings.showwarning
            warnings.showwarning = to_logging


_ACTIVE = _Active()


@contextmanager
def quiet_reused_logs(
    secret_values: Set[str], silenced: Sequence[str] = ()
) -> Iterator[None]:
    """Scrub reused code's log records, tracebacks dropped, while a probe runs.

    Every record not from FRAMEWORK_LOGGERS is rewritten as logging makes it
    (setLogRecordFactory): message scrubbed with `secret_values`, args,
    exc_info and stack_info cleared. That covers loggers and handlers created
    mid-probe, non-propagating loggers, lastResort, and Logger subclasses
    that do not override makeRecord; warnings.warn is routed to logging
    meanwhile, so it is covered as `py.warnings`.

    `silenced` loggers (code that logs source values with no credential
    shape) are raised above CRITICAL: they and children inheriting the level
    make no records, and a child with its own level is still scrubbed.

    Not covered: a record built directly (LogRecord(...), makeLogRecord) and
    handed to a handler; fields passed as `extra=`, which makeRecord adds
    after the factory runs (a JSON formatter, or a DATAHUB_LOG_CONFIG_FILE
    format naming them, prints them); a record factory installed mid-guard
    that does not call the one it replaced, which turns scrubbing off until
    the next guard opens; and a silenced logger whose level is reset
    mid-guard, which is then only scrubbed.

    Guards may nest or close in any order. When the last one closes,
    exception or not, its factory comes out unless a third party's wrapper
    sits over it (left there, it passes records through); warning capture it
    turned on goes off, with any capture turned on by someone else meanwhile;
    and each silenced level is restored unless someone else changed it.
    DATAHUB_PROBE_VERBOSE_LOGS=1 turns the guard off.
    """
    if get_probe_verbose_logs():
        yield
        return
    guard = _Guard(secret_values, tuple(silenced))
    try:
        _ACTIVE.open(guard)
        yield
    finally:
        _ACTIVE.close(guard)
