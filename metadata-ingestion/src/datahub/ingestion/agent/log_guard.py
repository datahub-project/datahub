import logging
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Callable, Dict, Iterator, Sequence, Set, Tuple

from datahub.configuration.env_vars import get_probe_verbose_logs
from datahub.ingestion.agent.redact import scrub_text
from datahub.ingestion.agent.verdicts import ProbeInternalError

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
    # The caller's set, not a copy: the CLI masks its own output against it.
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
        # Each silenced logger's level before the first open guard naming it.
        self.saved_levels: Dict[str, int] = {}

    def open(self, guard: _Guard) -> None:
        with self.lock:
            self.guards = (*self.guards, guard)
            current = logging.getLogRecordFactory()
            if not isinstance(current, _Scrubbing):
                logging.setLogRecordFactory(_Scrubbing(current))
            if getattr(logging, "_warnings_showwarning", None) is None:
                logging.captureWarnings(True)
                self.turned_on_capture = True
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
    that do not override makeRecord; warning capture is on meanwhile, so
    warnings.warn is covered as `py.warnings`.

    `silenced` loggers (code that logs source values with no credential
    shape) are raised above CRITICAL: they and children inheriting the level
    make no records, and a child with its own level is still scrubbed. Naming
    a framework logger or an ancestor of one is a provider defect.

    Not covered: a record built directly (LogRecord(...), makeLogRecord) and
    handed to a handler; capture someone else turns on mid-guard goes off
    with the last guard. Guards may nest or close in any order, and all is
    undone on exit. DATAHUB_PROBE_VERBOSE_LOGS=1 turns the guard off.
    """
    for name in silenced:
        if name in ("", logging.root.name) or any(
            _under(name, (f,)) or _under(f, (name,)) for f in FRAMEWORK_LOGGERS
        ):
            raise ProbeInternalError(
                f"silenced_loggers cannot name '{name}': it would hide the probe "
                f"framework's own logs; this is a defect in the probe provider"
            )
    if get_probe_verbose_logs():
        yield
        return
    guard = _Guard(secret_values, tuple(silenced))
    try:
        _ACTIVE.open(guard)
        yield
    finally:
        _ACTIVE.close(guard)
