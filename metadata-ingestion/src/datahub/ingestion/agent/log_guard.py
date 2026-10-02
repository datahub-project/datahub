import logging
import sys
import threading
import warnings
from contextlib import contextmanager
from functools import partial
from typing import (
    Callable,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    TextIO,
    Tuple,
    Type,
    Union,
)

from datahub.configuration.env_vars import get_probe_verbose_logs
from datahub.ingestion.agent.redact import scrub_text

# Loggers whose text the framework writes itself, and so may show as logged,
# tracebacks included. Every other record reaching a guarded handler is reused
# code -- a source, a driver, an SDK, or one of datahub's shared modules a
# provider calls into -- and is scrubbed with its traceback dropped. Default
# deny: a library nobody thought to list is still covered.
FRAMEWORK_LOGGERS: Tuple[str, ...] = (
    "datahub.ingestion.agent",
    "datahub.cli",
    "datahub.masking",
    "datahub.entrypoints",
    "datahub.telemetry",
)

# Loggers of reused code known to be noisy: ingestion sources and the
# SDKs/drivers they call. On top of the scrubbing every non-framework record
# gets, these are floored at WARNING, because their DEBUG output is volume as
# well as risk (botocore logged google-auth's refresh error with the
# subject-token path; Kafka Connect's fetchers log raw connect URLs). That
# includes a provider's own `logger.debug` under `datahub.ingestion.source`.
REUSED_LOGGERS: Tuple[str, ...] = (
    "datahub.ingestion.source",
    "botocore",
    "boto3",
    "urllib3",
    "google",
    "azure",
    "databricks",
    "snowflake",
    "pyiceberg",
    "looker_sdk",
    "tableauserverclient",
    "sqlalchemy",
    "requests_oauthlib",
    "oauthlib",
    "msal",
    "httpx",
    # Warning capture, which the guard turns on for its duration (and the
    # masking bootstrap for the CLI): a library's warnings.warn() text
    # arrives here, at WARNING.
    "py.warnings",
)


def _under(name: str, prefixes: Tuple[str, ...]) -> bool:
    return any(name == g or name.startswith(g + ".") for g in prefixes)


def _is_reused(name: str) -> bool:
    return _under(name, REUSED_LOGGERS)


class _ScrubFilter(logging.Filter):
    """Scrubs every record not logged by the framework, dropping its traceback,
    holds the noisy reused loggers to WARNING or above, and drops a provider's
    silenced loggers outright. Applied in Logger.callHandlers (see
    quiet_reused_logs), so it sees every record on its way to any handler;
    only the framework's own pass untouched.
    """

    def __init__(self, secret_values: Set[str], silenced: Tuple[str, ...] = ()) -> None:
        super().__init__()
        # The caller's set, not a copy: the CLI's set is what it masks its own
        # output against, so the two cannot drift.
        self._secrets = secret_values
        self._silenced = silenced

    def filter(self, record: logging.LogRecord) -> bool:
        if _under(record.name, FRAMEWORK_LOGGERS):
            return True
        # Checked after the framework's own loggers, so a provider can never
        # hide the probe's diagnostics.
        if _under(record.name, self._silenced):
            return False
        # The level floor on the logger stops most of these before a record is
        # built; this catches a child logger created inside the guard with its
        # own DEBUG level, which no floor was applied to.
        if _is_reused(record.name) and record.levelno < logging.WARNING:
            return False
        try:
            message = record.getMessage()
        except Exception:
            # Mismatched args: logging would report the error itself later,
            # with the raw args in it.
            try:
                message = str(record.msg)
            except Exception:
                # Raising here would escape into the caller's logging call.
                message = "<unprintable log message>"
        record.msg = scrub_text(message, self._secrets)
        record.args = None
        record.exc_info = None
        record.exc_text = None
        record.stack_info = None
        return True


def _guarded_loggers() -> List[logging.Logger]:
    """Each reused logger, plus every existing child of one, parents first."""
    names = set(REUSED_LOGGERS)
    names.update(
        name
        for name, obj in list(logging.Logger.manager.loggerDict.items())
        if isinstance(obj, logging.Logger) and _is_reused(name)
    )
    return [logging.getLogger(name) for name in sorted(names)]


_CallHandlers = Callable[[logging.Logger, logging.LogRecord], None]


class _ActiveGuards:
    """The open guards, and the one callHandlers wrapper that applies them.

    One wrapper for the whole process rather than one per guard, so the
    order guards close in does not matter: a guard closing removes only its
    own filter, and the wrapper comes off the class when the last one
    closes. It comes off only if the class still holds it -- an SDK that
    patched callHandlers on top while a guard was open (error reporters
    do) keeps its patch, and the wrapper, left underneath it, applies
    nothing until a guard opens again.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # Replaced, never mutated, so the wrapper reads it without the lock.
        self.guards: Tuple[logging.Filter, ...] = ()
        # What the wrapper calls through to. Set for as long as the wrapper
        # is anywhere in the chain, which is also how a second install on
        # top of a patch that sits over it -- a loop -- is avoided.
        self.below: Optional[_CallHandlers] = None
        # warnings.showwarning as it was before the guard's own went in,
        # kept for as long as _guard_showwarning may be in the chain.
        self.saved_showwarning: Optional[Callable[..., None]] = None

    def open(self, guard: logging.Filter) -> None:
        with self._lock:
            self.guards = (*self.guards, guard)
            self._capture_warnings()
            if self.below is None:
                self.below = vars(logging.Logger)["callHandlers"]
                setattr(logging.Logger, "callHandlers", _scrubbing_call_handlers)  # noqa: B010

    def close(self, guard: logging.Filter) -> None:
        with self._lock:
            kept = list(self.guards)
            # The last occurrence, by identity: the same filter is never
            # opened twice, but equality is not identity for a Filter
            # subclass that defines __eq__.
            for i in range(len(kept) - 1, -1, -1):
                if kept[i] is guard:
                    del kept[i]
                    break
            self.guards = tuple(kept)
            if (
                not self.guards
                and vars(logging.Logger)["callHandlers"] is _scrubbing_call_handlers
            ):
                setattr(logging.Logger, "callHandlers", self.below)  # noqa: B010
                self.below = None
            if not self.guards and warnings.showwarning is _guard_showwarning:
                warnings.showwarning = self.saved_showwarning or _print_warning
                self.saved_showwarning = None

    def _capture_warnings(self) -> None:
        """Route warnings.warn into logging, as py.warnings, so the scrub
        sees it -- unless logging.captureWarnings already does.

        The guard's own showwarning rather than captureWarnings(True), so the
        exit can tell its capture from one turned on inside the guard (the
        masking bootstrap running mid-probe), which it must leave on. Like
        the callHandlers wrapper, it comes out only if it is still in place.
        """
        if getattr(logging, "_warnings_showwarning", None) is not None:
            return
        if warnings.showwarning is _guard_showwarning:
            return
        self.saved_showwarning = warnings.showwarning
        warnings.showwarning = _guard_showwarning


_ACTIVE = _ActiveGuards()


def _print_warning(
    message: Union[Warning, str],
    category: Type[Warning],
    filename: str,
    lineno: int,
    file: Optional[TextIO] = None,
    line: Optional[str] = None,
) -> None:
    text = warnings.formatwarning(message, category, filename, lineno, line)
    try:
        (file or sys.stderr).write(text)
    except OSError:
        pass


def _guard_showwarning(
    message: Union[Warning, str],
    category: Type[Warning],
    filename: str,
    lineno: int,
    file: Optional[TextIO] = None,
    line: Optional[str] = None,
) -> None:
    # As logging.captureWarnings does it: to the py.warnings logger, unless
    # the caller named a file to write to.
    if _ACTIVE.guards and file is None:
        text = warnings.formatwarning(message, category, filename, lineno, line)
        logging.getLogger("py.warnings").warning("%s", text)
        return
    # No guard open: left in a chain someone built on top of it, so behave
    # as what it replaced.
    (_ACTIVE.saved_showwarning or _print_warning)(
        message, category, filename, lineno, file, line
    )


def _scrubbing_call_handlers(logger: logging.Logger, record: logging.LogRecord) -> None:
    # Innermost guard first, as nested handler filters would have run.
    for guard in reversed(_ACTIVE.guards):
        if not guard.filter(record):
            return
    below = _ACTIVE.below
    if below is not None:
        below(logger, record)


@contextmanager
def quiet_reused_logs(
    secret_values: Set[str], silenced: Sequence[str] = ()
) -> Iterator[None]:
    """Keep reused code's logs scrubbed, without tracebacks, while a probe runs.

    Every record not from FRAMEWORK_LOGGERS is scrubbed and loses its
    traceback; REUSED_LOGGERS are also floored at WARNING.
    datahub's shared modules a provider calls into (datahub.utilities,
    datahub.ingestion.api) are scrubbed but not floored: they also serve the
    framework and the CLI, and flooring them would hide the probe's own
    diagnostics from `--debug`.

    `silenced` names loggers whose records are dropped at every level, not
    scrubbed: reused code a provider knows logs values read from the source
    (connector configs, response bodies) that have no credential shape for
    scrub_text to find. A framework logger cannot be silenced.

    The scrub runs in logging.Logger.callHandlers, which every record passes
    before any handler sees it. So it covers loggers and handlers created
    inside the guard (a library imported mid-probe that configures its own),
    handlers on loggers that stop propagation, and logging.lastResort. Not
    covered: a Logger subclass that overrides callHandlers, and code that
    calls a handler directly instead of logging through a logger. Warning
    capture is on for the guard's duration, so a library's warnings.warn is
    logged as `py.warnings` and scrubbed too, with or without the masking
    bootstrap.

    Every change is undone on exit, exception or not, so a nested guard (or
    two probes in one process) leaves logging as it found it; a nested
    guard's scrub runs before the outer one's. Guards on different threads
    may close in any order as far as the scrub goes (see _ActiveGuards):
    while any is open, every guard's scrub applies to every record. The
    WARNING floors are still per guard, restored to what each saw on entry.
    """
    if get_probe_verbose_logs():
        yield
        return
    undo: List[Callable[[], None]] = []
    try:
        guard = _ScrubFilter(secret_values, tuple(silenced))
        for logger in _guarded_loggers():
            if logger.getEffectiveLevel() < logging.WARNING:
                undo.append(partial(logger.setLevel, logger.level))
                logger.setLevel(logging.WARNING)
        _ACTIVE.open(guard)
        undo.append(partial(_ACTIVE.close, guard))
        yield
    finally:
        for step in reversed(undo):
            step()
