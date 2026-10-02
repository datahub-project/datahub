import logging
from contextlib import contextmanager
from functools import partial
from typing import Callable, Iterator, List, Sequence, Set, Tuple

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
    # logging.captureWarnings, which the guard turns on for its duration: a
    # library's warnings.warn() text arrives here, at WARNING.
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


def _guarded_call_handlers(
    guard: logging.Filter, inner: _CallHandlers
) -> _CallHandlers:
    def call_handlers(logger: logging.Logger, record: logging.LogRecord) -> None:
        if guard.filter(record):
            inner(logger, record)

    return call_handlers


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

    Every change is undone in reverse order on exit, exception or not, so a
    nested guard (or two probes in one process) leaves logging exactly as it
    found it; a nested guard's scrub runs before the outer one's. Not safe
    against two guards on different threads exiting out of order: each
    restores what it saw on entry.
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
        # vars(), not getattr: the exact attribute the class held, so the
        # restore leaves it as it was, an outer guard's wrapper included.
        saved: _CallHandlers = vars(logging.Logger)["callHandlers"]
        setattr(  # noqa: B010
            logging.Logger, "callHandlers", _guarded_call_handlers(guard, saved)
        )
        undo.append(partial(setattr, logging.Logger, "callHandlers", saved))
        if getattr(logging, "_warnings_showwarning", None) is None:
            logging.captureWarnings(True)
            undo.append(partial(logging.captureWarnings, False))
        yield
    finally:
        for step in reversed(undo):
            step()
