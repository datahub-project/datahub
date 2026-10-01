import logging
from contextlib import contextmanager
from functools import partial
from typing import Callable, Dict, Iterator, List, Optional, Sequence, Set, Tuple

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
    # logging.captureWarnings, which the recipe CLI's masking bootstrap turns
    # on: a library's warnings.warn() text arrives here, at WARNING.
    "py.warnings",
)


def _under(name: str, prefixes: Tuple[str, ...]) -> bool:
    return any(name == g or name.startswith(g + ".") for g in prefixes)


def _is_reused(name: str) -> bool:
    return _under(name, REUSED_LOGGERS)


class _ScrubFilter(logging.Filter):
    """Scrubs every record not logged by the framework, dropping its traceback,
    holds the noisy reused loggers to WARNING or above, and drops a provider's
    silenced loggers outright.

    Installed on handlers as well as loggers, so it sees records from every
    logger that reaches those handlers; only the framework's own pass
    untouched.
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


def _reachable_handlers(loggers: List[logging.Logger]) -> List[logging.Handler]:
    """Every handler a record from `loggers` can propagate to.

    Not just the root's: `datahub --debug` gives the `datahub` logger its own
    handlers and stops propagation there, so a DEBUG record from a source never
    reaches the root at all.
    """
    found: Dict[int, logging.Handler] = {}
    for logger in [*loggers, logging.getLogger()]:
        current: Optional[logging.Logger] = logger
        while current is not None:
            for handler in current.handlers:
                found.setdefault(id(handler), handler)
            if not current.propagate:
                break
            current = current.parent
    return list(found.values())


@contextmanager
def quiet_reused_logs(
    secret_values: Set[str], silenced: Sequence[str] = ()
) -> Iterator[None]:
    """Keep reused code's logs scrubbed, without tracebacks, while a probe runs.

    Every record not from FRAMEWORK_LOGGERS that reaches a handler is scrubbed
    and loses its traceback; REUSED_LOGGERS are also floored at WARNING.
    datahub's shared modules a provider calls into (datahub.utilities,
    datahub.ingestion.api) are scrubbed but not floored: they also serve the
    framework and the CLI, and flooring them would hide the probe's own
    diagnostics from `--debug`.

    `silenced` names loggers whose records are dropped at every level, not
    scrubbed: reused code a provider knows logs values read from the source
    (connector configs, response bodies) that have no credential shape for
    scrub_text to find. A framework logger cannot be silenced.

    Every change is undone in reverse order on exit, exception or not, so a
    nested guard (or two probes in one process) leaves logging exactly as it
    found it. Not safe against two guards on different threads exiting out of
    order: each restores the levels it saw on entry.
    """
    if get_probe_verbose_logs():
        yield
        return
    undo: List[Callable[[], None]] = []
    try:
        guard = _ScrubFilter(secret_values, tuple(silenced))
        loggers = _guarded_loggers()
        for logger in loggers:
            if logger.getEffectiveLevel() < logging.WARNING:
                undo.append(partial(logger.setLevel, logger.level))
                logger.setLevel(logging.WARNING)
            logger.addFilter(guard)
            undo.append(partial(logger.removeFilter, guard))
        # A logger created inside the guard (a lazy import) has no filter of
        # its own; the handlers its records reach do. Every existing logger's
        # chain, not just the guarded ones': a library that attached its own
        # handler and stopped propagation is reused code too.
        existing = [
            obj
            for obj in list(logging.Logger.manager.loggerDict.values())
            if isinstance(obj, logging.Logger)
        ]
        handlers = _reachable_handlers([*loggers, *existing])
        # Written to when a record finds no handler on its chain at all: a
        # logger created inside the guard with propagate off, say. Not covered:
        # a handler added inside the guard to such a logger.
        if logging.lastResort is not None and logging.lastResort not in handlers:
            handlers.append(logging.lastResort)
        for handler in handlers:
            handler.addFilter(guard)
            undo.append(partial(handler.removeFilter, guard))
        yield
    finally:
        for step in reversed(undo):
            step()
