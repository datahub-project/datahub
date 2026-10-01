import logging
from contextlib import contextmanager
from functools import partial
from typing import Callable, Dict, Iterator, List, Optional, Set, Tuple

from datahub.configuration.env_vars import get_probe_verbose_logs
from datahub.ingestion.agent.redact import scrub_text

# Loggers of code a probe reuses: ingestion sources and the SDKs/drivers they
# call. Their DEBUG output and tracebacks quote connection strings, request
# URLs and token-endpoint bodies (botocore logged google-auth's refresh error
# with the subject-token path; Kafka Connect's fetchers log raw connect URLs).
#
# Not the framework's own `datahub.ingestion.agent` or the CLI's loggers: what
# they log is written here, and flooring them would hide the probe's own
# diagnostics from `--debug`.
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
)


def _is_reused(name: str) -> bool:
    return any(name == g or name.startswith(g + ".") for g in REUSED_LOGGERS)


class _ScrubFilter(logging.Filter):
    """Holds a reused logger's records to scrubbed WARNING-or-above text.

    Installed on handlers as well as loggers, so it sees records from every
    logger that reaches those handlers and must leave the others untouched.
    """

    def __init__(self, secret_values: Set[str]) -> None:
        super().__init__()
        # The caller's set, not a copy: the CLI's set is what it masks its own
        # output against, so the two cannot drift.
        self._secrets = secret_values

    def filter(self, record: logging.LogRecord) -> bool:
        if not _is_reused(record.name):
            return True
        # The level floor on the logger stops most of these before a record is
        # built; this catches a child logger created inside the guard with its
        # own DEBUG level, which no floor was applied to.
        if record.levelno < logging.WARNING:
            return False
        try:
            message = record.getMessage()
        except Exception:
            # Mismatched args: logging would report the error itself later,
            # with the raw args in it.
            message = str(record.msg)
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
def quiet_reused_logs(secret_values: Set[str]) -> Iterator[None]:
    """Keep reused code's logs to scrubbed warnings while a probe runs.

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
        guard = _ScrubFilter(secret_values)
        loggers = _guarded_loggers()
        for logger in loggers:
            if logger.getEffectiveLevel() < logging.WARNING:
                undo.append(partial(logger.setLevel, logger.level))
                logger.setLevel(logging.WARNING)
            logger.addFilter(guard)
            undo.append(partial(logger.removeFilter, guard))
        # A logger created inside the guard (a lazy import) has no filter of
        # its own; the handlers its records reach do.
        for handler in _reachable_handlers(loggers):
            handler.addFilter(guard)
            undo.append(partial(handler.removeFilter, guard))
        yield
    finally:
        for step in reversed(undo):
            step()
