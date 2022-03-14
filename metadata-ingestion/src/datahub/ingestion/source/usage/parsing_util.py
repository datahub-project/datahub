import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)


@contextmanager
def parse_with_exception_logging(self, parse_entry, entry_identifier, *args, **kwds):
    try:
        yield
    except Exception as e:
        self.error(
            logger,
            entry_identifier,
            f"unable to parse entry: {parse_entry!r}: {e}",
        )
