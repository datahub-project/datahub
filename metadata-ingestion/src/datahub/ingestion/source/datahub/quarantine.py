import json
import logging
import threading
from typing import Any, Dict, Optional, TextIO

logger = logging.getLogger(__name__)


class QuarantineWriter:
    """Appends rows that could not be parsed to an NDJSON file, one JSON object per line.

    Rows reach this writer when no valid MCP could be built from them, so they cannot go
    to the pipeline's dead-letter queue, which stores replayable MCPs. The artifact is
    diagnostic: it makes every dropped row enumerable so it can be fixed at the source.

    The file is opened on first write, so a run that drops nothing leaves nothing behind.
    Lines are flushed as they are written so the file survives an abrupt kill; parse
    errors are rare enough that the per-line flush is not a throughput concern.
    """

    def __init__(self, filename: str) -> None:
        self.filename = filename
        self.records_written = 0
        self._file: Optional[TextIO] = None
        self._lock = threading.Lock()
        self._disabled = False

    def write(self, row: Dict[str, Any], error: str) -> None:
        if self._disabled:
            return

        # default=str because createdon is a datetime.
        line = json.dumps({"error": error, "row": row}, default=str)

        with self._lock:
            try:
                if self._file is None:
                    self._file = open(self.filename, "a", encoding="utf-8")
                    logger.info(f"Writing unparseable rows to {self.filename}")
                self._file.write(line + "\n")
                self._file.flush()
                self.records_written += 1
            except OSError as e:
                logger.warning(
                    f"Disabling the parse-error quarantine, cannot write to "
                    f"{self.filename}: {e}"
                )
                self._disabled = True
                self._file = None

    def close(self) -> None:
        with self._lock:
            if self._file is not None:
                self._file.close()
                self._file = None
