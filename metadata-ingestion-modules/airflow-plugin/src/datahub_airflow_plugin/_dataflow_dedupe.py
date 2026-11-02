"""Track which DAG runs have already had their DataFlow emitted from a task event.

On Airflow 3 every task instance runs in its own process, so an in-memory set only
dedupes within one task. A marker file per (dag_id, run_id) in a worker-local
directory extends the dedupe to every task that runs on the same worker. Any
filesystem error fails open: the caller emits as it would without dedupe.
"""

import hashlib
import logging
import os
import tempfile
import threading
import time
from collections import OrderedDict
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_DEFAULT_MAX_ENTRIES = 1024
_DEFAULT_MARKER_TTL_SECONDS = 24 * 60 * 60


def default_marker_dir() -> Path:
    return Path(tempfile.gettempdir()) / "datahub_airflow_plugin" / "dataflow_runs"


class DagRunEmissionTracker:
    def __init__(
        self,
        marker_dir: Optional[Path] = None,
        max_entries: int = _DEFAULT_MAX_ENTRIES,
        marker_ttl_seconds: int = _DEFAULT_MARKER_TTL_SECONDS,
    ) -> None:
        self._marker_dir = marker_dir or default_marker_dir()
        self._max_entries = max_entries
        self._marker_ttl_seconds = marker_ttl_seconds
        self._claimed: "OrderedDict[str, None]" = OrderedDict()
        self._lock = threading.Lock()

    @staticmethod
    def _key(dag_id: str, run_id: str) -> str:
        return hashlib.sha256(f"{dag_id}\x00{run_id}".encode()).hexdigest()

    def claim(self, dag_id: str, run_id: str) -> bool:
        """Return True if this caller should emit the DataFlow for this DAG run."""
        key = self._key(dag_id, run_id)
        with self._lock:
            if key in self._claimed:
                self._claimed.move_to_end(key)
                return False
            self._claimed[key] = None
            while len(self._claimed) > self._max_entries:
                self._claimed.popitem(last=False)
        return self._claim_marker(key)

    def _claim_marker(self, key: str) -> bool:
        path = self._marker_dir / key
        try:
            self._marker_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            os.close(fd)
        except FileExistsError:
            return self._reclaim_if_stale(path)
        except OSError as e:
            logger.debug(f"DataFlow dedupe marker unavailable ({e}); emitting anyway")
            return True
        # Prune roughly once per 256 claims so the directory stays bounded.
        if key.startswith("00"):
            self._prune_stale_markers()
        return True

    def _reclaim_if_stale(self, path: Path) -> bool:
        try:
            if time.time() - path.stat().st_mtime < self._marker_ttl_seconds:
                return False
            os.utime(path)
        except OSError as e:
            logger.debug(f"DataFlow dedupe marker unreadable ({e}); emitting anyway")
        return True

    def _prune_stale_markers(self) -> None:
        cutoff = time.time() - self._marker_ttl_seconds
        try:
            for marker in self._marker_dir.iterdir():
                try:
                    if marker.stat().st_mtime < cutoff:
                        marker.unlink()
                except OSError:
                    continue
        except OSError as e:
            logger.debug(f"Skipping DataFlow dedupe marker pruning ({e})")
