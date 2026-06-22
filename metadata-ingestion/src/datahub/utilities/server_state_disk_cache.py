"""Best-effort on-disk JSON cache keyed by (server URL, server commit hash).

For data that is immutable for a given server version (entity/aspect specs,
GraphQL schema introspection, ...). The commit hash is part of the key, so an
entry self-invalidates on server upgrade — a stale entry is simply never read.

All operations are best-effort: any I/O error is treated as a cache miss and
never raised. Writes are atomic (temp file + rename) so a crash can't leave a
truncated file, and symlinked cache paths are refused to avoid writing through
to unintended targets.
"""

import hashlib
import json
import logging
import os
import tempfile
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Inlined rather than imported from datahub.cli.config_utils: this is a
# low-level utility and must not depend on the higher-level cli package.
_DATAHUB_ROOT = os.path.join(os.path.expanduser("~"), ".datahub")
_TMP_PREFIX = ".cache_"
_STALE_TMP_AGE_SECONDS = 3600


class ServerStateDiskCache:
    """A namespaced disk cache of JSON values keyed by (server_url, commit_hash)."""

    def __init__(self, namespace: str, *, max_age_days: int = 7) -> None:
        self._dir = Path(_DATAHUB_ROOT) / namespace
        self._max_age_days = max_age_days

    def _path(self, server_url: str, commit_hash: str) -> Path:
        key = hashlib.sha256(f"{server_url}:{commit_hash}".encode()).hexdigest()[:32]
        return self._dir / f"{key}.json"

    def get(self, server_url: str, commit_hash: str) -> Optional[dict]:
        """Return the cached value, or None on miss/error."""
        try:
            path = self._path(server_url, commit_hash)
            if path.is_symlink():
                logger.warning("Ignoring symlinked cache file: %s", path)
                return None
            # Direct read (no exists() check) to avoid a TOCTOU race.
            return json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except json.JSONDecodeError:
            # Corrupt file (e.g. truncated by a crash before atomic writes
            # landed) — drop it so we re-fetch instead of failing every get().
            logger.debug("Removing corrupt disk cache file: %s", path, exc_info=True)
            try:
                path.unlink()
            except OSError:
                pass
            return None
        except Exception:
            logger.debug("Disk cache miss", exc_info=True)
            return None

    def put(self, server_url: str, commit_hash: str, value: dict) -> None:
        """Store a value. Best-effort — never raises."""
        try:
            path = self._path(server_url, commit_hash)
            if path.is_symlink():
                logger.warning("Refusing to write to symlinked cache path: %s", path)
                return
            path.parent.mkdir(parents=True, exist_ok=True)
            fd, tmp_path = tempfile.mkstemp(
                dir=str(path.parent), suffix=".tmp", prefix=_TMP_PREFIX
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(value, f)
                os.replace(tmp_path, str(path))
            except BaseException:
                # Clean up the temp file on any failure, including KeyboardInterrupt.
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
            self._cleanup()
        except Exception:
            logger.debug("Failed to write disk cache", exc_info=True)

    def _cleanup(self) -> None:
        """Drop cache files older than max_age_days and stale temp files."""
        try:
            cutoff = time.time() - self._max_age_days * 86400
            for f in self._dir.glob("*.json"):
                try:
                    if f.stat().st_mtime < cutoff:
                        f.unlink()
                except FileNotFoundError:
                    pass  # Another process already removed it.
                except Exception:
                    pass  # Permission errors etc. — skip this file.
            tmp_cutoff = time.time() - _STALE_TMP_AGE_SECONDS
            for f in self._dir.glob(f"{_TMP_PREFIX}*.tmp"):
                try:
                    if f.stat().st_mtime < tmp_cutoff:
                        f.unlink()
                except Exception:
                    pass
        except Exception:
            logger.debug("Cache cleanup error", exc_info=True)
