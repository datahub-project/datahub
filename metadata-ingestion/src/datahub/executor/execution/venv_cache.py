"""Locking and eviction for the node-local venv cache.

Split from venv_utils, which promises in its own docstring to work "without
performing any actual venv creation or management". Everything with a side
effect lives here.
"""

import fcntl
import logging
import os
import pathlib
from typing import Optional

logger = logging.getLogger(__name__)


class EntryLock:
    """An flock on one cache entry.

    Sound because the cache is node-local: one pod is the only writer. The lock
    lives as long as the file descriptor, so a crashed executor releases every
    lock it held with its fds -- nothing needs cleaning up after a crash.

    Every failure path returns False rather than raising. The cache is an
    optimisation; a task that could have run must never fail because of it.
    """

    def __init__(self, lock_path: pathlib.Path) -> None:
        self._lock_path = lock_path
        self._fd: Optional[int] = None

    @property
    def held(self) -> bool:
        return self._fd is not None

    def acquire(self, *, exclusive: bool, blocking: bool = True) -> bool:
        mode = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
        if not blocking:
            mode |= fcntl.LOCK_NB
        try:
            # The cache directory structure is created by the cache setup path,
            # not here -- EntryLock only locks an entry that already exists.
            fd = os.open(self._lock_path, os.O_RDWR | os.O_CREAT, 0o644)
        except OSError:
            logger.debug("venv cache: cannot open lock %s", self._lock_path)
            return False
        try:
            fcntl.flock(fd, mode)
        except OSError:
            # BlockingIOError for a contended LOCK_NB, and ENOLCK/EINVAL on a
            # filesystem without lock support. Both mean "no cache", not "fail".
            os.close(fd)
            return False
        self._fd = fd
        return True

    def downgrade_to_shared(self) -> None:
        """Convert an exclusive hold to shared on the same fd.

        Used by the build path: build under exclusive, then hold shared for the
        task's life so eviction stays out without shutting other runs out.
        """
        if self._fd is None:
            return
        try:
            fcntl.flock(self._fd, fcntl.LOCK_SH)
        except OSError:
            logger.debug("venv cache: could not downgrade %s", self._lock_path)

    def release(self) -> None:
        fd, self._fd = self._fd, None
        if fd is None:
            return
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError:
            pass
        try:
            os.close(fd)
        except OSError:
            pass
