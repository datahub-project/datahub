"""End to end: the venv cache lock follows the process actually using the venv.

The unit tests pin down each piece -- the descriptor handoff, close-not-unlock,
the wrapper passing the descriptor on. This drives the whole chain with real
processes, because the property that matters only exists across them:

    executor  --pass_fds-->  wrapper  --pass_fds-->  datahub CLI

and the claim is that eviction refuses the entry for exactly as long as ANY of
those processes is alive, with no cleanup code running when they die. SIGKILL
is used throughout because that is the case nothing else here can cover: an
OOM kill or a node drain runs no `finally`, no signal handler, nothing.

Production code at every hop: EntryLock and SubProcessTaskUtil.lock_handoff in
the executor, the real wrapper_common.run_datahub_subprocess for the second
hop, and the real evict_stale_entries as the adversary trying to reclaim the
entry. Only the datahub CLI itself is a stand-in, since what it does once it
holds the descriptor is irrelevant here.
"""

import contextlib
import os
import pathlib
import signal
import subprocess
import sys
import textwrap
import time
from typing import Callable, Optional

import psutil
import pytest

from datahub.executor.execution import venv_utils
from datahub.executor.execution.venv_cache import EntryLock, evict_stale_entries

pytestmark = pytest.mark.skipif(
    not hasattr(os, "killpg"), reason="needs POSIX process groups"
)

# Generous, because CI runners vary; every wait polls and returns as soon as
# the condition holds, so the budget costs nothing when things work.
_DEADLINE_SEC = 30

# The stand-in CLI: announce its PID once it holds the inherited descriptor,
# then outlive everything above it.
_CLI = textwrap.dedent(
    """
    import pathlib, os, sys, time
    pathlib.Path(sys.argv[1]).write_text(str(os.getpid()))
    time.sleep(300)
    """
)

# The wrapper hop, running the real production helper. It blocks streaming the
# CLI's output, so it stays alive holding its own copy until it is killed.
_WRAPPER = textwrap.dedent(
    """
    import sys
    from datahub.executor.execution import wrapper_common
    cli_ready = sys.argv[1]
    sys.exit(wrapper_common.run_datahub_subprocess(
        [sys.executable, "-c", {cli!r}, cli_ready], "{{}}"
    ))
    """
).format(cli=_CLI)

# The executor side: take the entry SHARED exactly as setup_venv does, hand it
# to the wrapper through the real lock_handoff, then idle until SIGKILLed --
# deliberately never releasing, since a killed executor never gets to.
_EXECUTOR = textwrap.dedent(
    """
    import os, pathlib, subprocess, sys, time
    from datahub.executor.execution.runner import VenvConfig, VenvReference
    from datahub.executor.execution.sub_process_task_common import SubProcessTaskUtil
    from datahub.executor.execution.venv_cache import EntryLock

    lock_path, venv_loc, wrapper_pid_file, cli_ready = sys.argv[1:5]
    lock = EntryLock(pathlib.Path(lock_path))
    if not lock.try_acquire(exclusive=False).ok:
        sys.exit("could not take the entry SHARED")
    ref = VenvReference(
        venv_loc=pathlib.Path(venv_loc),
        venv_config=VenvConfig(version="0.1.0"),
        lock=lock,
    )
    fds, env_extra = SubProcessTaskUtil.lock_handoff(ref)
    if not fds:
        sys.exit("lock_handoff produced no descriptor")
    wrapper = subprocess.Popen(
        [sys.executable, "-c", {wrapper!r}, cli_ready],
        env={{**os.environ, **env_extra}},
        pass_fds=fds,
        # As the ingestion task does, so the wrapper and CLI survive the
        # executor rather than dying with its process group.
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    pathlib.Path(wrapper_pid_file).write_text(str(wrapper.pid))
    time.sleep(300)
    """
).format(wrapper=_WRAPPER)


def _wait_for(
    predicate: Callable[[], bool],
    what: str,
    *,
    unless_exited: Optional[subprocess.Popen] = None,
) -> None:
    """Poll until `predicate` holds.

    `unless_exited` names a process whose early death means the wait can
    never succeed. Without it, a setup failure surfaces as a bare timeout
    after the full deadline -- which reads as flakiness and gets retried
    rather than read.
    """
    deadline = time.monotonic() + _DEADLINE_SEC
    while time.monotonic() < deadline:
        if predicate():
            return
        if unless_exited is not None and unless_exited.poll() is not None:
            err = unless_exited.stderr.read() if unless_exited.stderr else b""
            # The last line is the reason: sys.exit(msg) writes it there, after
            # any import-time warnings the child printed first.
            lines = err.decode(errors="replace").strip().splitlines()
            raise AssertionError(
                f"the executor exited ({unless_exited.returncode}) before "
                f"{what}: {lines[-1] if lines else '(no output)'}"
            )
        time.sleep(0.05)
    raise AssertionError(f"timed out after {_DEADLINE_SEC}s waiting for {what}")


def _read_pid(path: pathlib.Path, executor: subprocess.Popen) -> int:
    _wait_for(
        lambda: path.exists() and path.read_text().strip() != "",
        f"{path.name} to be written",
        unless_exited=executor,
    )
    return int(path.read_text().strip())


def _peer_can_take_exclusive(lock_path: pathlib.Path) -> bool:
    """Whether an evictor could take this entry right now."""
    peer = EntryLock(lock_path)
    if peer.try_acquire(exclusive=True).ok:
        peer.release()
        return True
    return False


def _alive(pid: int) -> bool:
    """Whether `pid` is still running, counting a zombie as dead.

    A zombie is exactly equivalent to a reaped process for this test: the
    kernel closes every descriptor at exit, before the process becomes a
    zombie, so its copy of the lock is already gone. And reaping is not ours
    to wait for -- once the executor is dead, the wrapper is reparented to
    PID 1, and a container init that does not reap would leave it a zombie
    forever, turning a correct run into a timeout.
    """
    try:
        return psutil.Process(pid).status() != psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return False


def _sigkill(pid: int) -> None:
    with contextlib.suppress(ProcessLookupError):
        os.kill(pid, signal.SIGKILL)


def _make_entry(cache_root: pathlib.Path) -> pathlib.Path:
    """A complete, long-idle cache entry: the first thing eviction would take."""
    venv = cache_root / f"{venv_utils.ENTRY_PREFIX}e2e-0.1.0-abc"
    (venv / "bin").mkdir(parents=True)
    (venv / "bin" / "python").touch()
    venv_utils.mark_venv_complete(venv)
    venv_utils.touch_last_used(venv)
    stale = time.time() - 30 * 24 * 3600
    os.utime(venv / venv_utils.LAST_USED_MARKER, (stale, stale))
    return venv


def _try_to_evict(cache_root: pathlib.Path) -> int:
    """Everything is over limit and past max age: evict anything it can take."""
    return evict_stale_entries(cache_root, max_entries=0, max_age_sec=1)


def test_the_entry_stays_protected_until_the_last_process_using_it_dies(
    tmp_path: pathlib.Path,
) -> None:
    cache_root = tmp_path / "_venv_cache"
    venv = _make_entry(cache_root)
    lock_path = cache_root / f"{venv.name}.lock"
    wrapper_pid_file = tmp_path / "wrapper.pid"
    cli_ready = tmp_path / "cli.pid"

    executor = subprocess.Popen(
        [
            sys.executable,
            "-c",
            _EXECUTOR,
            str(lock_path),
            str(venv),
            str(wrapper_pid_file),
            str(cli_ready),
        ],
        start_new_session=True,
        # Captured so a setup failure reports its reason instead of timing out.
        stderr=subprocess.PIPE,
    )
    wrapper_pid = cli_pid = None
    try:
        wrapper_pid = _read_pid(wrapper_pid_file, executor)
        # The CLI writes its PID only once it is running with the inherited
        # descriptor, so from here all three processes hold a copy.
        cli_pid = _read_pid(cli_ready, executor)
        assert not _peer_can_take_exclusive(lock_path), "the entry was never locked"

        # 1. The executor dies without running a line of cleanup.
        _sigkill(executor.pid)
        executor.wait(timeout=_DEADLINE_SEC)

        assert not _peer_can_take_exclusive(lock_path), (
            "the lock died with the executor: nothing reached the wrapper, "
            "so a killed executor leaves a running ingestion evictable"
        )
        assert _try_to_evict(cache_root) == 0
        assert venv.is_dir(), "eviction deleted a venv two processes still use"

        # 2. The wrapper dies too. The CLI keeps running -- it has its own
        #    session precisely so that it can -- and must still be protected.
        _sigkill(wrapper_pid)
        _wait_for(lambda: not _alive(wrapper_pid), "the wrapper to exit")
        assert _alive(cli_pid), "the stand-in CLI died with the wrapper"

        assert not _peer_can_take_exclusive(lock_path), (
            "the lock died with the wrapper: the descriptor never reached the "
            "datahub CLI, which is still executing out of this venv"
        )
        assert _try_to_evict(cache_root) == 0
        assert venv.is_dir(), "eviction deleted the venv the CLI is running from"

        # 3. The last user dies. Now, and only now, the entry is reclaimable
        #    -- still with no cleanup code having run anywhere.
        _sigkill(cli_pid)
        _wait_for(
            lambda: _peer_can_take_exclusive(lock_path),
            "the kernel to release the lock after the last holder died",
        )
        assert _try_to_evict(cache_root) == 1
        assert not venv.exists(), "an entry nothing uses any more was not reclaimed"
    finally:
        _sigkill(executor.pid)
        for pid in (wrapper_pid, cli_pid):
            if pid is not None:
                _sigkill(pid)
        if wrapper_pid is not None:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(wrapper_pid, signal.SIGKILL)
        with contextlib.suppress(Exception):
            executor.wait(timeout=5)
