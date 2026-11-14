"""Read embedded OpenSearch readiness without blocking on a silent child."""

from __future__ import annotations

import queue
import re
import subprocess
import threading
from typing import TextIO


def wait_for_ready(
    proc: subprocess.Popen[str], log_file: TextIO, timeout_seconds: float
) -> int | None:
    ready_events: queue.Queue[int | None] = queue.Queue()
    assert proc.stdout is not None

    def drain_output() -> None:
        assert proc.stdout is not None
        for line in proc.stdout:
            log_file.write(line)
            log_file.flush()
            match = re.search(r"READY (\d+)\s*$", line)
            if match:
                ready_events.put(int(match.group(1)))
        ready_events.put(None)

    threading.Thread(target=drain_output, daemon=True).start()
    try:
        return ready_events.get(timeout=timeout_seconds)
    except queue.Empty:
        return None
