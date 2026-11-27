"""Boot diagnostics kept separate from credential-bearing stack discovery."""

from __future__ import annotations

import json
import os
import stat
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path


class BootTimings:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.started = time.monotonic()
        self.warm = True
        self.phases: dict[str, float] = {}
        self.save()

    @contextmanager
    def phase(self, name: str) -> Iterator[None]:
        started = time.monotonic()
        try:
            yield
        finally:
            # Several instances can perform the same phase. Keep their cumulative cost.
            self.phases[name] = self.phases.get(name, 0.0) + time.monotonic() - started
            self.save()

    def save(self) -> dict[str, object]:
        result: dict[str, object] = {
            "warm": self.warm,
            "total_s": round(time.monotonic() - self.started, 2),
            "phases": {name: round(value, 2) for name, value in self.phases.items()},
        }
        # Publish each completed phase so failed boots retain useful diagnostics too.
        temporary = self.path.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(result, indent=2) + "\n")
        temporary.replace(self.path)
        return result


def dir_size_mb(root: Path) -> float:
    total = 0
    for directory, _, files in os.walk(root):
        for name in files:
            try:
                info = os.lstat(Path(directory) / name)
            except FileNotFoundError:
                # Database and log files may rotate during the walk.
                continue
            if stat.S_ISREG(info.st_mode):
                total += info.st_size
    return total / (1024 * 1024)
