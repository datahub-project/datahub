#!/usr/bin/env python3
"""Manage one detached OpenSearch 3 node shared by local GMS stacks (stdlib only)."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import queue
import re
import signal
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import TextIO

REPO_ROOT = Path(__file__).resolve().parents[3]
MAIN_CLASS = "com.linkedin.metadata.opensearch.embedded.EmbeddedOpenSearchMain"


def default_shared_dir() -> Path:
    return Path(
        os.environ.get("GMS_LITE_OPENSEARCH_DIR", "~/.datahub/local/opensearch-3")
    )


def request_json(port: int, path: str, method: str = "GET") -> object:
    request = urllib.request.Request(f"http://localhost:{port}{path}", method=method)
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


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


def index_names(port: int) -> list[str]:
    rows = request_json(port, "/_cat/indices?format=json&h=index&expand_wildcards=all")
    assert isinstance(rows, list)
    return [str(row["index"]) for row in rows]


def delete_prefixes(port: int, prefixes: list[str]) -> None:
    """Enumerate first, then delete exact names; never send a wildcard DELETE."""
    if not prefixes or any(not re.fullmatch(r"[a-z0-9]+", p) for p in prefixes):
        raise ValueError("expected nonempty launcher index prefixes")

    def owned(name: str) -> bool:
        return any(name.startswith(prefix + "_") for prefix in prefixes)

    def delete(path: str, name: str) -> None:
        try:
            request_json(port, path + urllib.parse.quote(name, safe=""), "DELETE")
        except urllib.error.HTTPError as error:
            if error.code != 404:
                raise

    # Data streams must go before their hidden backing indices.
    streams = request_json(port, "/_data_stream")
    assert isinstance(streams, dict)
    for stream in streams.get("data_streams", []):
        if owned(stream["name"]):
            delete("/_data_stream/", stream["name"])
    aliases = request_json(port, "/_alias?expand_wildcards=all")
    assert isinstance(aliases, dict)
    for index, info in aliases.items():
        for alias in info.get("aliases", {}):
            if owned(alias):
                delete("/" + urllib.parse.quote(index, safe="") + "/_alias/", alias)
    for name in index_names(port):
        if owned(name):
            delete("/", name)
    for endpoint, key in (
        ("_index_template", "index_templates"),
        ("_component_template", "component_templates"),
    ):
        templates = request_json(port, "/" + endpoint)
        assert isinstance(templates, dict)
        for template in templates.get(key, []):
            if owned(template["name"]):
                delete("/" + endpoint + "/", template["name"])
    templates = request_json(port, "/_template")
    assert isinstance(templates, dict)
    for name in templates:
        if owned(name):
            delete("/_template/", name)


class SharedOpenSearchManager:
    READY_TIMEOUT_SECONDS = 900

    def __init__(self, shared_dir: Path, repo_root: Path = REPO_ROOT) -> None:
        self.shared_dir = shared_dir.expanduser().resolve()
        self.repo_root = repo_root
        self.pid_file = self.shared_dir / "pid"
        self.port_file = self.shared_dir / "port"
        self.log_file = self.shared_dir / "log"

    @contextmanager
    def _lock(self) -> Iterator[None]:
        self.shared_dir.mkdir(parents=True, exist_ok=True)
        # Never unlink this file: waiters must continue locking the same inode.
        with (self.shared_dir / "startup.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            yield

    @staticmethod
    def _read_number(path: Path) -> int | None:
        try:
            value = int(path.read_text().strip())
            return value if value > 0 else None
        except (OSError, ValueError):
            return None

    @staticmethod
    def _alive(pid: int) -> bool:
        result = subprocess.run(
            ["ps", "-p", str(pid), "-o", "stat="],
            capture_output=True,
            text=True,
            check=False,
        )
        return (
            result.returncode == 0
            and bool(result.stdout.strip())
            and not result.stdout.strip().startswith("Z")
        )

    def _verify_identity(self, pid: int) -> None:
        result = subprocess.run(
            ["ps", "-ww", "-p", str(pid), "-o", "command="],
            capture_output=True,
            text=True,
            check=False,
        )
        command = result.stdout.strip()
        if f" {MAIN_CLASS} " not in command or not command.endswith(
            f" --data {self.shared_dir}"
        ):
            raise RuntimeError(
                f"refusing to signal or reuse unrelated PID {pid}; recorded in {self.pid_file}"
            )

    @staticmethod
    def _healthy(port: int) -> bool:
        try:
            root = request_json(port, "/")
            assert isinstance(root, dict)
            version = root.get("version", {}).get("number", "unknown")
            if not str(version).startswith("3."):
                raise RuntimeError(
                    f"OpenSearch {version} on port {port}: expected major version 3; refusing to reuse or replace it"
                )
            # New primary shards can briefly be red while another stack bootstraps.
            health = request_json(
                port, "/_cluster/health?wait_for_status=yellow&timeout=5s"
            )
            return isinstance(health, dict) and health.get("status") in (
                "yellow",
                "green",
            )
        except (urllib.error.URLError, OSError, ValueError):
            return False

    def status(self) -> dict[str, object]:
        with self._lock():
            pid = self._read_number(self.pid_file)
            port = self._read_number(self.port_file)
            running = pid is not None and self._alive(pid)
            if running:
                assert pid is not None
                self._verify_identity(pid)
            return {
                "pid": pid,
                "port": port,
                "running": running,
                "healthy": running and port is not None and self._healthy(port),
                "stateDir": str(self.shared_dir),
            }

    def get_or_start(self) -> int:
        with self._lock():
            pid = self._read_number(self.pid_file)
            port = self._read_number(self.port_file)
            if pid is not None and self._alive(pid):
                self._verify_identity(pid)
                if port is not None and self._healthy(port):
                    return port
                self._stop_locked()
            self.port_file.unlink(missing_ok=True)
            return self._start()

    def _start(self) -> int:
        script = (
            self.repo_root
            / "metadata-service/opensearch-embedded/build/install/opensearch-embedded/bin/opensearch-embedded"
        )
        if not script.exists():
            raise RuntimeError(
                f"missing {script}; build :metadata-service:opensearch-embedded:installDist first"
            )
        # Direct file output survives the launcher exiting; no pipe/thread owns the shared log.
        with self.log_file.open("w") as output:
            process = subprocess.Popen(
                [str(script), "--port", "0", "--data", str(self.shared_dir)],
                cwd=self.repo_root,
                stdin=subprocess.DEVNULL,
                stdout=output,
                stderr=subprocess.STDOUT,
                text=True,
                start_new_session=True,
            )
        self.pid_file.write_text(str(process.pid))
        try:
            deadline = time.monotonic() + self.READY_TIMEOUT_SECONDS
            with self.log_file.open() as output:
                while time.monotonic() < deadline and process.poll() is None:
                    line = output.readline()
                    if match := re.search(r"READY (\d+)\s*$", line):
                        port = int(match.group(1))
                        if self._healthy(port):
                            self.port_file.write_text(str(port))
                            return port
                    if not line:
                        time.sleep(0.1)
            raise RuntimeError(
                f"shared OpenSearch failed to become ready; see {self.log_file}. Data retained; repair it or choose a different --dir"
            )
        except BaseException:
            # This handle still refers to the child we just created, including before shell exec.
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
            self.pid_file.unlink(missing_ok=True)
            self.port_file.unlink(missing_ok=True)
            raise

    def _stop_locked(self) -> bool:
        pid = self._read_number(self.pid_file)
        stopped = pid is not None and self._alive(pid)
        if stopped:
            assert pid is not None
            self._verify_identity(pid)
            os.kill(pid, signal.SIGTERM)
            deadline = time.monotonic() + 30
            while self._alive(pid) and time.monotonic() < deadline:
                time.sleep(0.1)
            if self._alive(pid):
                self._verify_identity(pid)
                os.kill(pid, signal.SIGKILL)
                deadline = time.monotonic() + 5
                while self._alive(pid) and time.monotonic() < deadline:
                    time.sleep(0.1)
                if self._alive(pid):
                    raise RuntimeError(f"OpenSearch PID {pid} did not stop")
        self.pid_file.unlink(missing_ok=True)
        self.port_file.unlink(missing_ok=True)
        return stopped

    def stop(self) -> bool:
        with self._lock():
            return self._stop_locked()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["status", "start", "stop"])
    parser.add_argument("--dir", type=Path, default=default_shared_dir())
    args = parser.parse_args()
    manager = SharedOpenSearchManager(args.dir)
    try:
        if args.command == "status":
            print(json.dumps(manager.status(), indent=2))
        elif args.command == "start":
            print(f"OpenSearch ready on port {manager.get_or_start()}")
        else:
            print("stopped" if manager.stop() else "not running")
    except RuntimeError as error:
        raise SystemExit(str(error)) from error


if __name__ == "__main__":
    main()
