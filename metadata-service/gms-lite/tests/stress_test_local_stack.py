#!/usr/bin/env python3
"""Opt-in real-stack checks. Append/flush each verdict to results.jsonl; resume completed cases."""

from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from pathlib import Path
from typing import TypedDict, cast

REPO_ROOT = Path(__file__).resolve().parents[3]
LAUNCHER = REPO_ROOT / "metadata-service/gms-lite/bin/gms-lite-stack.py"
MANAGER = LAUNCHER.with_name("_opensearch_manager.py")
URN = "urn:li:dataset:(urn:li:dataPlatform:hive,gms_lite_verification,PROD)"


class StackInfo(TypedDict):
    bootId: str
    instances: dict[str, str]
    indexPrefixes: dict[str, str]
    opensearchPort: int
    opensearchShared: bool
    adminTokens: dict[str, str]


def request(url: str, token: str = "", body: object = None) -> object:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(
        url,
        headers=headers,
        data=json.dumps(body).encode() if body is not None else None,
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            content = response.read().decode()
    except urllib.error.HTTPError as error:
        raise RuntimeError(
            f"HTTP {error.code} {url}: {error.read().decode()}"
        ) from error
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        return content


def graphql(info: StackInfo, instance: str, query: str) -> dict[str, object]:
    result = request(
        f"{info['instances'][instance]}/api/graphql",
        info["adminTokens"][instance],
        {"query": query},
    )
    if not isinstance(result, dict) or result.get("errors"):
        raise AssertionError(f"GraphQL failed: {result}")
    return cast(dict[str, object], result["data"])


def ingest(info: StackInfo, instance: str) -> None:
    request(
        f"{info['instances'][instance]}/openapi/v3/entity/dataset?async=false",
        info["adminTokens"][instance],
        [
            {
                "urn": URN,
                "datasetKey": {
                    "value": {
                        "name": "gms_lite_verification",
                        "platform": "urn:li:dataPlatform:hive",
                        "origin": "PROD",
                    }
                },
                "datasetProperties": {
                    "value": {
                        "name": "GMS lite verification",
                        "description": "Local persistence and isolation check",
                    }
                },
            }
        ],
    )


def dataset(info: StackInfo, instance: str) -> object:
    data = graphql(
        info,
        instance,
        "{ dataset(urn: " + json.dumps(URN) + ") { urn properties { description } } }",
    )["dataset"]
    # Typed entity resolvers can return an URN shell even when the entity does not exist.
    return data if isinstance(data, dict) and data.get("properties") else None


def entity_exists(info: StackInfo, instance: str) -> bool:
    return (
        graphql(info, instance, "{ entityExists(urn: " + json.dumps(URN) + ") }")[
            "entityExists"
        ]
        is True
    )


def wait_for(
    description: str, condition: Callable[[], bool], timeout: float = 60
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(1)
    raise AssertionError(f"timed out: {description}")


def manager_command(shared: Path, command: str) -> str:
    return subprocess.check_output(
        [sys.executable, str(MANAGER), command, "--dir", str(shared)], text=True
    )


def shared_status(shared: Path) -> dict[str, object]:
    return cast(dict[str, object], json.loads(manager_command(shared, "status")))


def port_closed(port: int) -> bool:
    with socket.socket() as probe:
        return probe.connect_ex(("127.0.0.1", port)) != 0


class LocalStack:
    def __init__(
        self,
        directory: Path,
        processes: int,
        port: int,
        shared: Path,
        *,
        private: bool = False,
    ) -> None:
        self.directory = directory
        self.processes = processes
        self.port = port
        self.shared = shared
        self.private = private
        self.old_boot: str | None = None
        self.process: subprocess.Popen[str] | None = None
        self.info: StackInfo | None = None
        self.child_pids: set[int] = set()

    def start(self, *, fresh: bool = False) -> StackInfo:
        self.launch(fresh=fresh)
        return self.wait_ready()

    def launch(self, *, fresh: bool = False) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        # A previous discovery file must never make a failed restart look healthy.
        discovery = self.directory / "stack.json"
        self.old_boot = (
            json.loads(discovery.read_text()).get("bootId")
            if discovery.exists()
            else None
        )
        with (self.directory.parent / f"{self.directory.name}-launcher.log").open(
            "w"
        ) as output:
            self.process = subprocess.Popen(
                [
                    sys.executable,
                    str(LAUNCHER),
                    "--skip-build",
                    "--processes",
                    str(self.processes),
                    "--port-base",
                    str(self.port),
                    "--data-dir",
                    str(self.directory),
                    "--shared-opensearch-dir",
                    str(self.shared),
                    *(["--private-opensearch"] if self.private else []),
                    *(["--fresh"] if fresh else []),
                ],
                cwd=REPO_ROOT,
                stdout=output,
                stderr=subprocess.STDOUT,
                text=True,
                start_new_session=True,
            )

    def wait_ready(self) -> StackInfo:
        assert self.process is not None
        discovery = self.directory / "stack.json"
        deadline = time.monotonic() + 900
        while time.monotonic() < deadline:
            self.child_pids.update(self.descendants(self.process.pid))
            if self.process.poll() is not None:
                raise AssertionError(
                    f"launcher exited {self.process.returncode}; see {self.directory.parent / (self.directory.name + '-launcher.log')}"
                )
            if discovery.exists():
                candidate = json.loads(discovery.read_text())
                if not candidate.get("bootId") or candidate["bootId"] == self.old_boot:
                    time.sleep(0.1)
                    continue
                self.info = cast(StackInfo, candidate)
                for instance, url in self.info["instances"].items():
                    request(f"{url}/health")
                    request(f"{url}/config", self.info["adminTokens"][instance])
                print(
                    f"  ready: {self.info['instances']}; children={sorted(self.child_pids)}",
                    flush=True,
                )
                return self.info
            time.sleep(1)
        raise AssertionError(
            f"stack.json did not appear; see {self.directory.parent / (self.directory.name + '-launcher.log')}"
        )

    @staticmethod
    def descendants(parent: int) -> set[int]:
        rows = subprocess.check_output(["ps", "-axo", "pid=,ppid="], text=True)
        pairs = [tuple(map(int, line.split())) for line in rows.splitlines()]
        found = {parent}
        while True:
            children = {pid for pid, ppid in pairs if ppid in found}
            if children <= found:
                return found - {parent}
            found.update(children)

    def stop(self, group: bool = False) -> None:
        if self.process is None:
            return
        if self.process.poll() is None:
            if group:
                os.killpg(self.process.pid, signal.SIGTERM)
            else:
                self.process.terminate()
            self.process.wait(timeout=120)
        if self.info is not None:
            assert self.process.returncode == 0, (
                f"launcher exit {self.process.returncode}"
            )
        shared_pid_file = self.shared / "pid"
        if shared_pid_file.exists():
            self.child_pids.discard(int(shared_pid_file.read_text()))
        for pid in self.child_pids:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                continue
            raise AssertionError(f"child PID {pid} survived shutdown")
        assert not (self.directory / "stack.json").exists(), (
            "stale discovery after shutdown"
        )
        if self.info:
            ports = [
                *(
                    []
                    if self.info["opensearchShared"]
                    else [self.info["opensearchPort"]]
                ),
                *range(self.port, self.port + self.processes),
            ]
            for port in ports:
                with socket.socket() as probe:
                    assert probe.connect_ex(("127.0.0.1", port)) != 0, (
                        f"port {port} survived shutdown"
                    )
            if self.info["opensearchShared"]:
                assert shared_status(self.shared)["healthy"], (
                    "shared node died with stack"
                )
        self.process = None
        self.child_pids.clear()
        print(
            "  SIGTERM: owned children exited, discovery removed, owned ports closed",
            flush=True,
        )


def persistence(directory: Path, port: int, shared: Path) -> None:
    """Cold bootstrap, ingest/read/search, spool handoff, warm restart, and stop."""
    stack = LocalStack(directory / "persistence", 1, port, shared)
    try:
        info = stack.start()
        ingest(info, "gms-0")
        wait_for("entity read after ingest", lambda: dataset(info, "gms-0") is not None)
        query = '{ search(input: { type: DATASET, query: "gms_lite_verification", start: 0, count: 5 }) { total } }'

        def searchable() -> bool:
            result = graphql(info, "gms-0", query)["search"]
            return isinstance(result, dict) and int(result["total"]) >= 1

        wait_for("search indexing", searchable)
        before = shared_status(shared)
        stack.stop()
        assert shared_status(shared)["pid"] == before["pid"]
        spool = stack.directory / "gms-0/queue-spool.json"
        assert json.loads(spool.read_text())["topics"], "queue spool was not persisted"
        print(
            f"  entity read/search OK; saved spool: {spool.stat().st_size} bytes",
            flush=True,
        )
        restored = stack.start()
        assert shared_status(shared)["pid"] == before["pid"]
        assert restored["opensearchPort"] == info["opensearchPort"]
        assert restored["opensearchShared"]
        assert restored["bootId"] != info["bootId"]
        assert dataset(restored, "gms-0") is not None, "entity lost on restart"
        assert restored["indexPrefixes"] == info["indexPrefixes"]
        gms_log = (stack.directory / "logs/gms-0-gms.log").read_text()
        assert "Loaded queue spool" in gms_log, "GMS did not load the persisted spool"
        print(
            "  warm restart: entity retained, prefix retained, spool loaded", flush=True
        )
    finally:
        stack.stop()


def parallel(directory: Path, port: int, shared: Path) -> None:
    """Two GMS processes share a node with separate databases, spools and indices."""
    stack = LocalStack(directory / "parallel", 2, port, shared)
    try:
        info = stack.start()
        assert len(set(info["indexPrefixes"].values())) == 2
        ingest(info, "gms-0")
        wait_for("first instance entity", lambda: dataset(info, "gms-0") is not None)
        assert not entity_exists(info, "gms-1"), (
            "entity leaked into the second instance"
        )
        indices = request(
            f"http://localhost:{info['opensearchPort']}/_cat/indices?format=json"
        )
        assert isinstance(indices, list)
        for prefix in info["indexPrefixes"].values():
            assert any(row["index"].startswith(prefix + "_") for row in indices)
        print(
            "  entity isolated; distinct prefixes verified on the shared node",
            flush=True,
        )
    finally:
        stack.stop(group=True)


def stale(directory: Path, port: int, shared: Path) -> None:
    """Occupied ports are refused and stale discovery is replaced on a subsequent start."""
    stack = LocalStack(directory / "stale", 1, port, shared)
    stack.directory.mkdir(parents=True, exist_ok=True)
    (stack.directory / "stack.json").write_text(
        '{"instances":{"gms-0":"http://localhost:1"}}'
    )
    # An unrelated listener must survive refusal, even when stale discovery exists.
    with socket.socket() as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", port))
        listener.listen()
        result = subprocess.run(
            [
                sys.executable,
                str(LAUNCHER),
                "--skip-build",
                "--port-base",
                str(port),
                "--data-dir",
                str(stack.directory),
                "--shared-opensearch-dir",
                str(shared),
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )
        assert result.returncode != 0 and "in use" in result.stderr, result.stderr
    try:
        stack.start()
    finally:
        stack.stop()


def concurrent(directory: Path, port: int, shared: Path) -> None:
    """Race two cold launchers against an absent shared node, then check isolation."""
    manager_command(shared, "stop")
    first = LocalStack(directory / "concurrent-a", 1, port, shared)
    second = LocalStack(directory / "concurrent-b", 1, port + 1, shared)
    try:
        first.launch()
        second.launch()
        a, b = first.wait_ready(), second.wait_ready()
        assert a["opensearchPort"] == b["opensearchPort"]
        assert a["opensearchShared"] and b["opensearchShared"]
        assert set(a["indexPrefixes"].values()).isdisjoint(b["indexPrefixes"].values())
        ingest(a, "gms-0")
        wait_for("first stack entity", lambda: dataset(a, "gms-0") is not None)
        assert not entity_exists(b, "gms-0"), "entity leaked between stacks"
        status = shared_status(shared)
        assert status["healthy"]
        print(
            f"  concurrent stacks share PID {status['pid']} / port {status['port']}; isolated",
            flush=True,
        )
    finally:
        try:
            first.stop()
        finally:
            second.stop()


def replace_node(directory: Path, port: int, shared: Path, sig: signal.Signals) -> None:
    stack = LocalStack(
        directory / ("dead" if sig == signal.SIGKILL else "degraded"), 1, port, shared
    )
    try:
        info = stack.start()
        ingest(info, "gms-0")
        wait_for(
            "entity before replacement", lambda: dataset(info, "gms-0") is not None
        )
        stack.stop()
        before = shared_status(shared)
        os.kill(int(str(before["pid"])), sig)
        if sig == signal.SIGKILL:
            wait_for(
                "dead node port closes", lambda: port_closed(int(str(before["port"])))
            )
        restored = stack.start()
        after = shared_status(shared)
        assert before["pid"] != after["pid"], "node was not replaced"
        assert after["healthy"]
        assert dataset(restored, "gms-0") is not None, "replacement lost data"
        print(
            f"  replaced {sig.name} node {before['pid']} -> {after['pid']}; data retained",
            flush=True,
        )
    finally:
        stack.stop()


def dead(directory: Path, port: int, shared: Path) -> None:
    replace_node(directory, port, shared, signal.SIGKILL)


def degraded(directory: Path, port: int, shared: Path) -> None:
    # A stopped JVM still owns its ports but cannot answer health checks or process SIGTERM.
    replace_node(directory, port, shared, signal.SIGSTOP)


def private(directory: Path, port: int, shared: Path) -> None:
    stack = LocalStack(directory / "private", 1, port, shared, private=True)
    try:
        info = stack.start()
        assert not info["opensearchShared"]
        ingest(info, "gms-0")
        wait_for("private instance entity", lambda: dataset(info, "gms-0") is not None)
    finally:
        stack.stop(group=True)


def fresh(directory: Path, port: int, shared: Path) -> None:
    first = LocalStack(directory / "fresh-a", 1, port, shared)
    second = LocalStack(directory / "fresh-b", 1, port + 1, shared)
    try:
        a, b = first.start(), second.start()
        ingest(a, "gms-0")
        ingest(b, "gms-0")
        wait_for("first entity", lambda: dataset(a, "gms-0") is not None)
        wait_for("second entity", lambda: dataset(b, "gms-0") is not None)
        first.stop()
        a = first.start(fresh=True)
        assert not entity_exists(a, "gms-0"), "fresh stack retained search data"
        assert dataset(b, "gms-0") is not None, "fresh deleted another stack's data"
        ingest(a, "gms-0")
        wait_for(
            "entity before search removal", lambda: dataset(a, "gms-0") is not None
        )
        first.stop()
        prefix = a["indexPrefixes"]["gms-0"]
        names = request(
            f"http://localhost:{a['opensearchPort']}/_cat/indices?format=json"
        )
        assert isinstance(names, list)
        for row in names:
            if row["index"].startswith(prefix + "_"):
                req = urllib.request.Request(
                    f"http://localhost:{a['opensearchPort']}/" + row["index"],
                    method="DELETE",
                )
                with urllib.request.urlopen(req, timeout=30):
                    pass
        a = first.start()
        query = '{ search(input: { type: DATASET, query: "gms_lite_verification", start: 0, count: 5 }) { total } }'
        wait_for(
            "search restored from H2",
            lambda: (
                cast(dict[str, int], graphql(a, "gms-0", query)["search"])["total"] >= 1
            ),
        )
        assert dataset(b, "gms-0") is not None
        print("  fresh isolated; missing shared indices restored from H2", flush=True)
    finally:
        try:
            first.stop()
        finally:
            second.stop()


def startup_shutdown(directory: Path, port: int, shared: Path) -> None:
    stack = LocalStack(directory / "startup-shutdown", 1, port, shared)
    try:
        stack.launch()
        assert stack.process is not None

        def upgrading() -> bool:
            assert stack.process is not None
            stack.child_pids.update(stack.descendants(stack.process.pid))
            return (stack.directory / "logs/gms-0-systemupdate.log").exists() and bool(
                stack.child_pids
            )

        wait_for("upgrade child started", upgrading)
    finally:
        stack.stop(group=True)
    assert port_closed(port)
    assert shared_status(shared)["healthy"]
    print(
        "  SIGTERM during SystemUpdate: upgrade child exited, shared node survived",
        flush=True,
    )


def manager_stop(directory: Path, port: int, shared: Path) -> None:
    manager_command(shared, "start")
    before = shared_status(shared)
    manager_command(shared, "stop")
    assert not shared_status(shared)["running"]
    wait_for("manager stop closes port", lambda: port_closed(int(str(before["port"]))))
    assert (
        subprocess.run(
            ["ps", "-p", str(before["pid"]), "-o", "pid="], capture_output=True
        ).returncode
        != 0
    )
    print(
        f"  manager stop: PID {before['pid']} exited, port {before['port']} closed",
        flush=True,
    )


def main() -> None:
    cases = {
        "persistence": persistence,
        "parallel": parallel,
        "stale": stale,
        "concurrent": concurrent,
        "dead": dead,
        "degraded": degraded,
        "private": private,
        "fresh": fresh,
        "startup_shutdown": startup_shutdown,
        "manager_stop": manager_stop,
    }
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--port-base", type=int, default=18090)
    parser.add_argument("--test", nargs="+", choices=list(cases), default=list(cases))
    args = parser.parse_args()

    def interrupted(signum: int, frame: object) -> None:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    args.data_dir.mkdir(parents=True, exist_ok=True)
    results = args.data_dir / "results.jsonl"
    previous = (
        [json.loads(line) for line in results.read_text().splitlines()]
        if results.exists()
        else []
    )
    done = {row["case"] for row in previous}
    failed = any(not row["passed"] for row in previous if row["case"] in args.test)
    with tempfile.TemporaryDirectory(prefix="gms-lite-shared-") as temporary:
        shared = Path(temporary)
        print(f"Temporary shared OpenSearch state: {shared}", flush=True)
        try:
            for index, name in enumerate(args.test, 1):
                if name in done:
                    print(
                        f"{index}/{len(args.test)} {name}: already recorded; skipped",
                        flush=True,
                    )
                    continue
                print(
                    f"{index}/{len(args.test)} {name}: running (tail -f {results})",
                    flush=True,
                )
                started = time.monotonic()
                error = ""
                try:
                    cases[name](args.data_dir, args.port_base, shared)
                except Exception as exception:
                    error = f"{type(exception).__name__}: {exception}"
                    failed = True
                row = {
                    "case": name,
                    "passed": not error,
                    "error": error,
                    "seconds": round(time.monotonic() - started, 2),
                }
                with results.open("a") as output:
                    output.write(json.dumps(row) + "\n")
                    output.flush()
                print(json.dumps(row), flush=True)
        finally:
            manager_command(shared, "stop")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
