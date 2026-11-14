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
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from pathlib import Path
from typing import TypedDict, cast

REPO_ROOT = Path(__file__).resolve().parents[3]
LAUNCHER = REPO_ROOT / "metadata-service/gms-lite/bin/gms-lite-stack.py"
URN = "urn:li:dataset:(urn:li:dataPlatform:hive,gms_lite_verification,PROD)"


class StackInfo(TypedDict):
    bootId: str
    instances: dict[str, str]
    indexPrefixes: dict[str, str]
    opensearchPort: int
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


class LocalStack:
    def __init__(self, directory: Path, processes: int, port: int) -> None:
        self.directory = directory
        self.processes = processes
        self.port = port
        self.process: subprocess.Popen[str] | None = None
        self.info: StackInfo | None = None
        self.child_pids: set[int] = set()

    def start(self) -> StackInfo:
        self.directory.mkdir(parents=True, exist_ok=True)
        # A previous discovery file must never make a failed restart look healthy.
        discovery = self.directory / "stack.json"
        old_boot = (
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
                ],
                cwd=REPO_ROOT,
                stdout=output,
                stderr=subprocess.STDOUT,
                text=True,
                start_new_session=True,
            )
        deadline = time.monotonic() + 900
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise AssertionError(
                    f"launcher exited {self.process.returncode}; see {output.name}"
                )
            if discovery.exists():
                candidate = json.loads(discovery.read_text())
                if not candidate.get("bootId") or candidate["bootId"] == old_boot:
                    time.sleep(0.1)
                    continue
                self.info = cast(StackInfo, candidate)
                self.child_pids = self.descendants(self.process.pid)
                for instance, url in self.info["instances"].items():
                    request(f"{url}/health")
                    request(f"{url}/config", self.info["adminTokens"][instance])
                print(
                    f"  ready: {self.info['instances']}; children={sorted(self.child_pids)}",
                    flush=True,
                )
                return self.info
            time.sleep(1)
        raise AssertionError(f"stack.json did not appear; see {output.name}")

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
                self.info["opensearchPort"],
                *range(self.port, self.port + self.processes),
            ]
            for port in ports:
                with socket.socket() as probe:
                    assert probe.connect_ex(("127.0.0.1", port)) != 0, (
                        f"port {port} survived shutdown"
                    )
        self.process = None
        print(
            "  SIGTERM: children exited, discovery removed, all ports closed",
            flush=True,
        )


def persistence(directory: Path, port: int) -> None:
    """Cold bootstrap, ingest/read/search, spool handoff, warm restart, and stop."""
    stack = LocalStack(directory / "persistence", 1, port)
    try:
        info = stack.start()
        ingest(info, "gms-0")
        wait_for("entity read after ingest", lambda: dataset(info, "gms-0") is not None)
        query = '{ search(input: { type: DATASET, query: "gms_lite_verification", start: 0, count: 5 }) { total } }'

        def searchable() -> bool:
            result = graphql(info, "gms-0", query)["search"]
            return isinstance(result, dict) and int(result["total"]) >= 1

        wait_for("search indexing", searchable)
        stack.stop()
        spool = stack.directory / "gms-0/queue-spool.json"
        assert json.loads(spool.read_text())["topics"], "queue spool was not persisted"
        print(
            f"  entity read/search OK; saved spool: {spool.stat().st_size} bytes",
            flush=True,
        )
        restored = stack.start()
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


def parallel(directory: Path, port: int) -> None:
    """Two GMS processes share a node with separate databases, spools and indices."""
    stack = LocalStack(directory / "parallel", 2, port)
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


def stale(directory: Path, port: int) -> None:
    """Occupied ports are refused and stale discovery is replaced on a subsequent start."""
    stack = LocalStack(directory / "stale", 1, port)
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


def main() -> None:
    cases = {"persistence": persistence, "parallel": parallel, "stale": stale}
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--port-base", type=int, default=18090)
    parser.add_argument("--test", nargs="+", choices=list(cases), default=list(cases))
    args = parser.parse_args()
    args.data_dir.mkdir(parents=True, exist_ok=True)
    results = args.data_dir / "results.jsonl"
    previous = (
        [json.loads(line) for line in results.read_text().splitlines()]
        if results.exists()
        else []
    )
    done = {row["case"] for row in previous}
    failed = any(not row["passed"] for row in previous if row["case"] in args.test)
    for index, name in enumerate(args.test, 1):
        if name in done:
            print(
                f"{index}/{len(args.test)} {name}: already recorded; skipped",
                flush=True,
            )
            continue
        print(
            f"{index}/{len(args.test)} {name}: running (tail -f {results})", flush=True
        )
        started = time.monotonic()
        error = ""
        try:
            cases[name](args.data_dir, args.port_base)
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
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
