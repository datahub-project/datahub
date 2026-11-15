"""Fast, offline checks for launcher ownership, shutdown, and environment propagation."""

from __future__ import annotations

import argparse
import base64
import errno
import fcntl
import importlib.util
import io
import json
import multiprocessing
import os
import re
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
from itertools import count
from multiprocessing.connection import Connection
from pathlib import Path
from typing import TextIO
from unittest.mock import Mock, patch

BIN_DIR = Path(__file__).resolve().parents[1] / "bin"
sys.path.insert(0, str(BIN_DIR))
from _opensearch_manager import (  # noqa: E402
    OpenSearchHealthTimeout,
    SharedOpenSearchManager,
    delete_prefixes,
    wait_for_ready,
)
from _opensearch_manager import (  # noqa: E402
    main as manager_main,
)
from _system_update_checker import SystemUpdateChecker  # noqa: E402

spec = importlib.util.spec_from_file_location(
    "gms_lite_stack", BIN_DIR / "gms-lite-stack.py"
)
assert spec is not None and spec.loader is not None
stack = importlib.util.module_from_spec(spec)
spec.loader.exec_module(stack)


def hold_port(connection: Connection) -> None:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        connection.send(listener.getsockname()[1])
        connection.recv()


class LauncherSafetyTest(unittest.TestCase):
    def test_frontend_port_cannot_overlap_gms(self) -> None:
        with (
            patch.object(
                sys,
                "argv",
                [
                    "gms-lite-stack.py",
                    "--port-base",
                    "18090",
                    "--frontend-port",
                    "18090",
                ],
            ),
            patch.object(stack, "run_stack") as run,
        ):
            with self.assertRaises(SystemExit) as error:
                stack.main()
            self.assertEqual(error.exception.code, 2)
            run.assert_not_called()

    def test_occupied_port_is_rejected_without_terminating_owner(self) -> None:
        parent, child = multiprocessing.Pipe()
        owner = multiprocessing.Process(target=hold_port, args=(child,))
        owner.start()
        try:
            with self.assertRaises(SystemExit):
                stack.ensure_ports_available([parent.recv()])
            self.assertTrue(owner.is_alive())
        finally:
            parent.send("stop")
            owner.join(timeout=2)
            if owner.is_alive():
                owner.terminate()
                owner.join(timeout=2)
            parent.close()
            child.close()

    def test_readiness_timeout_and_logged_marker(self) -> None:
        for script, expected in [
            ("import time; time.sleep(60)", None),
            ("print('OpenSearchRunner READY 9200', flush=True)", 9200),
        ]:
            with (
                self.subTest(expected=expected),
                tempfile.TemporaryFile(mode="w+") as output,
            ):
                child = subprocess.Popen(
                    [sys.executable, "-c", script], stdout=subprocess.PIPE, text=True
                )
                try:
                    start = time.monotonic()
                    self.assertEqual(wait_for_ready(child, output, 0.5), expected)
                    self.assertLess(time.monotonic() - start, 2)
                finally:
                    child.terminate()
                    child.wait(timeout=5)
                    assert child.stdout is not None
                    child.stdout.close()

    def test_env_file_reaches_gms_without_rewiring_isolated_storage(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            env_file = directory / "overrides.env"
            env_file.write_text(
                "export POLICY_CACHE_REFRESH_INTERVAL_SECONDS=10\nSERVER_PORT=1\nINDEX_PREFIX=other\n"
                + "".join(f"{key}=override\n" for key in stack.SEARCH_CONNECTION_ENV)
                + "ELASTICSEARCH_HOST=other\nELASTICSEARCH_PORT=1\n"
            )
            with patch.dict(
                os.environ,
                {
                    **{key: "inherited" for key in stack.SEARCH_CONNECTION_ENV},
                    "ELASTICSEARCH_HOST": "other",
                    "ELASTICSEARCH_PORT": "1",
                },
            ):
                inherited = stack.instance_env(
                    {},
                    "gms-0",
                    8090,
                    9200,
                    directory,
                    stack.load_credentials(directory),
                    True,
                )
            env = stack.instance_env(
                stack.read_env_file(env_file),
                "gms-0",
                8090,
                9200,
                directory,
                stack.load_credentials(directory),
                True,
            )
            other = stack.instance_env(
                {},
                "gms-1",
                8091,
                9200,
                directory,
                stack.load_credentials(directory),
                True,
            )
            self.assertEqual(env["POLICY_CACHE_REFRESH_INTERVAL_SECONDS"], "10")
            self.assertEqual(env["SERVER_PORT"], "8090")
            for effective in (env, inherited):
                for key, expected in stack.SEARCH_CONNECTION_ENV.items():
                    self.assertEqual(effective[key], expected, key)
                self.assertEqual(effective["ELASTICSEARCH_URI"], "")
                self.assertEqual(effective["ELASTICSEARCH_USERNAME"], "")
                self.assertEqual(effective["ELASTICSEARCH_CLUSTERS_PRIMARY_URI"], "")
                self.assertEqual(
                    effective["ELASTICSEARCH_CLUSTERS_PRIMARY_USERNAME"], ""
                )
                self.assertEqual(effective["ELASTICSEARCH_HOST"], "localhost")
                self.assertEqual(effective["ELASTICSEARCH_PORT"], "9200")
            self.assertNotEqual(env["INDEX_PREFIX"], other["INDEX_PREFIX"])
            self.assertNotEqual(
                env["DATAHUB_PGQUEUE_INMEMORY_SPOOL_FILE"],
                other["DATAHUB_PGQUEUE_INMEMORY_SPOOL_FILE"],
            )
            self.assertEqual(env["ELASTICSEARCH_SHIM_ENGINE_TYPE"], "OPENSEARCH_3")
            self.assertEqual(
                env["ELASTICSEARCH_INDEX_DOC_IDS_SCHEMA_FIELD_HASH_ID_ENABLED"], "true"
            )
            for profiles, expected, upgrade in [
                ("", "upgrade", "SystemUpdate"),
                ("debug", "debug,upgrade", "SystemUpdate"),
                ("upgrade,debug", "upgrade,debug", "SystemUpdate"),
                ("debug", "debug,upgrade", "RestoreIndices"),
            ]:
                with (
                    self.subTest(profiles=profiles),
                    patch.object(stack.Processes, "start") as start,
                ):
                    processes = stack.Processes()
                    process = start.return_value
                    process.wait.return_value = 0
                    processes.children.append(process)
                    stack.run_upgrade(
                        processes,
                        {**env, "SPRING_PROFILES_ACTIVE": profiles},
                        directory,
                        "gms-0",
                        "1g",
                        upgrade,
                    )
                    upgrade_env = start.call_args.args[1]
                    self.assertEqual(upgrade_env["SPRING_PROFILES_ACTIVE"], expected)
                    self.assertEqual(
                        upgrade_env["POLICY_CACHE_REFRESH_INTERVAL_SECONDS"], "10"
                    )
                    self.assertEqual(upgrade_env["EBEAN_AUTOCREATE"], "false")
                    for key, expected_value in stack.SEARCH_CONNECTION_ENV.items():
                        self.assertEqual(upgrade_env[key], expected_value, key)

    def test_search_override_paths_match_shipped_yaml(self) -> None:
        yaml = (
            BIN_DIR.parents[2]
            / "metadata-service/configuration/src/main/resources/application.yaml"
        )
        placeholders = {
            name
            for names, _ in stack.SEARCH_CONNECTION_PROPERTIES.values()
            for name in names
        }
        found: dict[str, tuple[str, ...]] = {}
        parents: list[tuple[int, str]] = []
        # Only mapping entries are needed for these properties; avoid a runtime YAML dependency.
        for line in yaml.read_text().splitlines():
            match = re.match(r"^(\s*)([\w-]+):(?:\s|$)(.*)", line)
            if not match:
                continue
            indent, key, value = match.groups()
            while parents and parents[-1][0] >= len(indent):
                parents.pop()
            parents.append((len(indent), key))
            names = tuple(re.findall(r"\$\{([A-Z_0-9]+):", value))
            if placeholders.intersection(names):
                found[".".join(key for _, key in parents)] = names
        expected = {
            path: names
            for path, (names, _) in stack.SEARCH_CONNECTION_PROPERTIES.items()
        }
        self.assertEqual(found, expected)
        self.assertEqual(
            stack.spring_env_name("spring.main.log-startup-info"),
            "SPRING_MAIN_LOGSTARTUPINFO",
        )
        self.assertEqual(
            stack.spring_env_name("my.service[0].other"), "MY_SERVICE_0_OTHER"
        )

    def test_live_directory_refuses_fresh_and_stale_discovery_is_removed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary) / "stack"
            with stack.data_directory(directory, False):
                (directory / "stack.json").write_text("stale")
                with self.assertRaises(SystemExit):
                    with stack.data_directory(directory, True):
                        self.fail("live directory was acquired twice")
                self.assertTrue((directory / "stack.json").exists())
            self.assertFalse((directory / "stack.json").exists())
            (directory / "stack.json").write_text("stale")
            with stack.data_directory(directory, False):
                self.assertFalse((directory / "stack.json").exists())

    def test_incomplete_bootstrap_cannot_skip_system_update(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            instance = directory / "gms-0"
            instance.mkdir()
            checker = SystemUpdateChecker()
            prefix = "abc"
            indices = [
                "abc_datasetindex_v2",
                "abc_corpuserindex_v2",
                "abcd_datasetindex_v2",
            ]
            checker.mark_complete(directory, "gms-0", prefix, indices)
            self.assertFalse(checker.is_complete(directory, "gms-0", prefix, indices))
            (instance / "gms.mv.db").touch()
            self.assertTrue(checker.is_complete(directory, "gms-0", prefix, indices))
            self.assertFalse(
                checker.is_complete(directory, "gms-0", prefix, indices[1:])
            )
            self.assertFalse(
                checker.is_complete(
                    directory, "gms-0", prefix, ["abc_datahub_usage_event"]
                )
            )
            (instance / "system-update-done").write_text(
                "SystemUpdate and RestoreIndices completed\n"
            )
            self.assertFalse(checker.is_complete(directory, "gms-0", prefix, indices))
            (instance / "system-update-done").unlink()
            self.assertFalse(checker.is_complete(directory, "gms-0", prefix, indices))

    def test_env_file_preserves_inner_quotes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "values.env"
            path.write_text("DOUBLE=\"value'\"\nSINGLE='a'\nPLAIN=plain\n")
            self.assertEqual(
                stack.read_env_file(path),
                {"DOUBLE": "value'", "SINGLE": "a", "PLAIN": "plain"},
            )

    def test_token_expiry_uses_server_claim(self) -> None:
        payload = (
            base64.urlsafe_b64encode(json.dumps({"exp": 1794614400}).encode())
            .decode()
            .rstrip("=")
        )
        self.assertEqual(
            stack.token_expires_at(f"header.{payload}.signature"),
            "2026-11-14T00:00:00Z",
        )

    def test_group_signals_stop_child_groups_and_grandchildren(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            script = f"""
import argparse
import importlib.util, os, pathlib, signal, sys, time
sys.path.insert(0, {str(BIN_DIR)!r})
spec = importlib.util.spec_from_file_location('stack', {str(BIN_DIR / "gms-lite-stack.py")!r})
stack = importlib.util.module_from_spec(spec)
spec.loader.exec_module(stack)
processes = stack.Processes()
def stop(*args):
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    raise SystemExit(0)
signal.signal(signal.SIGTERM, stop)
try:
    child = processes.start([sys.executable, '-c',
        'import subprocess, sys, time; subprocess.Popen([sys.executable, \"-c\", \"import time; time.sleep(60)\"]); time.sleep(60)'],
        dict(stack.os.environ), pathlib.Path({str(directory / "child.log")!r}))
    # Write then rename so the test never reads a created-but-empty file.
    pathlib.Path({str(directory / "group.tmp")!r}).write_text(str(child.pid))
    os.replace({str(directory / "group.tmp")!r}, {str(directory / "group")!r})
    time.sleep(60)
finally:
    processes.stop()
"""
            launcher = subprocess.Popen(
                [sys.executable, "-c", script],
                start_new_session=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                deadline = time.monotonic() + 5
                while (
                    not (directory / "group").exists() and time.monotonic() < deadline
                ):
                    time.sleep(0.05)
                group = int((directory / "group").read_text())
                time.sleep(0.2)
                os.killpg(launcher.pid, signal.SIGTERM)
                _, errors = launcher.communicate(timeout=10)
                self.assertEqual(launcher.returncode, 0, errors)
                with self.assertRaises(ProcessLookupError):
                    os.killpg(group, 0)
            finally:
                if launcher.poll() is None:
                    launcher.kill()
                    launcher.communicate(timeout=5)


class FakeManager(SharedOpenSearchManager):
    @staticmethod
    def _alive(pid: int) -> bool:
        return True

    def _verify_identity(self, pid: int) -> None:
        pass

    @staticmethod
    def _healthy(port: int) -> bool:
        return True

    def _start(self) -> int:
        with (self.shared_dir / "starts").open("a") as output:
            output.write("start\n")
        time.sleep(0.2)
        self.pid_file.write_text("1")
        self.port_file.write_text("12345")
        return 12345


def get_shared(directory: str, connection: Connection) -> None:
    connection.send(FakeManager(Path(directory)).get_or_start())


class SharedOpenSearchSafetyTest(unittest.TestCase):
    def test_concurrent_get_or_start_starts_exactly_once(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            connections = [multiprocessing.Pipe() for _ in range(2)]
            children = [
                multiprocessing.Process(target=get_shared, args=(temporary, child))
                for _, child in connections
            ]
            try:
                for child in children:
                    child.start()
                for parent, _ in connections:
                    self.assertTrue(parent.poll(5))
                    self.assertEqual(parent.recv(), 12345)
                for child in children:
                    child.join(timeout=5)
                    self.assertEqual(child.exitcode, 0)
                self.assertEqual((Path(temporary) / "starts").read_text(), "start\n")
            finally:
                for child in children:
                    if child.is_alive():
                        child.terminate()
                        child.join(timeout=5)
                for parent, child in connections:
                    parent.close()
                    child.close()

    def test_shared_start_reads_prefixed_ready_and_detaches_output(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            script = (
                root
                / "metadata-service/opensearch-embedded/build/install/opensearch-embedded/bin/opensearch-embedded"
            )
            script.parent.mkdir(parents=True)
            script.write_text(
                f"#!{sys.executable}\n"
                "import time\nprint('[INFO ][stdout] READY 12345', flush=True)\ntime.sleep(60)\n"
            )
            script.chmod(0o700)
            state = root / "state"
            state.mkdir()
            manager = SharedOpenSearchManager(state, root)
            popen = subprocess.Popen
            children: list[subprocess.Popen[str]] = []

            def launch(
                command: list[str],
                *,
                cwd: Path,
                stdin: int,
                stdout: TextIO,
                stderr: int,
                text: bool,
                start_new_session: bool,
            ) -> subprocess.Popen[str]:
                child = popen(
                    command,
                    cwd=cwd,
                    stdin=stdin,
                    stdout=stdout,
                    stderr=stderr,
                    text=text,
                    start_new_session=start_new_session,
                )
                children.append(child)
                return child

            with (
                patch(
                    "_opensearch_manager.subprocess.Popen", side_effect=launch
                ) as start,
                patch.object(manager, "_healthy", return_value=True),
            ):
                try:
                    self.assertEqual(manager.get_or_start(), 12345)
                    pid = int(manager.pid_file.read_text())
                    self.assertEqual(os.getsid(pid), pid)
                    self.assertNotEqual(os.getsid(pid), os.getsid(0))
                    self.assertTrue(start.call_args.kwargs["start_new_session"])
                    self.assertNotEqual(
                        start.call_args.kwargs["stdout"], subprocess.PIPE
                    )
                    self.assertIn("READY 12345", manager.log_file.read_text())
                finally:
                    for child in children:
                        child.terminate()
                        child.wait(timeout=5)

    def test_unrelated_pid_is_never_signalled_or_replaced(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            manager = SharedOpenSearchManager(Path(temporary))
            manager.pid_file.write_text(str(os.getpid()))
            manager.port_file.write_text("12345")
            with patch("_opensearch_manager.os.kill") as kill:
                for action in (manager.stop, manager.get_or_start):
                    with self.assertRaisesRegex(RuntimeError, "unrelated PID"):
                        action()
                kill.assert_not_called()
            self.assertEqual(manager.pid_file.read_text(), str(os.getpid()))

    def test_new_node_timeout_preserves_state_and_startup_failures_clean_up(
        self,
    ) -> None:
        for outcome in (
            "timeout",
            "early_exit",
            "no_ready",
            "wrong_version",
            "interrupt",
        ):
            with (
                self.subTest(outcome=outcome),
                tempfile.TemporaryDirectory() as temporary,
            ):
                root = Path(temporary)
                script = (
                    root
                    / "metadata-service/opensearch-embedded/build/install/opensearch-embedded/bin/opensearch-embedded"
                )
                script.parent.mkdir(parents=True)
                script.write_text("#!/bin/sh\n")
                script.chmod(0o700)
                state = root / "state"
                state.mkdir()
                manager = SharedOpenSearchManager(state, root)
                manager.health_timeout_seconds = 0.1
                manager.READY_TIMEOUT_SECONDS = 0.1
                version = "2.19.1" if outcome == "wrong_version" else "3.6.0"

                def launch(*args: object, **kwargs: object) -> object:
                    # Simulate the child writing READY through the actual log descriptor.
                    if outcome not in ("early_exit", "no_ready"):
                        manager.log_file.write_text("[INFO ][stdout] READY 12345\n")
                    return child

                with (
                    patch(
                        "_opensearch_manager.subprocess.Popen", side_effect=launch
                    ) as start,
                    patch(
                        "_opensearch_manager.request_json",
                        side_effect=[
                            {"version": {"number": version}},
                            {"status": "red"},
                        ],
                    ),
                ):
                    child = Mock(pid=123)
                    child.poll.return_value = 1 if outcome == "early_exit" else None
                    if outcome == "interrupt":
                        health_patch = patch.object(
                            manager, "_healthy", side_effect=KeyboardInterrupt
                        )
                    else:
                        health_patch = patch.object(manager, "_verify_identity")
                    with health_patch:
                        with self.assertRaises(
                            (RuntimeError, KeyboardInterrupt)
                        ) as raised:
                            manager.get_or_start()
                    start.assert_called_once()
                    if outcome == "timeout":
                        self.assertIsInstance(raised.exception, OpenSearchHealthTimeout)
                        self.assertIn("node left running", str(raised.exception))
                        child.terminate.assert_not_called()
                        self.assertEqual(manager.pid_file.read_text(), "123")
                        self.assertEqual(manager.port_file.read_text(), "12345")
                        with (
                            patch.object(manager, "_alive", return_value=True),
                            patch.object(manager, "_verify_identity"),
                            patch.object(manager, "_healthy", return_value=True),
                        ):
                            self.assertEqual(manager.get_or_start(), 12345)
                        start.assert_called_once()
                    else:
                        self.assertEqual(
                            child.terminate.call_count, int(outcome != "early_exit")
                        )
                        self.assertFalse(manager.pid_file.exists())
                        self.assertFalse(manager.port_file.exists())

    def test_status_is_quick_json_and_probes_outside_lock(self) -> None:
        for response in ({"status": "red"}, TimeoutError("paused")):
            with (
                self.subTest(response=response),
                tempfile.TemporaryDirectory() as temporary,
            ):
                manager = SharedOpenSearchManager(Path(temporary))
                manager.pid_file.write_text("123")
                manager.port_file.write_text("12345")

                def probe(port: int, path: str, *, timeout: float) -> object:
                    self.assertLessEqual(timeout, 1)
                    with (manager.shared_dir / "startup.lock").open("a") as lock:
                        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    self.assertNotIn("wait_for_status", path)
                    if isinstance(response, Exception):
                        raise response
                    return {"version": {"number": "3.6.0"}} if path == "/" else response

                with (
                    patch.object(manager, "_alive", return_value=True),
                    patch.object(manager, "_verify_identity"),
                    patch("_opensearch_manager.request_json", side_effect=probe),
                    patch(
                        "_opensearch_manager.SharedOpenSearchManager",
                        return_value=manager,
                    ),
                    patch("_opensearch_manager.signal.signal"),
                    patch.object(
                        sys, "argv", ["manager", "status", "--dir", temporary]
                    ),
                    patch("sys.stdout", new_callable=io.StringIO) as output,
                ):
                    started = time.monotonic()
                    manager_main()
                    self.assertLess(time.monotonic() - started, 1)
                    status = json.loads(output.getvalue())
                    self.assertTrue(status["running"])
                    self.assertFalse(status["healthy"])
                    self.assertTrue(status["reason"])

    def test_other_major_is_refused_before_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            manager = SharedOpenSearchManager(Path(temporary))
            manager.pid_file.write_text("123")
            manager.port_file.write_text("12345")
            with (
                patch.object(manager, "_alive", return_value=True),
                patch.object(manager, "_verify_identity"),
                patch(
                    "_opensearch_manager.request_json",
                    return_value={"version": {"number": "2.19.1"}},
                ),
                patch.object(manager, "_stop_locked") as stop,
                patch.object(manager, "_start") as start,
            ):
                with self.assertRaisesRegex(RuntimeError, "expected major version 3"):
                    manager.get_or_start()
                stop.assert_not_called()
                start.assert_not_called()

    def test_dead_or_refused_node_is_replaced_with_data_retained(self) -> None:
        for alive in (False, True):
            with self.subTest(alive=alive), tempfile.TemporaryDirectory() as temporary:
                manager = SharedOpenSearchManager(Path(temporary))
                manager.pid_file.write_text("123")
                manager.port_file.write_text("12345")
                data = manager.shared_dir / "data"
                data.mkdir()
                (data / "index").write_text("preserved")
                with (
                    patch.object(manager, "_alive", return_value=alive),
                    patch.object(manager, "_verify_identity"),
                    patch(
                        "_opensearch_manager.request_json",
                        side_effect=urllib.error.URLError(
                            ConnectionRefusedError(errno.ECONNREFUSED, "refused")
                        ),
                    ),
                    patch.object(manager, "_stop_locked") as stop,
                    patch.object(manager, "_start", return_value=23456),
                ):
                    self.assertEqual(manager.get_or_start(), 23456)
                    self.assertEqual(stop.call_count, int(alive))
                    self.assertEqual((data / "index").read_text(), "preserved")

    def test_live_slow_node_waits_then_reuses_or_fails_without_replacement(
        self,
    ) -> None:
        for recovers in (False, True):
            with (
                self.subTest(recovers=recovers),
                tempfile.TemporaryDirectory() as temporary,
            ):
                manager = SharedOpenSearchManager(Path(temporary))
                manager.health_timeout_seconds = 10
                manager.pid_file.write_text("123")
                manager.port_file.write_text("12345")
                responses = [
                    {"version": {"number": "3.6.0"}},
                    {"status": "red"},
                    {"status": "yellow"} if recovers else {"status": "red"},
                ]
                with (
                    patch.object(manager, "_alive", return_value=True),
                    patch.object(manager, "_verify_identity"),
                    patch(
                        "_opensearch_manager.request_json",
                        side_effect=lambda *args, **kwargs: (
                            responses.pop(0) if len(responses) > 1 else responses[0]
                        ),
                    ),
                    patch("_opensearch_manager.time.sleep"),
                    patch("_opensearch_manager.time.monotonic", side_effect=count()),
                    patch.object(manager, "_stop_locked") as stop,
                    patch.object(manager, "_start") as start,
                ):
                    if recovers:
                        self.assertEqual(manager.get_or_start(), 12345)
                    else:
                        with self.assertRaisesRegex(RuntimeError, "node left running"):
                            manager.get_or_start()
                    stop.assert_not_called()
                    start.assert_not_called()
                self.assertEqual(manager.pid_file.read_text(), "123")

    def test_launcher_shutdown_leaves_shared_node_outside_owned_children(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            args = argparse.Namespace(
                private_opensearch=False, shared_opensearch_dir=Path(temporary)
            )
            processes = stack.Processes()
            with patch.object(stack, "SharedOpenSearchManager") as manager:
                manager.return_value.get_or_start.return_value = 12345
                manager.return_value.shared_dir = Path(temporary)
                self.assertEqual(
                    stack.start_opensearch(args, processes, Path(temporary)), 12345
                )
                processes.stop()
                self.assertFalse(processes.children)
                manager.return_value.stop.assert_not_called()

    def test_fresh_deletes_exact_prefixes_and_all_named_resources(self) -> None:
        resources: dict[str, object] = {
            "/_data_stream": {
                "data_streams": [{"name": "abc_events"}, {"name": "abcd_events"}]
            },
            "/_alias?expand_wildcards=all": {
                "abc_dataset": {"aliases": {"abc_alias": {}, "abcd_alias": {}}},
                "abcd_dataset": {"aliases": {"abcd_alias": {}}},
            },
            "/_cat/indices?format=json&h=index&expand_wildcards=all": [
                {"index": name}
                for name in (
                    "abc_dataset",
                    "abcd_dataset",
                    "abc2_dataset",
                    ".system",
                    "def_dataset",
                )
            ],
            "/_index_template": {
                "index_templates": [{"name": "abc_template"}, {"name": "abcd_template"}]
            },
            "/_component_template": {
                "component_templates": [
                    {"name": "abc_component"},
                    {"name": "abcd_component"},
                ]
            },
            "/_template": {"abc_legacy": {}, "abcd_legacy": {}},
        }
        deleted: list[str] = []

        def request(port: int, path: str, method: str = "GET") -> object:
            if method == "DELETE":
                deleted.append(path)
                return {"acknowledged": True}
            return resources[path]

        with patch("_opensearch_manager.request_json", side_effect=request):
            delete_prefixes(12345, ["abc", "def"])
        self.assertEqual(
            set(deleted),
            {
                "/_data_stream/abc_events",
                "/abc_dataset/_alias/abc_alias",
                "/abc_dataset",
                "/def_dataset",
                "/_index_template/abc_template",
                "/_component_template/abc_component",
                "/_template/abc_legacy",
            },
        )


if __name__ == "__main__":
    unittest.main()
