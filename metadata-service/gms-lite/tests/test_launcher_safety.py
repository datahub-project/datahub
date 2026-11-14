"""Fast, offline checks for launcher ownership, shutdown, and environment propagation."""

from __future__ import annotations

import importlib.util
import multiprocessing
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from multiprocessing.connection import Connection
from pathlib import Path
from unittest.mock import patch

BIN_DIR = Path(__file__).resolve().parents[1] / "bin"
sys.path.insert(0, str(BIN_DIR))
from _opensearch_manager import wait_for_ready  # noqa: E402
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
            self.assertNotEqual(env["INDEX_PREFIX"], other["INDEX_PREFIX"])
            self.assertNotEqual(
                env["DATAHUB_PGQUEUE_INMEMORY_SPOOL_FILE"],
                other["DATAHUB_PGQUEUE_INMEMORY_SPOOL_FILE"],
            )
            self.assertEqual(env["ELASTICSEARCH_SHIM_ENGINE_TYPE"], "OPENSEARCH_3")
            self.assertEqual(
                env["ELASTICSEARCH_INDEX_DOC_IDS_SCHEMA_FIELD_HASH_ID_ENABLED"], "true"
            )
            for profiles, expected in [
                ("", "upgrade"),
                ("debug", "debug,upgrade"),
                ("upgrade,debug", "upgrade,debug"),
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
                        "SystemUpdate",
                    )
                    upgrade_env = start.call_args.args[1]
                    self.assertEqual(upgrade_env["SPRING_PROFILES_ACTIVE"], expected)
                    self.assertEqual(
                        upgrade_env["POLICY_CACHE_REFRESH_INTERVAL_SECONDS"], "10"
                    )
                    self.assertEqual(upgrade_env["EBEAN_AUTOCREATE"], "false")

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
            checker.mark_complete(directory, "gms-0")
            self.assertFalse(checker.is_complete(directory, "gms-0"))
            (instance / "gms.mv.db").touch()
            self.assertTrue(checker.is_complete(directory, "gms-0"))
            (instance / "system-update-done").unlink()
            self.assertFalse(checker.is_complete(directory, "gms-0"))

    def test_group_signals_stop_child_groups_and_grandchildren(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            script = f"""
import importlib.util, pathlib, signal, sys, time
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
    pathlib.Path({str(directory / "group")!r}).write_text(str(child.pid))
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


if __name__ == "__main__":
    unittest.main()
