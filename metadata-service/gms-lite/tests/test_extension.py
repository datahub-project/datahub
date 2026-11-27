"""Generic provider integration without building jars or starting a search node."""

import argparse
import base64
import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts/dev"))

from extensions.datahub_local_extension import (  # noqa: E402
    Context,
    Description,
    Service,
)
from test_launcher_safety import stack  # noqa: E402


class ExtensionTest(unittest.TestCase):
    def test_environment_precedence_bootstrap_and_service_discovery(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            artifact = directory / "artifact"
            artifact.touch()
            env_file = directory / "overrides.env"
            env_file.write_text("SETTING=user\n")
            extension = Mock()
            layer = directory / "layer"
            layer.mkdir()
            (layer / "gms.env").write_text("SETTING=default\n")
            (layer / "system-update.env").write_text("BOOTSTRAP_INPUT=resource\n")
            (layer / "worker.env").write_text(
                "SETTING=service-default\nWORKER_PORT=${GMS_LITE_WORKER_PORT}\n"
            )
            extension.describe.return_value = Description(
                port_bases={"worker": 9203}, layer_dirs=(layer,)
            )
            extension.placeholders.return_value = {"GMS_LITE_WORKER_PORT": "9203"}
            extension.services.side_effect = lambda ctx: (
                Service(
                    "worker",
                    ("worker",),
                    directory,
                    ("worker",),
                    "http://localhost:9203/health",
                ),
            )
            processes = Mock()
            processes.start.return_value = Mock(pid=12345)
            processes.check.side_effect = SystemExit("test finished")
            args = argparse.Namespace(
                data_dir=directory,
                extension="example:extension",
                extension_options="{}",
                extension_slot=0,
                skip_build=True,
                env_files=[str(env_file)],
                job=None,
                mode="local",
                processes=1,
                port_base=8080,
                opensearch="2",
                auth=True,
                fresh=False,
                private_opensearch=False,
                previous_instances=[],
                heap="1g",
                frontend_port=None,
            )
            token = (
                "header."
                + base64.urlsafe_b64encode(json.dumps({"exp": 1893456000}).encode())
                .decode()
                .rstrip("=")
                + ".signature"
            )
            output = io.StringIO()
            with (
                contextlib.redirect_stdout(output),
                patch.object(stack, "mint_admin_token", return_value=token),
                patch.object(stack, "wait_admin_access"),
                patch.object(stack, "load", return_value=extension),
                patch.object(stack, "WAR", artifact),
                patch.object(stack, "UPGRADE_JAR", artifact),
                patch.object(stack, "distribution_script", return_value=artifact),
                patch.object(stack, "start_opensearch", return_value=19200),
                patch.object(stack, "index_names", return_value=[]),
                patch.object(stack, "ensure_usage_event_index"),
                patch.object(stack, "run_upgrade") as upgrade,
                patch.object(stack, "http_ok", return_value=True),
                patch.object(stack, "SystemUpdateChecker") as checker,
            ):
                checker.return_value.is_complete.return_value = False
                with self.assertRaises(SystemExit):
                    stack.run_stack(args, processes)
            bootstrap_env = upgrade.call_args_list[0].args[1]
            self.assertEqual(bootstrap_env["BOOTSTRAP_INPUT"], "resource")
            self.assertEqual(bootstrap_env["SETTING"], "user")
            self.assertEqual(bootstrap_env["SERVER_PORT"], "8080")
            service_env = processes.start.call_args.args[1]
            self.assertEqual(service_env["SETTING"], "user")
            self.assertNotIn("SERVER_PORT", service_env)
            self.assertFalse(any(key.startswith("EBEAN_") for key in service_env))
            self.assertNotIn("DATAHUB_TOKEN_SERVICE_SIGNING_KEY", service_env)
            self.assertEqual(service_env["WORKER_PORT"], "9203")
            self.assertEqual(service_env["DATAHUB_GMS_URL"], "http://localhost:8080")
            self.assertEqual(service_env["METADATA_SERVICE_AUTH_ENABLED"], "true")
            discovery = json.loads((directory / "stack.json").read_text())
            self.assertEqual(
                discovery["services"], {"worker": {"gms-0": "http://localhost:9203"}}
            )
            self.assertTrue(discovery["serviceHealth"]["worker"]["gms-0"]["required"])
            self.assertEqual(discovery["servicePids"]["worker"]["gms-0"], 12345)
            extension.after_ready.assert_called_once()
            timings = json.loads((directory / "timings.json").read_text())
            self.assertIn("service:worker", timings["phases"])
            self.assertIn("restore_indices", timings["phases"])
            self.assertFalse(timings["warm"])
            self.assertEqual(discovery["adminTokens"], {"gms-0": token})
            self.assertNotIn(token, output.getvalue())
            self.assertNotIn(discovery["systemClientSecret"], output.getvalue())

    def test_failed_optional_services_are_stopped_and_not_advertised(self) -> None:
        for outcome in ("spawn-error", "timeout"):
            with (
                self.subTest(outcome=outcome),
                tempfile.TemporaryDirectory() as temporary,
            ):
                directory = Path(temporary)
                artifact = directory / "artifact"
                artifact.touch()
                extension = Mock()
                extension.describe.return_value = Description(
                    port_bases={"worker": 9203}
                )
                extension.placeholders.return_value = {}
                extension.services.return_value = (
                    Service(
                        "worker",
                        ("worker",),
                        directory,
                        ("worker",),
                        "http://localhost:9203/health",
                        timeout=0,
                        required=False,
                    ),
                )
                child = Mock(pid=12345)
                child.poll.return_value = None
                processes = Mock(children=[child], optional={12345})
                processes.check.side_effect = (
                    [None, SystemExit()] if outcome == "timeout" else SystemExit()
                )

                def start(command: list[str], *args: object, **kwargs: object) -> Mock:
                    if command == ["worker"] and outcome == "spawn-error":
                        raise OSError("unavailable executable")
                    return child

                processes.start.side_effect = start
                args = argparse.Namespace(
                    data_dir=directory,
                    extension="example:extension",
                    extension_options="{}",
                    extension_slot=0,
                    skip_build=True,
                    env_files=[],
                    job=None,
                    mode="local",
                    processes=1,
                    port_base=8080,
                    opensearch="2",
                    auth=False,
                    fresh=False,
                    private_opensearch=False,
                    previous_instances=[],
                    heap="1g",
                    frontend_port=None,
                )
                with (
                    patch.object(stack, "load", return_value=extension),
                    patch.object(stack, "WAR", artifact),
                    patch.object(stack, "UPGRADE_JAR", artifact),
                    patch.object(stack, "distribution_script", return_value=artifact),
                    patch.object(stack, "start_opensearch", return_value=19200),
                    patch.object(stack, "index_names", return_value=[]),
                    patch.object(stack, "ensure_usage_event_index"),
                    patch.object(
                        stack, "http_ok", side_effect=lambda url: ":9203" not in url
                    ),
                    patch.object(stack, "SystemUpdateChecker") as checker,
                ):
                    checker.return_value.is_complete.return_value = True
                    with self.assertRaises(SystemExit):
                        stack.run_stack(args, processes)
                discovery = json.loads((directory / "stack.json").read_text())
                self.assertEqual(discovery["services"], {})
                self.assertEqual(discovery["serviceHealth"], {})
                self.assertNotIn("worker", discovery["servicePids"])
                if outcome == "timeout":
                    processes.stop_group.assert_called_once_with(child)
                    self.assertNotIn(child, processes.children)

    def test_stop_callback_runs_after_process_shutdown(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            extension = Mock()
            extension.describe.return_value = Description()
            extension.placeholders.return_value = {}
            extension.services.return_value = ()
            ctx = Context(directory, directory, "gms-0", {}, {})
            events: list[str] = []
            extension.after_stop.side_effect = lambda _: events.append("callback")
            processes = Mock()
            processes.stop.side_effect = lambda: events.append("stop")

            def run(args: argparse.Namespace, processes: object) -> None:
                args.extension_contexts = {"gms-0": ctx}
                raise SystemExit(0)

            with (
                patch.object(stack, "load", return_value=extension),
                patch.object(stack, "Processes", return_value=processes),
                patch.object(stack, "ensure_ports_available"),
                patch.object(stack.signal, "signal"),
                patch.object(stack, "run_stack", side_effect=run),
                patch(
                    "sys.argv",
                    [
                        "gms-lite-stack.py",
                        "--data-dir",
                        str(directory),
                        "--opensearch",
                        "2",
                        "--extension",
                        "example:extension",
                    ],
                ),
            ):
                with self.assertRaises(SystemExit):
                    stack.main()
            self.assertEqual(events, ["stop", "callback"])
            self.assertFalse((directory / "stack.json").exists())
