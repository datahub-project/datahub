"""Tests for datahub.executor.execution.wrapper_common.

These helpers run inside the short-lived wrapper subprocess, so they are
exercised here directly rather than through a task.
"""

import io
import json
import resource
import signal
import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import yaml

from datahub.executor.execution import wrapper_common
from datahub.executor.wrappers import run_ingest


class TestParseBoolEnv:
    @pytest.mark.parametrize("value", ["true", "TRUE", "True", "1", "yes", "YES"])
    def test_truthy_values(self, monkeypatch: pytest.MonkeyPatch, value: str) -> None:
        monkeypatch.setenv("SOME_FLAG", value)
        assert wrapper_common.parse_bool_env("SOME_FLAG", default=False)

    @pytest.mark.parametrize("value", ["false", "FALSE", "False", "0", "no", "NO"])
    def test_falsy_values(self, monkeypatch: pytest.MonkeyPatch, value: str) -> None:
        monkeypatch.setenv("SOME_FLAG", value)
        assert not wrapper_common.parse_bool_env("SOME_FLAG", default=True)

    def test_unset_returns_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("SOME_FLAG", raising=False)
        assert wrapper_common.parse_bool_env("SOME_FLAG", default=True)
        assert not wrapper_common.parse_bool_env("SOME_FLAG", default=False)

    def test_unrecognized_value_returns_default(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # An operator typo must not silently flip the flag; fall back to default.
        monkeypatch.setenv("SOME_FLAG", "maybe")
        assert wrapper_common.parse_bool_env("SOME_FLAG", default=True)
        assert not wrapper_common.parse_bool_env("SOME_FLAG", default=False)


class TestSetupMemoryLimit:
    def test_no_limit_configured_is_noop(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("EXECUTOR_TASK_MEMORY_LIMIT", raising=False)

        with patch.object(resource, "setrlimit") as mock_setrlimit:
            wrapper_common.setup_memory_limit()

        mock_setrlimit.assert_not_called()

    def test_applies_configured_limit(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("EXECUTOR_TASK_MEMORY_LIMIT", "2048")

        with patch.object(resource, "setrlimit") as mock_setrlimit:
            wrapper_common.setup_memory_limit()

        mock_setrlimit.assert_called_once_with(resource.RLIMIT_AS, (2048, 2048))

    def test_non_numeric_limit_does_not_raise(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A bad env value must not take down the ingestion run.
        monkeypatch.setenv("EXECUTOR_TASK_MEMORY_LIMIT", "2GB")

        with patch.object(resource, "setrlimit") as mock_setrlimit:
            wrapper_common.setup_memory_limit()

        mock_setrlimit.assert_not_called()

    def test_setrlimit_failure_does_not_raise(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Hard limits below the requested value make setrlimit raise; the run
        # should continue unlimited rather than fail.
        monkeypatch.setenv("EXECUTOR_TASK_MEMORY_LIMIT", "2048")

        with patch.object(
            resource, "setrlimit", side_effect=ValueError("not permitted")
        ):
            wrapper_common.setup_memory_limit()


class TestWrapperStdinContent:
    """Tests for what the wrapper pipes to `datahub ingest -c -` stdin.

    The wrapper always resolves ${VAR} in memory and pipes plain YAML.
    """

    def test_resolves_secrets_in_yaml(self) -> None:
        """build_datahub_stdin resolves ${VAR} using secrets dict."""
        recipe_yaml = "password: ${DB_PASS}\nhost: localhost\n"
        result = wrapper_common.build_datahub_stdin(recipe_yaml, {"DB_PASS": "s3cret"})

        parsed = yaml.safe_load(result)
        assert parsed["password"] == "s3cret"
        assert parsed["host"] == "localhost"
        assert "${DB_PASS}" not in result

    def test_leaves_unknown_vars_intact(self) -> None:
        """${VAR} not in secrets dict is left as-is for datahub's env fallback."""
        recipe_yaml = "a: ${KNOWN}\nb: ${UNKNOWN}\n"
        result = wrapper_common.build_datahub_stdin(recipe_yaml, {"KNOWN": "val"})

        parsed = yaml.safe_load(result)
        assert parsed["a"] == "val"
        assert "${UNKNOWN}" in result

    def test_empty_secrets(self) -> None:
        """With no secrets, values pass through unchanged."""
        recipe_yaml = "host: localhost\nport: 5432\n"
        result = wrapper_common.build_datahub_stdin(recipe_yaml, {})

        parsed = yaml.safe_load(result)
        assert parsed["host"] == "localhost"
        assert parsed["port"] == 5432

    def test_sigterm_handler_is_installed_before_the_child_is_spawned(self) -> None:
        """A cancellation arriving in the startup window must not orphan the child.

        If the handler is registered after Popen, a SIGTERM in between hits the
        default disposition: the wrapper dies and the datahub process it just
        spawned keeps running. Pinning the ORDER rather than the handler's body,
        since the body is only reachable via a real signal.
        """
        order: list[str] = []

        def fake_signal(sig: int, _handler: Any) -> None:
            if sig == signal.SIGTERM:
                order.append("handler")

        def fake_popen(*_args: Any, **_kwargs: Any) -> MagicMock:
            order.append("popen")
            proc = MagicMock()
            proc.stdin = MagicMock()
            proc.stdout = iter([])
            proc.wait.return_value = 0
            return proc

        with (
            patch(
                "datahub.executor.execution.wrapper_common.signal.signal",
                side_effect=fake_signal,
            ),
            patch(
                "datahub.executor.execution.wrapper_common.subprocess.Popen",
                side_effect=fake_popen,
            ),
        ):
            wrapper_common.run_datahub_subprocess(["/bin/true"], "recipe: {}")

        assert order == ["handler", "popen"]

    def test_ingestion_wrapper_pipes_resolved_yaml_to_subprocess(
        self, tmp_path: Path
    ) -> None:
        """End-to-end: ingestion wrapper resolves secrets and pipes plain YAML to Popen."""
        recipe = {"source": {"type": "test", "config": {"pw": "${SECRET}"}}}
        envelope = json.dumps(
            {
                "__recipe_yaml__": yaml.dump(recipe),
                "__secrets__": {"SECRET": "hidden"},
                "__report_out_file__": str(tmp_path / "report.json"),
                "__debug_mode__": "false",
            }
        )

        mock_process = MagicMock()
        mock_process.stdin = MagicMock()
        mock_process.stdout = iter([])
        mock_process.wait.return_value = 0

        venv_dir = tmp_path / "venv" / "bin"
        venv_dir.mkdir(parents=True)
        (venv_dir / "python").touch()
        (venv_dir / "datahub").touch()

        with (
            patch.object(sys, "argv", ["wrapper", str(tmp_path / "venv")]),
            patch.object(sys, "stdin", io.StringIO(envelope)),
            patch.object(run_ingest, "check_cli_flag_support", return_value=True),
            patch.object(run_ingest, "register_secrets_for_masking"),
            patch(
                "datahub.executor.execution.wrapper_common.subprocess.Popen",
                return_value=mock_process,
            ) as mock_popen,
            pytest.raises(SystemExit),
        ):
            run_ingest.main()

        # Wrapper pipes plain resolved YAML, not JSON envelope
        written = mock_process.stdin.write.call_args[0][0]
        parsed = yaml.safe_load(written)
        assert parsed["source"]["config"]["pw"] == "hidden"
        assert "${SECRET}" not in written

        # Venv isolation, asserted on the command actually spawned rather than by
        # grepping the wrapper's source: the CLI must come from the target venv, not
        # from whatever `datahub` happens to be on PATH.
        cmd = mock_popen.call_args[0][0]
        assert cmd[0] == str(venv_dir / "datahub")


class TestSigtermDuringSpawn:
    """The window the ORDER test cannot cover.

    The handler is installed before Popen, but `process` stays unbound until Popen
    returns. A signal landing in between used to find None, assume there was nothing to
    kill, and exit -- orphaning the child it had just spawned.
    """

    @pytest.fixture(autouse=True)
    def restore_sigterm(self):
        previous = signal.getsignal(signal.SIGTERM)
        yield
        signal.signal(signal.SIGTERM, previous)

    def _child(self) -> MagicMock:
        proc = MagicMock()
        proc.stdin = MagicMock()
        proc.stdout = iter([])
        proc.poll.return_value = None  # still running when the handler looks
        proc.wait.return_value = 0
        return proc

    def test_the_child_is_terminated_rather_than_orphaned(self) -> None:
        proc = self._child()

        def fake_popen(*_args: Any, **_kwargs: Any) -> MagicMock:
            signal.raise_signal(signal.SIGTERM)  # `process` is still None here
            return proc

        with (
            patch(
                "datahub.executor.execution.wrapper_common.subprocess.Popen",
                side_effect=fake_popen,
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            wrapper_common.run_datahub_subprocess(["/bin/true"], "recipe: {}")

        proc.terminate.assert_called_once()
        assert exc_info.value.code == 128 + signal.SIGTERM

    def test_a_signal_is_not_dropped_when_the_spawn_itself_fails(self) -> None:
        """The replay after the assignment is unreachable if Popen raises, so the
        deferred signal has to be honoured on that path too."""

        def failing_popen(*_args: Any, **_kwargs: Any) -> MagicMock:
            signal.raise_signal(signal.SIGTERM)
            raise FileNotFoundError("no such binary")

        with (
            patch(
                "datahub.executor.execution.wrapper_common.subprocess.Popen",
                side_effect=failing_popen,
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            wrapper_common.run_datahub_subprocess(["/nonexistent"], "recipe: {}")

        assert exc_info.value.code == 128 + signal.SIGTERM


class TestCliFlagProbe:
    """A probe that fails is not the same as a flag that is absent.

    Callers turn False into "you are likely running an old version", so a broken venv
    must not be reported that way.
    """

    def _script(self, tmp_path: Path, name: str, body: str) -> Path:
        path = tmp_path / name
        path.write_text(f"#!/bin/sh\n{body}\n")
        path.chmod(0o755)
        return path

    def test_a_failing_probe_surfaces_its_own_error(
        self, tmp_path: Path, capsys: pytest.CaptureFixture
    ) -> None:
        broken = self._script(
            tmp_path,
            "broken",
            "echo 'ImportError: no module named snowflake' >&2\nexit 1",
        )

        assert wrapper_common.check_cli_flag_support(broken, "some-flag") is False
        assert "ImportError: no module named snowflake" in capsys.readouterr().err

    def test_a_genuinely_missing_flag_is_reported_silently(
        self, tmp_path: Path, capsys: pytest.CaptureFixture
    ) -> None:
        old_cli = self._script(
            tmp_path, "old", "echo 'Usage: datahub ingest run'\nexit 0"
        )

        assert wrapper_common.check_cli_flag_support(old_cli, "some-flag") is False
        assert capsys.readouterr().err == ""
