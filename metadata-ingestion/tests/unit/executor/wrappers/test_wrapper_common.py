"""Tests for datahub.executor.execution.wrapper_common.

These helpers run inside the short-lived wrapper subprocess, so they are
exercised here directly rather than through a task.
"""

import io
import json
import resource
import sys
from pathlib import Path
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
            ),
            pytest.raises(SystemExit),
        ):
            run_ingest.main()

        # Wrapper pipes plain resolved YAML, not JSON envelope
        written = mock_process.stdin.write.call_args[0][0]
        parsed = yaml.safe_load(written)
        assert parsed["source"]["config"]["pw"] == "hidden"
        assert "${SECRET}" not in written
