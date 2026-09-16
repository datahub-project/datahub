"""Tests for SubProcessTaskUtil and SubProcessRecipeTaskArgs.

Covers subprocess error formatting, execution-directory cleanup, the JSON-string
field validators that accommodate what the UI sends, and env var merging.
"""

import errno
import inspect
import json
import os
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from pydantic import ValidationError

from datahub.executor.execution.runner import LogHolder
from datahub.executor.execution.sub_process_task_common import (
    SubProcessRecipeTaskArgs,
    SubProcessTaskUtil,
    unprotectable_disclosed_values,
)
from datahub.executor.execution.task import TaskError
from datahub.masking.secret_registry import SecretRegistry


class TestFormatSubprocessError:
    """Tests for SubProcessTaskUtil.format_subprocess_error()"""

    def test_calledprocesserror_with_stderr(self) -> None:
        """Test formatting CalledProcessError with stderr attribute"""
        error = subprocess.CalledProcessError(
            returncode=1,
            cmd=["/usr/bin/uv", "pip", "install", "-r", "requirements.txt"],
        )
        error.stderr = (
            "Command failed with captured output:\n"
            "× No solution found when resolving dependencies:\n"
            "╰─▶ Because example-lib==1.0.0 is required\n"
            "    and example-private-plugins requires example-lib==1.0.1,\n"
            "    we can conclude that your requirements are unsatisfiable."
        )

        result = SubProcessTaskUtil.format_subprocess_error(error)

        assert "returned non-zero exit status 1" in result
        assert "Command failed with captured output:" in result
        assert "No solution found when resolving dependencies:" in result
        assert "your requirements are unsatisfiable" in result

    def test_calledprocesserror_with_output(self) -> None:
        """Test formatting CalledProcessError with output attribute"""
        captured_output = (
            "Using Python 3.11.14 environment\n"
            "× No solution found when resolving dependencies:\n"
            "╰─▶ Because only example-private-plugins==1.0.0 is available"
        )
        error = subprocess.CalledProcessError(
            returncode=1,
            cmd=["/usr/bin/uv", "pip", "install", "acryl-datahub"],
            output=captured_output,
        )

        result = SubProcessTaskUtil.format_subprocess_error(error)

        assert "returned non-zero exit status 1" in result
        assert "Using Python 3.11.14 environment" in result
        assert "No solution found when resolving dependencies:" in result

    def test_calledprocesserror_with_both_stderr_and_output(self) -> None:
        """Test that stderr takes precedence over output when both are present"""
        error = subprocess.CalledProcessError(
            returncode=1, cmd=["command"], output="output text"
        )
        error.stderr = "stderr text"

        result = SubProcessTaskUtil.format_subprocess_error(error)

        assert "stderr text" in result
        assert "output text" not in result

    def test_calledprocesserror_without_details(self) -> None:
        """Test formatting CalledProcessError with no stderr/output attributes"""
        error = subprocess.CalledProcessError(returncode=1, cmd=["command"])

        result = SubProcessTaskUtil.format_subprocess_error(error)

        # Should just return the base error message
        assert "returned non-zero exit status 1" in result
        assert "\n\n" not in result  # No extra details appended

    def test_calledprocesserror_with_empty_stderr(self) -> None:
        """Test formatting CalledProcessError with empty stderr"""
        error = subprocess.CalledProcessError(returncode=1, cmd=["command"])
        error.stderr = ""

        result = SubProcessTaskUtil.format_subprocess_error(error)

        # Empty string is falsy, so should not append details
        assert "returned non-zero exit status 1" in result
        assert "\n\n" not in result

    def test_regular_exception(self) -> None:
        """Test formatting regular non-subprocess exceptions"""
        error = ValueError("Invalid configuration value")

        result = SubProcessTaskUtil.format_subprocess_error(error)

        assert result == "Invalid configuration value"

    def test_runtime_error(self) -> None:
        """Test formatting RuntimeError"""
        error = RuntimeError("Something went wrong")

        result = SubProcessTaskUtil.format_subprocess_error(error)

        assert result == "Something went wrong"

    def test_exception_with_no_message(self) -> None:
        """Test formatting exception with no message"""
        error = Exception()

        result = SubProcessTaskUtil.format_subprocess_error(error)

        assert result == ""

    def test_multiline_stderr_preserved(self) -> None:
        """Test that multiline stderr output is preserved correctly"""
        error = subprocess.CalledProcessError(returncode=137, cmd=["test"])
        error.stderr = "Line 1\nLine 2\nLine 3\n"

        result = SubProcessTaskUtil.format_subprocess_error(error)

        assert "Line 1" in result
        assert "Line 2" in result
        assert "Line 3" in result
        assert result.count("\n") >= 3

    def test_realistic_uv_dependency_error(self) -> None:
        """Test with realistic uv pip install dependency resolution error"""
        error = subprocess.CalledProcessError(
            returncode=1,
            cmd=[
                "/usr/bin/uv",
                "pip",
                "install",
                "-r",
                "/tmp/datahub/ingest/exec-id/venv-example-plugin-0123456789abcdef/requirements.txt",
            ],
        )
        error.stderr = """Command failed with captured output:
Using Python 3.11.14 environment at: /tmp/datahub/ingest/exec-id/venv-example-plugin-0123456789abcdef
  × No solution found when resolving dependencies:
  ╰─▶ Because only
      example-private-plugins[example-plugin]==1.0.0
      is available and example-private-plugins==1.0.0 depends on
      example-lib==1.0.1, we can conclude that all versions of
      example-private-plugins[example-plugin] depend on
      example-lib==1.0.1.
      And because you require
      example-lib[example-plugin]==1.0.0 and
      example-private-plugins[example-plugin], we can conclude that
      your requirements are unsatisfiable."""

        result = SubProcessTaskUtil.format_subprocess_error(error)

        # Verify all key parts of the error are present
        assert "returned non-zero exit status 1" in result
        assert "Command failed with captured output:" in result
        assert "No solution found when resolving dependencies:" in result
        assert "example-lib==1.0.1" in result
        assert "example-lib[example-plugin]==1.0.0" in result
        assert "your requirements are unsatisfiable" in result


class TestSubProcessTaskUtilRemoveDirectory:
    """Tests for SubProcessTaskUtil._remove_directory."""

    def test_remove_directory_success(self) -> None:
        with tempfile.TemporaryDirectory() as parent:
            target = Path(parent) / "to-remove"
            target.mkdir()
            (target / "file.txt").write_text("x")

            SubProcessTaskUtil._remove_directory(str(target))

            assert not target.exists()

    def test_remove_directory_does_not_throw_on_file_not_found(self) -> None:
        # Does not throw when shutil.rmtree throws FileNotFoundError.
        with patch("shutil.rmtree", side_effect=FileNotFoundError("missing")):
            SubProcessTaskUtil._remove_directory("/tmp/nonexistent")

    def test_remove_directory_does_not_throw_on_directory_not_empty(self) -> None:
        # Does not throw when shutil.rmtree throws OSError(ENOTEMPTY) — the
        # symptom seen when a kernel core dump is still being written.
        with patch(
            "shutil.rmtree",
            side_effect=OSError(errno.ENOTEMPTY, "Directory not empty", "core"),
        ):
            SubProcessTaskUtil._remove_directory("/tmp/dir-with-core")

    def test_remove_directory_does_not_throw_on_permission_error(self) -> None:
        # Does not throw when shutil.rmtree throws PermissionError.
        with patch("shutil.rmtree", side_effect=PermissionError("denied")):
            SubProcessTaskUtil._remove_directory("/tmp/no-perms")


class TestSubProcessRecipeTaskArgsJSONParsing:
    """Test JSON parsing validators for extra_pip_requirements, extra_pip_plugins, and extra_env_vars."""

    def test_extra_pip_requirements_with_json_list_string(self):
        """Test that JSON list strings are parsed correctly."""
        args = SubProcessRecipeTaskArgs.model_validate(
            {
                "recipe": "{}",
                "extra_pip_requirements": '["package1", "package2"]',
            }
        )
        assert args.extra_pip_requirements == ["package1", "package2"]

    def test_extra_pip_requirements_with_empty_string(self):
        """Test that empty strings are handled as empty lists (UI edge case)."""
        args = SubProcessRecipeTaskArgs.model_validate(
            {
                "recipe": "{}",
                "extra_pip_requirements": "",
            }
        )
        assert args.extra_pip_requirements == []

    def test_extra_pip_requirements_with_empty_json_array_string(self):
        """Test that '[]' string is parsed as empty list."""
        args = SubProcessRecipeTaskArgs.model_validate(
            {
                "recipe": "{}",
                "extra_pip_requirements": "[]",
            }
        )
        assert args.extra_pip_requirements == []

    def test_extra_pip_requirements_with_actual_list(self):
        """Test that actual Python lists are passed through unchanged."""
        args = SubProcessRecipeTaskArgs.model_validate(
            {
                "recipe": "{}",
                "extra_pip_requirements": ["package1", "package2"],
            }
        )
        assert args.extra_pip_requirements == ["package1", "package2"]

    def test_extra_pip_requirements_with_empty_list(self):
        """Test that empty Python lists are passed through unchanged."""
        args = SubProcessRecipeTaskArgs.model_validate(
            {
                "recipe": "{}",
                "extra_pip_requirements": [],
            }
        )
        assert args.extra_pip_requirements == []

    def test_extra_pip_requirements_with_invalid_json(self):
        """Test that invalid JSON strings raise validation errors with helpful messages."""
        with pytest.raises(ValidationError) as exc_info:
            SubProcessRecipeTaskArgs.model_validate(
                {
                    "recipe": "{}",
                    "extra_pip_requirements": "{invalid json}",
                }
            )
        # Verify the error message mentions JSON parsing
        assert "Expecting property name" in str(exc_info.value)

    def test_extra_pip_requirements_with_null_string_literal(self):
        """Test that 'null' string literal raises validation error (None is not a valid list)."""
        with pytest.raises(ValidationError) as exc_info:
            SubProcessRecipeTaskArgs.model_validate(
                {
                    "recipe": "{}",
                    "extra_pip_requirements": "null",
                }
            )
        # Verify error message indicates None is not valid for list field
        assert "list" in str(exc_info.value).lower()

    def test_extra_pip_plugins_with_json_list_string(self):
        """Test that JSON list strings are parsed correctly for plugins."""
        args = SubProcessRecipeTaskArgs.model_validate(
            {
                "recipe": "{}",
                "extra_pip_plugins": '["plugin1", "plugin2"]',
            }
        )
        assert args.extra_pip_plugins == ["plugin1", "plugin2"]

    def test_extra_pip_plugins_with_empty_string(self):
        """Test that empty strings are handled as empty lists (UI edge case)."""
        args = SubProcessRecipeTaskArgs.model_validate(
            {
                "recipe": "{}",
                "extra_pip_plugins": "",
            }
        )
        assert args.extra_pip_plugins == []

    def test_extra_pip_plugins_with_actual_list(self):
        """Test that actual Python lists are passed through unchanged."""
        args = SubProcessRecipeTaskArgs.model_validate(
            {
                "recipe": "{}",
                "extra_pip_plugins": ["plugin1"],
            }
        )
        assert args.extra_pip_plugins == ["plugin1"]

    def test_extra_env_vars_with_json_dict_string(self):
        """Test that JSON dict strings are parsed correctly."""
        args = SubProcessRecipeTaskArgs.model_validate(
            {
                "recipe": "{}",
                "extra_env_vars": '{"VAR1": "value1", "VAR2": "value2"}',
            }
        )
        assert args.extra_env_vars == {"VAR1": "value1", "VAR2": "value2"}

    def test_extra_env_vars_with_empty_string(self):
        """Test that empty strings are handled as empty dicts (UI edge case)."""
        args = SubProcessRecipeTaskArgs.model_validate(
            {
                "recipe": "{}",
                "extra_env_vars": "",
            }
        )
        assert args.extra_env_vars == {}

    def test_extra_env_vars_with_empty_json_object_string(self):
        """Test that '{}' string is parsed as empty dict."""
        args = SubProcessRecipeTaskArgs.model_validate(
            {
                "recipe": "{}",
                "extra_env_vars": "{}",
            }
        )
        assert args.extra_env_vars == {}

    def test_extra_env_vars_with_actual_dict(self):
        """Test that actual Python dicts are passed through unchanged."""
        args = SubProcessRecipeTaskArgs.model_validate(
            {
                "recipe": "{}",
                "extra_env_vars": {"VAR1": "value1"},
            }
        )
        assert args.extra_env_vars == {"VAR1": "value1"}

    def test_extra_env_vars_with_empty_dict(self):
        """Test that empty Python dicts are passed through unchanged."""
        args = SubProcessRecipeTaskArgs.model_validate(
            {
                "recipe": "{}",
                "extra_env_vars": {},
            }
        )
        assert args.extra_env_vars == {}

    def test_extra_env_vars_with_invalid_json(self):
        """Test that invalid JSON strings raise validation errors."""
        with pytest.raises(ValidationError) as exc_info:
            SubProcessRecipeTaskArgs.model_validate(
                {
                    "recipe": "{}",
                    "extra_env_vars": "{invalid: json}",
                }
            )
        # Verify the error message mentions JSON parsing
        assert "Expecting property name" in str(exc_info.value)

    def test_extra_env_vars_with_null_string_literal(self):
        """Test that 'null' string literal raises validation error (None is not a valid dict)."""
        with pytest.raises(ValidationError) as exc_info:
            SubProcessRecipeTaskArgs.model_validate(
                {
                    "recipe": "{}",
                    "extra_env_vars": "null",
                }
            )
        # Verify error message indicates None is not valid for dict field
        assert "dict" in str(exc_info.value).lower()

    def test_all_fields_with_defaults(self):
        """Test that all JSON fields have proper defaults when not provided."""
        args = SubProcessRecipeTaskArgs.model_validate(
            {
                "recipe": "{}",
            }
        )
        assert args.extra_pip_requirements == []
        assert args.extra_pip_plugins == []
        assert args.extra_env_vars == {}

    def test_combined_json_string_fields(self):
        """Test multiple JSON string fields together."""
        args = SubProcessRecipeTaskArgs.model_validate(
            {
                "recipe": "{}",
                "extra_pip_requirements": '["pkg1", "pkg2"]',
                "extra_pip_plugins": '["plugin1"]',
                "extra_env_vars": '{"KEY": "value"}',
            }
        )
        assert args.extra_pip_requirements == ["pkg1", "pkg2"]
        assert args.extra_pip_plugins == ["plugin1"]
        assert args.extra_env_vars == {"KEY": "value"}

    def test_combined_empty_strings(self):
        """Test multiple empty string fields together (UI edge case)."""
        args = SubProcessRecipeTaskArgs.model_validate(
            {
                "recipe": "{}",
                "extra_pip_requirements": "",
                "extra_pip_plugins": "",
                "extra_env_vars": "",
            }
        )
        assert args.extra_pip_requirements == []
        assert args.extra_pip_plugins == []
        assert args.extra_env_vars == {}

    def test_validation_error_message_for_wrong_type_in_json(self):
        """Test that validation errors have helpful messages when JSON contains wrong types."""
        # If JSON is valid but contains wrong type, Pydantic validation should catch it
        # For example, passing a string instead of list
        with pytest.raises(ValidationError) as exc_info:
            SubProcessRecipeTaskArgs.model_validate(
                {
                    "recipe": "{}",
                    "extra_pip_requirements": '"not_a_list"',  # Valid JSON but wrong type
                }
            )
        # Verify error mentions list type
        assert "list" in str(exc_info.value).lower()


class TestGetCombinedEnvVars:
    """Tests for get_combined_env_vars() environment merging logic."""

    def test_filters_empty_strings(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Empty string values in extra_env_vars should not override system vars."""
        monkeypatch.setenv("VAR2", "system_value")

        args = SubProcessRecipeTaskArgs(
            recipe='{"source": {"type": "test"}}',
            version="0.12.0",
            extra_env_vars={"VAR1": "value1", "VAR2": "", "VAR3": "value3"},
        )

        combined_env = args.get_combined_env_vars()

        assert combined_env.get("VAR1") == "value1"
        assert combined_env.get("VAR3") == "value3"
        # Empty string filtered out, so system value preserved
        assert combined_env.get("VAR2") == "system_value"

    def test_user_overrides_system(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """User-provided extra_env_vars should override system environment variables."""
        monkeypatch.setenv("TEST_VAR1", "system_value1")
        monkeypatch.setenv("TEST_VAR2", "system_value2")

        args = SubProcessRecipeTaskArgs(
            recipe='{"source": {"type": "test"}}',
            version="0.12.0",
            extra_env_vars={
                "TEST_VAR1": "user_override1",
                "TEST_VAR2": "user_override2",
                "NEW_VAR": "new_value",
            },
        )

        combined_env = args.get_combined_env_vars()

        assert combined_env.get("TEST_VAR1") == "user_override1"
        assert combined_env.get("TEST_VAR2") == "user_override2"
        assert combined_env.get("NEW_VAR") == "new_value"


class TestSharedRecipeTaskSkeleton:
    """The steps every recipe task shares.

    Each task had its own copy, and the copies had drifted: an envelope missing
    the pip-referenced env secrets, a venv failure reporting neither stderr nor
    logs, and a report shipped unmasked where the guarantee is to withhold it.
    These pin the consolidated behaviour, so the next task inherits it.
    """

    @staticmethod
    def _args(**kwargs: object) -> SubProcessRecipeTaskArgs:
        return SubProcessRecipeTaskArgs(
            recipe='{"source": {"type": "mysql"}}', **kwargs
        )

    def test_an_empty_extra_env_var_does_not_override_the_real_one(self) -> None:
        """get_combined_env_vars filters empty values on purpose.

        One task then merged `venv_ref.extra_envs()` on top, which returns them
        unfiltered -- putting "" back over a real os.environ value. Only that
        task did it, which is how it went unnoticed.
        """
        venv_ref = Mock()
        venv_ref.venv_loc = "/tmp/venv"
        args = self._args(extra_env_vars={"SHARED_SKELETON_PROBE": ""})

        with patch.dict(os.environ, {"SHARED_SKELETON_PROBE": "real-value"}):
            env = SubProcessTaskUtil.build_subprocess_env(args, venv_ref)

        assert env["SHARED_SKELETON_PROBE"] == "real-value"
        assert env["VENV_PATH"] == "/tmp/venv"
        assert env["DATAHUB_ENABLE_SECRET_MASKING"] == "true"

    def test_a_store_sourced_secret_rides_the_envelope_and_not_the_environment(
        self,
    ) -> None:
        """Where the boundary actually is, in both directions.

        A store-resolved secret reaches the child through stdin, so it stays
        off /proc/<pid>/environ and `ps e` and is not inherited by everything
        the child spawns. An env-SOURCED one is a different case and IS
        inherited, because os.environ is passed through wholesale -- the
        docstring on build_subprocess_env says so, and asserting otherwise
        would be asserting a property this code does not have.

        The previous version of this test could not fail: it invented the
        value "envelope-only", handed it only to build_stdin_envelope, and
        then checked it was absent from an environment it had never been given
        to. Review was right that it proved nothing; the fix review suggested
        -- resolve ${A_SECRET} from a patched environment and assert it is
        absent -- would have asserted the opposite of the documented
        behaviour, so the boundary is pinned in both directions instead.
        """
        venv_ref = Mock()
        venv_ref.venv_loc = "/tmp/venv"
        args = self._args()

        with patch.dict(os.environ, {"AN_ENV_SOURCED_SECRET": "inherited-on-purpose"}):
            env = SubProcessTaskUtil.build_subprocess_env(args, venv_ref)

        envelope = json.loads(
            SubProcessTaskUtil.build_stdin_envelope(
                args, {"source": {"type": "mysql"}}, {"A_SECRET": "envelope-only"}
            )
        )

        # From the store: in the envelope, nowhere in the environment.
        assert envelope["__secrets__"] == {"A_SECRET": "envelope-only"}
        assert "A_SECRET" not in env
        assert "envelope-only" not in env.values()
        # build_subprocess_env is never handed secret_values at all, which is
        # what makes the first two assertions structural rather than lucky.
        assert (
            "secret_values"
            not in inspect.signature(SubProcessTaskUtil.build_subprocess_env).parameters
        )

        # From the environment: inherited, and documented as such.
        assert env["AN_ENV_SOURCED_SECRET"] == "inherited-on-purpose"

    @pytest.mark.asyncio
    async def test_a_venv_failure_reports_the_captured_stderr(self) -> None:
        """format_subprocess_error pulls a CalledProcessError's captured output.

        One task dropped it and raised bare `str(e)`, so a pip/uv resolution
        failure surfaced as "Command '[...]' returned non-zero exit status 1"
        with the actual reason discarded.
        """
        error = subprocess.CalledProcessError(returncode=1, cmd=["uv", "pip"])
        error.stderr = "No solution found when resolving dependencies"

        with patch(
            "datahub.executor.execution.sub_process_task_common.setup_venv",
            side_effect=error,
        ):
            with pytest.raises(TaskError) as err:
                await SubProcessTaskUtil.setup_task_venv(
                    self._args(), "mysql", tempfile.mkdtemp()
                )

        assert "No solution found when resolving dependencies" in str(err.value)

    @pytest.mark.asyncio
    async def test_a_venv_failure_also_reports_what_the_setup_logged(self) -> None:
        """The holder is kept rather than inlined, so a failure has both."""
        logs = LogHolder()

        with patch(
            "datahub.executor.execution.sub_process_task_common.setup_venv",
            side_effect=RuntimeError("boom"),
        ):
            with pytest.raises(TaskError):
                await SubProcessTaskUtil.setup_task_venv(
                    self._args(), "mysql", tempfile.mkdtemp(), logs=logs
                )

        captured = logs.get_logs()
        assert "Setting up venv for plugin 'mysql'" in captured
        assert "Venv setup failed: boom" in captured

    def test_logs_that_cannot_be_masked_are_withheld_like_the_report(
        self, tmp_path: Path
    ) -> None:
        """Fail-closed for BOTH outputs.

        The tasks disagreed: one withheld the report but shipped the logs
        unmasked, and the logs are rendered in the UI and handed to the agent
        verbatim. masking_filter.py's guarantee does not distinguish them.
        """
        report_file = tmp_path / "report.json"
        report_file.write_text('{"nodes": [{"name": "a_real_table"}]}')
        ctx = Mock()
        report = Mock()
        ctx.get_report.return_value = report

        with patch(
            "datahub.executor.execution.sub_process_task_common.SecretMaskingFilter",
            side_effect=RuntimeError("masking circuit open"),
        ):
            # Must not raise: this is called from a `finally`.
            SubProcessTaskUtil.finalize_task_output(
                str(report_file), str(tmp_path), ["a log line\n"], ctx
            )

        shipped_report = report.set_structured_report.call_args[0][0]
        shipped_logs = report.set_logs.call_args[0][0]
        assert "a_real_table" not in shipped_report
        assert "withheld" in shipped_report
        assert "a log line" not in shipped_logs
        assert "withheld" in shipped_logs


class TestUnprotectableDisclosedSecrets:
    """A resolved secret equal to a value the recipe states in the clear.

    Registering it masks that value everywhere it occurs -- in the structured
    report, in the task logs, and inside unrelated words that merely contain
    it. A password of "datahub" turned the log line
    `datahub_executor.coordinator.ingestion` into
    `***REDACTED:PW***_executor.coordinator.ingestion`, and a probe verdict's
    target from `datahub.orders` into `***REDACTED:PW***.orders`.

    Masking cannot protect such a value: the recipe already states it under a
    non-secret key, and the mask is itself what tells a reader that the secret
    equals the identifier they can see.
    """

    RECIPE = json.dumps(
        {
            "source": {
                "type": "mysql",
                "config": {
                    "host_port": "mysql:3306",
                    "username": "u",
                    "password": "${PW}",
                    "database": "datahub",
                },
            }
        }
    )

    @staticmethod
    def _registered(recipe: str, env: dict) -> set:
        seen: dict = {}

        def _capture(_self: object, secrets: dict) -> None:
            seen.update(secrets)

        ctx = Mock()
        ctx.exec_id = "exec-1"
        ctx.get_report.return_value = Mock()
        executor_ctx = Mock()
        executor_ctx.get_secret_stores.return_value = []

        with (
            patch.dict(os.environ, env),
            patch.object(SecretRegistry, "register_secrets_batch", _capture),
            patch(
                "datahub.executor.execution.sub_process_task_common.initialize_secret_masking"
            ),
        ):
            try:
                SubProcessTaskUtil._resolve_recipe(
                    recipe, execution_ctx=ctx, executor_ctx=executor_ctx
                )
            except Exception:
                # A malformed recipe raises at the JSON parse, which happens
                # AFTER registration on purpose -- so what was registered by
                # then is exactly what the caller wants to inspect.
                pass
        return set(seen.values())

    def test_a_secret_equal_to_a_plain_config_value_is_not_registered(self) -> None:
        registered = self._registered(self.RECIPE, {"PW": "datahub"})
        assert "datahub" not in registered

    def test_an_ordinary_secret_is_still_registered(self) -> None:
        registered = self._registered(self.RECIPE, {"PW": "hunter2"})
        assert "hunter2" in registered

    def test_a_secret_matching_an_inline_secret_literal_is_still_registered(
        self,
    ) -> None:
        """The exemption is for NON-secret keys only.

        A recipe with `password: p` and `database: p` discloses the credential
        itself, and the report travels further than the recipe does.
        """
        recipe = json.dumps(
            {
                "source": {
                    "type": "mysql",
                    "config": {
                        "host_port": "mysql:3306",
                        # An inline secret under any hint-matching key, and the
                        # same string as a plain value. The ref must still be
                        # registered: the recipe discloses the credential.
                        "token": "shared-with-database",
                        "database": "shared-with-database",
                        "password": "${PW}",
                    },
                }
            }
        )
        assert "shared-with-database" in self._registered(
            recipe, {"PW": "shared-with-database"}
        )

    def test_a_malformed_recipe_still_registers_its_secrets(self) -> None:
        """The exemption is best-effort and must never be why a recipe fails.

        Refs are resolved and registered BEFORE the JSON parse on purpose, so
        a parse error quoting the offending document cannot echo an unmasked
        secret. Reading the recipe to compute the exemption must not disturb
        that: an unparseable recipe discloses nothing, so nothing is exempt.
        """
        truncated = '{"source": {"config": {"password": "${PW}"'
        assert "hunter2" in self._registered(truncated, {"PW": "hunter2"})


class TestDisclosedValues:
    """What the executor treats as "the recipe already states this in the clear"."""

    def test_a_credential_under_a_sensitive_parent_is_not_disclosed(self) -> None:
        """Same defect as secret_registry.plain_config_values had, second copy.

        `sensitive` is decided per key and then dropped at the recursion, so a
        sensitive key holding a MAPPING had its children judged by their own
        names. `token: {access: ...}` was read as plainly disclosed and
        exempted from registration -- which on this side means the parent's
        own logs and structured report stop masking it.
        """
        disclosed = unprotectable_disclosed_values(
            json.dumps(
                {
                    "source": {
                        "type": "mysql",
                        "config": {
                            "database": "analytics",
                            "token": {"access": "acc3ssvalue"},
                            "credential": {"private_key": {"pem": "p3mvalue"}},
                        },
                    }
                }
            )
        )

        assert "acc3ssvalue" not in disclosed, disclosed
        assert "p3mvalue" not in disclosed, disclosed
        # The converse: a plain identifier under a plain key is still
        # disclosed, which is the whole reason this function exists.
        assert "analytics" in disclosed, disclosed
