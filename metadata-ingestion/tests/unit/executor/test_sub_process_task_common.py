"""Tests for SubProcessTaskUtil and SubProcessRecipeTaskArgs.

Covers subprocess error formatting, execution-directory cleanup, the JSON-string
field validators that accommodate what the UI sends, and env var merging.
"""

import errno
import inspect
import json
import os
import pathlib
import subprocess
import tempfile
import time
from collections import deque
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from pydantic import ValidationError

from datahub.executor.execution import venv_utils
from datahub.executor.execution.runner import LogHolder
from datahub.executor.execution.sub_process_task_common import (
    SubProcessRecipeTaskArgs,
    SubProcessTaskUtil,
    unprotectable_disclosed_values,
)
from datahub.executor.execution.task import TaskError
from datahub.masking.masking_filter import SecretMaskingFilter
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

    @pytest.mark.asyncio
    async def test_a_venv_failure_does_not_leave_the_run_directory_behind(
        self, tmp_path: Path
    ) -> None:
        """What is in that directory is why this matters.

        prepare_recipe_run mkdirs exec_out_dir and then builds the venv inside
        it, and setup_venv writes an EXPANDED requirements file -- env-var
        templates resolved, so a private index URL carries its token in clear
        text on disk. The only caller invokes prepare_recipe_run outside the
        try whose finally calls finalize_task_output, so a venv failure left
        that file behind with nothing scheduled to remove it.

        Only what this function created: exec_out_dir may already exist
        because a caller laid artifact directories out under it first, and
        removing someone else's directory on the way out of a failure is a
        worse bug than the one being fixed.
        """
        exec_out_dir = tmp_path / "exec-123"

        with (
            patch.object(
                SubProcessTaskUtil,
                "_resolve_recipe",
                return_value=({"source": {"type": "mysql"}}, {}),
            ),
            patch.object(
                SubProcessTaskUtil,
                "setup_task_venv",
                side_effect=RuntimeError("uv could not resolve dependencies"),
            ),
        ):
            with pytest.raises(RuntimeError, match="uv could not resolve"):
                await SubProcessTaskUtil.prepare_recipe_run(
                    self._args(),
                    execution_ctx=Mock(),
                    executor_ctx=Mock(),
                    exec_out_dir=str(exec_out_dir),
                )

        assert not exec_out_dir.exists(), "the run directory outlived the failure"

    @pytest.mark.asyncio
    async def test_a_failure_after_the_venv_releases_its_cache_lock(
        self, tmp_path: Path
    ) -> None:
        """The window between setup_task_venv returning and PreparedRun existing.

        PreparedRun is the only thing that carries venv_ref out to the caller
        whose finally releases the lock, so a failure while BUILDING it --
        build_subprocess_env or build_stdin_envelope raising, or a
        cancellation between them -- leaves the entry held SHARED with no
        owner. Eviction needs a non-blocking exclusive, so that entry becomes
        unreclaimable for the life of the pod.
        """
        venv_ref = Mock()
        venv_ref.venv_loc = "/tmp/venv"

        with (
            patch.object(
                SubProcessTaskUtil,
                "_resolve_recipe",
                return_value=({"source": {"type": "mysql"}}, {}),
            ),
            patch.object(SubProcessTaskUtil, "setup_task_venv", return_value=venv_ref),
            patch.object(
                SubProcessTaskUtil,
                "build_stdin_envelope",
                side_effect=RuntimeError("envelope failed"),
            ),
        ):
            with pytest.raises(RuntimeError, match="envelope failed"):
                await SubProcessTaskUtil.prepare_recipe_run(
                    self._args(),
                    execution_ctx=Mock(),
                    executor_ctx=Mock(),
                    exec_out_dir=str(tmp_path / "exec-789"),
                )

        venv_ref.lock.release.assert_called_once()

    @pytest.mark.asyncio
    async def test_a_directory_the_caller_already_made_is_left_alone(
        self, tmp_path: Path
    ) -> None:
        """The converse, so the cleanup cannot grow into deleting a caller's work."""
        exec_out_dir = tmp_path / "exec-456"
        exec_out_dir.mkdir()
        (exec_out_dir / "caller_artifact.json").write_text("{}")

        with (
            patch.object(
                SubProcessTaskUtil,
                "_resolve_recipe",
                return_value=({"source": {"type": "mysql"}}, {}),
            ),
            patch.object(
                SubProcessTaskUtil, "setup_task_venv", side_effect=RuntimeError("boom")
            ),
        ):
            with pytest.raises(RuntimeError):
                await SubProcessTaskUtil.prepare_recipe_run(
                    self._args(),
                    execution_ctx=Mock(),
                    executor_ctx=Mock(),
                    exec_out_dir=str(exec_out_dir),
                )

        assert (exec_out_dir / "caller_artifact.json").exists()

    def test_a_caller_extension_cannot_switch_masking_off(self) -> None:
        """`extra` is for task-specific additions, not for this key.

        The merge put `extra` last, so a caller passing
        DATAHUB_ENABLE_SECRET_MASKING=false turned masking off in the child --
        and the child reads that flag to decide whether to register secrets at
        all, so every later mask in that process becomes a no-op. A knob that
        can be turned off by accident is not a guarantee.

        The user's own extra_env_vars were never able to do this (they merge
        first, and the flag is set after them); this closes the same door on
        the internal extension point.
        """
        venv_ref = Mock()
        venv_ref.venv_loc = "/tmp/venv"

        env = SubProcessTaskUtil.build_subprocess_env(
            self._args(),
            venv_ref,
            extra={"DATAHUB_ENABLE_SECRET_MASKING": "false", "TASK_THING": "kept"},
        )

        assert env["DATAHUB_ENABLE_SECRET_MASKING"] == "true"
        # And the rest of `extra` still arrives -- the point is one key, not
        # a neutered extension point.
        assert env["TASK_THING"] == "kept"

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

    @pytest.mark.asyncio
    async def test_the_setup_log_describes_the_venv_that_is_actually_built(
        self,
    ) -> None:
        """The mode line consulted `args`; the venv is built from the override.

        setup_task_venv takes `version` precisely so a task can run somewhere
        other than args.version -- the probe task asks for the executor
        image's own libraries. resolved_version feeds VenvConfig, but the
        branch choosing between "Bundled" and "dynamic" called
        args.should_use_bundled_venv(), which is `args.version == "bundled"`.
        So an override inverted the log: setup diagnostics named the mode the
        run did not use, which is the one thing they exist to tell you.
        """
        logs = LogHolder()
        captured_config = {}

        async def _fake_setup_venv(venv_config, **kwargs):
            captured_config["version"] = venv_config.version
            ref = Mock()
            ref.venv_loc = "/tmp/venv"
            return ref

        with patch(
            "datahub.executor.execution.sub_process_task_common.setup_venv",
            side_effect=_fake_setup_venv,
        ):
            await SubProcessTaskUtil.setup_task_venv(
                self._args(version="bundled"),
                "mysql",
                tempfile.mkdtemp(),
                version="native",
                logs=logs,
            )

        captured = logs.get_logs()
        # What was built, and what the log said about it, have to agree.
        assert captured_config["version"] == "native"
        assert "Creating dynamic venv" in captured
        assert "Bundled" not in captured

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

    def test_an_empty_run_is_not_reported_as_a_masking_failure(
        self, tmp_path: Path
    ) -> None:
        """Nothing to show and could-not-show are different answers.

        _masked returns None when masking fails and the masked text when it
        works -- and the call sites joined the two with `or`, so an empty log
        stream or an empty report produced "withheld: they could not be
        masked, and showing them unmasked could leak a credential."

        That is false, and it is the expensive kind of false: an operator who
        sees it on every quiet run learns to skip the message that means a
        credential nearly escaped.
        """
        report_file = tmp_path / "report.json"
        report_file.write_text("")
        ctx = Mock()
        report = Mock()
        ctx.get_report.return_value = report

        SubProcessTaskUtil.finalize_task_output(
            str(report_file), str(tmp_path), [], ctx
        )

        assert report.set_structured_report.call_args[0][0] == ""
        assert report.set_logs.call_args[0][0] == ""


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
            except json.JSONDecodeError:
                # ONE test passes a truncated recipe, and the parse happens
                # AFTER registration on purpose -- so what was registered by
                # then is exactly what that test inspects.
                #
                # Narrowed from `except Exception`, which also swallowed any
                # unrelated failure. That matters most for the NEGATIVE
                # assertions here: "the exempted value is not registered" is
                # satisfied just as well by a resolution that fell over
                # before registering anything at all.
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


def test_the_two_layers_agree_about_strings_in_a_list() -> None:
    """plain_config_values and the executor's split must see the same values.

    A string directly inside a list is stated by the recipe exactly as one
    under a key is. The executor's walk recursed past them, so canonical
    returned {'public','analytics','staging'} for a recipe where this
    returned nothing -- and a value one layer treats as disclosed and the
    other does not is a value the two exempt differently.
    """
    from datahub.masking.secret_registry import (
        SENSITIVE_KEY_HINTS,
        plain_config_values,
    )

    recipe = {"source": {"config": {"schema_allow": ["public", "analytics"]}}}

    canonical = plain_config_values(recipe, SENSITIVE_KEY_HINTS)
    from datahub.executor.execution.sub_process_task_common import (
        _plain_and_inline_secret_values,
    )

    plain, _inline = _plain_and_inline_secret_values(recipe)

    assert canonical == {"public", "analytics"}
    assert plain == canonical


def test_extra_cannot_overwrite_the_envelopes_own_keys() -> None:
    """`extra` merged last could replace __recipe_yaml__ or __secrets__ --
    the recipe the child runs, or everything it masks against."""
    args = SubProcessRecipeTaskArgs(recipe='{"source": {"type": "mysql"}}')
    envelope = json.loads(
        SubProcessTaskUtil.build_stdin_envelope(
            args,
            {"source": {"type": "mysql"}},
            {"PW": "real-secret"},
            extra={"__secrets__": {}, "__recipe_yaml__": "hijacked", "ok": 1},
        )
    )

    assert envelope["__recipe_yaml__"] != "hijacked"
    assert envelope["__secrets__"].get("PW") == "real-secret"
    assert envelope["ok"] == 1, "an ordinary extra key is still passed through"


def test_finalize_releases_the_venv_cache_lock(tmp_path: Path) -> None:
    """Held for the task's life so eviction cannot delete a venv mid-run --
    which means something has to let go of it, or the entry becomes immortal
    and the cache can never be trimmed.
    """
    from datahub.executor.execution.runner import VenvConfig, VenvReference
    from datahub.executor.execution.venv_cache import EntryLock

    lock = EntryLock(tmp_path / "entry.lock")
    assert lock.acquire(exclusive=False)
    venv_ref = VenvReference(
        venv_loc=tmp_path / "venv-x",
        venv_config=VenvConfig(version="0.15.0.1", main_plugin="snowflake"),
        lock=lock,
    )

    SubProcessTaskUtil.finalize_task_output(
        str(tmp_path / "absent-report.json"),
        str(tmp_path / "exec-dir"),
        [],
        Mock(),
        venv_ref=venv_ref,
    )

    assert not lock.held
    assert EntryLock(tmp_path / "entry.lock").acquire(exclusive=True), (
        "the entry is still locked, so eviction can never reclaim it"
    )


def test_finalize_without_a_venv_ref_is_unchanged(tmp_path: Path) -> None:
    """Every existing caller passes nothing, and the ephemeral path has no
    lock to release."""
    exec_dir = tmp_path / "exec-dir"
    exec_dir.mkdir()

    SubProcessTaskUtil.finalize_task_output(
        str(tmp_path / "absent-report.json"), str(exec_dir), [], Mock()
    )

    assert not exec_dir.exists()


def test_finalizing_stamps_the_entry_as_used_before_letting_go(
    tmp_path: pathlib.Path,
) -> None:
    """A long run must not look idle the moment it ends.

    The hit path touches the marker when the task STARTS, and eviction is
    LRU, so the recorded age is really "age since this run began". An
    ingestion running longer than DATAHUB_VENV_CACHE_MAX_AGE_HOURS would
    therefore become eligible for age eviction the instant it finishes,
    having been in continuous use the entire time -- only the shared lock
    kept it alive, and finalize is where that lock goes away.
    """
    venv_loc = tmp_path / "venv-demo-data-abc123"
    venv_loc.mkdir()
    venv_utils.touch_last_used(venv_loc)
    # As if the run had been going for three hours.
    began = time.time() - 3 * 3600
    os.utime(venv_loc / venv_utils.LAST_USED_MARKER, (began, began))

    venv_ref = Mock()
    venv_ref.venv_loc = str(venv_loc)

    SubProcessTaskUtil.finalize_task_output(
        str(tmp_path / "absent-report.json"),
        str(tmp_path / "exec-out"),
        deque(),
        Mock(),
        masking_filter=SecretMaskingFilter(),
        venv_ref=venv_ref,
    )

    assert venv_utils.last_used_at(venv_loc) > began + 3000, (
        "the entry still records when the run started, so a run longer than "
        "the max age is evictable the moment it ends"
    )
    venv_ref.lock.release.assert_called_once()
