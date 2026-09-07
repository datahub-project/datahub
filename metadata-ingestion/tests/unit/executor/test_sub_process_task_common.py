"""Tests for SubProcessTaskUtil and SubProcessRecipeTaskArgs.

Covers subprocess error formatting, execution-directory cleanup, the JSON-string
field validators that accommodate what the UI sends, and env var merging.
"""

import errno
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from datahub.executor.execution.sub_process_task_common import (
    SubProcessRecipeTaskArgs,
    SubProcessTaskUtil,
)


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
