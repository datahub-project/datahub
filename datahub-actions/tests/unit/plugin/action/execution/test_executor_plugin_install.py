"""Tests for external plugin resolution in ExecutorAction."""

import json
import os
from unittest.mock import MagicMock, patch

import pytest

from datahub.plugin.github_resolver import ResolvedGitSource, ResolvedWheel
from datahub_actions.plugin.action.execution.executor_action import ExecutorAction

RESOLVE_PATH = "datahub.plugin.github_resolver.resolve_github_spec"
DOWNLOAD_PATH = "datahub.plugin.github_resolver.download_wheel"


@pytest.fixture
def executor_action():
    """Create an ExecutorAction without full initialization."""
    action = object.__new__(ExecutorAction)
    action.ctx = MagicMock()
    action.dispatcher = MagicMock()
    return action


class TestInstallExternalPlugins:
    def test_no_datahub_plugins_key(self, executor_action):
        """No-op when datahub_plugins key is absent."""
        args = {"recipe": "{}"}
        with patch(RESOLVE_PATH) as mock_resolve:
            executor_action._install_external_plugins(args)
            mock_resolve.assert_not_called()
        assert "extra_pip_requirements" not in args

    def test_empty_string(self, executor_action):
        """No-op when datahub_plugins is an empty string."""
        args = {"datahub_plugins": ""}
        with patch(RESOLVE_PATH) as mock_resolve:
            executor_action._install_external_plugins(args)
            mock_resolve.assert_not_called()

    def test_wheel_is_downloaded_locally(self, executor_action):
        """Wheel specs are downloaded locally for private repo support."""
        specs = ["github:acme/source-a"]
        args = {"datahub_plugins": json.dumps(specs)}

        fake = ResolvedWheel(
            download_url="https://github.com/acme/source-a/releases/download/v1.0/a.whl",
            version="1.0",
        )

        with (
            patch(RESOLVE_PATH, return_value=fake),
            patch(
                DOWNLOAD_PATH, return_value="/tmp/datahub-plugin-xyz/a.whl"
            ) as mock_dl,
        ):
            executor_action._install_external_plugins(args)
            mock_dl.assert_called_once_with(fake, expected_sha256=None)

        reqs = json.loads(args["extra_pip_requirements"])
        assert reqs == ["/tmp/datahub-plugin-xyz/a.whl"]

    def test_object_entry_forwards_sha256(self, executor_action):
        """A {spec, sha256} entry threads the checksum to download_wheel."""
        specs = [{"spec": "github:acme/source-a@v1.0", "sha256": "abc123"}]
        args = {"datahub_plugins": json.dumps(specs)}

        fake = ResolvedWheel(
            download_url="https://github.com/acme/source-a/releases/download/v1.0/a.whl",
            version="1.0",
        )

        with (
            patch(RESOLVE_PATH, return_value=fake),
            patch(DOWNLOAD_PATH, return_value="/tmp/a.whl") as mock_dl,
        ):
            executor_action._install_external_plugins(args)
            mock_dl.assert_called_once_with(fake, expected_sha256="abc123")

        reqs = json.loads(args["extra_pip_requirements"])
        assert reqs == ["/tmp/a.whl"]

    def test_git_fallback_uses_url_directly(self, executor_action):
        """Non-wheel specs (git+https://) are passed as URLs without download."""
        specs = ["github:acme/source-b@v2.0"]
        args = {"datahub_plugins": json.dumps(specs)}

        fake = ResolvedGitSource(
            download_url="git+https://github.com/acme/source-b.git@v2.0",
            version="2.0",
        )

        with (
            patch(RESOLVE_PATH, return_value=fake),
            patch(DOWNLOAD_PATH) as mock_dl,
        ):
            executor_action._install_external_plugins(args)
            mock_dl.assert_not_called()

        reqs = json.loads(args["extra_pip_requirements"])
        assert reqs == [fake.download_url]

    def test_mixed_wheel_and_git(self, executor_action):
        """Handles a mix of wheel and git+https specs."""
        specs = ["github:acme/source-a", "github:acme/source-b@v2.0"]
        args = {"datahub_plugins": json.dumps(specs)}

        fake_a = ResolvedWheel(
            download_url="https://github.com/acme/source-a/releases/download/v1.0/a.whl",
            version="1.0",
        )
        fake_b = ResolvedGitSource(
            download_url="git+https://github.com/acme/source-b.git@v2.0",
            version="2.0",
        )

        with (
            patch(RESOLVE_PATH, side_effect=[fake_a, fake_b]),
            patch(DOWNLOAD_PATH, return_value="/tmp/datahub-plugin-xyz/a.whl"),
        ):
            executor_action._install_external_plugins(args)

        reqs = json.loads(args["extra_pip_requirements"])
        assert reqs == ["/tmp/datahub-plugin-xyz/a.whl", fake_b.download_url]

    def test_appends_to_existing_extra_pip_requirements(self, executor_action):
        """Resolved requirements are appended to pre-existing extra_pip_requirements."""
        specs = ["github:acme/source-a"]
        args = {
            "datahub_plugins": json.dumps(specs),
            "extra_pip_requirements": json.dumps(["sqlparse==0.4.3"]),
        }

        fake = ResolvedWheel(
            download_url="https://github.com/acme/source-a/releases/download/v1.0/a.whl",
            version="1.0",
        )

        with (
            patch(RESOLVE_PATH, return_value=fake),
            patch(DOWNLOAD_PATH, return_value="/tmp/datahub-plugin-xyz/a.whl"),
        ):
            executor_action._install_external_plugins(args)

        reqs = json.loads(args["extra_pip_requirements"])
        assert reqs == ["sqlparse==0.4.3", "/tmp/datahub-plugin-xyz/a.whl"]

    def test_malformed_extra_pip_requirements_falls_back_to_empty(
        self, executor_action
    ):
        """Malformed extra_pip_requirements is recovered by starting with an empty list."""
        specs = ["github:acme/source-a"]
        args = {
            "datahub_plugins": json.dumps(specs),
            "extra_pip_requirements": "{not valid json",
        }

        fake = ResolvedWheel(
            download_url="https://github.com/acme/source-a/releases/download/v1.0/a.whl",
            version="1.0",
        )

        with (
            patch(RESOLVE_PATH, return_value=fake),
            patch(DOWNLOAD_PATH, return_value="/tmp/datahub-plugin-xyz/a.whl"),
        ):
            executor_action._install_external_plugins(args)

        reqs = json.loads(args["extra_pip_requirements"])
        assert reqs == ["/tmp/datahub-plugin-xyz/a.whl"]

    def test_raises_on_resolve_failure(self, executor_action):
        """Resolution failures raise RuntimeError with details."""
        specs = ["github:acme/bad-plugin", "github:acme/good-plugin"]
        args = {"datahub_plugins": json.dumps(specs)}

        fake_good = ResolvedWheel(
            download_url="https://github.com/acme/good-plugin/releases/download/v1.0/good.whl",
            version="1.0",
        )

        with (
            patch(RESOLVE_PATH, side_effect=[RuntimeError("not found"), fake_good]),
            patch(DOWNLOAD_PATH, return_value="/tmp/datahub-plugin-xyz/good.whl"),
            pytest.raises(RuntimeError, match="Failed to resolve external plugin"),
        ):
            executor_action._install_external_plugins(args)

    def test_skips_blank_specs(self, executor_action):
        """Empty strings in the list are skipped."""
        specs = ["github:acme/source-a", "", "  "]
        args = {"datahub_plugins": json.dumps(specs)}

        fake = ResolvedWheel(
            download_url="https://github.com/acme/source-a/releases/download/v1.0/a.whl",
            version="1.0",
        )

        with (
            patch(RESOLVE_PATH, return_value=fake) as mock_resolve,
            patch(DOWNLOAD_PATH, return_value="/tmp/datahub-plugin-xyz/a.whl"),
        ):
            executor_action._install_external_plugins(args)
            mock_resolve.assert_called_once_with("github:acme/source-a")

    def test_bare_string_spec(self, executor_action):
        """A bare string (not JSON array) is treated as a single spec."""
        args = {"datahub_plugins": "github:acme/source-a"}

        fake = ResolvedWheel(
            download_url="https://github.com/acme/source-a/releases/download/v1.0/a.whl",
            version="1.0",
        )

        with (
            patch(RESOLVE_PATH, return_value=fake),
            patch(DOWNLOAD_PATH, return_value="/tmp/datahub-plugin-xyz/a.whl"),
        ):
            executor_action._install_external_plugins(args)

        reqs = json.loads(args["extra_pip_requirements"])
        assert reqs == ["/tmp/datahub-plugin-xyz/a.whl"]

    def test_pypi_spec_passed_directly(self, executor_action):
        """PyPI specs are added as pip requirements without GitHub resolution."""
        specs = ["pypi:my-datahub-plugin==1.2.3"]
        args = {"datahub_plugins": json.dumps(specs)}

        with patch(RESOLVE_PATH) as mock_resolve:
            executor_action._install_external_plugins(args)
            mock_resolve.assert_not_called()

        reqs = json.loads(args["extra_pip_requirements"])
        assert reqs == ["my-datahub-plugin==1.2.3"]

    def test_mixed_pypi_and_github(self, executor_action):
        """Handles a mix of PyPI and GitHub specs."""
        specs = ["pypi:my-plugin==1.0", "github:acme/source-a"]
        args = {"datahub_plugins": json.dumps(specs)}

        fake = ResolvedWheel(
            download_url="https://github.com/acme/source-a/releases/download/v1.0/a.whl",
            version="1.0",
        )

        with (
            patch(RESOLVE_PATH, return_value=fake),
            patch(DOWNLOAD_PATH, return_value="/tmp/datahub-plugin-xyz/a.whl"),
        ):
            executor_action._install_external_plugins(args)

        reqs = json.loads(args["extra_pip_requirements"])
        assert reqs == ["my-plugin==1.0", "/tmp/datahub-plugin-xyz/a.whl"]

    def test_non_list_json_raises_value_error(self, executor_action):
        """JSON string (not array) raises ValueError."""
        args = {"datahub_plugins": '"github:acme/source-a"'}

        with pytest.raises(ValueError, match="must be a JSON array"):
            executor_action._install_external_plugins(args)


class TestGitHubTokenInjection:
    """Tests for GITHUB_TOKEN injection and restoration in _install_external_plugins."""

    def test_injects_token_from_extra_env_vars(self, executor_action):
        """GITHUB_TOKEN from extra_env_vars is injected into os.environ."""
        specs = ["github:acme/source-a"]
        args = {
            "datahub_plugins": json.dumps(specs),
            "extra_env_vars": json.dumps({"GITHUB_TOKEN": "gh-secret-123"}),
        }

        captured_token = {}

        def capture_resolve(spec):
            captured_token["value"] = os.environ.get("GITHUB_TOKEN")
            return ResolvedWheel(
                download_url="https://example.com/a.whl",
                version="1.0",
            )

        with (
            patch(RESOLVE_PATH, side_effect=capture_resolve),
            patch(DOWNLOAD_PATH, return_value="/tmp/a.whl"),
            patch.dict(os.environ, {}, clear=False),
        ):
            # Remove GITHUB_TOKEN if it exists
            os.environ.pop("GITHUB_TOKEN", None)
            executor_action._install_external_plugins(args)

        assert captured_token["value"] == "gh-secret-123"
        # Token should be cleaned up
        assert "GITHUB_TOKEN" not in os.environ

    def test_restores_original_token(self, executor_action):
        """Pre-existing GITHUB_TOKEN is restored after resolution."""
        specs = ["github:acme/source-a"]
        args = {
            "datahub_plugins": json.dumps(specs),
            "extra_env_vars": json.dumps({"GITHUB_TOKEN": "injected-token"}),
        }

        fake = ResolvedWheel(
            download_url="https://example.com/a.whl",
            version="1.0",
        )

        with (
            patch(RESOLVE_PATH, return_value=fake),
            patch(DOWNLOAD_PATH, return_value="/tmp/a.whl"),
            patch.dict(os.environ, {"GITHUB_TOKEN": "original-token"}),
        ):
            executor_action._install_external_plugins(args)
            assert os.environ["GITHUB_TOKEN"] == "original-token"

    def test_restores_token_on_error(self, executor_action):
        """GITHUB_TOKEN is restored even when resolution fails."""
        specs = ["github:acme/bad-plugin"]
        args = {
            "datahub_plugins": json.dumps(specs),
            "extra_env_vars": json.dumps({"GITHUB_TOKEN": "injected-token"}),
        }

        with patch.dict(os.environ, {"GITHUB_TOKEN": "original-token"}):
            with (
                patch(RESOLVE_PATH, side_effect=RuntimeError("boom")),
                patch(DOWNLOAD_PATH),
                pytest.raises(RuntimeError),
            ):
                executor_action._install_external_plugins(args)
            # Check inside patch.dict scope so env is still patched
            assert os.environ["GITHUB_TOKEN"] == "original-token"

    def test_no_injection_without_extra_env_token(self, executor_action):
        """No injection when extra_env_vars doesn't contain GITHUB_TOKEN."""
        specs = ["github:acme/source-a"]
        args = {
            "datahub_plugins": json.dumps(specs),
            "extra_env_vars": json.dumps({"OTHER_VAR": "value"}),
        }

        captured_token = {}

        def capture_resolve(spec):
            captured_token["value"] = os.environ.get("GITHUB_TOKEN")
            return ResolvedWheel(
                download_url="https://example.com/a.whl",
                version="1.0",
            )

        with (
            patch(RESOLVE_PATH, side_effect=capture_resolve),
            patch(DOWNLOAD_PATH, return_value="/tmp/a.whl"),
            patch.dict(os.environ, {}, clear=False),
        ):
            os.environ.pop("GITHUB_TOKEN", None)
            executor_action._install_external_plugins(args)

        assert captured_token["value"] is None


class TestReportExecutionFailure:
    """Tests for _report_execution_failure GMS emission."""

    def test_emits_failure_mcp(self, executor_action):
        """Verifies the method emits an MCP with FAILURE status to GMS."""
        mock_graph = MagicMock()
        executor_action.ctx.graph.graph = mock_graph

        with patch(
            "datahub_actions.plugin.action.execution.executor_action.time"
        ) as mock_time:
            mock_time.time.return_value = 1000.0
            executor_action._report_execution_failure(
                "exec-123", "Something went wrong"
            )

        mock_graph.emit_mcp.assert_called_once()
        call_args = mock_graph.emit_mcp.call_args
        mcp = call_args[0][0]
        assert mcp.entityType == "dataHubExecutionRequest"
        assert mcp.changeType == "UPSERT"
        assert mcp.aspect.status == "FAILURE"
        assert mcp.aspect.report == "Something went wrong"
        assert call_args[1]["async_flag"] is False

    def test_handles_none_exec_id(self, executor_action):
        """Uses 'unknown' when exec_id is None."""
        mock_graph = MagicMock()
        executor_action.ctx.graph.graph = mock_graph

        executor_action._report_execution_failure(None, "error msg")

        mcp = mock_graph.emit_mcp.call_args[0][0]
        assert mcp.entityKeyAspect.id == "unknown"

    def test_logs_error_when_emit_fails(self, executor_action):
        """Logs original error when GMS emission itself fails."""
        mock_graph = MagicMock()
        mock_graph.emit_mcp.side_effect = Exception("GMS unreachable")
        executor_action.ctx.graph.graph = mock_graph

        # Should not raise — it catches its own errors
        executor_action._report_execution_failure("exec-456", "original error")

        # Verify emit was attempted
        mock_graph.emit_mcp.assert_called_once()


class TestPluginInstallAbortFlow:
    """End-to-end: when _install_external_plugins raises, dispatch must NOT run
    and _report_execution_failure MUST be called."""

    def _make_exec_event(self, exec_id: str, args: dict) -> MagicMock:
        """Build a minimal MetadataChangeLog-like event for _handle_execution_request_input."""
        event = MagicMock()
        event.get.side_effect = lambda key: {
            "entityUrn": f"urn:li:dataHubExecutionRequest:{exec_id}",
            "entityKeyAspect": None,
            "aspect": MagicMock(
                get=lambda k: (
                    json.dumps(
                        {"task": "RUN_INGEST", "executorId": "default", "args": args}
                    )
                    if k == "value"
                    else "application/json"
                )
            ),
        }.get(key)
        return event

    def test_dispatch_not_called_on_plugin_failure(self, executor_action):
        """When _install_external_plugins raises, dispatcher.dispatch must NOT be called."""
        args = {"datahub_plugins": json.dumps(["github:acme/broken"])}
        event = self._make_exec_event("exec-999", args)

        with (
            patch.object(
                executor_action,
                "_install_external_plugins",
                side_effect=RuntimeError("resolve failed"),
            ),
            patch.object(executor_action, "_report_execution_failure") as mock_report,
        ):
            executor_action._handle_execution_request_input(event)

        executor_action.dispatcher.dispatch.assert_not_called()
        mock_report.assert_called_once()
        call_args = mock_report.call_args
        assert call_args[0][0] == "exec-999"
        assert "resolve failed" in call_args[0][1]

    def test_dispatch_proceeds_when_no_plugins(self, executor_action):
        """When no datahub_plugins key, dispatch proceeds normally."""
        args = {"recipe": "{}"}
        event = self._make_exec_event("exec-100", args)

        executor_action._handle_execution_request_input(event)

        executor_action.dispatcher.dispatch.assert_called_once()
