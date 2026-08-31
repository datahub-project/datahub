import os
import re
import subprocess
from unittest.mock import MagicMock, mock_open, patch

import pytest

from datahub.utilities.caller_context import (
    _CALLER_SIGNATURES,
    _get_ancestor_chain,
    _get_full_command,
    _get_parent_pid,
    _get_process_name,
    _match_signatures,
    _read_parent_env_linux,
    _read_parent_env_macos,
    identify_caller,
)

# All signature env vars that tests need to control.
_ALL_SIGNATURE_KEYS = set()
for marker in _CALLER_SIGNATURES:
    _ALL_SIGNATURE_KEYS.add(marker.split("=", 1)[0] if "=" in marker else marker)
_ALL_SIGNATURE_KEYS.add("CI")
_ALL_SIGNATURE_KEYS.add("DATAHUB_CALLER")


@pytest.fixture(autouse=True)
def _clean_env_and_cache():
    """Clear the lru_cache and remove all signature env vars between tests."""
    identify_caller.cache_clear()
    saved = {k: os.environ.pop(k) for k in _ALL_SIGNATURE_KEYS if k in os.environ}
    yield
    identify_caller.cache_clear()
    for k, v in saved.items():
        os.environ[k] = v


class TestIdentifyCaller:
    def test_explicit_declaration(self):
        os.environ["DATAHUB_CALLER"] = "my-custom-tool"
        assert identify_caller() == "my-custom-tool"

    def test_explicit_declaration_takes_precedence(self):
        os.environ["DATAHUB_CALLER"] = "my-tool"
        os.environ["GITHUB_ACTIONS"] = "true"
        assert identify_caller() == "my-tool"

    def test_github_actions(self):
        os.environ["GITHUB_ACTIONS"] = "true"
        assert identify_caller() == "github-actions"

    def test_jenkins(self):
        os.environ["JENKINS_URL"] = "http://ci.example.com"
        assert identify_caller() == "jenkins"

    def test_generic_ci(self):
        os.environ["CI"] = "true"
        assert identify_caller() == "ci"

    def test_claude_code(self):
        os.environ["CLAUDECODE"] = "1"
        assert identify_caller() == "claude-code"

    def test_cursor(self):
        os.environ["CURSOR_TRACE_ID"] = "abc123"
        assert identify_caller() == "cursor"

    def test_langchain_tracing(self):
        os.environ["LANGCHAIN_TRACING_V2"] = "true"
        assert identify_caller() == "langchain"

    def test_langsmith(self):
        os.environ["LANGSMITH_API_KEY"] = "ls-abc123"
        assert identify_caller() == "langchain"

    def test_langgraph(self):
        os.environ["LANGGRAPH_API_URL"] = "http://localhost:8123"
        assert identify_caller() == "langgraph"

    def test_never_raises(self):
        """identify_caller() must always return a string, never raise."""
        with patch(
            "datahub.utilities.caller_context._identify_caller_inner",
            side_effect=RuntimeError("boom"),
        ):
            assert identify_caller() == "unknown"

    def test_result_format(self):
        """Result is always a simple string safe for use as a header value."""
        result = identify_caller()
        assert isinstance(result, str)
        assert len(result) < 200
        assert "\n" not in result
        assert "\r" not in result

    def test_caching(self):
        """Second call returns cached result without re-running detection."""
        os.environ["DATAHUB_CALLER"] = "tool-a"
        first = identify_caller()
        os.environ["DATAHUB_CALLER"] = "tool-b"
        second = identify_caller()
        assert first == second == "tool-a"


class TestMatchSignatures:
    def test_key_presence_match(self):
        assert (
            _match_signatures("GITHUB_ACTIONS=true\nPATH=/usr/bin") == "github-actions"
        )

    def test_key_value_no_false_positive(self):
        """A key that exists but doesn't have the = marker pattern shouldn't match."""
        assert _match_signatures("SOMETHING_ELSE=1") is None

    def test_no_match(self):
        assert _match_signatures("PATH=/usr/bin\nHOME=/root") is None


class TestReadParentEnvLinux:
    def test_reads_proc_environ(self):
        fake_env = "KEY1=val1\0KEY2=val2\0"
        with patch("builtins.open", mock_open(read_data=fake_env)):
            result = _read_parent_env_linux(1234)
        assert result == "KEY1=val1\nKEY2=val2\n"

    def test_returns_none_on_permission_error(self):
        with patch("builtins.open", side_effect=PermissionError):
            assert _read_parent_env_linux(1234) is None

    def test_returns_none_on_file_not_found(self):
        with patch("builtins.open", side_effect=FileNotFoundError):
            assert _read_parent_env_linux(1234) is None


class TestReadParentEnvMacos:
    def _mock_run(self, stdout="", returncode=0, side_effect=None):
        mock_result = MagicMock()
        mock_result.stdout = stdout
        mock_result.returncode = returncode
        if side_effect:
            return patch("subprocess.run", side_effect=side_effect)
        return patch("subprocess.run", return_value=mock_result)

    def test_returns_stdout_on_success(self):
        with self._mock_run(stdout="CLAUDECODE=1 /usr/bin/python"):
            result = _read_parent_env_macos(1234)
        assert result == "CLAUDECODE=1 /usr/bin/python"

    def test_returns_none_on_nonzero_exit(self):
        with self._mock_run(returncode=1):
            assert _read_parent_env_macos(1234) is None

    def test_returns_none_on_timeout(self):
        with self._mock_run(side_effect=subprocess.TimeoutExpired(cmd="ps", timeout=2)):
            assert _read_parent_env_macos(1234) is None


class TestGetProcessName:
    def _mock_run(self, stdout="", returncode=0, side_effect=None):
        mock_result = MagicMock()
        mock_result.stdout = stdout
        mock_result.returncode = returncode
        if side_effect:
            return patch("subprocess.run", side_effect=side_effect)
        return patch("subprocess.run", return_value=mock_result)

    def test_extracts_basename(self):
        with self._mock_run(stdout="/usr/bin/zsh\n"):
            assert _get_process_name(100) == "zsh"

    def test_simple_name(self):
        with self._mock_run(stdout="python\n"):
            assert _get_process_name(100) == "python"

    def test_returns_none_on_nonzero_exit(self):
        with self._mock_run(returncode=1):
            assert _get_process_name(100) is None

    def test_returns_none_on_timeout(self):
        with self._mock_run(side_effect=subprocess.TimeoutExpired(cmd="ps", timeout=2)):
            assert _get_process_name(100) is None

    def test_java_becomes_gradle_when_gradle_daemon(self):
        """Java processes running GradleDaemon should be reported as gradle."""
        with patch("subprocess.run") as mock_run:
            # First call: ps -o comm= returns "java"
            # Second call: ps -o command= returns full command with GradleDaemon
            mock_run.side_effect = [
                MagicMock(returncode=0, stdout="java\n"),
                MagicMock(
                    returncode=0,
                    stdout="java -Xmx512m org.gradle.launcher.daemon.bootstrap.GradleDaemon 8.5\n",
                ),
            ]
            assert _get_process_name(100) == "gradle"

    def test_java_stays_java_for_non_gradle(self):
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = [
                MagicMock(returncode=0, stdout="java\n"),
                MagicMock(returncode=0, stdout="java -jar myapp.jar\n"),
            ]
            assert _get_process_name(100) == "java"


class TestGetFullCommand:
    def test_returns_stripped_stdout(self):
        mock_result = MagicMock(returncode=0, stdout="  /usr/bin/python script.py  \n")
        with patch("subprocess.run", return_value=mock_result):
            assert _get_full_command(100) == "/usr/bin/python script.py"

    def test_returns_none_on_failure(self):
        mock_result = MagicMock(returncode=1)
        with patch("subprocess.run", return_value=mock_result):
            assert _get_full_command(100) is None

    def test_returns_none_on_timeout(self):
        with patch(
            "subprocess.run",
            side_effect=subprocess.TimeoutExpired(cmd="ps", timeout=2),
        ):
            assert _get_full_command(100) is None


class TestGetParentPid:
    def test_returns_ppid(self):
        mock_result = MagicMock(returncode=0, stdout="  42  \n")
        with patch("subprocess.run", return_value=mock_result):
            assert _get_parent_pid(100) == 42

    def test_returns_none_for_init(self):
        """PID 1 (init) should be treated as end of chain."""
        mock_result = MagicMock(returncode=0, stdout="1\n")
        with patch("subprocess.run", return_value=mock_result):
            assert _get_parent_pid(100) is None

    def test_returns_none_on_failure(self):
        mock_result = MagicMock(returncode=1)
        with patch("subprocess.run", return_value=mock_result):
            assert _get_parent_pid(100) is None

    def test_returns_none_on_bad_output(self):
        mock_result = MagicMock(returncode=0, stdout="not-a-number\n")
        with patch("subprocess.run", return_value=mock_result):
            assert _get_parent_pid(100) is None


class TestGetAncestorChain:
    def test_walks_process_tree(self):
        with (
            patch(
                "datahub.utilities.caller_context._get_process_name",
                side_effect=["zsh", "tmux", "login"],
            ),
            patch(
                "datahub.utilities.caller_context._get_parent_pid",
                side_effect=[200, 300, None],
            ),
        ):
            chain = _get_ancestor_chain(max_depth=4)
        assert chain == ["zsh", "tmux", "login"]

    def test_stops_on_none_process_name(self):
        with (
            patch(
                "datahub.utilities.caller_context._get_process_name",
                side_effect=["zsh", None],
            ),
            patch(
                "datahub.utilities.caller_context._get_parent_pid",
                return_value=200,
            ),
        ):
            chain = _get_ancestor_chain(max_depth=4)
        assert chain == ["zsh"]

    def test_respects_max_depth(self):
        with (
            patch(
                "datahub.utilities.caller_context._get_process_name",
                return_value="zsh",
            ),
            patch(
                "datahub.utilities.caller_context._get_parent_pid",
                return_value=200,
            ),
        ):
            chain = _get_ancestor_chain(max_depth=2)
        assert len(chain) == 2


class TestIdentifyCallerTier3:
    """Tier 3: parent process environment detection."""

    def test_linux_parent_env_detection(self):
        with (
            patch("platform.system", return_value="Linux"),
            patch(
                "datahub.utilities.caller_context._read_parent_env_linux",
                return_value="CURSOR_TRACE_ID=abc123\nPATH=/usr/bin",
            ),
            patch(
                "datahub.utilities.caller_context._get_ancestor_chain",
                return_value=[],
            ),
        ):
            assert identify_caller() == "cursor"

    def test_macos_parent_env_detection(self):
        with (
            patch("platform.system", return_value="Darwin"),
            patch(
                "datahub.utilities.caller_context._read_parent_env_macos",
                return_value="JENKINS_URL=http://ci.local CLAUDECODE=1",
            ),
            patch(
                "datahub.utilities.caller_context._get_ancestor_chain",
                return_value=[],
            ),
        ):
            # CLAUDECODE comes first in _CALLER_SIGNATURES
            assert identify_caller() == "claude-code"

    def test_no_parent_env_falls_through_to_tier4(self):
        with (
            patch("platform.system", return_value="Linux"),
            patch(
                "datahub.utilities.caller_context._read_parent_env_linux",
                return_value=None,
            ),
            patch(
                "datahub.utilities.caller_context._get_ancestor_chain",
                return_value=["zsh"],
            ),
        ):
            assert identify_caller() == "terminal"


class TestIdentifyCallerTier4:
    """Tier 4: process tree name heuristics."""

    def test_process_hint_cursor(self):
        with (
            patch("platform.system", return_value="Linux"),
            patch(
                "datahub.utilities.caller_context._read_parent_env_linux",
                return_value=None,
            ),
            patch(
                "datahub.utilities.caller_context._get_ancestor_chain",
                return_value=["node", "cursor", "login"],
            ),
        ):
            assert identify_caller() == "cursor"

    def test_process_hint_claude(self):
        with (
            patch("platform.system", return_value="Linux"),
            patch(
                "datahub.utilities.caller_context._read_parent_env_linux",
                return_value=None,
            ),
            patch(
                "datahub.utilities.caller_context._get_ancestor_chain",
                return_value=["zsh", "claude"],
            ),
        ):
            assert identify_caller() == "claude-code"

    def test_human_terminal_shell_parent(self):
        with (
            patch("platform.system", return_value="Linux"),
            patch(
                "datahub.utilities.caller_context._read_parent_env_linux",
                return_value=None,
            ),
            patch(
                "datahub.utilities.caller_context._get_ancestor_chain",
                return_value=["bash", "sshd"],
            ),
        ):
            assert identify_caller() == "terminal"

    def test_fish_shell_detected(self):
        with (
            patch("platform.system", return_value="Linux"),
            patch(
                "datahub.utilities.caller_context._read_parent_env_linux",
                return_value=None,
            ),
            patch(
                "datahub.utilities.caller_context._get_ancestor_chain",
                return_value=["fish"],
            ),
        ):
            assert identify_caller() == "terminal"

    def test_unknown_parent_returned_as_name(self):
        """If parent isn't a shell or known hint, return the process name."""
        with (
            patch("platform.system", return_value="Linux"),
            patch(
                "datahub.utilities.caller_context._read_parent_env_linux",
                return_value=None,
            ),
            patch(
                "datahub.utilities.caller_context._get_ancestor_chain",
                return_value=["supervisord", "systemd"],
            ),
        ):
            assert identify_caller() == "supervisord"

    def test_empty_chain_returns_unknown(self):
        with (
            patch("platform.system", return_value="Linux"),
            patch(
                "datahub.utilities.caller_context._read_parent_env_linux",
                return_value=None,
            ),
            patch(
                "datahub.utilities.caller_context._get_ancestor_chain",
                return_value=[],
            ),
        ):
            assert identify_caller() == "unknown"


class TestUserAgentIntegration:
    """Verify that identify_caller() is wired into the REST emitter User-Agent."""

    def test_user_agent_includes_caller(self):
        """The User-Agent string should embed the caller label as component/caller."""
        from unittest.mock import MagicMock

        from datahub.emitter.rest_emitter import RequestsSessionConfig

        os.environ["DATAHUB_CALLER"] = "test-tool"

        config = RequestsSessionConfig(datahub_component="datahub")

        session = MagicMock()
        session.headers = {}

        ua = config._get_user_agent_string(session)
        # Should match: DataHub-Client/1.0 (<mode>; datahub/test-tool; <version>)
        assert "datahub/test-tool" in ua
        assert re.match(r"DataHub-Client/1\.0 \(\w+; datahub/test-tool; .+\)", ua)

    def test_user_agent_with_env_detected_caller(self):
        """Env-detected callers (e.g. github-actions) appear in the User-Agent."""
        from unittest.mock import MagicMock

        from datahub.emitter.rest_emitter import RequestsSessionConfig

        os.environ["GITHUB_ACTIONS"] = "true"

        config = RequestsSessionConfig(datahub_component="datahub")

        session = MagicMock()
        session.headers = {}

        ua = config._get_user_agent_string(session)
        assert "datahub/github-actions" in ua
