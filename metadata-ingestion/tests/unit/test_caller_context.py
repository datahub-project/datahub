import os
import re
from unittest.mock import patch

import pytest

from datahub.utilities.caller_context import _CALLER_SIGNATURES, identify_caller

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
