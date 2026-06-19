from unittest.mock import MagicMock, patch

from datahub.telemetry.telemetry import _get_cli_context


class TestGetCliContext:
    def test_reads_context_from_click(self):
        mock_ctx = MagicMock()
        mock_ctx.obj = {"context": {"skill": "datahub-audit", "run_id": "abc123"}}
        with patch("click.get_current_context", return_value=mock_ctx):
            result = _get_cli_context()
        assert result == {"ctx_skill": "datahub-audit", "ctx_run_id": "abc123"}

    def test_returns_empty_when_no_click_context(self):
        with patch("click.get_current_context", return_value=None):
            result = _get_cli_context()
        assert result == {}

    def test_returns_empty_when_no_context_key(self):
        mock_ctx = MagicMock()
        mock_ctx.obj = {}
        with patch("click.get_current_context", return_value=mock_ctx):
            result = _get_cli_context()
        assert result == {}

    def test_returns_empty_on_exception(self):
        with patch("click.get_current_context", side_effect=RuntimeError):
            result = _get_cli_context()
        assert result == {}
