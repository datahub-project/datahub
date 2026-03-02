"""Tests for dh.cli — Click CLI integration."""

from __future__ import annotations

from click.testing import CliRunner

from dh.cli import main


class TestCliHelp:
    def test_group_help(self):
        runner = CliRunner()
        result = runner.invoke(main, ["--help"])
        assert result.exit_code == 0
        assert "multi-worktree support" in result.output

    def test_deploy_help_shows_profile(self):
        runner = CliRunner()
        result = runner.invoke(main, ["deploy", "--help"])
        assert result.exit_code == 0
        assert "--profile" in result.output
        assert "minimal" in result.output
        assert "consumers" in result.output
        assert "backend" in result.output

    def test_deploy_invalid_profile(self):
        runner = CliRunner()
        result = runner.invoke(main, ["deploy", "--profile", "invalid"])
        assert result.exit_code != 0
        assert "Invalid value" in result.output or "invalid" in result.output.lower()

    def test_deploy_help_shows_chart_options(self):
        runner = CliRunner()
        result = runner.invoke(main, ["deploy", "--help"])
        assert "--chart-version" in result.output
        assert "--chart-path" in result.output

    def test_infra_up_help(self):
        runner = CliRunner()
        result = runner.invoke(main, ["infra-up", "--help"])
        assert result.exit_code == 0
        assert "--chart-version" in result.output

    def test_cluster_up_help(self):
        runner = CliRunner()
        result = runner.invoke(main, ["cluster-up", "--help"])
        assert result.exit_code == 0
