from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from datahub.cli.lineage_cli import (
    _format_json,
    _format_table,
    _name_from_urn,
    _platform_from_urn,
    lineage,
)
from datahub.sdk.lineage_client import LineageResult


class TestNameFromUrn:
    def test_dataset_urn(self):
        urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.my_table,PROD)"
        assert _name_from_urn(urn) == "db.schema.my_table"

    def test_chart_urn(self):
        urn = "urn:li:chart:(looker,dashboard_elements.221)"
        assert _name_from_urn(urn) == "dashboard_elements.221"

    def test_dashboard_urn(self):
        urn = "urn:li:dashboard:(looker,dashboards.53)"
        assert _name_from_urn(urn) == "dashboards.53"

    def test_datajob_urn(self):
        urn = "urn:li:dataJob:(urn:li:dataFlow:(airflow,my_dag,PROD),my_task)"
        assert _name_from_urn(urn) == "my_task"

    def test_invalid_urn_returns_original(self):
        assert _name_from_urn("not-a-urn") == "not-a-urn"


class TestPlatformFromUrn:
    def test_chart_urn(self):
        assert _platform_from_urn("urn:li:chart:(looker,elements.1)") == "looker"

    def test_dashboard_urn(self):
        assert _platform_from_urn("urn:li:dashboard:(tableau,dash.1)") == "tableau"

    def test_dataset_urn_returns_none(self):
        urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.table,PROD)"
        assert _platform_from_urn(urn) is None

    def test_invalid_urn_returns_none(self):
        assert _platform_from_urn("not-a-urn") is None


class TestFormatTable:
    def _make_result(self, **kwargs: object) -> LineageResult:
        return LineageResult(
            urn=str(
                kwargs.get(
                    "urn",
                    "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.table,PROD)",
                )
            ),
            type=str(kwargs.get("type", "DATASET")),
            hops=int(kwargs.get("hops", 1)),
            direction=kwargs.get("direction", "upstream"),  # type: ignore[arg-type]
            platform=kwargs.get("platform", "snowflake"),  # type: ignore[arg-type]
            name=kwargs.get("name", "my_table"),  # type: ignore[arg-type]
        )

    def test_empty_results(self):
        assert "No upstream lineage found" in _format_table([], "upstream")

    def test_single_result(self):
        result = self._make_result()
        output = _format_table([result], "upstream")
        assert "my_table" in output
        assert "snowflake" in output
        assert "DATASET" in output

    def test_sorts_by_hops(self):
        r1 = self._make_result(hops=2, name="second")
        r2 = self._make_result(hops=1, name="first")
        output = _format_table([r1, r2], "upstream")
        assert output.index("first") < output.index("second")

    def test_name_fallback_to_urn(self):
        result = self._make_result(name="")
        output = _format_table([result], "upstream")
        assert "db.table" in output


class TestFormatJson:
    def _make_result(self, **kwargs: object) -> LineageResult:
        return LineageResult(
            urn=str(
                kwargs.get(
                    "urn",
                    "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.table,PROD)",
                )
            ),
            type=str(kwargs.get("type", "DATASET")),
            hops=int(kwargs.get("hops", 1)),
            direction=kwargs.get("direction", "upstream"),  # type: ignore[arg-type]
            platform=kwargs.get("platform", "snowflake"),  # type: ignore[arg-type]
            name=kwargs.get("name", "my_table"),  # type: ignore[arg-type]
        )

    def test_json_output(self):
        import json

        result = self._make_result()
        output = json.loads(_format_json([result], "upstream", 3, 100))
        assert output["metadata"]["direction"] == "upstream"
        assert output["metadata"]["count"] == 1
        assert len(output["results"]) == 1
        assert output["results"][0]["name"] == "my_table"
        assert output["results"][0]["platform"] == "snowflake"

    def test_json_capped_hint(self):
        import json

        results = [self._make_result() for _ in range(5)]
        output = json.loads(_format_json(results, "downstream", 3, 5))
        assert output["metadata"]["capped"] is True
        assert "hint" in output["metadata"]
        assert "--count" in output["metadata"]["hint"]

    def test_json_hops_hint(self):
        import json

        result = self._make_result()
        output = json.loads(_format_json([result], "upstream", 1, 100))
        assert output["metadata"]["capped"] is False
        assert "hint" in output["metadata"]
        assert "--hops" in output["metadata"]["hint"]

    def test_name_fallback_in_json(self):
        import json

        result = self._make_result(name="")
        output = json.loads(_format_json([result], "upstream", 3, 100))
        assert output["results"][0]["name"] == "db.table"

    def test_platform_fallback_for_chart(self):
        import json

        result = self._make_result(
            urn="urn:li:chart:(looker,elements.1)",
            type="CHART",
            platform=None,
            name=None,
        )
        output = json.loads(_format_json([result], "upstream", 3, 100))
        assert output["results"][0]["platform"] == "looker"
        assert output["results"][0]["name"] == "elements.1"


class TestLineageCli:
    def test_help(self):
        runner = CliRunner()
        result = runner.invoke(lineage, ["--help"])
        assert result.exit_code == 0
        assert "Explore lineage" in result.output

    def test_agent_context(self):
        runner = CliRunner()
        result = runner.invoke(lineage, ["--agent-context"])
        assert result.exit_code == 0
        assert "Agent Context" in result.output
        assert "upstream" in result.output

    @patch("datahub.cli.lineage_cli.get_default_graph")
    @patch("datahub.cli.lineage_cli.DataHubClient")
    def test_upstream_lineage(self, mock_client_cls, mock_graph):
        mock_graph.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_graph.return_value.__exit__ = MagicMock(return_value=False)

        mock_lineage = MagicMock()
        mock_lineage.get_lineage.return_value = [
            LineageResult(
                urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.upstream_table,PROD)",
                type="DATASET",
                hops=1,
                direction="upstream",
                platform="snowflake",
                name="upstream_table",
            )
        ]
        mock_client_cls.return_value.lineage = mock_lineage

        runner = CliRunner()
        result = runner.invoke(
            lineage,
            [
                "--urn",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.my_table,PROD)",
                "--direction",
                "upstream",
            ],
        )
        assert result.exit_code == 0
        assert "upstream_table" in result.output
        mock_lineage.get_lineage.assert_called_once()

    def test_column_with_non_dataset_urn(self):
        runner = CliRunner()
        result = runner.invoke(
            lineage,
            [
                "--urn",
                "urn:li:chart:(looker,elements.1)",
                "--column",
                "col1",
            ],
        )
        assert result.exit_code != 0 or "only supported for dataset" in result.output

    def test_no_urn_shows_help(self):
        runner = CliRunner()
        result = runner.invoke(lineage, [])
        assert result.exit_code == 0
        assert "Explore lineage" in result.output

    @patch("datahub.cli.lineage_cli.get_default_graph")
    @patch("datahub.cli.lineage_cli.DataHubClient")
    def test_downstream_json_output(self, mock_client_cls, mock_graph):
        mock_graph.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_graph.return_value.__exit__ = MagicMock(return_value=False)

        mock_lineage = MagicMock()
        mock_lineage.get_lineage.return_value = [
            LineageResult(
                urn="urn:li:dataset:(urn:li:dataPlatform:dbt,db.downstream,PROD)",
                type="DATASET",
                hops=1,
                direction="downstream",
                platform="dbt",
                name="downstream",
            ),
            LineageResult(
                urn="urn:li:dashboard:(looker,dashboards.1)",
                type="DASHBOARD",
                hops=2,
                direction="downstream",
                platform=None,
                name=None,
            ),
        ]
        mock_client_cls.return_value.lineage = mock_lineage

        runner = CliRunner()
        result = runner.invoke(
            lineage,
            [
                "--urn",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.table,PROD)",
                "--direction",
                "downstream",
                "--format",
                "json",
            ],
        )
        assert result.exit_code == 0
        import json as json_mod

        data = json_mod.loads(result.output)
        assert data["metadata"]["direction"] == "downstream"
        assert data["metadata"]["count"] == 2
        assert len(data["results"]) == 2

    @patch("datahub.cli.lineage_cli.get_default_graph")
    @patch("datahub.cli.lineage_cli.DataHubClient")
    def test_empty_lineage(self, mock_client_cls, mock_graph):
        mock_graph.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_graph.return_value.__exit__ = MagicMock(return_value=False)

        mock_lineage = MagicMock()
        mock_lineage.get_lineage.return_value = []
        mock_client_cls.return_value.lineage = mock_lineage

        runner = CliRunner()
        result = runner.invoke(
            lineage,
            [
                "--urn",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.table,PROD)",
                "--direction",
                "upstream",
            ],
        )
        assert result.exit_code == 0
        assert "No upstream lineage found" in result.output

    @patch("datahub.cli.lineage_cli.get_default_graph")
    @patch("datahub.cli.lineage_cli.DataHubClient")
    def test_capped_results_summary(self, mock_client_cls, mock_graph):
        mock_graph.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_graph.return_value.__exit__ = MagicMock(return_value=False)

        # Return exactly count results to trigger capped message
        mock_lineage = MagicMock()
        mock_lineage.get_lineage.return_value = [
            LineageResult(
                urn=f"urn:li:dataset:(urn:li:dataPlatform:snowflake,db.t{i},PROD)",
                type="DATASET",
                hops=1,
                direction="downstream",
                platform="snowflake",
                name=f"t{i}",
            )
            for i in range(5)
        ]
        mock_client_cls.return_value.lineage = mock_lineage

        runner = CliRunner()
        result = runner.invoke(
            lineage,
            [
                "--urn",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.table,PROD)",
                "--direction",
                "downstream",
                "--count",
                "5",
            ],
        )
        assert result.exit_code == 0
        assert "capped at --count 5" in result.output

    @patch("datahub.cli.lineage_cli.get_default_graph")
    @patch("datahub.cli.lineage_cli.DataHubClient")
    def test_hops_hint_in_summary(self, mock_client_cls, mock_graph):
        mock_graph.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_graph.return_value.__exit__ = MagicMock(return_value=False)

        mock_lineage = MagicMock()
        mock_lineage.get_lineage.return_value = [
            LineageResult(
                urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.t1,PROD)",
                type="DATASET",
                hops=1,
                direction="upstream",
                platform="snowflake",
                name="t1",
            )
        ]
        mock_client_cls.return_value.lineage = mock_lineage

        runner = CliRunner()
        result = runner.invoke(
            lineage,
            [
                "--urn",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.table,PROD)",
                "--direction",
                "upstream",
                "--hops",
                "1",
            ],
        )
        assert result.exit_code == 0
        assert "increase --hops" in result.output

    def test_hops_warning(self):
        runner = CliRunner()
        # hops=5 should warn but we need to mock graph to avoid connection
        # Just verify the warning text is in the code path
        result = runner.invoke(
            lineage,
            [
                "--urn",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.table,PROD)",
                "--hops",
                "5",
            ],
        )
        # Will fail due to no graph, but warning should still appear
        assert "will be treated as unlimited" in (result.output + (result.stderr or ""))

    @patch("datahub.cli.lineage_cli.get_default_graph")
    @patch("datahub.cli.lineage_cli.DataHubClient")
    def test_path_subcommand(self, mock_client_cls, mock_graph):
        mock_graph.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_graph.return_value.__exit__ = MagicMock(return_value=False)

        target_urn = "urn:li:dashboard:(looker,dashboards.1)"
        mock_lineage = MagicMock()
        mock_lineage.get_lineage.return_value = [
            LineageResult(
                urn=target_urn,
                type="DASHBOARD",
                hops=3,
                direction="downstream",
                platform=None,
                name=None,
            )
        ]
        mock_client_cls.return_value.lineage = mock_lineage

        runner = CliRunner()
        result = runner.invoke(
            lineage,
            [
                "path",
                "--from",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.table,PROD)",
                "--to",
                target_urn,
            ],
        )
        assert result.exit_code == 0
        assert "Path found" in result.output

    @patch("datahub.cli.lineage_cli.get_default_graph")
    @patch("datahub.cli.lineage_cli.DataHubClient")
    def test_path_not_found(self, mock_client_cls, mock_graph):
        mock_graph.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_graph.return_value.__exit__ = MagicMock(return_value=False)

        mock_lineage = MagicMock()
        mock_lineage.get_lineage.return_value = []
        mock_client_cls.return_value.lineage = mock_lineage

        runner = CliRunner()
        result = runner.invoke(
            lineage,
            [
                "path",
                "--from",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.a,PROD)",
                "--to",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.b,PROD)",
            ],
        )
        assert result.exit_code == 0
        assert "No path found" in result.output

    def test_path_column_with_non_dataset(self):
        runner = CliRunner()
        result = runner.invoke(
            lineage,
            [
                "path",
                "--from",
                "urn:li:chart:(looker,chart.1)",
                "--from-column",
                "col1",
                "--to",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.t,PROD)",
            ],
        )
        assert result.exit_code != 0
