"""Tests for assertion running methods in DataHubGraph."""

from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.sdk.assertion_client import AssertionClient
from datahub.utilities.graphql_query_adapter import minify_graphql_query


@pytest.fixture
def mock_graph():
    """Create a mocked DataHubGraph instance."""
    with patch("datahub.emitter.rest_emitter.DataHubRestEmitter.test_connection"):
        graph = DataHubGraph(DatahubClientConfig(server="http://fake-domain.local"))
        return graph


class TestRunAssertion:
    """Tests for the run_assertion method."""

    def test_run_assertion_minimal(self, mock_graph):
        """Test run_assertion with only required parameters."""
        with patch.object(mock_graph, "execute_graphql") as mock_execute:
            mock_execute.return_value = {
                "runAssertion": {
                    "type": "SUCCESS",
                    "nativeResults": [],
                }
            }

            result = mock_graph.run_assertion(urn="urn:li:assertion:test123")

            assert result is not None
            mock_execute.assert_called_once()

            # Verify variables - should only contain assertionUrn and parameters
            variables = mock_execute.call_args.kwargs["variables"]
            assert variables["assertionUrn"] == "urn:li:assertion:test123"
            assert variables["parameters"] == []
            assert "saveResult" not in variables
            assert "async" not in variables

    def test_run_assertion_with_save_result(self, mock_graph):
        """Test run_assertion with save_result parameter."""
        with patch.object(mock_graph, "execute_graphql") as mock_execute:
            mock_execute.return_value = {"runAssertion": {"type": "SUCCESS"}}

            mock_graph.run_assertion(urn="urn:li:assertion:test123", save_result=True)

            variables = mock_execute.call_args.kwargs["variables"]
            assert variables["saveResult"] is True

    def test_run_assertion_with_async_flag(self, mock_graph):
        """Test run_assertion with async_flag parameter."""
        with patch.object(mock_graph, "execute_graphql") as mock_execute:
            mock_execute.return_value = {"runAssertion": {"type": "SUCCESS"}}

            mock_graph.run_assertion(urn="urn:li:assertion:test123", async_flag=True)

            variables = mock_execute.call_args.kwargs["variables"]
            assert variables["async"] is True

    def test_run_assertion_with_parameters(self, mock_graph):
        """Test run_assertion with dynamic parameters."""
        with patch.object(mock_graph, "execute_graphql") as mock_execute:
            mock_execute.return_value = {"runAssertion": {"type": "SUCCESS"}}

            mock_graph.run_assertion(
                urn="urn:li:assertion:test123",
                parameters={"threshold": "100", "date": "2024-01-01"},
            )

            variables = mock_execute.call_args.kwargs["variables"]
            assert len(variables["parameters"]) == 2
            param_dict = {p["key"]: p["value"] for p in variables["parameters"]}
            assert param_dict["threshold"] == "100"
            assert param_dict["date"] == "2024-01-01"


class TestRunAssertions:
    """Tests for the run_assertions method."""

    def test_run_assertions_minimal(self, mock_graph):
        """Test run_assertions with only required parameters."""
        with patch.object(mock_graph, "execute_graphql") as mock_execute:
            mock_execute.return_value = {
                "runAssertions": {
                    "passingCount": 2,
                    "failingCount": 0,
                }
            }

            result = mock_graph.run_assertions(
                urns=["urn:li:assertion:test1", "urn:li:assertion:test2"]
            )

            assert result is not None
            mock_execute.assert_called_once()

            # Verify variables
            variables = mock_execute.call_args.kwargs["variables"]
            assert variables["assertionUrns"] == [
                "urn:li:assertion:test1",
                "urn:li:assertion:test2",
            ]
            assert variables["parameters"] == []
            assert "saveResult" not in variables
            assert "async" not in variables

    def test_run_assertions_with_save_result(self, mock_graph):
        """Test run_assertions with save_result parameter."""
        with patch.object(mock_graph, "execute_graphql") as mock_execute:
            mock_execute.return_value = {"runAssertions": {"passingCount": 1}}

            mock_graph.run_assertions(
                urns=["urn:li:assertion:test1"], save_result=False
            )

            variables = mock_execute.call_args.kwargs["variables"]
            assert variables["saveResult"] is False

    def test_run_assertions_with_async_flag(self, mock_graph):
        """Test run_assertions with async_flag parameter."""
        with patch.object(mock_graph, "execute_graphql") as mock_execute:
            mock_execute.return_value = {"runAssertions": {"passingCount": 1}}

            mock_graph.run_assertions(urns=["urn:li:assertion:test1"], async_flag=True)

            variables = mock_execute.call_args.kwargs["variables"]
            assert variables["async"] is True

    def test_run_assertions_with_parameters(self, mock_graph):
        """Test run_assertions with dynamic parameters."""
        with patch.object(mock_graph, "execute_graphql") as mock_execute:
            mock_execute.return_value = {"runAssertions": {"passingCount": 1}}

            mock_graph.run_assertions(
                urns=["urn:li:assertion:test1"], parameters={"env": "prod"}
            )

            variables = mock_execute.call_args.kwargs["variables"]
            assert len(variables["parameters"]) == 1
            assert variables["parameters"][0]["key"] == "env"
            assert variables["parameters"][0]["value"] == "prod"


class TestRunAssertionsForAsset:
    """Tests for the run_assertions_for_asset method."""

    def test_run_assertions_for_asset_minimal(self, mock_graph):
        """Test run_assertions_for_asset with only required parameters."""
        with patch.object(mock_graph, "execute_graphql") as mock_execute:
            mock_execute.return_value = {
                "runAssertionsForAsset": {
                    "passingCount": 3,
                    "failingCount": 1,
                }
            }

            result = mock_graph.run_assertions_for_asset(
                urn="urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)"
            )

            assert result is not None
            mock_execute.assert_called_once()

            # Verify variables
            variables = mock_execute.call_args.kwargs["variables"]
            assert (
                variables["assetUrn"]
                == "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)"
            )
            assert variables["parameters"] == []
            assert "tagUrns" not in variables
            assert "async" not in variables

    def test_run_assertions_for_asset_with_tag_urns(self, mock_graph):
        """Test run_assertions_for_asset with tag_urns parameter."""
        with patch.object(mock_graph, "execute_graphql") as mock_execute:
            mock_execute.return_value = {"runAssertionsForAsset": {"passingCount": 2}}

            mock_graph.run_assertions_for_asset(
                urn="urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)",
                tag_urns=["urn:li:tag:critical"],
            )

            variables = mock_execute.call_args.kwargs["variables"]
            assert variables["tagUrns"] == ["urn:li:tag:critical"]

    def test_run_assertions_for_asset_with_async_flag(self, mock_graph):
        """Test run_assertions_for_asset with async_flag parameter."""
        with patch.object(mock_graph, "execute_graphql") as mock_execute:
            mock_execute.return_value = {"runAssertionsForAsset": {"passingCount": 1}}

            mock_graph.run_assertions_for_asset(
                urn="urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)",
                async_flag=True,
            )

            variables = mock_execute.call_args.kwargs["variables"]
            assert variables["async"] is True

    def test_run_assertions_for_asset_with_parameters(self, mock_graph):
        """Test run_assertions_for_asset with dynamic parameters."""
        with patch.object(mock_graph, "execute_graphql") as mock_execute:
            mock_execute.return_value = {"runAssertionsForAsset": {"passingCount": 1}}

            mock_graph.run_assertions_for_asset(
                urn="urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)",
                parameters={"threshold": "50", "window": "24h"},
            )

            variables = mock_execute.call_args.kwargs["variables"]
            assert len(variables["parameters"]) == 2
            param_dict = {p["key"]: p["value"] for p in variables["parameters"]}
            assert param_dict["threshold"] == "50"
            assert param_dict["window"] == "24h"


class TestAssertionResultFragmentIncludesSeverity:
    """The shared assertionResult fragment must expose severity to read callers."""

    def test_run_assertion_query_includes_severity(self, mock_graph):
        with patch.object(mock_graph, "execute_graphql") as mock_execute:
            mock_execute.return_value = {"runAssertion": {"type": "SUCCESS"}}
            mock_graph.run_assertion(urn="urn:li:assertion:test123")

            query = mock_execute.call_args.kwargs["query"]
            assert "fragment assertionResult on AssertionResult" in query
            assert "severity" in query

    def test_run_assertions_query_includes_severity(self, mock_graph):
        with patch.object(mock_graph, "execute_graphql") as mock_execute:
            mock_execute.return_value = {"runAssertions": {"passingCount": 0}}
            mock_graph.run_assertions(urns=["urn:li:assertion:test1"])

            query = mock_execute.call_args.kwargs["query"]
            assert "fragment assertionResult on AssertionResult" in query
            assert "severity" in query

    def test_run_assertions_for_asset_query_includes_severity(self, mock_graph):
        with patch.object(mock_graph, "execute_graphql") as mock_execute:
            mock_execute.return_value = {"runAssertionsForAsset": {"passingCount": 0}}
            mock_graph.run_assertions_for_asset(
                urn="urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)"
            )

            query = mock_execute.call_args.kwargs["query"]
            assert "fragment assertionResult on AssertionResult" in query
            assert "severity" in query


class TestReportAssertionResult:
    """Tests for the report_assertion_result method."""

    def test_report_assertion_result_without_severity(self, mock_graph):
        with patch.object(mock_graph, "execute_graphql") as mock_execute:
            mock_execute.return_value = {"reportAssertionResult": True}

            mock_graph.report_assertion_result(
                urn="urn:li:assertion:test123",
                timestamp_millis=1700000000000,
                type="SUCCESS",
            )

            query = mock_execute.call_args.kwargs["query"]
            variables = mock_execute.call_args.kwargs["variables"]
            assert "$severity: AssertionResultSeverity" in query
            assert "severity: $severity" in query
            assert variables["severity"] is None

    def test_report_assertion_result_with_severity(self, mock_graph):
        with patch.object(mock_graph, "execute_graphql") as mock_execute:
            mock_execute.return_value = {"reportAssertionResult": True}

            mock_graph.report_assertion_result(
                urn="urn:li:assertion:test123",
                timestamp_millis=1700000000000,
                type="FAILURE",
                severity="HIGH",
            )

            variables = mock_execute.call_args.kwargs["variables"]
            assert variables["severity"] == "HIGH"
            assert variables["type"] == "FAILURE"

    def test_report_assertion_result_omits_unset_numeric_fields(self, mock_graph):
        # Servers that predate the numeric fields reject unknown input fields, so
        # without them the request must stay exactly as it was before.
        expected_query = """
            mutation reportAssertionResult(
                $assertionUrn: String!,
                $timestampMillis: Long!,
                $type: AssertionResultType!,
                $properties: [StringMapEntryInput!],
                $externalUrl: String,
                $error: AssertionResultErrorInput,
                $severity: AssertionResultSeverity,
            ) {
                reportAssertionResult(urn: $assertionUrn, result: {
                    timestampMillis: $timestampMillis
                    type: $type
                    properties: $properties
                    externalUrl: $externalUrl
                    error: $error
                    severity: $severity
                })
            }
        """
        with patch.object(mock_graph, "execute_graphql") as mock_execute:
            mock_execute.return_value = {"reportAssertionResult": True}

            mock_graph.report_assertion_result(
                urn="urn:li:assertion:test123",
                timestamp_millis=1700000000000,
                type="SUCCESS",
            )

            assert minify_graphql_query(
                mock_execute.call_args.kwargs["query"]
            ) == minify_graphql_query(expected_query)
            assert mock_execute.call_args.kwargs["variables"] == {
                "assertionUrn": "urn:li:assertion:test123",
                "timestampMillis": 1700000000000,
                "type": "SUCCESS",
                "properties": None,
                "externalUrl": None,
                "error": None,
                "severity": None,
            }

    def test_report_assertion_result_includes_only_set_numeric_fields(self, mock_graph):
        with patch.object(mock_graph, "execute_graphql") as mock_execute:
            mock_execute.return_value = {"reportAssertionResult": True}

            mock_graph.report_assertion_result(
                urn="urn:li:assertion:test123",
                timestamp_millis=1700000000000,
                type="FAILURE",
                actual_agg_value=0.0,
                missing_count=120,
            )

            query = mock_execute.call_args.kwargs["query"]
            variables = mock_execute.call_args.kwargs["variables"]
            assert "$actualAggValue: Float," in query
            assert "actualAggValue: $actualAggValue" in query
            assert "$missingCount: Long," in query
            assert "missingCount: $missingCount" in query
            assert "rowCount" not in query
            assert "unexpectedCount" not in query
            assert variables["actualAggValue"] == 0.0
            assert variables["missingCount"] == 120
            assert "rowCount" not in variables
            assert "unexpectedCount" not in variables

    def test_report_assertion_result_coerces_numpy_values(self, mock_graph):
        np = pytest.importorskip("numpy")
        with patch.object(mock_graph, "execute_graphql") as mock_execute:
            mock_execute.return_value = {"reportAssertionResult": True}

            mock_graph.report_assertion_result(
                urn="urn:li:assertion:test123",
                timestamp_millis=1700000000000,
                type="FAILURE",
                actual_agg_value=np.float64(0.12),
                missing_count=np.int64(120),
            )

            variables = mock_execute.call_args.kwargs["variables"]
            assert type(variables["actualAggValue"]) is float
            assert type(variables["missingCount"]) is int
            assert variables["missingCount"] == 120

    @pytest.mark.parametrize(
        "kwargs, error",
        [
            ({"actual_agg_value": float("nan")}, ValueError),
            ({"actual_agg_value": float("inf")}, ValueError),
            ({"actual_agg_value": 1e39}, ValueError),
            ({"row_count": True}, TypeError),
            ({"missing_count": 1.5}, TypeError),
        ],
    )
    def test_report_assertion_result_rejects_invalid_numeric_values(
        self, mock_graph, kwargs, error
    ):
        with patch.object(mock_graph, "execute_graphql") as mock_execute:
            with pytest.raises(error):
                mock_graph.report_assertion_result(
                    urn="urn:li:assertion:test123",
                    timestamp_millis=1700000000000,
                    type="FAILURE",
                    **kwargs,
                )
            mock_execute.assert_not_called()


class TestAssertionClientReportAssertionResult:
    def test_forwards_numeric_fields_to_graph(self):
        graph = MagicMock()
        graph.report_assertion_result.return_value = True
        client = MagicMock()
        client._graph = graph

        AssertionClient(client).report_assertion_result(
            urn="urn:li:assertion:test123",
            timestamp_millis=1700000000000,
            type="SUCCESS",
            actual_agg_value=1.5,
            row_count=1000,
            unexpected_count=3,
            missing_count=4,
        )

        kwargs = graph.report_assertion_result.call_args.kwargs
        assert kwargs["actual_agg_value"] == 1.5
        assert kwargs["row_count"] == 1000
        assert kwargs["unexpected_count"] == 3
        assert kwargs["missing_count"] == 4
