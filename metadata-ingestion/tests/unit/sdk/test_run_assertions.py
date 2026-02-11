"""Tests for assertion running methods in DataHubGraph."""

from unittest.mock import patch

import pytest

from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph


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
