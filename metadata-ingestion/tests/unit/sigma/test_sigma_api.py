from typing import Dict
from unittest.mock import MagicMock, patch

import pytest
import requests

from datahub.ingestion.source.sigma.config import SigmaSourceConfig, SigmaSourceReport
from datahub.ingestion.source.sigma.data_classes import DatasetUpstream, SheetUpstream
from datahub.ingestion.source.sigma.sigma import SigmaSource
from datahub.ingestion.source.sigma.sigma_api import SigmaAPI


def _create_sigma_api() -> SigmaAPI:
    config = SigmaSourceConfig(
        client_id="test_client_id",
        client_secret="test_secret",
    )
    report = SigmaSourceReport()

    with patch.object(SigmaAPI, "_generate_token"):
        api = SigmaAPI(config=config, report=report)
    return api


class TestTokenRefreshOn401:
    def test_refreshes_token_and_retries_on_401(self) -> None:
        api = _create_sigma_api()
        api.refresh_token = "valid_refresh_token"

        unauthorized_response = MagicMock(status_code=401)
        ok_response = MagicMock(status_code=200)

        with (
            patch.object(
                api.session, "get", side_effect=[unauthorized_response, ok_response]
            ) as mock_get,
            patch.object(api, "_refresh_access_token") as mock_refresh,
        ):
            result = api._get_api_call("https://api.example.com/test")

        mock_refresh.assert_called_once()
        assert result.status_code == 200
        assert mock_get.call_count == 2

    def test_skips_refresh_when_no_refresh_token(self) -> None:
        api = _create_sigma_api()
        api.refresh_token = None

        unauthorized_response = MagicMock(status_code=401)

        with (
            patch.object(
                api.session, "get", return_value=unauthorized_response
            ) as mock_get,
            patch.object(api, "_refresh_access_token") as mock_refresh,
        ):
            result = api._get_api_call("https://api.example.com/test")

        mock_refresh.assert_not_called()
        assert result.status_code == 401
        assert mock_get.call_count == 1


def _make_element(element_id: str = "elem1", name: str = "My Chart") -> MagicMock:
    element = MagicMock(spec=["elementId", "name"])
    element.elementId = element_id
    element.name = name
    return element


def _make_workbook(workbook_id: str = "wb1", name: str = "My Workbook") -> MagicMock:
    workbook = MagicMock(spec=["workbookId", "name"])
    workbook.workbookId = workbook_id
    workbook.name = name
    return workbook


def _lineage_response(payload: dict) -> MagicMock:
    resp = MagicMock(status_code=200)
    resp.json.return_value = payload
    return resp


class TestGetElementUpstreamSources:
    def test_sheet_node_missing_element_id_is_skipped_with_warning(self) -> None:
        api = _create_sigma_api()
        element = _make_element()
        workbook = _make_workbook()

        lineage_response = MagicMock(status_code=200)
        lineage_response.json.return_value = {
            "dependencies": {
                "tgt_node": {
                    "nodeId": "tgt_node",
                    "elementId": "elem1",
                    "name": "My Chart",
                    "type": "sheet",
                },
                "bad_sheet_node": {
                    "nodeId": "bad_sheet_node",
                    # no elementId key — API contract violation
                    "name": "Upstream Without Id",
                    "type": "sheet",
                },
            },
            "edges": [
                {"source": "bad_sheet_node", "target": "tgt_node", "type": "source"}
            ],
        }

        with patch.object(api, "_get_api_call", return_value=lineage_response):
            result = api._get_element_upstream_sources(element, workbook)

        assert result == {}
        assert len(api.report.warnings) == 1
        assert "My Chart" in api.report.warnings[0].context[0]
        assert "My Workbook" in api.report.warnings[0].context[0]

    def test_unknown_node_type_is_skipped_with_warning(self) -> None:
        api = _create_sigma_api()
        element = _make_element()
        workbook = _make_workbook()

        lineage_response = MagicMock(status_code=200)
        lineage_response.json.return_value = {
            "dependencies": {
                "tgt_node": {
                    "nodeId": "tgt_node",
                    "elementId": "elem1",
                    "name": "My Chart",
                    "type": "sheet",
                },
                "unknown_node": {
                    "nodeId": "unknown_node",
                    "name": "Future Node Type",
                    "type": "formula",  # hypothetical future Sigma node type
                },
            },
            "edges": [
                {"source": "unknown_node", "target": "tgt_node", "type": "source"}
            ],
        }

        with patch.object(api, "_get_api_call", return_value=lineage_response):
            result = api._get_element_upstream_sources(element, workbook)

        assert result == {}
        assert len(api.report.warnings) == 1
        assert "My Chart" in api.report.warnings[0].context[0]
        assert "My Workbook" in api.report.warnings[0].context[0]

    def test_dataset_upstream_is_extracted(self) -> None:
        api = _create_sigma_api()
        element = _make_element()
        workbook = _make_workbook()

        with patch.object(
            api,
            "_get_api_call",
            return_value=_lineage_response(
                {
                    "dependencies": {
                        "tgt_node": {
                            "nodeId": "tgt_node",
                            "elementId": "elem1",
                            "name": "My Chart",
                            "type": "sheet",
                        },
                        "ds_node": {
                            "nodeId": "ds_node",
                            "name": "MY_DATASET",
                            "type": "dataset",
                        },
                    },
                    "edges": [
                        {"source": "ds_node", "target": "tgt_node", "type": "source"}
                    ],
                }
            ),
        ):
            result = api._get_element_upstream_sources(element, workbook)

        assert len(result) == 1
        assert "ds_node" in result
        assert isinstance(result["ds_node"], DatasetUpstream)
        assert result["ds_node"].name == "MY_DATASET"
        assert len(api.report.warnings) == 0

    def test_sheet_upstream_is_extracted(self) -> None:
        api = _create_sigma_api()
        element = _make_element()
        workbook = _make_workbook()

        with patch.object(
            api,
            "_get_api_call",
            return_value=_lineage_response(
                {
                    "dependencies": {
                        "tgt_node": {
                            "nodeId": "tgt_node",
                            "elementId": "elem1",
                            "name": "My Chart",
                            "type": "sheet",
                        },
                        "upstream_sheet": {
                            "nodeId": "upstream_sheet",
                            "elementId": "other_elem",
                            "name": "Other Chart",
                            "type": "sheet",
                        },
                    },
                    "edges": [
                        {
                            "source": "upstream_sheet",
                            "target": "tgt_node",
                            "type": "source",
                        }
                    ],
                }
            ),
        ):
            result = api._get_element_upstream_sources(element, workbook)

        assert len(result) == 1
        assert "upstream_sheet" in result
        assert isinstance(result["upstream_sheet"], SheetUpstream)
        assert result["upstream_sheet"].element_id == "other_elem"
        assert len(api.report.warnings) == 0

    def test_table_and_join_sources_are_silently_skipped(self) -> None:
        api = _create_sigma_api()
        element = _make_element()
        workbook = _make_workbook()

        with patch.object(
            api,
            "_get_api_call",
            return_value=_lineage_response(
                {
                    "dependencies": {
                        "tgt_node": {
                            "nodeId": "tgt_node",
                            "elementId": "elem1",
                            "type": "sheet",
                        },
                        "table_node": {
                            "nodeId": "table_node",
                            "name": "WAREHOUSE_TABLE",
                            "type": "table",
                        },
                        "join_node": {"nodeId": "join_node", "type": "join"},
                    },
                    "edges": [
                        {
                            "source": "table_node",
                            "target": "join_node",
                            "type": "source",
                        },
                        {
                            "source": "join_node",
                            "target": "tgt_node",
                            "type": "source",
                        },
                    ],
                }
            ),
        ):
            result = api._get_element_upstream_sources(element, workbook)

        assert result == {}
        assert len(api.report.warnings) == 0

    def test_join_pass_through_exposes_sheet_upstream(self) -> None:
        api = _create_sigma_api()
        element = _make_element()
        workbook = _make_workbook()

        with patch.object(
            api,
            "_get_api_call",
            return_value=_lineage_response(
                {
                    "dependencies": {
                        "tgt_node": {
                            "nodeId": "tgt_node",
                            "elementId": "elem1",
                            "type": "sheet",
                        },
                        "join_node": {"nodeId": "join_node", "type": "join"},
                        "sheet_A": {
                            "nodeId": "sheet_A",
                            "elementId": "upstream_elem_A",
                            "name": "Sheet A",
                            "type": "sheet",
                        },
                    },
                    "edges": [
                        {
                            "source": "sheet_A",
                            "target": "join_node",
                            "type": "source",
                        },
                        {
                            "source": "join_node",
                            "target": "tgt_node",
                            "type": "source",
                        },
                    ],
                }
            ),
        ):
            result = api._get_element_upstream_sources(element, workbook)

        assert len(result) == 1
        assert "sheet_A" in result
        assert isinstance(result["sheet_A"], SheetUpstream)
        assert result["sheet_A"].element_id == "upstream_elem_A"
        assert len(api.report.warnings) == 0

    def test_multi_hop_join_chain_exposes_sheet_upstream(self) -> None:
        """BFS walks through chained join nodes (join_1 → join_2 → sheet_B → tgt)."""
        api = _create_sigma_api()
        element = _make_element()
        workbook = _make_workbook()

        with patch.object(
            api,
            "_get_api_call",
            return_value=_lineage_response(
                {
                    "dependencies": {
                        "tgt_node": {
                            "nodeId": "tgt_node",
                            "elementId": "elem1",
                            "type": "sheet",
                        },
                        "join_1": {"nodeId": "join_1", "type": "join"},
                        "join_2": {"nodeId": "join_2", "type": "join"},
                        "sheet_B": {
                            "nodeId": "sheet_B",
                            "elementId": "upstream_elem_B",
                            "name": "Sheet B",
                            "type": "sheet",
                        },
                    },
                    "edges": [
                        {"source": "sheet_B", "target": "join_2", "type": "source"},
                        {"source": "join_2", "target": "join_1", "type": "source"},
                        {"source": "join_1", "target": "tgt_node", "type": "source"},
                    ],
                }
            ),
        ):
            result = api._get_element_upstream_sources(element, workbook)

        assert len(result) == 1
        assert "sheet_B" in result
        assert isinstance(result["sheet_B"], SheetUpstream)
        assert result["sheet_B"].element_id == "upstream_elem_B"
        assert len(api.report.warnings) == 0

    def test_unrelated_edge_is_not_attributed(self) -> None:
        """BFS must not capture sources of edges not reachable from the seed node."""
        api = _create_sigma_api()
        element = _make_element()
        workbook = _make_workbook()

        with patch.object(
            api,
            "_get_api_call",
            return_value=_lineage_response(
                {
                    "dependencies": {
                        "tgt_node": {
                            "nodeId": "tgt_node",
                            "elementId": "elem1",
                            "type": "sheet",
                        },
                        "upstream_sheet": {
                            "nodeId": "upstream_sheet",
                            "elementId": "other_elem",
                            "type": "sheet",
                        },
                        "unrelated_sheet": {
                            "nodeId": "unrelated_sheet",
                            "elementId": "yet_another_elem",
                            "type": "sheet",
                        },
                        "unrelated_target": {
                            "nodeId": "unrelated_target",
                            "type": "table",
                        },
                    },
                    "edges": [
                        # reachable: upstream_sheet → tgt_node
                        {
                            "source": "upstream_sheet",
                            "target": "tgt_node",
                            "type": "source",
                        },
                        # NOT reachable from tgt_node via reverse BFS
                        {
                            "source": "unrelated_sheet",
                            "target": "unrelated_target",
                            "type": "source",
                        },
                    ],
                }
            ),
        ):
            result = api._get_element_upstream_sources(element, workbook)

        assert len(result) == 1
        assert "upstream_sheet" in result
        assert "unrelated_sheet" not in result
        assert len(api.report.warnings) == 0

    def test_dataset_node_with_null_name_is_dropped_but_siblings_survive(
        self,
    ) -> None:
        api = _create_sigma_api()
        element = _make_element()
        workbook = _make_workbook()

        with patch.object(
            api,
            "_get_api_call",
            return_value=_lineage_response(
                {
                    "dependencies": {
                        "tgt_node": {
                            "nodeId": "tgt_node",
                            "elementId": "elem1",
                            "type": "sheet",
                        },
                        "null_name_dataset": {
                            "nodeId": "null_name_dataset",
                            "name": None,
                            "type": "dataset",
                        },
                        "good_dataset": {
                            "nodeId": "good_dataset",
                            "name": "GOOD",
                            "type": "dataset",
                        },
                    },
                    "edges": [
                        {
                            "source": "null_name_dataset",
                            "target": "tgt_node",
                            "type": "source",
                        },
                        {
                            "source": "good_dataset",
                            "target": "tgt_node",
                            "type": "source",
                        },
                    ],
                }
            ),
        ):
            result = api._get_element_upstream_sources(element, workbook)

        # good_dataset is returned; null_name_dataset triggers ValidationError → dropped
        assert "good_dataset" in result
        assert isinstance(result["good_dataset"], DatasetUpstream)
        assert result["good_dataset"].name == "GOOD"
        assert "null_name_dataset" not in result
        assert len(api.report.warnings) == 1

    def test_dependencies_reference_missing_node_produces_parse_warning(
        self,
    ) -> None:
        api = _create_sigma_api()
        element = _make_element()
        workbook = _make_workbook()

        with patch.object(
            api,
            "_get_api_call",
            return_value=_lineage_response(
                {
                    "dependencies": {
                        "tgt_node": {
                            "nodeId": "tgt_node",
                            "elementId": "elem1",
                            "type": "sheet",
                        },
                        # "missing_source_node" is referenced in edges but absent from dependencies
                    },
                    "edges": [
                        {
                            "source": "missing_source_node",
                            "target": "tgt_node",
                            "type": "source",
                        }
                    ],
                }
            ),
        ):
            result = api._get_element_upstream_sources(element, workbook)

        assert result == {}
        assert len(api.report.warnings) == 1
        assert "My Chart" in api.report.warnings[0].context[0]
        assert "My Workbook" in api.report.warnings[0].context[0]

    def test_one_malformed_edge_skips_only_itself(self) -> None:
        """A missing 'target' key on one edge must not wipe the adjacency built from valid edges."""
        api = _create_sigma_api()
        element = _make_element()
        workbook = _make_workbook()

        with patch.object(
            api,
            "_get_api_call",
            return_value=_lineage_response(
                {
                    "dependencies": {
                        "tgt_node": {
                            "nodeId": "tgt_node",
                            "elementId": "elem1",
                            "type": "sheet",
                        },
                        "good_ds": {
                            "nodeId": "good_ds",
                            "name": "GOOD",
                            "type": "dataset",
                        },
                    },
                    "edges": [
                        {"source": "good_ds", "target": "tgt_node"},
                        {"source": "orphan_ds"},  # missing "target" — malformed
                    ],
                }
            ),
        ):
            result = api._get_element_upstream_sources(element, workbook)

        assert "good_ds" in result
        assert isinstance(result["good_ds"], DatasetUpstream)
        assert len(api.report.warnings) == 1  # malformed-edge warning only

    def test_request_exception_is_reported_and_returns_empty(self) -> None:
        api = _create_sigma_api()
        element = _make_element()
        workbook = _make_workbook()

        with patch.object(
            api,
            "_get_api_call",
            side_effect=requests.exceptions.ConnectionError("connection refused"),
        ):
            result = api._get_element_upstream_sources(element, workbook)

        assert result == {}
        assert len(api.report.warnings) == 1

    @pytest.mark.parametrize("status_code", [500, 403, 400])
    def test_500_403_400_short_circuit_without_warning(self, status_code: int) -> None:
        api = _create_sigma_api()
        element = _make_element()
        workbook = _make_workbook()

        response = MagicMock(status_code=status_code)
        with patch.object(api, "_get_api_call", return_value=response):
            result = api._get_element_upstream_sources(element, workbook)

        assert result == {}
        assert len(api.report.warnings) == 0


class TestGetElementInputDetails:
    """Unit tests for SigmaSource._get_element_input_details."""

    def _make_source(self) -> SigmaSource:
        source = SigmaSource.__new__(SigmaSource)
        source.config = SigmaSourceConfig(
            client_id="x",
            client_secret="y",
        )
        source.reporter = SigmaSourceReport()
        source.dataset_upstream_urn_mapping = {}
        source.platform = "sigma"
        return source

    def _make_element_obj(
        self, element_id: str, name: str, upstream_sources: dict
    ) -> MagicMock:
        element = MagicMock(spec=["elementId", "name", "query", "upstream_sources"])
        element.elementId = element_id
        element.name = name
        element.query = None
        element.upstream_sources = upstream_sources
        return element

    def _make_workbook_obj(
        self, path: str = "Workspace", name: str = "WB"
    ) -> MagicMock:
        wb = MagicMock(spec=["workbookId", "name", "path"])
        wb.workbookId = "wb1"
        wb.name = name
        wb.path = path
        return wb

    def test_filtered_sheet_upstream_increments_counter(self) -> None:
        source = self._make_source()
        workbook = self._make_workbook_obj()

        upstream_sources: Dict = {
            "sheet_node": SheetUpstream(
                element_id="missing_elem",  # not in chart map
                name="Missing Element",
            ),
        }
        element = self._make_element_obj("elem1", "My Chart", upstream_sources)

        elementId_to_chart_urn: Dict[str, str] = {}  # missing_elem absent → None lookup

        dataset_inputs, chart_urns = source._get_element_input_details(
            element, workbook, elementId_to_chart_urn
        )

        assert dataset_inputs == {}
        assert chart_urns == []
        assert source.reporter.num_filtered_sheet_upstreams == 1

    def test_sheet_upstream_in_map_produces_chart_entry(self) -> None:
        source = self._make_source()
        workbook = self._make_workbook_obj()

        upstream_sources: Dict = {
            "sheet_node": SheetUpstream(
                element_id="upstream_elem",
                name="Upstream Element",
            ),
        }
        element = self._make_element_obj("elem1", "My Chart", upstream_sources)
        elementId_to_chart_urn = {"upstream_elem": "urn:li:chart:(sigma,upstream_elem)"}

        dataset_inputs, chart_urns = source._get_element_input_details(
            element, workbook, elementId_to_chart_urn
        )

        assert dataset_inputs == {}
        assert chart_urns == ["urn:li:chart:(sigma,upstream_elem)"]
        assert source.reporter.num_filtered_sheet_upstreams == 0

    def test_duplicate_sheet_nodeids_same_element_produce_one_edge(self) -> None:
        """Two distinct nodeIds pointing to the same elementId collapse to one inputEdge."""
        source = self._make_source()
        workbook = self._make_workbook_obj()

        upstream_sources: Dict = {
            "node_a": SheetUpstream(element_id="upstream_elem", name="Node A"),
            "node_b": SheetUpstream(element_id="upstream_elem", name="Node B"),
        }
        element = self._make_element_obj("elem1", "My Chart", upstream_sources)
        elementId_to_chart_urn = {"upstream_elem": "urn:li:chart:(sigma,upstream_elem)"}

        dataset_inputs, chart_urns = source._get_element_input_details(
            element, workbook, elementId_to_chart_urn
        )

        assert dataset_inputs == {}
        assert chart_urns == ["urn:li:chart:(sigma,upstream_elem)"]
