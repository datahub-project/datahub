import datetime as _dt
from contextlib import ExitStack, contextmanager
from typing import Any, Dict, Iterator, List, Optional
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
import requests
from pydantic import ValidationError
from requests.adapters import HTTPAdapter

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sigma.config import (
    Constant,
    SigmaSourceConfig,
    SigmaSourceReport,
)
from datahub.ingestion.source.sigma.connection_registry import (
    SigmaConnectionRecord,
    SigmaConnectionRegistry,
)
from datahub.ingestion.source.sigma.data_classes import (
    DatasetUpstream,
    Element,
    File,
    SheetUpstream,
    SigmaDataModel,
    SigmaDataModelElement,
    SigmaDataset,
    WarehouseTableUpstream,
    Workbook,
    WorkbookLineageTableEntry,
    Workspace,
)
from datahub.ingestion.source.sigma.sigma import SigmaSource, _WorkbookWarehouseIndex
from datahub.ingestion.source.sigma.sigma_api import (
    _DATASET_SOURCES_NOT_FOUND_WARN_THRESHOLD,
    _MAX_ERROR_BODY_CHARS,
    SigmaAPI,
)
from datahub.metadata.schema_classes import (
    ChartInfoClass,
    OwnershipClass,
    SchemaMetadataClass,
)


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


def _real_workbook() -> Workbook:
    """A Workbook the lineage-pattern check can read, unlike a MagicMock."""
    return Workbook(
        workbookId="wb-1",
        name="Workbook",
        ownerId="u",
        createdBy="u",
        updatedBy="u",
        createdAt=_dt.datetime(2024, 1, 1, tzinfo=_dt.timezone.utc),
        updatedAt=_dt.datetime(2024, 1, 2, tzinfo=_dt.timezone.utc),
        url="https://sigma.example/wb",
        path="Workspace/Workbook",
        latestVersion=1,
    )


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

    def test_table_node_without_inode_prefix_increments_skip_counter(self) -> None:
        """A type=table node whose nodeId lacks the 'inode-' prefix is skipped."""
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
        assert api.report.chart_warehouse_table_node_skipped == 1
        assert len(api.report.warnings) == 0

    def test_table_node_with_inode_prefix_creates_warehouse_table_upstream(
        self,
    ) -> None:
        """A type=table node with 'inode-{urlId}' format is stored as WarehouseTableUpstream."""
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
                        "inode-abc123": {
                            "nodeId": "inode-abc123",
                            "name": "ORDERS",
                            "type": "table",
                        },
                    },
                    "edges": [
                        {
                            "source": "inode-abc123",
                            "target": "tgt_node",
                            "type": "source",
                        },
                    ],
                }
            ),
        ):
            result = api._get_element_upstream_sources(element, workbook)

        assert "inode-abc123" in result
        upstream = result["inode-abc123"]
        assert isinstance(upstream, WarehouseTableUpstream)
        assert upstream.url_id == "abc123"
        assert upstream.name == "ORDERS"
        assert api.report.chart_warehouse_table_node_skipped == 0

    def test_table_node_with_empty_url_id_increments_skip_counter(self) -> None:
        """A type=table node 'inode-' with nothing after the prefix is skipped."""
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
                        "inode-": {
                            "nodeId": "inode-",
                            "name": "ORDERS",
                            "type": "table",
                        },
                    },
                    "edges": [
                        {"source": "inode-", "target": "tgt_node", "type": "source"},
                    ],
                }
            ),
        ):
            result = api._get_element_upstream_sources(element, workbook)

        assert result == {}
        assert api.report.chart_warehouse_table_node_skipped == 1

    def test_table_node_missing_name_increments_skip_counter(self) -> None:
        """A type=table node with no 'name' field is skipped with skip counter incremented."""
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
                        "inode-abc999": {
                            "nodeId": "inode-abc999",
                            # no "name" key
                            "type": "table",
                        },
                    },
                    "edges": [
                        {
                            "source": "inode-abc999",
                            "target": "tgt_node",
                            "type": "source",
                        },
                    ],
                }
            ),
        ):
            result = api._get_element_upstream_sources(element, workbook)

        assert result == {}
        assert api.report.chart_warehouse_table_node_skipped == 1

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

    def test_dataset_node_with_null_name_parses_with_siblings(
        self,
    ) -> None:
        # ``DatasetUpstream.name`` is ``Optional[str]`` so a null-name node
        # no longer trips ValidationError at parse time. Both nodes land
        # in the upstream map; the chart-input path in ``sigma.py`` is
        # responsible for the SQL-correlated edge and bumping
        # ``chart_dataset_upstream_name_missing``.
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

        assert "good_dataset" in result
        assert isinstance(result["good_dataset"], DatasetUpstream)
        assert result["good_dataset"].name == "GOOD"
        assert "null_name_dataset" in result
        assert isinstance(result["null_name_dataset"], DatasetUpstream)
        assert result["null_name_dataset"].name is None
        assert len(api.report.warnings) == 0

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

    # --- WarehouseTableUpstream entity-level edge (direct BFS type=table nodes) ---

    def _make_source_with_registry(
        self, connection_id: str = "conn-1", platform: str = "snowflake"
    ) -> SigmaSource:
        source = self._make_source()
        record = SigmaConnectionRecord(
            connection_id=connection_id,
            name="Test Connection",
            sigma_type="Snowflake",
            datahub_platform=platform,
            is_mappable=True,
        )
        source.connection_registry = SigmaConnectionRegistry(
            by_id={connection_id: record}
        )
        source._warned_unvalidated_platforms = set()  # type: ignore[misc]
        source._no_platform_map_conn_ids = set()  # type: ignore[misc]
        source._ambiguous_table_name_warned = set()  # type: ignore[misc]
        return source

    def test_warehouse_table_upstream_emits_entity_level_input(self) -> None:
        """WarehouseTableUpstream resolves to a Dataset URN via urlId-based lookup."""
        source = self._make_source_with_registry()
        workbook = self._make_workbook_obj()

        warehouse_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.public.orders,PROD)"
        )
        wb_warehouse_table_index = _WorkbookWarehouseIndex(
            by_url_id={"abc123": warehouse_urn},
            by_name={"ORDERS": [warehouse_urn]},
        )
        upstream_sources: Dict = {
            "inode-abc123": WarehouseTableUpstream(
                type="table", url_id="abc123", name="ORDERS"
            ),
        }
        element = self._make_element_obj("elem1", "My Chart", upstream_sources)

        dataset_inputs, chart_urns = source._get_element_input_details(
            element, workbook, {}, wb_warehouse_table_index
        )

        assert len(dataset_inputs) == 1
        assert chart_urns == []
        assert source.reporter.chart_warehouse_upstream_emitted == 1
        assert source.reporter.chart_warehouse_table_name_unmatched == 0
        assert next(iter(dataset_inputs)) == warehouse_urn

    def test_warehouse_table_upstream_url_id_diverges_name_resolves(self) -> None:
        """BFS url_id differs from workbook lineage urlId; name-based fallback resolves.

        This is the Fivetran case: BFS nodeId carries a urlId that diverges
        from the urlId returned by /files/{inodeId} for cross-workbook tables.
        by_url_id misses, so the resolver falls back to by_name (single candidate).
        """
        source = self._make_source_with_registry()
        workbook = self._make_workbook_obj()

        warehouse_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,"
            "db.schema.stg_fivetran_log__incremental_mar,PROD)"
        )
        # by_url_id uses the authoritative /files urlId; BFS urlId is different.
        wb_warehouse_table_index = _WorkbookWarehouseIndex(
            by_url_id={"54d35z7J": warehouse_urn},  # authoritative /files urlId
            by_name={"STG_FIVETRAN_LOG__INCREMENTAL_MAR": [warehouse_urn]},
        )
        # BFS urlId ("13asMaOM...") diverges from the /files urlId ("54d35z7J..."),
        # so by_url_id misses; by_name fallback resolves the single candidate.
        upstream_sources: Dict = {
            "inode-13asMaOMeP3ltn3QWZxUl7": WarehouseTableUpstream(
                type="table",
                url_id="13asMaOMeP3ltn3QWZxUl7",
                name="STG_FIVETRAN_LOG__INCREMENTAL_MAR",
            ),
        }
        element = self._make_element_obj("elem1", "Total MAR", upstream_sources)

        dataset_inputs, chart_urns = source._get_element_input_details(
            element, workbook, {}, wb_warehouse_table_index
        )

        assert next(iter(dataset_inputs)) == warehouse_urn
        assert source.reporter.chart_warehouse_upstream_emitted == 1
        assert source.reporter.chart_warehouse_table_name_unmatched == 0

    def test_warehouse_table_upstream_unresolvable_increments_counter(
        self,
    ) -> None:
        """WarehouseTableUpstream not in either index bumps unmatched counter."""
        source = self._make_source_with_registry()
        workbook = self._make_workbook_obj()

        wb_warehouse_table_index = _WorkbookWarehouseIndex(by_url_id={}, by_name={})
        upstream_sources: Dict = {
            "inode-unknown": WarehouseTableUpstream(
                type="table", url_id="unknown", name="MISSING_TABLE"
            ),
        }
        element = self._make_element_obj("elem1", "My Chart", upstream_sources)

        dataset_inputs, chart_urns = source._get_element_input_details(
            element, workbook, {}, wb_warehouse_table_index
        )

        assert dataset_inputs == {}
        assert source.reporter.chart_warehouse_table_name_unmatched == 1
        assert source.reporter.chart_warehouse_upstream_emitted == 0

    def test_warehouse_table_upstream_dedup_two_nodes_same_table(self) -> None:
        """Two BFS table nodes with the same name produce one dataset_inputs entry."""
        source = self._make_source_with_registry()
        workbook = self._make_workbook_obj()

        warehouse_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.public.orders,PROD)"
        )
        wb_warehouse_table_index = _WorkbookWarehouseIndex(
            by_url_id={"abc1": warehouse_urn, "abc2": warehouse_urn},
            by_name={"ORDERS": [warehouse_urn]},
        )
        upstream_sources: Dict = {
            "inode-abc1": WarehouseTableUpstream(
                type="table", url_id="abc1", name="ORDERS"
            ),
            "inode-abc2": WarehouseTableUpstream(
                type="table", url_id="abc2", name="ORDERS"
            ),
        }
        element = self._make_element_obj("elem1", "My Chart", upstream_sources)

        dataset_inputs, _ = source._get_element_input_details(
            element, workbook, {}, wb_warehouse_table_index
        )

        # Both nodes share the same name → same URN → one dataset_inputs entry.
        assert len(dataset_inputs) == 1
        assert source.reporter.chart_warehouse_upstream_emitted == 1

    def test_warehouse_table_upstream_name_ambiguous_skips(self) -> None:
        """Multiple URNs share same table name and url_id misses by_url_id -> skip, bump ambiguous."""
        source = self._make_source_with_registry()
        workbook = self._make_workbook_obj()

        urn_a = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.s.orders_copy_a,PROD)"
        urn_b = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.s.orders_copy_b,PROD)"
        # Two tables share the same short name; BFS urlId not in by_url_id.
        wb_warehouse_table_index = _WorkbookWarehouseIndex(
            by_url_id={},  # BFS urlId absent -> falls back to name lookup
            by_name={"ORDERS": [urn_a, urn_b]},
        )
        upstream_sources: Dict = {
            "inode-abc123": WarehouseTableUpstream(
                type="table", url_id="abc123", name="ORDERS"
            ),
        }
        element = self._make_element_obj("elem1", "My Chart", upstream_sources)

        dataset_inputs, _ = source._get_element_input_details(
            element, workbook, {}, wb_warehouse_table_index
        )

        assert dataset_inputs == {}
        assert source.reporter.chart_warehouse_upstream_emitted == 0
        assert source.reporter.chart_warehouse_table_name_ambiguous == 1

    def test_warehouse_table_upstream_url_id_wins_over_name_collision(self) -> None:
        """urlId-based hit resolves even when by_name would be ambiguous."""
        source = self._make_source_with_registry()
        workbook = self._make_workbook_obj()

        urn_a = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.s.orders_copy_a,PROD)"
        urn_b = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.s.orders_copy_b,PROD)"
        # by_url_id maps the BFS urlId to urn_a; by_name has two entries (ambiguous).
        wb_warehouse_table_index = _WorkbookWarehouseIndex(
            by_url_id={"abc123": urn_a},
            by_name={"ORDERS": [urn_a, urn_b]},
        )
        upstream_sources: Dict = {
            "inode-abc123": WarehouseTableUpstream(
                type="table", url_id="abc123", name="ORDERS"
            ),
        }
        element = self._make_element_obj("elem1", "My Chart", upstream_sources)

        dataset_inputs, _ = source._get_element_input_details(
            element, workbook, {}, wb_warehouse_table_index
        )

        assert len(dataset_inputs) == 1
        assert next(iter(dataset_inputs)) == urn_a
        assert source.reporter.chart_warehouse_upstream_emitted == 1
        assert source.reporter.chart_warehouse_table_name_ambiguous == 0

    def test_warehouse_table_upstream_overlap_guard(self) -> None:
        """BFS and SQL-parser both resolve the same URN -> one dataset_inputs entry."""
        source = self._make_source_with_registry()
        workbook = self._make_workbook_obj()

        warehouse_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.public.orders,PROD)"
        )
        wb_warehouse_table_index = _WorkbookWarehouseIndex(
            by_url_id={"abc123": warehouse_urn},
            by_name={"ORDERS": [warehouse_urn]},
        )
        upstream_sources: Dict = {
            "inode-abc123": WarehouseTableUpstream(
                type="table", url_id="abc123", name="ORDERS"
            ),
        }
        element = self._make_element_obj("elem1", "My Chart", upstream_sources)
        # Simulate SQL parser also returning the same warehouse URN.
        element.query = "SELECT id FROM orders"

        # Patch create_lineage_sql_parsed_result to return warehouse_urn directly.
        import unittest.mock as mock

        with mock.patch(
            "datahub.ingestion.source.sigma.sigma.create_lineage_sql_parsed_result",
            return_value=[warehouse_urn],
        ):
            dataset_inputs, _ = source._get_element_input_details(
                element, workbook, {}, wb_warehouse_table_index
            )

        # BFS and SQL parser both resolved to the same URN -> deduplicated to one entry.
        assert len(dataset_inputs) == 1
        assert warehouse_urn in dataset_inputs
        assert source.reporter.chart_warehouse_upstream_emitted == 1

    def test_warehouse_table_upstream_none_index_skips_silently(self) -> None:
        """When wb_warehouse_table_index is None the upstream is silently skipped."""
        source = self._make_source_with_registry()
        workbook = self._make_workbook_obj()

        upstream_sources: Dict = {
            "inode-abc123": WarehouseTableUpstream(
                type="table", url_id="abc123", name="ORDERS"
            ),
        }
        element = self._make_element_obj("elem1", "My Chart", upstream_sources)

        dataset_inputs, chart_urns = source._get_element_input_details(
            element,
            workbook,
            {},  # elementId_to_chart_urn; wb_warehouse_table_index not passed -> defaults to None
        )

        assert dataset_inputs == {}
        assert source.reporter.chart_warehouse_upstream_emitted == 0
        assert source.reporter.chart_warehouse_table_name_unmatched == 0


class TestAssembleDataModelFileMetaFallback:
    """Exercise the ``_assemble_data_model`` fallback that fills
    ``workspaceId``/``path``/``badge``/``urlId`` from ``/files`` metadata
    when the ``/dataModels`` payload omits them. Integration fixtures
    happen to populate every field on ``/dataModels`` directly, so this
    branch would otherwise be untested. (See review #3.)
    """

    def _dm(self, **overrides: object) -> SigmaDataModel:
        base: Dict[str, object] = {
            "dataModelId": "dm-uuid-1",
            "name": "My DM",
            "createdAt": _dt.datetime(2024, 1, 1, tzinfo=_dt.timezone.utc),
            "updatedAt": _dt.datetime(2024, 1, 2, tzinfo=_dt.timezone.utc),
        }
        base.update(overrides)
        return SigmaDataModel.model_validate(base)

    def _file(self, **overrides: object) -> File:
        base: Dict[str, object] = {
            "id": "file-1",
            "name": "My DM",
            "parentId": "folder-1",
            "path": "Acryl Data/Marketing",
            "type": "data-model",
            "workspaceId": "ws-from-file",
            "urlId": "urlid-from-file",
            "badge": "certified",
        }
        base.update(overrides)
        return File.model_validate(base)

    @contextmanager
    def _patched_fetches(self, api: SigmaAPI) -> Iterator[None]:
        # Short-circuit the three per-DM fetches; we only care about the
        # fill-from-file-meta block. Nested context managers guarantee
        # cleanup even if a test assertion raises, unlike ``patch.stopall``.
        with (
            patch.object(api, "_get_data_model_elements", return_value=[]),
            patch.object(api, "_get_data_model_columns", return_value=[]),
            patch.object(api, "_get_data_model_lineage_entries", return_value=[]),
        ):
            yield

    def test_fills_missing_fields_from_file_meta(self) -> None:
        api = _create_sigma_api()
        dm = self._dm(workspaceId=None, path=None, urlId=None, badge=None)

        with self._patched_fetches(api):
            api._assemble_data_model(
                dm, self._file(), resolved_workspace_id="ws-from-file"
            )

        assert dm.workspaceId == "ws-from-file"
        assert dm.path == "Acryl Data/Marketing"
        assert dm.urlId == "urlid-from-file"
        assert dm.badge == "certified"

    def test_secondary_file_meta_fields_do_not_override_dm_payload(self) -> None:
        """``path`` / ``badge`` / ``urlId`` remain /dataModels-preferred:
        a future vendor change that populates them directly on the DM
        payload must not be silently overwritten by ``/files``.
        ``workspaceId`` is handled at the caller level now
        (see ``test_resolved_workspace_id_overrides_dm_payload``)."""
        api = _create_sigma_api()
        dm = self._dm(
            workspaceId="ws-from-dm",
            path="DM Path",
            urlId="urlid-from-dm",
            badge="dm-badge",
        )

        with self._patched_fetches(api):
            api._assemble_data_model(dm, self._file())

        assert dm.workspaceId == "ws-from-dm"
        assert dm.path == "DM Path"
        assert dm.urlId == "urlid-from-dm"
        assert dm.badge == "dm-badge"

    def test_resolved_workspace_id_overrides_dm_payload(self) -> None:
        """C2 regression: the caller in ``get_data_models`` resolves
        ``(file_meta.workspaceId, data_model.workspaceId)`` into a
        single "authoritative" workspace -- with /files preferred when
        it disagrees with the /dataModels payload -- and then uses
        that same id for filtering and rendering. If
        ``_assemble_data_model`` silently kept the DM-payload workspace
        when the caller passed a different ``resolved_workspace_id``,
        filtering would be done under workspace B while rendering /
        browse paths / per-workspace counters keyed off workspace A.
        """
        api = _create_sigma_api()
        dm = self._dm(workspaceId="ws-from-dm-payload")

        with self._patched_fetches(api):
            api._assemble_data_model(
                dm,
                self._file(workspaceId="ws-from-file"),
                resolved_workspace_id="ws-from-file",
            )

        assert dm.workspaceId == "ws-from-file", (
            "caller-resolved workspace must override the DM payload so "
            "filtering and rendering agree on a single workspace per DM"
        )

    def test_none_file_meta_is_safe(self) -> None:
        api = _create_sigma_api()
        dm = self._dm(workspaceId=None, path=None, urlId=None, badge=None)

        with self._patched_fetches(api):
            api._assemble_data_model(dm, None)

        assert dm.workspaceId is None
        assert dm.path is None
        assert dm.urlId is None
        assert dm.badge is None


class TestSourceDmElementNamesEdgeCases:
    """Guards the guards: the ``elif entry_type == "data-model"`` branch in
    ``_assemble_data_model`` rejects entries with a non-string dataModelId or
    a whitespace-only name.  Without these checks, a malformed API response
    could silently produce empty-string keys or blank element names in
    ``source_dm_element_names``.
    """

    def _dm(self) -> SigmaDataModel:
        return SigmaDataModel.model_validate(
            {
                "dataModelId": "dm-uuid-1",
                "name": "My DM",
                "createdAt": _dt.datetime(2024, 1, 1, tzinfo=_dt.timezone.utc),
                "updatedAt": _dt.datetime(2024, 1, 2, tzinfo=_dt.timezone.utc),
            }
        )

    def _assemble_with_entries(
        self, lineage_entries: List[Dict[str, Any]]
    ) -> SigmaDataModel:
        api = _create_sigma_api()
        dm = self._dm()
        with (
            patch.object(api, "_get_data_model_elements", return_value=[]),
            patch.object(api, "_get_data_model_columns", return_value=[]),
            patch.object(
                api, "_get_data_model_lineage_entries", return_value=lineage_entries
            ),
        ):
            api._assemble_data_model(dm, None)
        return dm

    def test_non_string_src_dm_id_is_ignored(self) -> None:
        # dataModelId is an int (malformed API response) — must not be stored.
        dm = self._assemble_with_entries(
            [{"type": "data-model", "dataModelId": 42, "name": "Sales"}]
        )
        assert dm.source_dm_element_names == {}

    def test_whitespace_only_name_is_ignored(self) -> None:
        # name is all whitespace — strip() would produce "", must not be stored.
        dm = self._assemble_with_entries(
            [{"type": "data-model", "dataModelId": "src-dm-id", "name": "   "}]
        )
        assert dm.source_dm_element_names == {}

    def test_valid_entry_is_stored(self) -> None:
        # Positive control: a well-formed entry must be stored normally.
        dm = self._assemble_with_entries(
            [{"type": "data-model", "dataModelId": "src-dm-id", "name": "  Revenue  "}]
        )
        assert dm.source_dm_element_names == {"src-dm-id": ["Revenue"]}


def _paginated_response(
    entries: List[Dict[str, Any]],
    *,
    next_page: Any = None,
    next_page_token: Any = None,
    status_code: int = 200,
) -> MagicMock:
    resp = MagicMock(status_code=status_code)
    resp.json.return_value = {
        "entries": entries,
        "nextPage": next_page,
        "nextPageToken": next_page_token,
    }
    return resp


class TestPaginatedRawEntries:
    """Regression coverage for ``_paginated_raw_entries``.

    ``_paginated_entries`` shares the same HTTP loop, so cycle protection
    and multi-page aggregation are covered transitively.
    """

    def test_collects_entries_across_pages_via_nextPage(self) -> None:
        api = _create_sigma_api()
        responses = [
            _paginated_response([{"id": "a"}, {"id": "b"}], next_page=2),
            _paginated_response([{"id": "c"}], next_page=None),
        ]
        with patch.object(api, "_get_api_call", side_effect=responses) as mock_get:
            entries = api._paginated_raw_entries(
                "https://api.example.com/dataModels/dm1/lineage",
                "test ctx",
            )
        assert [e["id"] for e in entries] == ["a", "b", "c"]
        assert mock_get.call_count == 2
        # Second call must include the ``page=2`` cursor.
        assert "page=2" in mock_get.call_args_list[1].args[0]

    def test_collects_entries_across_pages_via_nextPageToken(self) -> None:
        api = _create_sigma_api()
        responses = [
            _paginated_response([{"id": "a"}], next_page_token="tok-2"),
            _paginated_response([{"id": "b"}], next_page_token=None),
        ]
        with patch.object(api, "_get_api_call", side_effect=responses) as mock_get:
            entries = api._paginated_raw_entries(
                "https://api.example.com/foo", "test ctx"
            )
        assert [e["id"] for e in entries] == ["a", "b"]
        assert "nextPageToken=tok-2" in mock_get.call_args_list[1].args[0]

    def test_breaks_on_repeated_nextPageToken(self) -> None:
        """A broken Sigma proxy that echoes the same cursor forever must
        not hang ingestion or duplicate rows."""
        api = _create_sigma_api()
        # Every response returns the same token; without cycle protection
        # this would loop indefinitely.
        looping = _paginated_response([{"id": "x"}], next_page_token="same-tok")
        with patch.object(api, "_get_api_call", return_value=looping) as mock_get:
            entries = api._paginated_raw_entries(
                "https://api.example.com/foo", "test ctx"
            )
        # Page 1 is consumed (one entry); page 2 returns the same cursor
        # and is also consumed (second entry); page 3's repeat cursor is
        # detected and the loop breaks.
        assert len(entries) == 2
        assert mock_get.call_count == 2
        assert any(
            "cursor repeated" in warning.message.lower()
            for warning in api.report.warnings
        )

    def test_silent_statuses_swallow_first_page_error(self) -> None:
        """``/lineage`` returns 404 for DMs with no lineage graph; the
        paginator must treat that as an empty list without warning.
        """
        api = _create_sigma_api()
        empty_404 = MagicMock(status_code=404)
        with patch.object(api, "_get_api_call", return_value=empty_404):
            entries = api._paginated_raw_entries(
                "https://api.example.com/dataModels/dm1/lineage",
                "test ctx",
                silent_statuses=(400, 403, 404, 500),
            )
        assert entries == []
        assert not api.report.warnings


class TestGetWorkbookColumnFormulas:
    def test_collects_formulas_across_next_page(self) -> None:
        api = _create_sigma_api()
        responses = [
            _paginated_response(
                [
                    {
                        "elementId": "elem-1",
                        "name": "col-a",
                        "formula": "[Source/col-a]",
                        "columnId": "inode-abc/COL_A",
                    }
                ],
                next_page=2,
            ),
            _paginated_response(
                [
                    {
                        "elementId": "elem-2",
                        "name": "col-b",
                        "formula": "[Source/col-b]",
                    }
                ]
            ),
        ]

        with patch.object(api, "_get_api_call", side_effect=responses) as mock_get:
            formulas, col_ids = api.get_workbook_column_formulas("wb-1")

        assert formulas == {
            "elem-1": {"col-a": "[Source/col-a]"},
            "elem-2": {"col-b": "[Source/col-b]"},
        }
        assert col_ids == {"elem-1": {"col-a": "inode-abc/COL_A"}}
        assert "page=2" in mock_get.call_args_list[1].args[0]

    def test_collects_formulas_across_next_page_token(self) -> None:
        api = _create_sigma_api()
        responses = [
            _paginated_response(
                [
                    {
                        "elementId": "elem-1",
                        "name": "col-a",
                        "formula": "[Source/col-a]",
                    }
                ],
                next_page_token="tok&=2",
            ),
            _paginated_response(
                [
                    {
                        "elementId": "elem-2",
                        "name": "col-b",
                        "formula": "[Source/col-b]",
                    }
                ]
            ),
        ]

        with patch.object(api, "_get_api_call", side_effect=responses) as mock_get:
            formulas, col_ids = api.get_workbook_column_formulas("wb-1")

        assert formulas == {
            "elem-1": {"col-a": "[Source/col-a]"},
            "elem-2": {"col-b": "[Source/col-b]"},
        }
        assert col_ids == {}
        assert "nextPageToken=tok%26%3D2" in mock_get.call_args_list[1].args[0]

    def test_404_returns_empty_without_warning(self) -> None:
        api = _create_sigma_api()
        empty_404 = MagicMock(status_code=404)

        with patch.object(api, "_get_api_call", return_value=empty_404):
            formulas, col_ids = api.get_workbook_column_formulas("wb-1")

        assert formulas == {}
        assert col_ids == {}
        assert not api.report.warnings

    def test_mid_pagination_failure_preserves_partial_results_and_warns(self) -> None:
        api = _create_sigma_api()
        page_1 = _paginated_response(
            [
                {
                    "elementId": "elem-1",
                    "name": "col-a",
                    "formula": "[Source/col-a]",
                }
            ],
            next_page=2,
        )
        page_2 = MagicMock(status_code=500)
        http_error = requests.HTTPError("server error")
        http_error.response = page_2
        page_2.raise_for_status.side_effect = http_error

        with patch.object(api, "_get_api_call", side_effect=[page_1, page_2]):
            formulas, col_ids = api.get_workbook_column_formulas("wb-1")

        assert formulas == {"elem-1": {"col-a": "[Source/col-a]"}}
        assert col_ids == {}
        assert any(
            warning.title == "Sigma paginated endpoint aborted"
            for warning in api.report.warnings
        )


class TestGetWorkbookPages:
    def test_workbook_lineage_pattern_denied_skips_column_formula_fetch(self) -> None:
        api = _create_sigma_api()
        api.config.workbook_lineage_pattern = AllowDenyPattern(deny=[".*"])
        workbook = Workbook(
            workbookId="wb-1",
            name="Denied Workbook",
            ownerId="u",
            createdBy="u",
            updatedBy="u",
            createdAt=_dt.datetime(2024, 1, 1, tzinfo=_dt.timezone.utc),
            updatedAt=_dt.datetime(2024, 1, 2, tzinfo=_dt.timezone.utc),
            url="https://sigma.example/wb",
            path="Acryl Data/Denied Workbook",
            latestVersion=1,
        )
        pages_response = MagicMock(status_code=200)
        pages_response.json.return_value = {
            "entries": [{"pageId": "page-1", "name": "Page 1"}]
        }

        with (
            patch.object(api, "_get_api_call", return_value=pages_response),
            patch.object(api, "get_page_elements", return_value=[]),
            patch.object(api, "get_workbook_column_formulas") as mock_formulas,
        ):
            pages = api.get_workbook_pages(workbook)

        assert len(pages) == 1
        mock_formulas.assert_not_called()

    def test_workbook_lineage_pattern_denied_passes_no_column_formulas(self) -> None:
        api = _create_sigma_api()
        api.config.workbook_lineage_pattern = AllowDenyPattern(deny=[".*"])
        workbook = Workbook(
            workbookId="wb-1",
            name="Denied Workbook",
            ownerId="u",
            createdBy="u",
            updatedBy="u",
            createdAt=_dt.datetime(2024, 1, 1, tzinfo=_dt.timezone.utc),
            updatedAt=_dt.datetime(2024, 1, 2, tzinfo=_dt.timezone.utc),
            url="https://sigma.example/wb",
            path="Acryl Data/Denied Workbook",
            latestVersion=1,
        )
        pages_response = MagicMock(status_code=200)
        pages_response.json.return_value = {
            "entries": [{"pageId": "page-1", "name": "Page 1"}]
        }

        with (
            patch.object(api, "_get_api_call", return_value=pages_response),
            patch.object(api, "get_page_elements", return_value=[]) as mock_elements,
        ):
            api.get_workbook_pages(workbook)

        assert mock_elements.call_args.kwargs["column_formulas_by_element"] is None
        assert mock_elements.call_args.kwargs["column_ids_by_element"] is None

    def test_lineage_paginated_across_pages(self) -> None:
        """End-to-end: ``_get_data_model_lineage_entries`` returns every
        page of lineage, not just the first.

        Uses the real DM ``/lineage`` shape -- ``element`` rows carry
        ``elementId`` (not ``nodeId``) and ``sourceIds``, ``dataset``
        rows carry ``inodeId``. Earlier fixture shapes here used a
        synthetic ``nodeId`` that masked the shape-aware-key bug.
        """
        api = _create_sigma_api()
        page1 = _paginated_response(
            [
                {
                    "type": "dataset",
                    "name": "PETS",
                    "inodeId": "inode-PETS",
                },
                {"type": "element", "elementId": "e1", "sourceIds": []},
                {"type": "element", "elementId": "e2", "sourceIds": ["e1"]},
            ],
            next_page=2,
        )
        page2 = _paginated_response(
            [{"type": "element", "elementId": "e3", "sourceIds": ["e2"]}],
            next_page=None,
        )
        with patch.object(api, "_get_api_call", side_effect=[page1, page2]):
            entries = api._get_data_model_lineage_entries("dm-id")
        assert [
            (e["type"], e.get("elementId") or e.get("inodeId")) for e in entries
        ] == [
            ("dataset", "inode-PETS"),
            ("element", "e1"),
            ("element", "e2"),
            ("element", "e3"),
        ]

    def test_lineage_500_surfaces_warning(self) -> None:
        """M3 regression: a 500 on ``/lineage`` is *not* in
        ``silent_statuses`` for DM lineage, so a degraded Sigma region
        leaves a loud warning instead of producing zero aspects with
        zero telemetry. 404 stays silent (empty DMs).
        """
        api = _create_sigma_api()
        five_hundred = MagicMock(status_code=500)
        # Real ``requests`` attaches the response onto the HTTPError it
        # raises from ``raise_for_status``; ``_log_http_error`` reads
        # ``e.response.status_code`` off that, so the mocked error must
        # carry the response too or the test trips an unrelated
        # AttributeError before the warning is recorded.
        http_error = requests.exceptions.HTTPError("500 Server Error")
        http_error.response = five_hundred
        five_hundred.raise_for_status.side_effect = http_error
        with patch.object(api, "_get_api_call", return_value=five_hundred):
            entries = api._get_data_model_lineage_entries("dm-id")
        assert entries == []
        assert any(
            w.title == "Sigma paginated endpoint aborted"
            and "lineage" in str(w.context)
            for w in api.report.warnings
        )

    def test_paginator_500_on_page_2_surfaces_warning_preserves_page_1(self) -> None:
        """``_paginated_raw_entries`` applies ``silent_statuses`` only to
        the first page (docstring invariant). A 5xx on page 2 must
        surface a ``report.warning`` *and* preserve the page-1 entries
        already collected -- otherwise a transient mid-pagination
        failure would produce a silently-truncated entity feed.
        """
        api = _create_sigma_api()
        page1 = _paginated_response([{"id": "a"}, {"id": "b"}], next_page=2)
        five_hundred = MagicMock(status_code=500)
        http_error = requests.exceptions.HTTPError("500 Server Error")
        http_error.response = five_hundred
        five_hundred.raise_for_status.side_effect = http_error
        # Even with silent_statuses covering 500 (as /lineage *doesn't*
        # -- but this isolates the "page 2 + silent_statuses ignored"
        # invariant explicitly), the page-2 500 must still surface.
        with patch.object(api, "_get_api_call", side_effect=[page1, five_hundred]):
            entries = api._paginated_raw_entries(
                "https://api.example.com/foo",
                "test ctx",
                silent_statuses=(400, 403, 404, 500),
            )
        # Page-1 entries are preserved ("partial results before the break").
        assert [e["id"] for e in entries] == ["a", "b"]
        # Page-2 failure surfaces as a loud warning, regardless of
        # silent_statuses -- because the page-2 path does not consult it.
        assert any("Pagination aborted" in w.message for w in api.report.warnings), (
            "a 5xx on page 2 must surface a warning even when silent_statuses "
            "would swallow it on page 1"
        )


class TestPaginatedEntriesDedup:
    """Regression coverage for pagination dedup: an echoed cursor (or
    any server-side overlap between pages) must not leak duplicate
    typed entries to downstream emitters. The cycle guard itself still
    appends entries from the first *two* pages before firing; the
    natural-key dedup at the typed layer is what prevents double-MCP
    emission.
    """

    def test_paginated_entries_dedupes_by_key(self) -> None:
        api = _create_sigma_api()

        def _dm_payload(data_model_id: str) -> Dict[str, Any]:
            return {
                "dataModelId": data_model_id,
                "name": f"dm-{data_model_id}",
                "createdAt": _dt.datetime(2024, 1, 1, tzinfo=_dt.timezone.utc),
                "updatedAt": _dt.datetime(2024, 1, 2, tzinfo=_dt.timezone.utc),
            }

        # Two pages, each returning the same DM -- simulates the broken
        # proxy in test_breaks_on_repeated_nextPageToken but at the typed
        # layer. Only one DM must survive.
        page1 = _paginated_response([_dm_payload("dm-1")], next_page_token="same-tok")
        with patch.object(api, "_get_api_call", return_value=page1):
            results = api._paginated_entries(
                "https://api.example.com/dataModels",
                SigmaDataModel,
                "Unable to fetch sigma data models.",
                dedup_key=lambda dm: dm.dataModelId,
            )
        assert [dm.dataModelId for dm in results] == ["dm-1"]
        assert api.report.pagination_duplicate_entries_dropped == 1

    def test_paginated_entries_without_dedup_key_keeps_duplicates(self) -> None:
        """Callers that do not opt in to dedup retain existing
        behavior. Guards against silently breaking non-DM callers.
        """
        api = _create_sigma_api()

        dm_payload = {
            "dataModelId": "dm-1",
            "name": "dm-1",
            "createdAt": _dt.datetime(2024, 1, 1, tzinfo=_dt.timezone.utc),
            "updatedAt": _dt.datetime(2024, 1, 2, tzinfo=_dt.timezone.utc),
        }
        page1 = _paginated_response([dm_payload], next_page_token="same-tok")
        with patch.object(api, "_get_api_call", return_value=page1):
            results = api._paginated_entries(
                "https://api.example.com/dataModels",
                SigmaDataModel,
                "ctx",
            )
        assert [dm.dataModelId for dm in results] == ["dm-1", "dm-1"]
        assert api.report.pagination_duplicate_entries_dropped == 0

    def test_lineage_raw_dedupes_by_shape_aware_key(self) -> None:
        """Dedup must use the *real* DM ``/lineage`` shape --
        ``elementId`` for ``type: element`` rows,
        ``inodeId`` for ``type: dataset`` / ``type: table`` rows. An
        earlier version of this function keyed on ``(type, nodeId)``
        which collapsed to ``(type, "")`` for every real entry and
        silently discarded every element after the first.

        Verifies both:
        * two echoed-across-pages elements collapse to one (real dedup)
        * elements with the *same* type but distinct ``elementId``
          survive (what broke under the old key)
        """
        api = _create_sigma_api()
        # First page has four distinct rows (mixing element + dataset
        # shapes). Echoed cursor means the paginator will consume a
        # second page before the cycle guard fires; that second page
        # is the same payload, so every row is a cross-page duplicate
        # and must collapse on the natural key.
        page = _paginated_response(
            [
                {
                    "type": "dataset",
                    "name": "PETS",
                    "inodeId": "inode-PETS",
                },
                {"type": "element", "elementId": "e1", "sourceIds": []},
                {"type": "element", "elementId": "e2", "sourceIds": ["e1"]},
                {
                    "type": "table",
                    "name": "WAREHOUSE_TBL",
                    "inodeId": "inode-WAREHOUSE_TBL",
                },
            ],
            next_page_token="same-tok",
        )
        with patch.object(api, "_get_api_call", return_value=page):
            entries = api._get_data_model_lineage_entries("dm-id")
        # All four distinct rows survive -- the old ``nodeId`` key
        # would have collapsed elements {e1, e2} down to one and both
        # dataset/table rows to one apiece.
        assert [
            (e["type"], e.get("elementId") or e.get("inodeId")) for e in entries
        ] == [
            ("dataset", "inode-PETS"),
            ("element", "e1"),
            ("element", "e2"),
            ("table", "inode-WAREHOUSE_TBL"),
        ]
        # And the page-2 echo of the same four rows was all duplicates.
        assert api.report.pagination_duplicate_entries_dropped == 4

    def test_lineage_raw_preserves_entries_missing_natural_key(self) -> None:
        """C1 correctness invariant: entries whose shape does not carry
        the expected identifier (a future Sigma shape, or a malformed
        row) must be *preserved*, not collapsed under a shared empty
        key that would silently drop all but the first. Contrast with
        the old behavior where ``(type, "")`` collapsed every one.
        """
        api = _create_sigma_api()
        page = _paginated_response(
            [
                {"type": "element"},
                {"type": "element"},
                {"type": "dataset", "name": "PETS"},
            ],
            next_page=None,
        )
        with patch.object(api, "_get_api_call", return_value=page):
            entries = api._get_data_model_lineage_entries("dm-id")
        # All three preserved -- none count as dedup drops.
        assert len(entries) == 3
        assert api.report.pagination_duplicate_entries_dropped == 0

    def test_get_data_model_columns_keeps_all_elements_sharing_same_columnid(
        self,
    ) -> None:
        """Regression: dedup key must be (elementId, columnId), not columnId alone.

        Sigma reuses warehouse-native columnIds (e.g. CUSTOMER_ID) across
        customSQL elements that share a warehouse passthrough column. The old
        key dropped all but the first occurrence, removing columns from consumer
        elements' schemaMetadata.
        """
        api = _create_sigma_api()
        # Three elements all expose CUSTOMER_ID as a passthrough column.
        # With the old dedup key (columnId alone), only elem1's row would survive.
        page = _paginated_response(
            [
                {
                    "columnId": "CUSTOMER_ID",
                    "elementId": "elem1",
                    "name": "Customer Id",
                },
                {
                    "columnId": "CUSTOMER_ID",
                    "elementId": "elem2",
                    "name": "Customer Id",
                },
                {
                    "columnId": "CUSTOMER_ID",
                    "elementId": "elem3",
                    "name": "Customer Id",
                },
                # Unique columnIds must be unaffected.
                {
                    "columnId": "LIFETIME_VALUE",
                    "elementId": "elem3",
                    "name": "Lifetime Value",
                },
            ],
            next_page=None,
        )
        with patch.object(api, "_get_api_call", return_value=page):
            columns = api._get_data_model_columns("dm-1")
        element_ids = [c.elementId for c in columns]
        assert "elem1" in element_ids
        assert "elem2" in element_ids
        assert "elem3" in element_ids
        assert len(columns) == 4
        assert api.report.pagination_duplicate_entries_dropped == 0

    def test_get_data_model_columns_dedupes_cross_page_echo(self) -> None:
        """Cross-page echo of the same (elementId, columnId) is still deduplicated."""
        api = _create_sigma_api()
        col_row = {
            "columnId": "CUSTOMER_ID",
            "elementId": "elem1",
            "name": "Customer Id",
        }
        page = _paginated_response([col_row], next_page_token="same-tok")
        with patch.object(api, "_get_api_call", return_value=page):
            columns = api._get_data_model_columns("dm-1")
        assert len(columns) == 1
        assert api.report.pagination_duplicate_entries_dropped == 1

    def test_get_data_models_workspace_fallback_to_payload(self) -> None:
        """C2 regression: when ``/files`` is missing the DM row (or has
        no workspaceId) but the ``/dataModels`` payload names an
        allowed workspace, route through the workspace branch instead
        of dropping the DM (which is what the old ``file_meta`` only
        path did).
        """

        api = _create_sigma_api()
        dm_payload = {
            "dataModelId": "dm-uuid-1",
            "name": "My DM",
            "workspaceId": "ws-from-payload",
            "createdAt": _dt.datetime(2024, 1, 1, tzinfo=_dt.timezone.utc),
            "updatedAt": _dt.datetime(2024, 1, 2, tzinfo=_dt.timezone.utc),
        }
        ws = Workspace(
            workspaceId="ws-from-payload",
            name="Marketing",
            createdBy="u",
            createdAt=_dt.datetime(2024, 1, 1, tzinfo=_dt.timezone.utc),
            updatedAt=_dt.datetime(2024, 1, 2, tzinfo=_dt.timezone.utc),
        )
        with (
            patch.object(api, "_get_files_metadata", return_value={}),
            patch.object(
                api,
                "_paginated_entries",
                return_value=[SigmaDataModel.model_validate(dm_payload)],
            ),
            patch.object(api, "get_workspace", return_value=ws) as mock_ws,
            patch.object(api, "_assemble_data_model"),
        ):
            results = api.get_data_models()
        assert [dm.dataModelId for dm in results] == ["dm-uuid-1"]
        mock_ws.assert_called_once_with("ws-from-payload")
        # ``ingest_shared_entities`` stays at its default False; the DM
        # still lands because the payload workspace resolved normally.
        assert api.report.data_models_without_workspace == 0

    def test_paginated_entries_rate_limits_malformed_warnings(self) -> None:
        """m7: a malformed-row storm appends at most
        ``_MAX_MALFORMED_WARNINGS_PER_ENDPOINT`` context entries to the
        single merged warning (same title/message collapses to one
        StructuredLogEntry), with the full drop count tallied on
        ``pagination_malformed_entries_dropped`` for visibility.
        """
        api = _create_sigma_api()
        bad_rows = [{"broken": True} for _ in range(25)]
        resp = _paginated_response(bad_rows)
        with patch.object(api, "_get_api_call", return_value=resp):
            results = api._paginated_entries(
                "https://api.example.com/dataModels",
                SigmaDataModel,
                "ctx",
            )
        assert results == []
        assert api.report.pagination_malformed_entries_dropped == 25
        # Same (title, message) -> one StructuredLogEntry.
        entries = list(api.report.warnings)
        assert len(entries) == 1
        # Each ``self.report.warning(...)`` call appended one context
        # string; rate-limiter stopped after
        # ``_MAX_MALFORMED_WARNINGS_PER_ENDPOINT`` of them.
        assert len(entries[0].context) == api._MAX_MALFORMED_WARNINGS_PER_ENDPOINT


def _create_sigma_source(
    ingest_data_models: bool = True,
    ingest_owner: bool = True,
    data_model_pattern_overrides: Optional[Dict[str, Any]] = None,
) -> SigmaSource:
    """Build a minimal :class:`SigmaSource` for unit tests. The API
    constructor normally issues a token-exchange request; we patch that
    out so tests don't need network access.
    """

    config_kwargs: Dict[str, Any] = {
        "client_id": "test_client_id",
        "client_secret": "test_secret",
        "ingest_data_models": ingest_data_models,
        "ingest_owner": ingest_owner,
    }
    if data_model_pattern_overrides is not None:
        config_kwargs["data_model_pattern"] = data_model_pattern_overrides
    config = SigmaSourceConfig.model_validate(config_kwargs)
    ctx = PipelineContext(run_id="sigma-unit-test")
    with patch.object(SigmaAPI, "_generate_token"):
        return SigmaSource(config=config, ctx=ctx)


class TestSchemaMetadataEmission:
    """Covers ``_gen_data_model_element_schema_metadata`` invariants that
    downstream consumers depend on: stable field ordering across runs
    (Maj-2) and deterministic duplicate-fieldPath tie-breaking.
    """

    @staticmethod
    def _extract_schema(source: SigmaSource, element: Any) -> SchemaMetadataClass:
        wu = source._gen_data_model_element_schema_metadata(
            element_dataset_urn=(
                "urn:li:dataset:(urn:li:dataPlatform:sigma,dm-test.elem1,PROD)"
            ),
            element=element,
        )
        aspect = wu.metadata.aspect  # type: ignore[union-attr]
        assert isinstance(aspect, SchemaMetadataClass), (
            f"expected SchemaMetadata aspect, got {type(aspect).__name__}"
        )
        return aspect

    @classmethod
    def _schema_field_paths(cls, source: SigmaSource, element: Any) -> List[str]:
        schema_metadata = cls._extract_schema(source, element)
        return [field.fieldPath for field in schema_metadata.fields]

    @staticmethod
    def _make_element(columns: List[Dict[str, Any]]) -> Any:
        """Construct a :class:`SigmaDataModelElement` from raw column
        dicts. The element's ``_discard_api_bare_string_columns``
        validator runs in ``mode="before"`` and discards any non-dict
        entry in ``columns``, so tests must pass column dicts (mirroring
        the real ``/columns`` JSON payload), not pre-parsed column
        instances.
        """

        return SigmaDataModelElement.model_validate(
            {"elementId": "elem1", "name": "Elem 1", "columns": columns}
        )

    def test_field_order_is_stable_across_columns_reorder(self) -> None:
        """Maj-2 regression: Sigma's ``/columns`` endpoint has no
        documented ordering contract. Any reorder must not change the
        emitted ``SchemaMetadata.fields`` ordering, otherwise every
        ingest re-upserts the aspect with different ``fieldPath`` order
        and downstream aspect-version timelines churn.
        """
        source = _create_sigma_source()
        columns_a = [
            {"columnId": "c1", "name": "zebra"},
            {"columnId": "c2", "name": "alpha"},
            {"columnId": "c3", "name": "mango"},
        ]
        columns_b = list(reversed(columns_a))
        paths_a = self._schema_field_paths(source, self._make_element(columns_a))
        paths_b = self._schema_field_paths(source, self._make_element(columns_b))
        assert paths_a == ["alpha", "mango", "zebra"]
        assert paths_a == paths_b

    def test_duplicate_fieldpath_tiebreak_prefers_formula(self) -> None:
        """Tie-break rule 1: when two columns share a ``fieldPath``,
        the row with a non-empty ``formula`` wins -- that's the
        user-authored calculated field and the one Sigma surfaces in
        the UI. Pins which row survives, not just that a dedup
        happened.
        """
        source = _create_sigma_source()
        element = self._make_element(
            [
                {
                    "columnId": "c_native_aaaa",
                    "name": "metric",
                    "label": "native row",
                },
                {
                    "columnId": "c_calc_zzzz",
                    "name": "metric",
                    "label": "calc row",
                    "formula": "SUM([amount])",
                },
            ]
        )
        schema_metadata = self._extract_schema(source, element)
        assert len(schema_metadata.fields) == 1
        assert schema_metadata.fields[0].description == "calc row", (
            "formula-carrying row must win regardless of iteration order; "
            "got the native-row description instead"
        )
        assert (
            source.reporter.data_model_element_columns_duplicate_fieldpath_dropped == 1
        )

    def test_duplicate_fieldpath_tiebreak_uses_smallest_column_id(self) -> None:
        """Tie-break rule 2: when both columns have (or both lack) a
        ``formula``, the row with the lexicographically smallest
        ``columnId`` wins. Stability across runs is the contract;
        string compare is fine because Sigma columnIds are UUIDs.
        """
        source = _create_sigma_source()
        element = self._make_element(
            [
                {"columnId": "zzzz", "name": "metric", "label": "second by id"},
                {"columnId": "aaaa", "name": "metric", "label": "first by id"},
            ]
        )
        schema_metadata = self._extract_schema(source, element)
        assert len(schema_metadata.fields) == 1
        assert schema_metadata.fields[0].description == "first by id", (
            "tie-break must pick the smallest columnId regardless of the "
            "input order; got the higher-id row instead"
        )


class TestDataModelPatternWarning:
    """Maj-3 regression: the ``data_model_pattern`` check in
    ``SigmaSource.__init__`` must remain robust even if
    ``AllowDenyPattern.__eq__`` (which is ``__dict__``-based and
    sensitive to the ``@cached_property`` compiled-regex caches) gets
    confused by cache state. The current check compares the underlying
    ``allow`` / ``deny`` / ``ignoreCase`` fields directly, so it does
    not depend on any equality-via-``__dict__`` identity.
    """

    def test_default_pattern_does_not_warn(self) -> None:
        source = _create_sigma_source(ingest_data_models=False)
        assert not any(
            "data_model_pattern ignored" in (w.title or "")
            for w in source.reporter.warnings
        )

    def test_non_default_pattern_warns_when_dm_disabled(self) -> None:
        source = _create_sigma_source(
            ingest_data_models=False,
            data_model_pattern_overrides={"allow": ["^foo.*$"]},
        )
        assert any(
            "data_model_pattern ignored" in (w.title or "")
            for w in source.reporter.warnings
        ), "expected a warning when data_model_pattern is set but DMs are off"

    def test_warning_survives_cached_regex_state(self) -> None:
        """Regression guard for ``AllowDenyPattern`` cache-state
        fragility: calling ``.allowed()`` on the pattern materializes
        its ``@cached_property`` compiled-regex caches into the
        instance's ``__dict__``. ``AllowDenyPattern.__eq__`` compares
        ``__dict__`` directly, so if any future ``__init__`` reorder
        triggers ``.allowed()`` on ``data_model_pattern`` before this
        check runs, a ``!= AllowDenyPattern.allow_all()`` comparison
        would spuriously flip True (the fresh ``allow_all()`` has no
        cache entries) and the warning would fire even though the
        pattern is the default. The direct ``allow``/``deny``
        check we use now is cache-state-independent; this test pins
        that invariant.
        """

        config = SigmaSourceConfig.model_validate(
            {
                "client_id": "test_client_id",
                "client_secret": "test_secret",
                "ingest_data_models": False,
            }
        )
        config.data_model_pattern.allowed("warm-up-cache")
        assert "_compiled_allow" in config.data_model_pattern.__dict__, (
            "test setup invariant: expected .allowed() to populate the "
            "@cached_property backing store so the regression scenario "
            "is actually reproduced"
        )

        ctx = PipelineContext(run_id="sigma-unit-test")
        with patch.object(SigmaAPI, "_generate_token"):
            source = SigmaSource(config=config, ctx=ctx)
        assert not any(
            "data_model_pattern ignored" in (w.title or "")
            for w in source.reporter.warnings
        )

    def test_ignore_case_false_on_default_pattern_does_not_warn(self) -> None:
        """M3 edge case: a user who pins ``ignoreCase: False`` on an
        otherwise-default pattern is semantically still "match
        everything" (``.*`` matches regardless of case). The default
        detector must not treat that as "non-default" and fire a
        spurious "pattern ignored" warning.
        """
        source = _create_sigma_source(
            ingest_data_models=False,
            data_model_pattern_overrides={"ignoreCase": False},
        )
        assert not any(
            "data_model_pattern ignored" in (w.title or "")
            for w in source.reporter.warnings
        ), (
            "ignoreCase=False on an otherwise-default pattern is still "
            "semantically the default and must not trigger the ignored-pattern "
            "warning"
        )


class TestLineageDedupMerge:
    """M5 regression: :meth:`_get_data_model_lineage_entries` must merge
    ``sourceIds`` on shape-aware dedup collisions rather than silently
    dropping the second occurrence. Protects against:

    (a) a proxy that echoes the same cursor -- identical rows, union
        collapses to the same set; counters still bump so operators see
        the echo.
    (b) a future Sigma-side split of one element's lineage across
        multiple rows (e.g. versioned upstreams) -- we keep all
        ``sourceIds`` instead of losing the trailing rows'.
    """

    def test_element_source_ids_unioned_on_collision(self) -> None:
        api = _create_sigma_api()
        raw_entries = [
            {
                "type": "element",
                "elementId": "elem1",
                "sourceIds": ["inode-a", "inode-b"],
            },
            # Same elementId; different, partially-overlapping sourceIds.
            # Old behavior: dropped the second row, losing ``inode-c``.
            # New behavior: union into the first row, preserving ordering
            # and bumping the duplicate counter.
            {
                "type": "element",
                "elementId": "elem1",
                "sourceIds": ["inode-b", "inode-c"],
            },
        ]
        with patch.object(api, "_paginated_raw_entries", return_value=raw_entries):
            deduped = api._get_data_model_lineage_entries("dm-test")

        assert len(deduped) == 1, (
            "duplicate element rows must collapse to a single entry; got "
            f"{len(deduped)}"
        )
        assert deduped[0]["sourceIds"] == ["inode-a", "inode-b", "inode-c"], (
            "expected union of sourceIds preserving first-seen order; got "
            f"{deduped[0]['sourceIds']}"
        )
        assert api.report.pagination_duplicate_entries_dropped == 1, (
            "duplicate counter must still bump so operators notice the "
            "pagination echo / shape drift"
        )

    def test_identical_element_rows_union_is_idempotent(self) -> None:
        """Proxy-echo path: two truly identical rows stay identical
        after union; no phantom ``sourceIds`` appear from stringified
        set iteration order.
        """
        api = _create_sigma_api()
        raw_entries = [
            {
                "type": "element",
                "elementId": "elem1",
                "sourceIds": ["inode-a", "inode-b"],
            },
            {
                "type": "element",
                "elementId": "elem1",
                "sourceIds": ["inode-a", "inode-b"],
            },
        ]
        with patch.object(api, "_paginated_raw_entries", return_value=raw_entries):
            deduped = api._get_data_model_lineage_entries("dm-test")

        assert len(deduped) == 1
        assert deduped[0]["sourceIds"] == ["inode-a", "inode-b"]
        assert api.report.pagination_duplicate_entries_dropped == 1


class TestEagerDatasetUrnMapPopulation:
    """M6 regression: ``sigma_dataset_urn_by_url_id`` must be populated
    eagerly -- before DM iteration -- rather than as a side-effect of
    :meth:`_gen_dataset_workunit` yielding. An eager pre-pass decouples
    DM external-upstream resolution from the order in which the pipeline
    framework drains dataset vs. DM generators, so a future refactor
    that reorders or parallelizes the yields cannot silently burn
    through the ``unresolved_external`` counter.
    """

    def test_map_populated_before_any_workunit_is_yielded(self) -> None:
        source = _create_sigma_source()

        ds_a = SigmaDataset(
            datasetId="ds-a-id",
            name="DS A",
            description="",
            createdBy="u",
            createdAt=_dt.datetime(2024, 1, 1, tzinfo=_dt.timezone.utc),
            updatedAt=_dt.datetime(2024, 1, 2, tzinfo=_dt.timezone.utc),
            url="https://sigma.example/dataset/url-a",
        )
        ds_b = SigmaDataset(
            datasetId="ds-b-id",
            name="DS B",
            description="",
            createdBy="u",
            createdAt=_dt.datetime(2024, 1, 1, tzinfo=_dt.timezone.utc),
            updatedAt=_dt.datetime(2024, 1, 2, tzinfo=_dt.timezone.utc),
            url="https://sigma.example/dataset/url-b",
        )

        with (
            patch.object(source.sigma_api, "fill_workspaces", return_value=None),
            patch.object(
                source.sigma_api, "get_sigma_datasets", return_value=[ds_a, ds_b]
            ),
            patch.object(source.sigma_api, "get_sigma_workbooks", return_value=[]),
            patch.object(source.sigma_api, "get_data_models", return_value=[]),
            patch.object(source, "_get_allowed_workspaces", return_value=[]),
            patch.object(
                source,
                "_gen_sigma_dataset_upstream_lineage_workunit",
                return_value=iter([]),
            ),
            patch.object(source, "_gen_dataset_workunit") as mock_gen_dataset_workunit,
        ):
            # Assert the map is already populated the moment
            # ``_gen_dataset_workunit`` is invoked for the first dataset,
            # i.e. before any yield from the dataset generator has been
            # consumed by a hypothetical DM consumer downstream.
            captured_map_snapshots: List[Dict[str, str]] = []

            def _capture(dataset: Any) -> Iterator[Any]:
                captured_map_snapshots.append(dict(source.sigma_dataset_urn_by_url_id))
                return iter([])

            mock_gen_dataset_workunit.side_effect = _capture
            list(source.get_workunits_internal())

        assert len(captured_map_snapshots) == 2, (
            "expected _gen_dataset_workunit to be invoked once per dataset; got "
            f"{len(captured_map_snapshots)} calls"
        )
        for snap in captured_map_snapshots:
            assert ds_a.get_urn_part() in snap, (
                "sigma_dataset_urn_by_url_id must contain DS A before any "
                f"dataset workunit is yielded; snapshot was {snap!r}"
            )
            assert ds_b.get_urn_part() in snap, (
                "sigma_dataset_urn_by_url_id must contain DS B before any "
                f"dataset workunit is yielded; snapshot was {snap!r}"
            )


class TestChartInputsInsertionOrder:
    """C1 regression: :class:`ChartInfoClass`\\ ``.inputs`` must preserve
    insertion order of :meth:`_get_element_input_details`'s
    ``dataset_inputs`` keys, **not** emit a lex-sorted list.

    The split PR was ~600 lines of DM-specific churn; a stray
    ``sorted(dataset_inputs.keys())`` on the Chart path would
    silently churn every Sigma chart's ChartInfo aspect on first
    re-ingest after the upgrade (any tenant whose inputs were
    inserted in non-alphabetical order would get a new aspect
    version). This test pins the invariant at the level of the
    function that emits the aspect so any future "let's sort for
    determinism" refactor has to own a Updating-DataHub note and
    this test's expectations.
    """

    def test_chart_inputs_preserve_insertion_order(self) -> None:
        source = _create_sigma_source()

        # Insertion order is Z -> A, the inverse of lex order. A
        # silent ``sorted(...)`` would produce [A, Z] and fail the
        # assertion below.
        dataset_inputs: Dict[str, List[str]] = {
            "urn:li:dataset:(urn:li:dataPlatform:sigma,zzz-inserted-first,PROD)": [],
            "urn:li:dataset:(urn:li:dataPlatform:sigma,aaa-inserted-second,PROD)": [],
        }
        chart_input_urns: List[str] = []

        element = Element(
            elementId="elem-c1",
            name="Chart with two dataset inputs",
            url="https://sigma.example/wb/elem-c1",
            type="visualization",
            vizualizationType="bar",
            columns=["Col A"],
        )
        workbook = Workbook(
            workbookId="wb-c1",
            name="Workbook for C1 regression",
            ownerId="u",
            createdBy="u",
            updatedBy="u",
            createdAt=_dt.datetime(2024, 1, 1, tzinfo=_dt.timezone.utc),
            updatedAt=_dt.datetime(2024, 1, 2, tzinfo=_dt.timezone.utc),
            url="https://sigma.example/wb",
            path="Acryl Data/Acryl Workbook",
            latestVersion=1,
            workspaceId="ws-c1",
            pages=[],
        )

        with patch.object(
            source,
            "_get_element_input_details",
            return_value=(dataset_inputs, chart_input_urns),
        ):
            workunits = list(
                source._gen_elements_workunit(
                    elements=[element],
                    workbook=workbook,
                    all_input_fields=[],
                    paths=[],
                    elementId_to_chart_urn={},
                    wb_element_index={},
                    wb_warehouse_table_index=_WorkbookWarehouseIndex(
                        by_url_id={}, by_name={}
                    ),
                )
            )

        chart_info_aspects = [
            wu.metadata.aspect  # type: ignore[union-attr]
            for wu in workunits
            if isinstance(
                wu.metadata.aspect,  # type: ignore[union-attr]
                ChartInfoClass,
            )
        ]
        assert len(chart_info_aspects) == 1, (
            f"expected exactly one ChartInfo workunit per element; got "
            f"{len(chart_info_aspects)}"
        )
        emitted_inputs = chart_info_aspects[0].inputs
        assert emitted_inputs == list(dataset_inputs.keys()), (
            f"ChartInfo.inputs must preserve insertion order from "
            f"dataset_inputs.keys(); got {emitted_inputs!r}, expected "
            f"{list(dataset_inputs.keys())!r}"
        )


class TestDataModelElementOwner:
    """M1 regression: DM element Datasets must emit an
    :class:`OwnershipClass` aspect derived from ``data_model.createdBy``
    when :attr:`SigmaSourceConfig.ingest_owner` is true. Without this,
    "Datasets owned by X" filters in the DataHub UI silently miss DM
    elements even though the author shows up on the enclosing DM
    Container -- an asymmetry that is surprising because DM elements
    are advertised as first-class Datasets parallel to Sigma Datasets.
    """

    def _make_dm_with_one_element(self) -> SigmaDataModel:
        dm = SigmaDataModel(
            dataModelId="dm-owner-test",
            name="DM with owner",
            description="",
            createdBy="creator-1",
            createdAt=_dt.datetime(2024, 1, 1, tzinfo=_dt.timezone.utc),
            updatedAt=_dt.datetime(2024, 1, 2, tzinfo=_dt.timezone.utc),
            url="https://sigma.example/dm",
            urlId="dm-url-id",
            latestVersion=1,
            workspaceId="ws-1",
            path="Acryl Data",
        )
        dm.elements = [
            SigmaDataModelElement(
                elementId="elem-1",
                name="element one",
                type="table",
            )
        ]
        return dm

    def test_owner_emitted_when_createdBy_resolves_and_ingest_owner_true(
        self,
    ) -> None:
        source = _create_sigma_source(ingest_data_models=True)
        dm = self._make_dm_with_one_element()
        elementId_to_dataset_urn = {
            "elem-1": source._gen_data_model_element_urn(dm, dm.elements[0])
        }

        with patch.object(source.sigma_api, "get_user_name", return_value="jane.doe"):
            workunits = list(
                source._gen_data_model_workunit(dm, elementId_to_dataset_urn)
            )

        element_owner_aspects: List[OwnershipClass] = []
        for wu in workunits:
            aspect = wu.metadata.aspect  # type: ignore[union-attr]
            entity_urn = wu.metadata.entityUrn  # type: ignore[union-attr]
            if (
                isinstance(aspect, OwnershipClass)
                and entity_urn == elementId_to_dataset_urn["elem-1"]
            ):
                element_owner_aspects.append(aspect)
        assert len(element_owner_aspects) == 1, (
            "exactly one OwnershipClass aspect must be emitted per DM "
            f"element; got {len(element_owner_aspects)}"
        )
        owner_aspect = element_owner_aspects[0]
        owner_urns = [o.owner for o in owner_aspect.owners]
        assert owner_urns == ["urn:li:corpuser:jane.doe"], (
            f"DM element owner must be derived from data_model.createdBy "
            f"via get_user_name; got {owner_urns!r}"
        )

    def test_no_owner_when_ingest_owner_false(self) -> None:
        source = _create_sigma_source(ingest_data_models=True, ingest_owner=False)
        dm = self._make_dm_with_one_element()
        elementId_to_dataset_urn = {
            "elem-1": source._gen_data_model_element_urn(dm, dm.elements[0])
        }

        with patch.object(source.sigma_api, "get_user_name", return_value="jane.doe"):
            workunits = list(
                source._gen_data_model_workunit(dm, elementId_to_dataset_urn)
            )

        element_owner_aspects = []
        for wu in workunits:
            aspect = wu.metadata.aspect  # type: ignore[union-attr]
            entity_urn = wu.metadata.entityUrn  # type: ignore[union-attr]
            if (
                isinstance(aspect, OwnershipClass)
                and entity_urn == elementId_to_dataset_urn["elem-1"]
            ):
                element_owner_aspects.append(wu)
        assert element_owner_aspects == [], (
            "ingest_owner=False must suppress DM element OwnershipClass "
            "emission so operators who opt out of user-URN emission are "
            "respected"
        )

    def test_no_owner_when_createdBy_unresolved(self) -> None:
        source = _create_sigma_source(ingest_data_models=True)
        dm = self._make_dm_with_one_element()
        elementId_to_dataset_urn = {
            "elem-1": source._gen_data_model_element_urn(dm, dm.elements[0])
        }

        # get_user_name returning None models: member not in /members
        # (deleted user, admin-permission-only tenant, etc). We must
        # silently skip rather than emit an ``OwnershipClass`` with an
        # empty or None owner URN.
        with patch.object(source.sigma_api, "get_user_name", return_value=None):
            workunits = list(
                source._gen_data_model_workunit(dm, elementId_to_dataset_urn)
            )

        element_owner_aspects = []
        for wu in workunits:
            aspect = wu.metadata.aspect  # type: ignore[union-attr]
            entity_urn = wu.metadata.entityUrn  # type: ignore[union-attr]
            if (
                isinstance(aspect, OwnershipClass)
                and entity_urn == elementId_to_dataset_urn["elem-1"]
            ):
                element_owner_aspects.append(wu)
        assert element_owner_aspects == [], (
            "unresolved createdBy must not produce an OwnershipClass "
            "aspect on the DM element"
        )


class TestGetDataModelsPerDmIsolation:
    """M2 regression: :meth:`SigmaAPI.get_data_models` must isolate
    per-DM assembly failures so one malformed DM does not abort the
    whole DM feed. Mirrors :meth:`get_sigma_workbooks`, which
    swallows per-workbook failures for the same reason.
    """

    def _paginated_raw_entry(self, dm_id: str, name: str) -> Dict[str, Any]:
        return {
            "dataModelId": dm_id,
            "urlId": f"url-{dm_id}",
            "name": name,
            "description": "",
            "createdBy": "u",
            "createdAt": "2024-05-10T09:00:00.000Z",
            "updatedAt": "2024-05-12T10:00:00.000Z",
            "url": f"https://sigma.example/dm/{dm_id}",
            "latestVersion": 1,
            "workspaceId": "ws-1",
            "path": "Acryl Data",
        }

    def test_one_bad_dm_does_not_kill_the_feed(self) -> None:
        api = _create_sigma_api()

        # Force ``ingest_shared_entities`` so the "no workspace"
        # branch also exercises the try/except (though the main
        # scenario is a successful workspace lookup below).
        api.config.ingest_shared_entities = True

        raw = [
            SigmaDataModel.model_validate(
                self._paginated_raw_entry("dm-good-1", "Good DM 1")
            ),
            SigmaDataModel.model_validate(
                self._paginated_raw_entry("dm-bad", "Bad DM")
            ),
            SigmaDataModel.model_validate(
                self._paginated_raw_entry("dm-good-2", "Good DM 2")
            ),
        ]

        original_assemble = api._assemble_data_model

        def _assemble_side_effect(
            data_model: SigmaDataModel,
            file_meta: Optional[File],
            resolved_workspace_id: Optional[str] = None,
        ) -> None:
            if data_model.dataModelId == "dm-bad":
                raise RuntimeError("simulated vendor payload corruption")
            original_assemble(
                data_model, file_meta, resolved_workspace_id=resolved_workspace_id
            )

        with (
            patch.object(api, "_get_files_metadata", return_value={}),
            patch.object(api, "_paginated_entries", return_value=raw),
            # Skip the real ``get_workspace`` HTTP call: the M2 scenario
            # is "assembly raises", not "workspace lookup raises", and
            # a live request here would make this test take 30+ seconds
            # waiting for connection timeouts.
            patch.object(api, "get_workspace", return_value=None),
            patch.object(api, "_get_data_model_elements", return_value=[]),
            patch.object(api, "_get_data_model_columns", return_value=[]),
            patch.object(api, "_get_data_model_lineage_entries", return_value=[]),
            patch.object(
                api, "_assemble_data_model", side_effect=_assemble_side_effect
            ),
        ):
            data_models = api.get_data_models()

        returned_ids = {dm.dataModelId for dm in data_models}
        assert returned_ids == {"dm-good-1", "dm-good-2"}, (
            f"expected both good DMs to survive when a sibling DM's "
            f"assembly raises; got {returned_ids!r}"
        )
        warning_titles = [w.title for w in api.report.warnings]
        assert any(
            (t or "") == "Failed to assemble Sigma Data Model" for t in warning_titles
        ), (
            f"expected a structured warning pinpointing the bad DM; "
            f"got warnings {warning_titles!r}"
        )


class TestGetDataModelByUrlIdHttpStatusSurfaced:
    """PR2 review M1: a non-200 on the orphan-DM fetch used to be
    silently downgraded to ``logger.debug``, making 429 (rate-limited,
    retry later) indistinguishable from 403 / 404 (genuinely
    forbidden / deleted) in the ingestion report. Operators saw a
    single aggregate ``data_model_external_reference_unresolved``
    tick up with no hint at the cause. These tests pin that:

    1. any non-200 produces a structured ``SourceReport.warning`` with
       the status code in the title/context;
    2. 429 additionally bumps the dedicated
       ``data_model_external_reference_rate_limited`` counter so
       telemetry dashboards can alert on rate-limiting specifically.
    """

    def test_429_after_retries_bumps_rate_limit_counter_and_warns(self) -> None:
        api = _create_sigma_api()
        rate_limited = MagicMock(status_code=429)
        with patch.object(api, "_get_api_call", return_value=rate_limited):
            result = api.get_data_model_by_url_id("some-url-id")
        assert result is None, (
            "a 429 after the urllib3 retry budget must still resolve to "
            "None so the caller treats the DM as unresolved"
        )
        assert api.report.data_model_external_reference_rate_limited == 1, (
            "429s must bump the dedicated rate-limit counter so "
            "dashboards can distinguish transient rate-limiting from "
            "steady-state 'DM is forbidden'"
        )
        warning_titles = [w.title or "" for w in api.report.warnings]
        assert any("rate-limited" in t.lower() for t in warning_titles), (
            f"expected a rate-limit-specific warning title; "
            f"got titles {warning_titles!r}"
        )

    def test_403_surfaces_warning_with_status_code(self) -> None:
        """403 is the common case (admin-scope revoked / personal
        space not shared). Previously silent; now a structured
        warning with ``status=403`` in the context so the operator
        can grep the report.
        """
        api = _create_sigma_api()
        forbidden = MagicMock(status_code=403)
        with patch.object(api, "_get_api_call", return_value=forbidden):
            result = api.get_data_model_by_url_id("some-url-id")
        assert result is None
        # 403 is *not* rate limiting, so the rate-limit counter must
        # stay at zero -- otherwise alerting on it would fire for
        # steady-state authz denials.
        assert api.report.data_model_external_reference_rate_limited == 0
        warnings = api.report.warnings
        # Status code goes into ``context`` (not ``title``) so LossyList
        # groups all non-200 orphan fetches under one stable key.
        assert any(
            w.title == "Sigma orphan Data Model fetch returned non-200"
            for w in warnings
        ), (
            f"expected a stable non-200 warning title; "
            f"got {[w.title for w in warnings]!r}"
        )
        # ``context`` is accumulated into a list on ``StructuredLogEntry``;
        # stringify for substring matching so this is robust to the
        # framework-side format (list vs str) changing shape.
        assert any("http_status=403" in str(w.context) for w in warnings), (
            f"expected http_status=403 in the warning context for triage; "
            f"got {[w.context for w in warnings]!r}"
        )

    def test_200_path_still_returns_a_data_model(self) -> None:
        """Guardrail: the added status branching must not regress the
        happy path. A 200 response still resolves and returns a
        ``SigmaDataModel`` with no warnings surfaced.
        """

        api = _create_sigma_api()
        ok = MagicMock(status_code=200)
        ok.json.return_value = {
            "dataModelUrlId": "url-orphan",
            "urlId": None,
            "dataModelId": "dm-orphan",
            "name": "Orphan DM",
            "createdAt": _dt.datetime(2024, 1, 1, tzinfo=_dt.timezone.utc),
            "updatedAt": _dt.datetime(2024, 1, 2, tzinfo=_dt.timezone.utc),
        }
        with (
            patch.object(api, "_get_api_call", return_value=ok),
            patch.object(api, "_assemble_data_model"),
        ):
            result = api.get_data_model_by_url_id("url-orphan")
        assert result is not None
        assert result.dataModelId == "dm-orphan"
        assert api.report.data_model_external_reference_rate_limited == 0
        assert api.report.warnings == []


class TestMaxPersonalDmDiscoveryRoundsBounds:
    """PR2 review M2: ``max_personal_dm_discovery_rounds`` is a safety
    cap; ``0`` / negative values are rejected by pydantic (``ge=1``)
    at config-parse time with a clear error. The cap warning only
    fires when the abandoned unresolved set is non-empty (i.e. when
    the cap is actually cutting off work), so operators who set low
    values for fast-termination do not see spurious warnings on every
    run. Disabling discovery entirely is documented via
    ``ingest_shared_entities: False``.
    """

    def test_zero_rejected(self) -> None:
        with pytest.raises(Exception) as exc_info:
            SigmaSourceConfig(
                client_id="c",
                client_secret="s",
                max_personal_dm_discovery_rounds=0,
            )
        # Pydantic V2 raises ``ValidationError``; the exact class depends
        # on the pydantic version this repo pins, so match on the
        # message instead of the type.
        assert "max_personal_dm_discovery_rounds" in str(exc_info.value)

    def test_negative_rejected(self) -> None:
        with pytest.raises(Exception) as exc_info:
            SigmaSourceConfig(
                client_id="c",
                client_secret="s",
                max_personal_dm_discovery_rounds=-3,
            )
        assert "max_personal_dm_discovery_rounds" in str(exc_info.value)

    def test_one_accepted(self) -> None:
        """``1`` is the minimum and matches the existing integration
        test that pins the cap-triggered warning behavior; guard
        that the pydantic bound did not off-by-one the lower edge.
        """
        cfg = SigmaSourceConfig(
            client_id="c",
            client_secret="s",
            max_personal_dm_discovery_rounds=1,
        )
        assert cfg.max_personal_dm_discovery_rounds == 1


class TestDataModelContainerUrnPlatformInstanceDisjoint:
    """PR2 review coverage gap: ``DataModelKey`` threads ``platform_instance``
    through to the DM Container URN. Without this, two ingestions against
    the same Sigma tenant under different ``platform_instance`` values
    would collide on one Container URN and silently overwrite each
    other's aspects. The invariant is "same dataModelId, different
    platform_instance => disjoint URNs." The integration-test matrix
    only covers the ``platform_instance=None`` case (because that is
    the recipe-default for the golden fixture), so this cheap unit
    pin prevents a future ``DataModelKey`` refactor from dropping
    ``instance`` out of the key without a loud test failure.
    """

    def _source(self, platform_instance: Optional[str]) -> SigmaSource:
        cfg = SigmaSourceConfig(
            client_id="c",
            client_secret="s",
            platform_instance=platform_instance,
        )
        with patch.object(SigmaAPI, "_generate_token"):
            return SigmaSource(ctx=PipelineContext(run_id="test"), config=cfg)

    def test_dm_container_urns_disjoint_across_platform_instances(self) -> None:
        prod = self._source("prod")
        staging = self._source("staging")
        none_inst = self._source(None)

        dm_id = "dm-same-id"
        prod_urn = prod._gen_data_model_key(dm_id).as_urn()
        staging_urn = staging._gen_data_model_key(dm_id).as_urn()
        none_urn = none_inst._gen_data_model_key(dm_id).as_urn()

        # Three distinct URNs -- the whole point of platform_instance.
        assert len({prod_urn, staging_urn, none_urn}) == 3, (
            f"DM Container URNs collided across platform_instance values: "
            f"prod={prod_urn!r}, staging={staging_urn!r}, none={none_urn!r}. "
            "DataModelKey.instance is likely not being honored."
        )


class TestPaginatorWarningTitleIncludesStatusCode:
    """PR2 review M1 (minor part): the paginator's exception-path
    warning has a stable title so LossyList can group all aborts
    under one key; the HTTP status code is surfaced in ``context``
    so operators can triage "429 on page N" vs "malformed JSON"
    without the title changing per-call.
    """

    def test_429_after_retries_emits_http_status_in_title(self) -> None:
        api = _create_sigma_api()
        rate_limited = MagicMock(status_code=429)
        http_error = requests.exceptions.HTTPError("429 Too Many Requests")
        http_error.response = rate_limited
        rate_limited.raise_for_status.side_effect = http_error
        with patch.object(api, "_get_api_call", return_value=rate_limited):
            entries = api._paginated_raw_entries(
                "https://api.example.com/dataModels",
                "Unable to fetch sigma data models.",
            )
        assert entries == []
        # Title is stable so LossyList groups all paginator aborts.
        titles = [w.title or "" for w in api.report.warnings]
        assert any(t == "Sigma paginated endpoint aborted" for t in titles), (
            f"expected stable 'Sigma paginated endpoint aborted' title; "
            f"got titles {titles!r}"
        )
        # HTTP status goes into context so operators can triage per-call.
        assert any("http_status=429" in str(w.context) for w in api.report.warnings), (
            f"expected 'http_status=429' in the paginator warning context "
            f"so operators can triage rate-limited pages specifically; "
            f"got contexts {[w.context for w in api.report.warnings]!r}"
        )


class TestCustomSqlDuplicateNameOverwrite:
    def test_second_definition_wins_and_warning_emitted(self) -> None:
        """Two same-name customSQL entries: second wins; report.warnings fires."""
        api = _create_sigma_api()
        dm = SigmaDataModel(
            dataModelId="dm-uuid",
            name="Test DM",
            createdAt=_dt.datetime(2024, 1, 1, tzinfo=_dt.timezone.utc),
            updatedAt=_dt.datetime(2024, 1, 1, tzinfo=_dt.timezone.utc),
        )

        lineage_entries = [
            {
                "name": "csql-1",
                "type": "customSQL",
                "connectionId": "conn-1",
                "definition": "SELECT A FROM DB.S.T1",
            },
            {
                "name": "csql-1",
                "type": "customSQL",
                "connectionId": "conn-1",
                "definition": "SELECT B FROM DB.S.T2",
            },
        ]

        with (
            patch.object(api, "_get_data_model_elements", return_value=[]),
            patch.object(api, "_get_data_model_columns", return_value=[]),
            patch.object(
                api, "_get_data_model_lineage_entries", return_value=lineage_entries
            ),
        ):
            api._assemble_data_model(dm, file_meta=None)

        assert dm.custom_sql_by_name["csql-1"].definition == "SELECT B FROM DB.S.T2"
        assert any(
            "duplicate" in (w.title or "").lower()
            or "duplicate" in (w.message or "").lower()
            for w in api.report.warnings
        )


class TestGetWorkbookLineageHttp:
    """HTTP-level dispatch for get_workbook_lineage: 200, 404, 429, 5xx, exception."""

    def test_200_returns_entries(self) -> None:
        api = _create_sigma_api()
        ok = MagicMock(status_code=200)
        ok.json.return_value = {
            "entries": [
                {
                    "type": "table",
                    "name": "MY_TABLE",
                    "connectionId": "conn-1",
                    "inodeId": "inode-1",
                }
            ],
            "nextPage": None,
        }
        with patch.object(api, "_get_api_call", return_value=ok):
            result = api.get_workbook_lineage("wb-1")
        assert result == [
            WorkbookLineageTableEntry(
                type="table", name="MY_TABLE", connectionId="conn-1", inodeId="inode-1"
            )
        ]
        assert not api.report.warnings

    def test_200_paginates(self) -> None:
        api = _create_sigma_api()
        page1 = MagicMock(status_code=200)
        page1.json.return_value = {
            "entries": [
                {
                    "type": "table",
                    "name": "T1",
                    "connectionId": "conn-1",
                    "inodeId": "inode-1",
                }
            ],
            "nextPage": "p2",
        }
        page2 = MagicMock(status_code=200)
        page2.json.return_value = {
            "entries": [
                {
                    "type": "table",
                    "name": "T2",
                    "connectionId": "conn-1",
                    "inodeId": "inode-2",
                }
            ],
            "nextPage": None,
        }
        with patch.object(api, "_get_api_call", side_effect=[page1, page2]):
            result = api.get_workbook_lineage("wb-1")
        assert result is not None
        assert len(result) == 2
        assert result[0].name == "T1"
        assert result[1].name == "T2"

    def test_404_returns_none_silently(self) -> None:
        api = _create_sigma_api()
        not_found = MagicMock(status_code=404)
        with patch.object(api, "_get_api_call", return_value=not_found):
            result = api.get_workbook_lineage("wb-deleted")
        assert result is None
        assert not api.report.warnings

    def test_429_returns_none_and_warns(self) -> None:
        api = _create_sigma_api()
        rate_limited = MagicMock(status_code=429)
        with patch.object(api, "_get_api_call", return_value=rate_limited):
            result = api.get_workbook_lineage("wb-1")
        assert result is None
        assert any(
            "rate-limited" in (w.title or "").lower() for w in api.report.warnings
        )

    def test_5xx_returns_none_and_warns(self) -> None:
        api = _create_sigma_api()
        server_error = MagicMock(status_code=500)
        with patch.object(api, "_get_api_call", return_value=server_error):
            result = api.get_workbook_lineage("wb-1")
        assert result is None
        assert api.report.warnings

    def test_exception_returns_none_and_warns(self) -> None:
        api = _create_sigma_api()
        with patch.object(
            api, "_get_api_call", side_effect=Exception("network failure")
        ):
            result = api.get_workbook_lineage("wb-1")
        assert result is None
        assert api.report.warnings


def _response(status_code: int, json_body: Any = None) -> MagicMock:
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = json_body
    return response


class TestGetDatasetSources:
    """/datasets/{id}/sources: a bare list, on a deprecated endpoint."""

    def test_returns_entries_on_200(self) -> None:
        api = _create_sigma_api()
        entries = [{"type": "table", "inodeId": "inode-1"}]
        with patch.object(api, "_get_api_call", return_value=_response(200, entries)):
            assert api.get_dataset_sources("ds-1") == entries

    def test_non_list_body_is_a_failure(self) -> None:
        # The envelope every other Sigma endpoint uses. Coercing it would
        # silently resolve zero sources instead of surfacing the change.
        # Individual entries are validated by the caller, not here.
        api = _create_sigma_api()
        with patch.object(
            api, "_get_api_call", return_value=_response(200, {"entries": []})
        ):
            assert api.get_dataset_sources("ds-1") is None
        assert api.report.dataset_sources_lookup_failed == 1

    def test_non_200_counts_as_failure(self) -> None:
        api = _create_sigma_api()
        with patch.object(api, "_get_api_call", return_value=_response(500)):
            assert api.get_dataset_sources("ds-1") is None
        assert api.report.dataset_sources_lookup_failed == 1

    def test_429_counted_separately(self) -> None:
        api = _create_sigma_api()
        with patch.object(api, "_get_api_call", return_value=_response(429)):
            assert api.get_dataset_sources("ds-1") is None
        assert api.report.dataset_sources_lookup_rate_limited == 1
        # Sub-bucket: the aggregate counts it too, matching the convention the
        # other Sigma rate-limit counters follow.
        assert api.report.dataset_sources_lookup_failed == 1

    def test_exception_is_contained(self) -> None:
        api = _create_sigma_api()
        with patch.object(api, "_get_api_call", side_effect=requests.RequestException):
            assert api.get_dataset_sources("ds-1") is None
        assert api.report.dataset_sources_lookup_failed == 1

    # --- not-found handling ------------------------------------------------
    # Routed by URL rather than by call order: the sequence depends on whether a
    # known-good reference exists, so an ordered mock hides mix-ups between "the
    # dataset that failed" and "the dataset that worked".

    @staticmethod
    def _router(
        sources: Dict[str, int], datasets: Optional[Dict[str, int]] = None
    ) -> Any:
        """Serve /sources and /datasets/{id} from per-dataset status maps.

        Any id absent from a map defaults to 200.
        """
        # `is None`, not `or {}`: an empty dict passed in is falsy, and
        # rebinding it would detach the closure from a caller that mutates the
        # map mid-test to simulate a dataset being archived.
        if datasets is None:
            datasets = {}

        def route(url: str) -> MagicMock:
            if url.endswith("/sources"):
                ds = url.rsplit("/", 2)[-2]
                code = sources.get(ds, 200)
                return _response(code, [] if code == 200 else None)
            ds = url.rsplit("/", 1)[-1]
            code = datasets.get(ds, 200)
            return _response(code, {} if code == 200 else None)

        return route

    def test_410_latches_immediately(self) -> None:
        # Unambiguous: no probe, and later datasets are skipped outright.
        api = _create_sigma_api()
        with patch.object(
            api, "_get_api_call", side_effect=self._router({"a": 410})
        ) as mocked:
            assert api.get_dataset_sources("a") is None
            assert api.get_dataset_sources("b") is None
            assert mocked.call_count == 1
        assert api.report.dataset_sources_endpoint_removed == 1
        assert api.report.dataset_sources_skipped_endpoint_gone == 1

    @pytest.mark.parametrize("status", [404, 409])
    def test_not_found_with_the_dataset_api_alive_is_one_dataset(
        self, status: int
    ) -> None:
        # Nothing has succeeded, so the fallback asks whether the API path is
        # there. It is, so only this dataset is missing. 409 counts because
        # Sigma answers 409 inode_archived, not 404 (verified live).
        api = _create_sigma_api()
        with patch.object(
            api, "_get_api_call", side_effect=self._router({"a": status})
        ):
            assert api.get_dataset_sources("a") is None
            assert api.get_dataset_sources("b") == []
        assert api.report.dataset_sources_endpoint_removed == 0
        assert api.report.dataset_sources_not_found == 1
        assert api.report.dataset_sources_lookup_failed == 1

    def test_not_found_with_the_api_path_gone_latches(self) -> None:
        # Nothing has succeeded and /datasets/{id} is 404 too: path-level.
        api = _create_sigma_api()
        with patch.object(
            api,
            "_get_api_call",
            side_effect=self._router({"a": 404}, datasets={"a": 404}),
        ):
            assert api.get_dataset_sources("a") is None
        assert api.report.dataset_sources_endpoint_removed == 1

    def test_reference_dataset_still_there_means_the_endpoint_went(self) -> None:
        # "a" resolved, so it is the reference. Later everything 404s including
        # the reference, while the reference dataset itself still exists.
        api = _create_sigma_api()
        sources: Dict[str, int] = {}
        with patch.object(api, "_get_api_call", side_effect=self._router(sources)):
            assert api.get_dataset_sources("a") == []
            sources.update({"a": 404, "b": 404})
            assert api.get_dataset_sources("b") is None
            assert api.get_dataset_sources("c") is None  # skipped
        assert api.report.dataset_sources_endpoint_removed == 1
        assert api.report.dataset_sources_skipped_endpoint_gone == 1

    def test_archived_reference_dataset_does_not_latch(self) -> None:
        # The reference is archived mid-run, which operators do while migrating.
        # That must drop the reference, not disable the route: every later
        # dataset would otherwise be skipped without a request.
        api = _create_sigma_api()
        sources: Dict[str, int] = {}
        datasets: Dict[str, int] = {}
        with patch.object(
            api, "_get_api_call", side_effect=self._router(sources, datasets)
        ):
            assert api.get_dataset_sources("a") == []
            sources.update({"a": 409, "b": 409})
            datasets["a"] = 409  # the reference itself is gone
            assert api.get_dataset_sources("b") is None
            assert api.get_dataset_sources("c") == []  # route still on
        assert api.report.dataset_sources_endpoint_removed == 0
        assert api._known_good_dataset_id != "a"

    def test_reference_still_resolving_means_one_dataset(self) -> None:
        api = _create_sigma_api()
        with patch.object(api, "_get_api_call", side_effect=self._router({"b": 404})):
            assert api.get_dataset_sources("a") == []
            assert api.get_dataset_sources("b") is None
            assert api.get_dataset_sources("c") == []
        assert api.report.dataset_sources_endpoint_removed == 0
        assert api.report.dataset_sources_not_found == 1

    def test_410_on_the_reference_reprobe_counts_as_removal(self) -> None:
        # 410 is the strongest removal signal, so it must count on the re-probe
        # too rather than being read as "this dataset is fine".
        api = _create_sigma_api()
        sources: Dict[str, int] = {}
        with patch.object(api, "_get_api_call", side_effect=self._router(sources)):
            assert api.get_dataset_sources("a") == []
            sources.update({"a": 410, "b": 404})
            assert api.get_dataset_sources("b") is None
        assert api.report.dataset_sources_endpoint_removed == 1

    def test_reprobe_exception_is_reported_not_just_logged(self) -> None:
        # The probe could not answer, so the route stays on. That must be
        # visible, or missing lineage has no accompanying signal.
        api = _create_sigma_api()
        state = {"raise": False}

        def route(url: str) -> MagicMock:
            if url.endswith("/a/sources"):
                if state["raise"]:
                    raise requests.RequestException("boom")
                return _response(200, [])
            return _response(404)

        with patch.object(api, "_get_api_call", side_effect=route):
            assert api.get_dataset_sources("a") == []
            state["raise"] = True
            assert api.get_dataset_sources("b") is None
        assert api.report.dataset_sources_endpoint_removed == 0
        assert any(
            "re-probe failed" in (w.title or "")
            for w in api.report.warnings  # type: ignore[attr-defined]
        )

    def test_every_dataset_not_found_escalates_to_a_warning(self) -> None:
        # Nothing ever succeeds and the API keeps answering, so there is no
        # reference to re-probe. Identical infos collapse into one entry, so
        # without escalation the run looks clean while losing all lineage.
        api = _create_sigma_api()
        with patch.object(
            api,
            "_get_api_call",
            side_effect=lambda url: (
                _response(409) if url.endswith("/sources") else _response(200, {})
            ),
        ):
            for i in range(_DATASET_SOURCES_NOT_FOUND_WARN_THRESHOLD):
                assert api.get_dataset_sources(f"ds-{i}") is None
        assert api.report.dataset_sources_not_found == (
            _DATASET_SOURCES_NOT_FOUND_WARN_THRESHOLD
        )
        assert len(api.report.warnings) == 1


class TestGetConnectionPath:
    """/connections/paths/{inodeId}: the connectionId + split path."""

    def test_returns_connection_and_path(self) -> None:
        api = _create_sigma_api()
        body = {"connectionId": "conn-1", "path": ["DB", "SCHEMA", "TABLE"]}
        with patch.object(api, "_get_api_call", return_value=_response(200, body)):
            result = api.get_connection_path("inode-1")
        assert result is not None
        assert result.connection_id == "conn-1"
        assert result.path == ["DB", "SCHEMA", "TABLE"]

    @pytest.mark.parametrize(
        "body",
        [
            {"path": ["DB", "SCHEMA", "TABLE"]},  # no connectionId
            {"connectionId": "", "path": ["DB", "SCHEMA", "TABLE"]},
            {"connectionId": "conn-1", "path": "DB/SCHEMA/TABLE"},  # not a list
            {"connectionId": "conn-1", "path": ["DB", ""]},  # empty segment
        ],
    )
    def test_unusable_body_is_a_failure(self, body: Dict[str, Any]) -> None:
        api = _create_sigma_api()
        with patch.object(api, "_get_api_call", return_value=_response(200, body)):
            assert api.get_connection_path("inode-1") is None
        assert api.report.connection_path_lookup_failed == 1

    def test_non_200_counts_as_failure(self) -> None:
        # 403 is the documented production failure: the credential may lack
        # permission to read /v2/connections/paths/{inodeId}.
        api = _create_sigma_api()
        with patch.object(api, "_get_api_call", return_value=_response(403)):
            assert api.get_connection_path("inode-1") is None
        assert api.report.connection_path_lookup_failed == 1
        assert api.report.connection_path_lookup_rate_limited == 0

    def test_429_counted_separately(self) -> None:
        api = _create_sigma_api()
        with patch.object(api, "_get_api_call", return_value=_response(429)):
            assert api.get_connection_path("inode-1") is None
        assert api.report.connection_path_lookup_rate_limited == 1
        assert api.report.connection_path_lookup_failed == 1

    def test_exception_is_contained(self) -> None:
        api = _create_sigma_api()
        with patch.object(api, "_get_api_call", side_effect=requests.RequestException):
            assert api.get_connection_path("inode-1") is None
        assert api.report.connection_path_lookup_failed == 1


class TestConnectionPathBodyShape:
    def test_non_object_body_is_reported_as_a_shape_problem(self) -> None:
        api = _create_sigma_api()
        with patch.object(api, "_get_api_call", return_value=_response(200, ["x"])):
            assert api.get_connection_path("inode-1") is None
        assert api.report.connection_path_lookup_failed == 1
        assert any(
            "unexpected body" in (w.title or "")
            for w in api.report.warnings  # type: ignore[attr-defined]
        )


class TestNotFoundDecisionMatrix:
    """Every path through the not-found decision, in one table.

    This exists because the logic has two similarly-named questions about
    different subjects — "is /sources failing for this dataset" versus "is the
    dataset API path gone" — and conflating them inverted the branch more than
    once during review. The table states the intended outcome for each
    combination so a future change cannot quietly redefine one of them.
    """

    @staticmethod
    def _run(
        *,
        target_src: int,
        ref_src: int = 200,
        ref_exists: int = 200,
        target_ds: int = 200,
        with_ref: bool = False,
    ) -> Dict[str, bool]:
        api = _create_sigma_api()
        warm = {"on": with_ref}

        def route(url: str) -> MagicMock:
            if url.endswith("/sources"):
                ds = url.rsplit("/", 2)[-2]
                if ds == "ref":
                    return _response(200, []) if warm["on"] else _response(ref_src)
                return _response(target_src)
            ds = url.rsplit("/", 1)[-1]
            return _response(ref_exists if ds == "ref" else target_ds, {})

        with patch.object(api, "_get_api_call", side_effect=route):
            if with_ref:
                api.get_dataset_sources("ref")  # establishes the reference
                warm["on"] = False
            api.get_dataset_sources("tgt")
        return {
            "latched": api.report.dataset_sources_endpoint_removed == 1,
            "ref_kept": api._known_good_dataset_id == "ref",
        }

    @pytest.mark.parametrize(
        ("desc", "kwargs", "latched", "ref_kept"),
        [
            # 410 is unambiguous, with or without a reference.
            ("410, no reference", {"target_src": 410}, True, False),
            (
                "410 beats a live reference",
                {"target_src": 410, "with_ref": True},
                True,
                True,
            ),
            # Nothing has succeeded: fall back to asking about the API path.
            (
                "404, API path alive",
                {"target_src": 404, "target_ds": 200},
                False,
                False,
            ),
            (
                "409, API path alive",
                {"target_src": 409, "target_ds": 200},
                False,
                False,
            ),
            ("404, API path gone", {"target_src": 404, "target_ds": 404}, True, False),
            # A reference exists: it decides.
            (
                "reference still resolves -> one dataset",
                {"target_src": 404, "with_ref": True, "ref_src": 200},
                False,
                True,
            ),
            (
                "reference dead but present -> endpoint gone",
                {
                    "target_src": 404,
                    "with_ref": True,
                    "ref_src": 404,
                    "ref_exists": 200,
                },
                True,
                True,
            ),
            (
                "reference dead and archived -> drop it, keep going",
                {
                    "target_src": 409,
                    "with_ref": True,
                    "ref_src": 409,
                    "ref_exists": 409,
                },
                False,
                False,
            ),
            (
                "reference dead, dataset API path gone (404) -> latch now",
                {
                    "target_src": 404,
                    "with_ref": True,
                    "ref_src": 404,
                    "ref_exists": 404,
                },
                True,
                True,
            ),
            (
                "reference dead, dataset API path gone (410) -> latch now",
                {
                    "target_src": 404,
                    "with_ref": True,
                    "ref_src": 404,
                    "ref_exists": 410,
                },
                True,
                True,
            ),
            (
                "reference probe inconclusive (500) -> rotate, do not latch",
                {
                    "target_src": 409,
                    "with_ref": True,
                    "ref_src": 409,
                    "ref_exists": 500,
                },
                False,
                False,
            ),
            (
                "410 on the reference re-probe -> endpoint gone",
                {
                    "target_src": 404,
                    "with_ref": True,
                    "ref_src": 410,
                    "ref_exists": 200,
                },
                True,
                True,
            ),
        ],
    )
    def test_matrix(
        self, desc: str, kwargs: Dict[str, Any], latched: bool, ref_kept: bool
    ) -> None:
        got = self._run(**kwargs)
        assert got == {"latched": latched, "ref_kept": ref_kept}, desc


def _http_error(
    status_code: int,
    *,
    json_body: Any = None,
    text: str = "",
    headers: Optional[Dict[str, str]] = None,
) -> requests.exceptions.HTTPError:
    response = MagicMock(spec=requests.Response)
    response.status_code = status_code
    response.headers = headers or {}
    response.text = text
    # Suppression reports len(.content): the bytes requests already buffered,
    # so nothing is decoded just to measure a body we are not going to quote.
    response.content = text.encode()
    response.json.side_effect = (
        (lambda: json_body) if json_body is not None else ValueError("no json")
    )
    return requests.exceptions.HTTPError(f"{status_code} Error", response=response)


_WORKBOOK_ROW: Dict[str, Any] = {
    "workbookId": "wb-1",
    "name": "WB",
    "createdBy": "u",
    "updatedBy": "u",
    "ownerId": "u",
    "createdAt": "2024-01-01T00:00:00Z",
    "updatedAt": "2024-01-01T00:00:00Z",
    "url": "http://x",
    "path": "ws",
    "latestVersion": 1,
}
_DATA_MODEL_ROW: Dict[str, Any] = {
    "dataModelId": "dm-ok",
    "urlId": "u1",
    "name": "DM",
    "createdBy": "u",
    "createdAt": "2024-01-01T00:00:00Z",
    "updatedAt": "2024-01-01T00:00:00Z",
    "url": "http://x/dm",
    "latestVersion": 1,
}
_DATASET_ROW: Dict[str, Any] = {
    "datasetId": "ds-1",
    "name": "DS",
    "description": "",
    "createdBy": "u",
    "createdAt": "2024-01-01T00:00:00Z",
    "updatedAt": "2024-01-01T00:00:00Z",
    "url": "http://x/ds-1",
}


def _fail_in_except(api: SigmaAPI, error: Exception, **kwargs: Any) -> None:
    """Call ``_log_http_error`` the way production does: inside an except."""
    try:
        raise error
    except Exception as e:
        api._log_http_error(
            message=f"Unable to fetch a thing. Exception: {e}", **kwargs
        )


def _contexts(entries: Any) -> str:
    return "".join(str(c) for entry in entries for c in entry.context)


class TestApiCallFailureReporting:
    """``_log_http_error`` is the terminal handler for most ``except`` blocks
    in the client, so a failure it does not record is invisible: it used to
    log a context-free status code and touch the report not at all."""

    @pytest.mark.parametrize(
        ("error", "expected_bucket"),
        [
            (_http_error(404), {"404": 1}),
            (ValueError("malformed json"), {"ValueError": 1}),
        ],
        ids=["http-status", "non-http-keys-by-exception-class"],
    )
    def test_every_failure_is_counted(
        self, error: Exception, expected_bucket: Dict[str, int]
    ) -> None:
        api = _create_sigma_api()
        _fail_in_except(api, error)

        assert api.report.api_call_failures_by_status_or_error == expected_bucket
        assert len(api.report.warnings) == 1

    def test_failures_bucket_by_sigma_error_code(self) -> None:
        """One status covers unrelated problems needing different fixes, so
        the status alone is not actionable."""
        api = _create_sigma_api()
        for code in ("inode_archived", "invalid_request", "inode_archived"):
            _fail_in_except(api, _http_error(400, json_body={"code": code}))

        assert api.report.api_call_failures_by_status_or_error == {"400": 3}
        assert api.report.api_call_failures_by_sigma_code == {
            "inode_archived": 2,
            "invalid_request": 1,
        }

    def test_report_warning_false_counts_without_warning(self) -> None:
        """Callers that emit a better-scoped entry suppress this warning; the
        failure must still be counted."""
        api = _create_sigma_api()
        _fail_in_except(api, _http_error(500), report_warning=False)

        assert api.report.api_call_failures_by_status_or_error == {"500": 1}
        assert len(api.report.warnings) == 0

    def test_the_warning_names_the_call_and_the_status(self) -> None:
        """The PR's headline claim: an operator can tell WHICH call failed
        without running with --debug."""
        api = _create_sigma_api()
        _fail_in_except(api, _http_error(429, headers={"Retry-After": "30"}))

        context = _contexts(api.report.warnings)
        assert "Unable to fetch a thing" in context
        assert "http_status=429" in context
        assert "retry_after=30" in context

    def test_calling_outside_an_except_is_loud(self) -> None:
        """Bucketing a non-exception under "NoneType" would be a silently
        mislabelled counter."""
        api = _create_sigma_api()
        with pytest.raises(AssertionError):
            api._log_http_error(message="no active exception")


class TestErrorBodyNeverLeaksOrRaises:
    """The body helpers run on a call that has ALREADY failed, so neither may
    raise, and neither may put upstream infrastructure detail into a report
    that is persisted and rendered in the UI.

    The raw fallback needs BOTH signals to agree -- the body parses as JSON
    AND is declared as JSON -- because Content-Type is set by whatever
    returned the body, which in the case this guards against is the proxy.
    """

    _LEAK = "internal-proxy-7.corp"

    @pytest.mark.parametrize(
        ("desc", "kwargs"),
        [
            ("honest html", {"headers": {"Content-Type": "text/html"}}),
            ("no content type", {}),
            (
                "text/plain, which a denylist of html would miss",
                {"headers": {"Content-Type": "text/plain"}},
            ),
            (
                "html LYING about being json -- caught by the parse",
                {"headers": {"Content-Type": "application/json"}},
            ),
            (
                "parseable json under a non-json type -- caught by the header",
                {
                    "headers": {"Content-Type": "text/html"},
                    "json_body": {"detail": _LEAK},
                },
            ),
            (
                "a json message under a non-json type -- no shortcut past it",
                {
                    "headers": {"Content-Type": "text/html"},
                    "json_body": {"message": _LEAK},
                },
            ),
        ],
    )
    def test_a_body_that_is_not_sigmas_own_json_is_suppressed(
        self, desc: str, kwargs: Dict[str, Any]
    ) -> None:
        api = _create_sigma_api()
        _fail_in_except(
            api, _http_error(502, text=f"<html>{self._LEAK}</html>", **kwargs)
        )

        context = _contexts(api.report.warnings)
        assert self._LEAK not in context, desc
        assert "suppressed" in context, desc

    def test_sigmas_own_message_is_preferred_over_the_raw_body(self) -> None:
        api = _create_sigma_api()
        _fail_in_except(
            api,
            _http_error(
                400,
                json_body={"code": "invalid_request", "message": "dependency cycle"},
                text=f"<html>{self._LEAK}</html>",
                headers={"Content-Type": "application/json"},
            ),
        )

        context = _contexts(api.report.warnings)
        assert "body=dependency cycle" in context
        assert self._LEAK not in context

    def test_a_json_body_without_a_message_is_truncated(self) -> None:
        api = _create_sigma_api()
        filler = "x" * 5000
        body = f'{{"detail": "{filler}"}}'
        _fail_in_except(
            api,
            _http_error(
                500,
                json_body={"detail": filler},
                text=body,
                headers={"Content-Type": "application/json"},
            ),
        )

        context = _contexts(api.report.warnings)
        assert body[:_MAX_ERROR_BODY_CHARS] in context
        assert body[: _MAX_ERROR_BODY_CHARS + 1] not in context

    def test_an_empty_body_is_reported_as_no_body_not_as_suppressed(self) -> None:
        """A bare 502 has nothing to hide; saying a body was suppressed
        invents one."""
        api = _create_sigma_api()
        _fail_in_except(api, _http_error(502, text=""))

        assert "suppressed" not in _contexts(api.report.warnings)

    def test_an_unreadable_body_does_not_mask_the_failure(self) -> None:
        api = _create_sigma_api()
        response = MagicMock(spec=requests.Response)
        response.status_code = 500
        response.headers = {}
        response.json.side_effect = ValueError("no json")
        response.content = b""
        type(response).text = PropertyMock(side_effect=RuntimeError("consumed"))
        _fail_in_except(
            api, requests.exceptions.HTTPError("500 Error", response=response)
        )

        assert "http_status=500" in _contexts(api.report.warnings)

    def test_a_non_dict_json_payload_yields_no_sigma_code(self) -> None:
        api = _create_sigma_api()
        _fail_in_except(api, _http_error(400, json_body=["not", "a", "dict"]))

        assert api.report.api_call_failures_by_sigma_code == {}


def _dead_call(api: SigmaAPI, status: int = 500) -> Any:
    return patch.object(SigmaAPI, "_get_api_call", side_effect=_http_error(status))


def _echoing_page() -> MagicMock:
    """A response whose nextPage cursor never advances -- a broken proxy."""
    page = MagicMock(status_code=200)
    page.json.return_value = {"entries": [{"id": "a"}], "nextPage": "1"}
    return page


class TestListingFailureWiring:
    """Every site that can lose entities, and which tier it belongs to.

    Run-wide losses fail the run, which is the only lever a source has over
    ``StaleEntityRemovalHandler``: it skips soft-deletion exactly when
    ``report.failures`` is non-empty. Losses scoped to one parent are counted
    instead, because that guard is tenant-wide and all-or-nothing -- failing
    on one flaky child call would freeze soft-deletion for everything, which
    accumulates orphans rather than preventing them.

    One case per site, so deleting any single recorder call fails a test.
    """

    def _run_wide(self, api: SigmaAPI, **kwargs: Any) -> None:
        api._paginated_raw_entries(
            "https://example.invalid/v2/dataModels",
            "Unable to fetch sigma data models.",
            enumerates_entities=True,
            **kwargs,
        )

    def _child(self, api: SigmaAPI, **kwargs: Any) -> None:
        api._paginated_raw_entries(
            "https://example.invalid/v2/dataModels/x/elements",
            "Unable to fetch elements for data model 'x'.",
            scoped_to_parent=True,
            **kwargs,
        )

    # (description, call, whether the caller fetches file metadata first --
    # stubbed there so the run under test records ONE failure, not two)
    _RUN_WIDE: List[Any] = [
        ("workspaces", lambda api: api.fill_workspaces(), False),
        ("datasets", lambda api: api.get_sigma_datasets(), True),
        ("workbooks", lambda api: api.get_sigma_workbooks(), True),
        (
            "file metadata, which drops every workbook",
            lambda api: api._get_files_metadata(file_type=Constant.WORKBOOK),
            False,
        ),
    ]

    @pytest.mark.parametrize(
        ("site", "call", "stub_files"), _RUN_WIDE, ids=[s for s, _, _ in _RUN_WIDE]
    )
    def test_a_run_wide_listing_failure_fails_the_run(
        self, site: str, call: Any, stub_files: bool
    ) -> None:
        api = _create_sigma_api()
        with ExitStack() as stack:
            if stub_files:
                stack.enter_context(
                    patch.object(SigmaAPI, "_get_files_metadata", return_value={})
                )
            stack.enter_context(_dead_call(api))
            call(api)

        assert api.report.entity_enumeration_failed == 1, site
        assert len(api.report.failures) == 1, site

    @pytest.mark.parametrize(
        "abort",
        ["http", "repeated-cursor"],
    )
    def test_a_paginated_listing_abort_fails_the_run(self, abort: str) -> None:
        """An echoed cursor truncates a listing exactly as an HTTP error
        does, so it takes the same accounting."""
        api = _create_sigma_api()
        if abort == "http":
            with _dead_call(api):
                self._run_wide(api)
        else:
            with patch.object(SigmaAPI, "_get_api_call", return_value=_echoing_page()):
                self._run_wide(api)

        assert api.report.pagination_aborted == 1
        assert api.report.entity_enumeration_failed == 1

    _CHILD: List[Any] = [
        (
            "a page's elements",
            lambda api: api.get_page_elements(MagicMock(), MagicMock()),
        ),
        ("a workbook's pages", lambda api: api.get_workbook_pages(_real_workbook())),
        ("a workspace lookup", lambda api: api.get_workspace("ws-1")),
        (
            "a file-path walk",
            lambda api: api.get_workspace_id_from_file_path("parent-1", "a/b"),
        ),
    ]

    @pytest.mark.parametrize(("site", "call"), _CHILD, ids=[s for s, _ in _CHILD])
    def test_a_child_listing_failure_is_counted_not_failed(
        self, site: str, call: Any
    ) -> None:
        api = _create_sigma_api()
        with _dead_call(api):
            call(api)

        assert api.report.child_entity_listing_failed == 1, site
        assert api.report.entity_enumeration_failed == 0, site
        assert len(api.report.failures) == 0, site

    @pytest.mark.parametrize("abort", ["http", "repeated-cursor"])
    def test_a_paginated_child_abort_is_counted_not_failed(self, abort: str) -> None:
        api = _create_sigma_api()
        if abort == "http":
            with _dead_call(api):
                self._child(api)
        else:
            with patch.object(SigmaAPI, "_get_api_call", return_value=_echoing_page()):
                self._child(api)

        assert api.report.pagination_aborted == 1
        assert api.report.child_entity_listing_failed == 1
        assert api.report.entity_enumeration_failed == 0

    def test_a_detail_call_is_neither(self) -> None:
        """A detail call dying leaves an entity thinner, not missing."""
        api = _create_sigma_api()
        with _dead_call(api):
            api._paginated_raw_entries(
                "https://example.invalid/v2/dataModels/x/columns",
                "Unable to fetch columns for data model 'x'.",
            )

        assert api.report.pagination_aborted == 1
        assert api.report.child_entity_listing_failed == 0
        assert len(api.report.failures) == 0
        assert len(api.report.warnings) == 1

    def test_the_child_counter_carries_an_explanation(self) -> None:
        """A bare "47" gives an operator no way to know why the run passed."""
        api = _create_sigma_api()
        api._record_child_listing_failure()

        assert len(api.report.infos) == 1


class TestRepeatedLookupsAreAskedOnce:
    """Negative caches, and the line they must not cross.

    Without them a broken parent is re-fetched once per child: the counter
    becomes lookups rather than distinct parents, and each retry re-pays the
    backoff. Applied too widely they are worse than the problem -- one blip
    latches the node for the run, so every sibling under it is dropped
    unretried and soft-deleted by a run that passes. Only a failure that
    will repeat may latch.
    """

    @pytest.mark.parametrize(
        ("status", "expected_calls"),
        [(400, 1), (500, 3), (401, 3)],
        ids=[
            "a-refusal-is-asked-once",
            "a-blip-is-asked-again",
            "a-401-after-a-failed-refresh-is-asked-again",
        ],
    )
    def test_a_broken_workspace(self, status: int, expected_calls: int) -> None:
        api = _create_sigma_api()
        with _dead_call(api, status=status) as call:
            for _ in range(3):
                assert api.get_workspace("ws-1") is None

        assert call.call_count == expected_calls
        # One per WORKSPACE either way. The lookup is re-asked on the
        # transient path, but the count is deduped separately, so the counter
        # does not mix units with the file-path walk beside it.
        assert api.report.child_entity_listing_failed == 1

    def test_an_inaccessible_workspace(self) -> None:
        """403 is a property of the workspace, not of the lookup."""
        api = _create_sigma_api()
        forbidden = MagicMock(spec=requests.Response, status_code=403)
        with patch.object(SigmaAPI, "_get_api_call", return_value=forbidden) as call:
            for _ in range(3):
                assert api.get_workspace("ws-1") is None

        assert call.call_count == 1
        assert api.report.non_accessible_workspaces_count == 1
        assert api.report.child_entity_listing_failed == 0

    @pytest.mark.parametrize(
        ("status", "expected_ancestor_calls"),
        [(400, 1), (500, 3)],
        ids=["a-refusal-is-asked-once", "a-blip-is-asked-again"],
    )
    def test_sibling_folders_under_one_broken_ancestor(
        self, status: int, expected_ancestor_calls: int
    ) -> None:
        """The repeat that actually happens, and the loss that hides in it.

        _file_path_walks already collapses the per-FILE case -- File.path is the
        FOLDER's path, so every file in a folder shares the walk's key.
        Sibling folders don't: each walks up and hits the same ancestor. A
        refusal there is the same answer every time, so it is asked once; a
        500 is not, and latching it would drop every sibling subtree for one
        bad call. 500 is deliberately absent from the retry status list, so
        nothing below re-asks it either.
        """
        api = _create_sigma_api()

        def _walk(url: str) -> Any:
            if url.endswith("/broken-ancestor"):
                raise _http_error(status)
            page = MagicMock(status_code=200)
            page.json.return_value = {"parentId": "broken-ancestor"}
            return page

        with patch.object(SigmaAPI, "_get_api_call", side_effect=_walk) as call:
            for folder in ("f1", "f2", "f3"):
                assert (
                    api.get_workspace_id_from_file_path(folder, "ws/dir/file") is None
                )

        assert (
            sum(c.args[0].endswith("/broken-ancestor") for c in call.call_args_list)
            == expected_ancestor_calls
        )
        # Counted once per ancestor either way: the walk dedupes the count
        # separately from the cache, so a re-ask does not inflate it.
        assert api.report.child_entity_listing_failed == 1

    @pytest.mark.parametrize(
        ("status", "expected_calls"),
        [(400, 1), (500, 3)],
        ids=["a-refusal-is-cached", "a-blip-is-not-cached-until-the-cap"],
    )
    def test_files_in_one_folder_after_a_failed_walk(
        self, status: int, expected_calls: int
    ) -> None:
        """Files in one folder share the walk's key, so a cached blip would
        drop every later file in it. Re-asking stops at the cap, or a node
        that keeps failing is retried once per file."""
        api = _create_sigma_api()
        with _dead_call(api, status=status) as call:
            for _ in range(5):
                assert api.get_workspace_id_from_file_path("f1", "ws/dir") is None

        assert call.call_count == expected_calls

    def test_the_walk_names_the_file_and_stays_terse(self) -> None:
        """The walk's own failure text is a report context too."""
        api = _create_sigma_api()
        with _dead_call(api):
            assert api.get_workspace_id_from_file_path("p-1", "ws/dir") is None

        assert "'ws/dir'" in _contexts(api.report.warnings)

    def test_a_data_model_walk_does_not_mask_a_workbook_loss(self) -> None:
        """Sharing one set between "known broken" and "already counted"
        silently undercounted: the DM walk marks the ancestor without
        counting -- the DM survives via its own payload -- and the workbook
        walk then short-circuits on it, though the workbook IS dropped."""
        api = _create_sigma_api()
        with _dead_call(api, status=404):
            api.get_workspace_id_from_file_path("p-1", "ws/dir", entity_removing=False)
            assert api.report.child_entity_listing_failed == 0
            api.get_workspace_id_from_file_path("p-1", "ws/other")

        assert api.report.child_entity_listing_failed == 1

    @pytest.mark.parametrize(
        ("file_type", "expected"),
        [(Constant.WORKBOOK, 1), (Constant.DATA_MODEL, 0)],
        ids=["workbook-folder-counts", "data-model-folder-does-not"],
    )
    def test_the_caller_decides_whether_a_broken_folder_counts(
        self, file_type: str, expected: int
    ) -> None:
        """Pins the CALL SITE, not just the parameter: _get_files_metadata is
        the only place that knows the file type, so it is what must pass
        entity_removing. The listing itself succeeds here; only the path walk
        inside it fails."""
        api = _create_sigma_api()
        listing = MagicMock(status_code=200)
        listing.json.return_value = {
            "entries": [
                {
                    "id": "f1",
                    "name": "f",
                    "parentId": "parent-1",
                    "path": "folder/a",
                    "type": file_type,
                }
            ],
            "nextPage": None,
        }
        calls = {"n": 0}

        def _first_ok_then_dead(url: str) -> Any:
            calls["n"] += 1
            if calls["n"] == 1:
                return listing
            raise _http_error(500)

        with patch.object(SigmaAPI, "_get_api_call", side_effect=_first_ok_then_dead):
            api._get_files_metadata(file_type=file_type)

        assert api.report.child_entity_listing_failed == expected

    def test_a_folder_holding_only_data_models_is_not_counted(self) -> None:
        """A data model survives a missing workspace by falling back to the
        /dataModels payload, so counting it would claim children went missing
        when none did."""
        api = _create_sigma_api()
        with _dead_call(api):
            assert (
                api.get_workspace_id_from_file_path(
                    "parent-1", "folder/a", entity_removing=False
                )
                is None
            )

        assert api.report.child_entity_listing_failed == 0
        assert api.report.api_call_failures_by_status_or_error == {"500": 1}


class TestOneActionableEntryPerRunWideFailure:
    """A run-wide failure produces exactly ONE report entry. The detail used
    to sit on a warning beside the failure, so the entry an operator acts on
    was the bare one."""

    def test_a_paginated_abort_emits_only_the_failure(self) -> None:
        api = _create_sigma_api()
        with _dead_call(api):
            api._paginated_raw_entries(
                "https://example.invalid/v2/dataModels",
                "Unable to fetch sigma data models.",
                enumerates_entities=True,
            )

        assert len(api.report.warnings) == 0
        context = _contexts(api.report.failures)
        for fact in ("http_status=500", "partial_results=0", "pages_read=0"):
            assert fact in context

    def test_a_repeated_cursor_emits_only_the_failure(self) -> None:
        api = _create_sigma_api()
        with patch.object(SigmaAPI, "_get_api_call", return_value=_echoing_page()):
            entries = api._paginated_raw_entries(
                "https://example.invalid/v2/dataModels",
                "Unable to fetch sigma data models.",
                enumerates_entities=True,
            )

        # Two rows from one distinct page: the mock echoes the same response,
        # so the row is collected twice before the repeated cursor is caught.
        # That duplication is pre-existing and deduped by _paginated_entries.
        assert len(entries) == 2
        assert len(api.report.warnings) == 0
        assert "repeated_cursor=" in _contexts(api.report.failures)

    def test_a_child_abort_keeps_its_warning(self) -> None:
        """There the warning is the only entry -- the counter emits an info."""
        api = _create_sigma_api()
        with _dead_call(api):
            api._paginated_raw_entries(
                "https://example.invalid/v2/dataModels/x/elements",
                "Unable to fetch elements for data model 'x'.",
                scoped_to_parent=True,
            )

        assert len(api.report.warnings) == 1
        assert len(api.report.failures) == 0

    def test_the_dataset_listing_emits_one_entry_not_three(self) -> None:
        """This site used to emit a bespoke warning, _log_http_error's
        generic warning and the failure for one event."""
        api = _create_sigma_api()
        with (
            patch.object(SigmaAPI, "_get_files_metadata", return_value={}),
            _dead_call(api, status=410),
        ):
            assert api.get_sigma_datasets() == []

        assert len(api.report.warnings) == 0
        assert len(api.report.failures) == 1
        # The fixed "a 404/410 suggests..." sentence used to live here too,
        # duplicating what the status remedy now says and eating truncation
        # budget ahead of the exception text.
        context = _contexts(api.report.failures)
        assert "gone or was never present" in context
        assert "ingest_datasets=False" in context
        assert api.report.datasets_listing_failed == 1


class TestFailureRemedies:
    """The failure message is fixed so report_log can group it, so the
    per-listing remedy lives in the context -- and leads it, because the
    context is truncated at 1000 chars."""

    @pytest.mark.parametrize(
        "toggle",
        [None, "ingest_datasets=False"],
        ids=["mandatory-listing", "optional-listing"],
    )
    @pytest.mark.parametrize(
        ("status", "transient", "expect", "toggle_allowed"),
        [
            (401, True, "rejected this connector's credentials", False),
            (403, False, "Grant the token the scope", True),
            (429, True, "Transient", False),
            (404, False, "gone or was never present", True),
            (500, True, "Transient", False),
            (None, True, "Transient", False),
            (None, False, "Neither refused nor transient", True),
            (418, False, "Neither refused nor transient", True),
        ],
        ids=[
            "rejected",
            "refused",
            "rate-limited",
            "gone",
            "server-error",
            "no-response",
            "internal-error",
            "odd",
        ],
    )
    def test_the_remedy_follows_the_status(
        self,
        status: Optional[int],
        transient: bool,
        expect: str,
        toggle_allowed: bool,
        toggle: Optional[str],
    ) -> None:
        """The old default told every untagged failure -- 500s, timeouts,
        connection errors, internal bugs -- to go and grant a scope.

        The toggle dimension is the one that matters: the branch used to sit
        ABOVE the status checks, so every listing that had one answered a
        500 or a timeout with "or set ingest_datasets=False" -- soft-deleting
        every object of that kind for an outage a re-run would fix.
        """
        api = _create_sigma_api()
        api._record_enumeration_failure(
            what="a listing",
            context="ctx",
            status=status,
            transient=transient,
            optional_feature=toggle,
        )

        context = _contexts(api.report.failures)
        assert expect in context
        if toggle and toggle_allowed:
            assert toggle in context
            assert "soft-deleting the objects you stop ingesting" in context
        else:
            assert "ingest_datasets" not in context
        # A listing with no toggle must not be sent after a setting that
        # would not help it.
        if toggle is None and status in (404, 410):
            assert "cannot be turned off" in context

    @pytest.mark.parametrize(
        ("status", "expect"),
        [(403, "Grant the token the scope"), (500, "Transient")],
        ids=["refused", "server-error"],
    )
    def test_a_paginated_listing_abort_carries_its_status(
        self, status: int, expect: str
    ) -> None:
        """/dataModels is the only paginated run-wide listing, and it reaches
        _record_enumeration_failure by a different path -- without its status
        a 403 there would read as "transient, re-run"."""
        api = _create_sigma_api()
        with _dead_call(api, status=status):
            api._paginated_raw_entries(
                "https://example.invalid/v2/dataModels",
                "Unable to fetch sigma data models.",
                enumerates_entities=True,
                optional_feature="ingest_data_models=False",
            )

        context = _contexts(api.report.failures)
        assert expect in context
        if status == 500:
            assert "ingest_data_models" not in context

    @pytest.mark.parametrize(
        ("site", "call", "stub_files"),
        [
            ("workspaces", lambda api: api.fill_workspaces(), False),
            ("datasets", lambda api: api.get_sigma_datasets(), True),
            ("workbooks", lambda api: api.get_sigma_workbooks(), True),
            (
                "file metadata",
                lambda api: api._get_files_metadata(file_type=Constant.WORKBOOK),
                False,
            ),
        ],
        ids=["workspaces", "datasets", "workbooks", "file-metadata"],
    )
    def test_every_listing_carries_its_status(
        self, site: str, call: Any, stub_files: bool
    ) -> None:
        """One wiring line per listing, and each is invisible without a test:
        drop it and a 403 there reads "transient, re-run" instead of "grant
        the scope". /workspaces in particular is a plausible place for a
        scope-limited token to be refused."""
        api = _create_sigma_api()
        with ExitStack() as stack:
            if stub_files:
                # Only where the caller fetches it first -- stubbing it for
                # the file-metadata case would replace the method under test.
                stack.enter_context(
                    patch.object(SigmaAPI, "_get_files_metadata", return_value={})
                )
            stack.enter_context(_dead_call(api, status=403))
            call(api)

        context = _contexts(api.report.failures)
        assert "Grant the token the scope" in context, site
        # The distinction cuts both ways: a refusal is not a bad row.
        assert "cannot parse" not in context, site

    def test_a_repeated_cursor_is_a_response_problem_not_an_outage(self) -> None:
        """Sigma answered; its pagination is what came back wrong."""
        api = _create_sigma_api()
        with patch.object(SigmaAPI, "_get_api_call", return_value=_echoing_page()):
            api._paginated_raw_entries(
                "https://example.invalid/v2/dataModels",
                "Unable to fetch sigma data models.",
                enumerates_entities=True,
            )

        context = _contexts(api.report.failures)
        assert "did not have the shape" in context
        assert "Transient" not in context

    def test_the_remedy_survives_context_truncation(self) -> None:
        """A proxied api_url plus a Retry-After header pushed a trailing
        remedy past the cut, losing the actionable half of the entry the
        fixed message points at."""
        api = _create_sigma_api()
        api._record_enumeration_failure(
            what="Sigma data models",
            context="x" * 2000,
            status=410,
            optional_feature="ingest_data_models=False",
        )

        assert "ingest_data_models=False" in _contexts(api.report.failures)

    @pytest.mark.parametrize(
        ("file_type", "expect"),
        [
            (Constant.DATASET, "ingest_datasets=False"),
            (Constant.WORKBOOK, "cannot be turned off"),
        ],
    )
    def test_file_metadata_remedies_match_the_file_type(
        self, file_type: str, expect: str
    ) -> None:
        """The dataset call is only reachable from get_sigma_datasets, which
        the toggle skips; the workbook one has no toggle. Shown on a 410,
        since that is the status where a toggle is the remedy at all."""
        api = _create_sigma_api()
        with _dead_call(api, status=410):
            assert api._get_files_metadata(file_type=file_type) == {}

        assert expect in _contexts(api.report.failures)

    @pytest.mark.parametrize(
        ("desc", "body", "expect"),
        [
            (
                "a bad row",
                {"entries": [{"id": "f-1"}], "nextPage": None},
                "cannot parse",
            ),
            ("a page with no entries", {"total": 0}, "did not have the shape"),
            (
                "a null row, which must not raise past the handler",
                {"entries": [None], "nextPage": None},
                "row id not present in the payload",
            ),
        ],
    )
    def test_file_metadata_classifies_its_own_failures(
        self, desc: str, body: Dict[str, Any], expect: str
    ) -> None:
        """The fourth hand-written listing. Its failure drops every workbook
        and repeats every run, so "grant the token the scope" would be a
        permanently wrong instruction on a permanently red run."""
        api = _create_sigma_api()
        page = MagicMock(status_code=200)
        page.json.return_value = body
        with patch.object(SigmaAPI, "_get_api_call", return_value=page):
            assert api._get_files_metadata(file_type=Constant.WORKBOOK) == {}

        context = _contexts(api.report.failures)
        assert expect in context, desc
        assert "Grant the token the scope" not in context
        assert "input_value" not in context

    @pytest.mark.parametrize(
        ("desc", "pages"),
        [
            ("page 2 dies after page 1 parsed", 2),
            ("page 1 dies with no payload at all", 1),
        ],
    )
    def test_an_http_failure_names_no_row(self, desc: str, pages: int) -> None:
        """The listings hold the last row for the whole listing, so on an
        HTTP failure it named whatever parsed fine just before -- "id=f-1,
        http_status=500" accuses an innocent row -- or reported no id at all
        when there had been no payload."""
        api = _create_sigma_api()
        first = MagicMock(status_code=200)
        first.json.return_value = {
            "entries": [
                {
                    "id": "f-1",
                    "name": "f",
                    "parentId": "p",
                    "path": "ws",
                    "type": "workbook",
                }
            ],
            "nextPage": 2,
        }
        calls = {"n": 0}

        def _then_dead(url: str) -> Any:
            calls["n"] += 1
            if calls["n"] < pages:
                return first
            raise _http_error(500)

        with patch.object(SigmaAPI, "_get_api_call", side_effect=_then_dead):
            api._get_files_metadata(file_type=Constant.WORKBOOK)

        context = _contexts(api.report.failures)
        assert "http_status=500" in context, desc
        assert "id=f-1" not in context, desc
        assert "row id not present" not in context, desc

    def test_file_metadata_keeps_the_pages_it_already_read(self) -> None:
        """Returning {} discarded rows that were read fine, dropping every
        workbook rather than only the ones the listing never reached. The
        other three listings already keep theirs; the run fails either way,
        so nothing is soft-deleted on the strength of a short map."""
        api = _create_sigma_api()
        first = MagicMock(status_code=200)
        first.json.return_value = {
            "entries": [
                {
                    "id": "f-1",
                    "name": "f",
                    "parentId": "p",
                    "path": "ws",
                    "type": "workbook",
                }
            ],
            "nextPage": 2,
        }
        with patch.object(
            SigmaAPI, "_get_api_call", side_effect=[first, _http_error(500)]
        ):
            got = api._get_files_metadata(file_type=Constant.WORKBOOK)

        assert list(got) == ["f-1"]
        assert api.report.entity_enumeration_failed == 1

    def test_file_metadata_names_the_offending_row(self) -> None:
        api = _create_sigma_api()
        page = MagicMock(status_code=200)
        page.json.return_value = {"entries": [{"id": "f-7"}], "nextPage": None}
        with patch.object(SigmaAPI, "_get_api_call", return_value=page):
            api._get_files_metadata(file_type=Constant.WORKBOOK)

        assert "id=f-7" in _contexts(api.report.failures)

    def test_data_model_file_metadata_keeps_its_warning(self) -> None:
        """No failure is recorded for data-model -- they fall back to their
        own payload -- so suppressing the warning too would leave the failure
        with no report entry at all."""
        api = _create_sigma_api()
        with _dead_call(api):
            assert api._get_files_metadata(file_type=Constant.DATA_MODEL) == {}

        assert len(api.report.failures) == 0
        assert len(api.report.warnings) == 1


class TestRetryPolicy:
    def test_500_is_not_retried(self) -> None:
        """Sigma answers 500 from the per-element lineage endpoint as its
        ordinary "no lineage metadata" reply. Retrying turns a routine answer
        into 4 requests and ~12s of backoff, once per element."""
        api = _create_sigma_api()
        adapter = api.session.get_adapter("https://example.invalid")
        assert isinstance(adapter, HTTPAdapter)

        assert 500 not in adapter.max_retries.status_forcelist
        assert {429, 502, 503, 504} <= set(adapter.max_retries.status_forcelist)

    def test_only_gets_are_replayed(self) -> None:
        """The token and refresh calls are POSTs and must not be replayed."""
        api = _create_sigma_api()
        adapter = api.session.get_adapter("https://example.invalid")
        assert isinstance(adapter, HTTPAdapter)
        allowed = adapter.max_retries.allowed_methods

        assert allowed is not None and allowed is not False
        assert set(allowed) == {"GET"}


def test_silent_statuses_cannot_hide_a_run_wide_listing() -> None:
    """Swallowing a status on a listing would report zero rows for a live
    endpoint -- the exact bug this guard exists to stop."""
    api = _create_sigma_api()
    with pytest.raises(AssertionError):
        api._paginated_raw_entries(
            "https://example.invalid/v2/dataModels",
            "Unable to fetch sigma data models.",
            silent_statuses=(404,),
            enumerates_entities=True,
        )


class TestPartialAndMalformedListings:
    """Two ways a listing loses entities without dying outright."""

    def test_a_dropped_row_on_a_run_wide_listing_fails_the_run(self) -> None:
        """A malformed row is an entity missing from this run, which
        stale-entity removal reads as deleted -- the same loss as an aborted
        listing, one row at a time. Reported once per endpoint, not per row."""
        api = _create_sigma_api()
        page = MagicMock(status_code=200)
        page.json.return_value = {
            "entries": [{"bad": 1}, {"bad": 2}],
            "nextPage": None,
        }
        with patch.object(SigmaAPI, "_get_api_call", return_value=page):
            assert (
                api._paginated_entries(
                    "https://example.invalid/v2/dataModels",
                    SigmaDataModel,
                    "Unable to fetch sigma data models.",
                    enumerates_entities=True,
                )
                == []
            )

        assert api.report.pagination_malformed_entries_dropped == 2
        assert api.report.entity_enumeration_failed == 1
        assert len(api.report.failures) == 1

    def test_a_dropped_row_on_a_child_listing_does_not(self) -> None:
        api = _create_sigma_api()
        page = MagicMock(status_code=200)
        page.json.return_value = {"entries": [{"bad": 1}], "nextPage": None}
        with patch.object(SigmaAPI, "_get_api_call", return_value=page):
            api._paginated_entries(
                "https://example.invalid/v2/dataModels/x/elements",
                SigmaDataModelElement,
                "Unable to fetch elements for data model 'x'.",
                scoped_to_parent=True,
            )

        assert api.report.pagination_malformed_entries_dropped == 1
        assert api.report.entity_enumeration_failed == 0

    @pytest.mark.parametrize(
        ("call", "entries_key"),
        [
            (lambda api: api.get_sigma_workbooks(), "workbooks"),
            (lambda api: api.get_sigma_datasets(), "datasets"),
        ],
        ids=["workbooks", "datasets"],
    )
    def test_pages_read_before_the_failure_are_kept(
        self, call: Any, entries_key: str
    ) -> None:
        """Returning [] made sense while the run still passed. Now the run
        fails either way, so discarding good pages only makes those entities
        go stale behind the failure."""
        api = _create_sigma_api()
        first = MagicMock(status_code=200)
        first.json.return_value = {
            "entries": [_WORKBOOK_ROW if entries_key == "workbooks" else _DATASET_ROW],
            "nextPage": 2,
        }
        calls = {"n": 0}

        def _one_good_page(url: str) -> Any:
            calls["n"] += 1
            if calls["n"] == 1:
                return first
            raise _http_error(500)

        # Pre-resolved so the workspace lookup does not also fail and drop
        # the row for an unrelated reason.
        api.workspaces["ws-1"] = Workspace(
            workspaceId="ws-1",
            name="WS",
            createdBy="u",
            createdAt=_dt.datetime(2024, 1, 1, tzinfo=_dt.timezone.utc),
            updatedAt=_dt.datetime(2024, 1, 2, tzinfo=_dt.timezone.utc),
        )
        entity_id = "wb-1" if entries_key == "workbooks" else "ds-1"
        files = {
            entity_id: File(
                id=entity_id,
                name="f",
                parentId="p",
                path="ws",
                type=entries_key[:-1],
                workspaceId="ws-1",
            )
        }
        with (
            patch.object(SigmaAPI, "_get_files_metadata", return_value=files),
            patch.object(SigmaAPI, "_get_api_call", side_effect=_one_good_page),
        ):
            got = call(api)

        assert len(got) == 1, "the page read before the failure was discarded"
        assert api.report.entity_enumeration_failed == 1


class TestUnparseableRowAdvice:
    """A malformed row means the call SUCCEEDED and this connector could not
    read the answer. Scope advice is wrong (nothing was refused) and toggle
    advice is worse (turning the feature off soft-deletes every object of
    that kind to avoid losing one). It also repeats identically every run, so
    the advice has to be something that actually clears it."""

    def _malformed_listing(self, api: SigmaAPI) -> None:
        page = MagicMock(status_code=200)
        page.json.return_value = {
            "entries": [{"dataModelId": "dm-7", "bad": 1}],
            "nextPage": None,
        }
        with patch.object(SigmaAPI, "_get_api_call", return_value=page):
            api._paginated_entries(
                "https://example.invalid/v2/dataModels",
                SigmaDataModel,
                "Unable to fetch sigma data models.",
                enumerates_entities=True,
                optional_feature="ingest_data_models=False",
            )

    def test_the_advice_is_not_about_scopes_or_toggles(self) -> None:
        api = _create_sigma_api()
        self._malformed_listing(api)

        context = _contexts(api.report.failures)
        assert "cannot parse" in context
        assert "upgrade the connector" in context
        # It may SAY granting a scope won't help; it must not advise it.
        assert "grant the token the scope" not in context
        assert "set ingest_data_models=False" not in context

    def test_the_offending_row_is_named(self) -> None:
        """A validation error names the FIELD, never the object, so without
        this an operator cannot find the row."""
        api = _create_sigma_api()
        self._malformed_listing(api)

        assert "dataModelId=dm-7" in _contexts(api.report.failures)

    @pytest.mark.parametrize(
        ("call", "row", "toggle"),
        [
            (lambda api: api.get_sigma_workbooks(), {"workbookId": "wb-1"}, None),
            (
                lambda api: api.get_sigma_datasets(),
                {"datasetId": "ds-1"},
                "ingest_datasets=False",
            ),
            (lambda api: api.fill_workspaces(), {"workspaceId": "ws-1"}, None),
        ],
        ids=["workbooks", "datasets", "workspaces"],
    )
    def test_every_hand_written_listing_gets_the_same_advice(
        self, call: Any, row: Dict[str, Any], toggle: Optional[str]
    ) -> None:
        """These three validate rows inside the listing's own try, so a bad
        row lands on the generic path with the wrong advice unless each site
        tags it. Workspace is the sharpest case: its validator read
        values["name"] directly, so a row missing it escaped as a bare
        KeyError and was reported as a malformed RESPONSE.
        """
        api = _create_sigma_api()
        page = MagicMock(status_code=200)
        page.json.return_value = {
            "entries": [{**row, "junk": "z" * 500}],
            "nextPage": None,
        }
        with (
            patch.object(SigmaAPI, "_get_files_metadata", return_value={}),
            patch.object(SigmaAPI, "_get_api_call", return_value=page),
        ):
            call(api)

        context = _contexts(api.report.failures)
        assert "cannot parse" in context
        assert "did not have the shape" not in context
        assert next(iter(row)) + "=" in context
        assert "Grant the token the scope" not in context
        # Turning datasets off to recover from one bad row would soft-delete
        # every one of them.
        if toggle:
            assert toggle not in context
        # pydantic echoes the whole row once per missing field, which fills
        # the 1000-char context cap on its own.
        assert "input_value" not in context
        assert "For further information visit" not in context

    def test_a_malformed_page_is_not_a_malformed_row(self) -> None:
        """A page with no `entries` costs every later page, not one entity,
        and an endpoint retired by changing shape rather than by a 404 is
        exactly when the toggle IS the fix."""
        api = _create_sigma_api()
        page = MagicMock(status_code=200)
        page.json.return_value = {"total": 0}  # no `entries`
        with (
            patch.object(SigmaAPI, "_get_files_metadata", return_value={}),
            patch.object(SigmaAPI, "_get_api_call", return_value=page),
        ):
            api.get_sigma_datasets()

        context = _contexts(api.report.failures)
        assert "did not have the shape" in context
        assert "every later page" in context
        assert "ingest_datasets=False" in context
        assert "cannot parse" not in context

    def test_one_bad_row_among_good_ones_is_counted_not_failed(self) -> None:
        """One bad row is the most bounded loss there is -- one entity -- and
        a row that fails to parse fails identically on every run, so failing
        would freeze soft-deletion tenant-wide with no remedy."""
        api = _create_sigma_api()
        page = MagicMock(status_code=200)
        page.json.return_value = {
            "entries": [{"dataModelId": "dm-bad"}, _DATA_MODEL_ROW],
            "nextPage": None,
        }
        with patch.object(SigmaAPI, "_get_api_call", return_value=page):
            got = api._paginated_entries(
                "https://example.invalid/v2/dataModels",
                SigmaDataModel,
                "Unable to fetch sigma data models.",
                enumerates_entities=True,
            )

        assert len(got) == 1
        assert api.report.pagination_malformed_entries_dropped == 1
        assert api.report.entity_enumeration_failed == 0
        assert len(api.report.failures) == 0

    def test_every_row_bad_is_indistinguishable_from_a_dead_listing(self) -> None:
        """The listing yielded nothing: the vendor changed a field, or the
        connector is behind. That is not a bounded loss."""
        api = _create_sigma_api()
        page = MagicMock(status_code=200)
        page.json.return_value = {
            "entries": [{"dataModelId": "dm-1"}, {"dataModelId": "dm-2"}],
            "nextPage": None,
        }
        with patch.object(SigmaAPI, "_get_api_call", return_value=page):
            assert (
                api._paginated_entries(
                    "https://example.invalid/v2/dataModels",
                    SigmaDataModel,
                    "Unable to fetch sigma data models.",
                    enumerates_entities=True,
                )
                == []
            )

        assert api.report.entity_enumeration_failed == 1
        context = _contexts(api.report.failures)
        assert "rows_dropped=2" in context
        assert "dataModelId=dm-1" in context
        assert "cannot parse" in context

    def test_a_dead_listing_is_one_failure_even_if_its_rows_were_bad_too(
        self,
    ) -> None:
        """Page 1 holding only bad rows and page 2 then dying is ONE dead
        listing. Filed twice, the entries contradict each other -- "transient,
        re-run" beside "repeats every run" -- and the counter doubles."""
        api = _create_sigma_api()
        first = MagicMock(status_code=200)
        first.json.return_value = {
            "entries": [{"dataModelId": "dm-bad"}],
            "nextPage": "2",
        }
        with patch.object(
            SigmaAPI, "_get_api_call", side_effect=[first, _http_error(500)]
        ):
            assert (
                api._paginated_entries(
                    "https://example.invalid/v2/dataModels",
                    SigmaDataModel,
                    "Unable to fetch sigma data models.",
                    enumerates_entities=True,
                )
                == []
            )

        assert api.report.entity_enumeration_failed == 1
        context = _contexts(api.report.failures)
        assert "Transient" in context
        assert "cannot parse" not in context

    @pytest.mark.parametrize(
        ("listing", "stub_files"),
        [
            pytest.param(
                lambda api: api._paginated_raw_entries(
                    "https://example.invalid/v2/dataModels",
                    "Unable to fetch sigma data models.",
                    enumerates_entities=True,
                ),
                False,
                id="the-paginated-helper",
            ),
            pytest.param(lambda api: api.fill_workspaces(), False, id="workspaces"),
            pytest.param(
                lambda api: api._get_files_metadata(file_type=Constant.WORKBOOK),
                False,
                id="file-metadata",
            ),
            pytest.param(lambda api: api.get_sigma_datasets(), True, id="datasets"),
            pytest.param(lambda api: api.get_sigma_workbooks(), True, id="workbooks"),
        ],
    )
    @pytest.mark.parametrize(
        "body",
        [
            pytest.param([{"id": "x"}], id="a-bare-list"),
            pytest.param({"entries": None, "nextPage": None}, id="a-null-entries"),
        ],
    )
    def test_a_body_of_the_wrong_type_is_a_shape_change_not_an_outage(
        self, listing: Any, stub_files: bool, body: Any
    ) -> None:
        """A bare list where the envelope belongs, or a null `entries`,
        raises TypeError (or AttributeError in the helper), and neither
        carries a status -- so untagged it reads as "re-run", advice for a
        response that will come back identical every time. Every run-wide
        listing that reads `entries` shares one envelope check for that
        reason."""
        api = _create_sigma_api()
        page = MagicMock(status_code=200)
        page.json.return_value = body
        with ExitStack() as stack:
            if stub_files:
                # Only where the caller fetches it first -- stubbing it for
                # the file-metadata case would replace the method under test.
                stack.enter_context(
                    patch.object(SigmaAPI, "_get_files_metadata", return_value={})
                )
            stack.enter_context(
                patch.object(SigmaAPI, "_get_api_call", return_value=page)
            )
            listing(api)

        context = _contexts(api.report.failures)
        assert "did not have the shape" in context
        assert "Transient" not in context

    def test_a_validation_error_does_not_eat_the_context_budget(self) -> None:
        """Pydantic echoes the whole row for EVERY missing field, which fills
        the 1000-char cap on its own and pushes out what follows."""
        api = _create_sigma_api()
        page = MagicMock(status_code=200)
        page.json.return_value = {
            "entries": [{"dataModelId": "dm-1", "junk": "y" * 400}],
            "nextPage": None,
        }
        with patch.object(SigmaAPI, "_get_api_call", return_value=page):
            api._paginated_entries(
                "https://example.invalid/v2/dataModels",
                SigmaDataModel,
                "Unable to fetch sigma data models.",
                enumerates_entities=True,
            )

        # Terse renders every missing field compactly ("name: Field
        # required; createdAt: ..."); pydantic's own text spends ~640 chars
        # echoing the row for each one, so the later fields fall off the end
        # of the 1000-char context.
        context = _contexts(api.report.failures)
        assert "input_value" not in context
        # Terse's exact shape: "loc: msg" joined by "; ". Pydantic's own text
        # never produces this, so reverting the call cannot pass.
        assert "name: Field required; createdAt: Field required" in context


def test_a_non_http_failure_says_so_rather_than_showing_a_null_status() -> None:
    """ "http_status=None" reads as a missing field, and "no response" would
    be a claim about Sigma this case cannot make -- a 200 this connector
    could not read lands here too."""
    api = _create_sigma_api()
    _fail_in_except(api, ValueError("validation error on a 200"))

    assert "no_http_error_response" in _contexts(api.report.warnings)


class TestMalformedPageShape:
    """`.get(ENTRIES, [])` made a page with no `entries` look like a page
    with none: a run-wide listing returned zero rows, reported nothing, and
    every entity it would have returned was soft-deleted."""

    def _list(self, api: SigmaAPI, body: Any) -> List[Dict[str, Any]]:
        page = MagicMock(status_code=200)
        if isinstance(body, Exception):
            page.json.side_effect = body
        else:
            page.json.return_value = body
        with patch.object(SigmaAPI, "_get_api_call", return_value=page):
            return api._paginated_raw_entries(
                "https://example.invalid/v2/dataModels",
                "Unable to fetch sigma data models.",
                enumerates_entities=True,
            )

    @pytest.mark.parametrize(
        ("desc", "body"),
        [
            ("no entries key at all", {"total": 0, "nextPage": None}),
            # Carries a cursor key, so only the entries check can catch it.
            ("entries is not a list", {"entries": "nope", "nextPage": None}),
            ("body is not JSON", requests.exceptions.JSONDecodeError("x", "y", 0)),
        ],
    )
    def test_a_page_of_the_wrong_shape_fails_the_run(
        self, desc: str, body: Any
    ) -> None:
        api = _create_sigma_api()
        assert self._list(api, body) == [], desc

        assert api.report.entity_enumeration_failed == 1, desc
        assert "did not have the shape" in _contexts(api.report.failures), desc

    def test_a_renamed_cursor_does_not_end_the_listing_silently(self) -> None:
        """Neither cursor key present: the loop used to break, every later
        page went missing, and the run passed -- so those entities were
        soft-deleted. The hand-written listings were already immune because
        they index NEXTPAGE directly."""
        api = _create_sigma_api()
        assert self._list(api, {"entries": [{"id": "a"}], "next": "2"}) == [{"id": "a"}]

        assert api.report.entity_enumeration_failed == 1
        assert "did not have the shape" in _contexts(api.report.failures)

    def test_a_last_page_still_ends_the_listing(self) -> None:
        """Sigma sends the key as null on the last page; that is not a
        renamed cursor."""
        api = _create_sigma_api()
        assert self._list(api, {"entries": [{"id": "a"}], "nextPage": None}) == [
            {"id": "a"}
        ]

        assert api.report.entity_enumeration_failed == 0

    def test_a_detail_endpoint_is_not_held_to_either_check(self) -> None:
        """A detail call truncating costs a thinner entity, not a deleted
        one, so /connections, columns and the lineage endpoints keep their
        lenient behaviour."""
        api = _create_sigma_api()
        page = MagicMock(status_code=200)
        page.json.return_value = {"total": 0}
        with patch.object(SigmaAPI, "_get_api_call", return_value=page):
            assert (
                api._paginated_raw_entries(
                    "https://example.invalid/v2/dataModels/x/lineage",
                    "Unable to fetch lineage.",
                )
                == []
            )

        assert len(api.report.failures) == 0
        assert api.report.entity_enumeration_failed == 0
        # Not even a warning: a detail endpoint answering without `entries`
        # is read as having none, exactly as before this PR.
        assert len(api.report.warnings) == 0

    def test_an_empty_page_is_still_an_empty_page(self) -> None:
        """A tenant with no data models answers `entries: []`, and that is
        not a failure."""
        api = _create_sigma_api()
        assert self._list(api, {"entries": [], "nextPage": None}) == []

        assert api.report.entity_enumeration_failed == 0
        assert len(api.report.failures) == 0

    @pytest.mark.parametrize(
        "lookup",
        [
            lambda api: api.get_workspace_id_from_file_path("p-1", "a/b"),
            lambda api: api.get_workspace("ws-1"),
        ],
        ids=["file-path-walk", "workspace-lookup"],
    )
    @pytest.mark.parametrize("shared", [False, True], ids=["dropped", "kept"])
    def test_shared_entities_decides_whether_anything_was_lost(
        self, shared: bool, lookup: Any
    ) -> None:
        """With shared entities ON the caller keeps the workbook or dataset
        anyway, so a failed lookup costs nothing -- counting it would put a
        false "children are missing" in front of an operator."""
        api = _create_sigma_api()
        api.config.ingest_shared_entities = shared
        with _dead_call(api):
            assert lookup(api) is None

        assert api.report.child_entity_listing_failed == (0 if shared else 1)

    def test_a_partial_columns_fetch_counts_every_abort(self) -> None:
        """warnings.total_elements counts distinct warning KEYS, and every
        abort shares one title+message, so three aborted calls read as one."""
        api = _create_sigma_api()
        with _dead_call(api):
            api.get_workbook_column_formulas("wb-1")
            api.get_workbook_column_formulas("wb-2")

        assert api.report.column_formulas_fetch_partial == 2

    def test_mostly_bad_rows_fail_even_with_one_good_one(self) -> None:
        """`not results` was too weak: 1 good row among many bad ones passed,
        and fail_safe_threshold does not backstop it -- that measures all
        URNs of every type, so lost Data Models stay far under 75% on a
        tenant of thousands of charts."""
        api = _create_sigma_api()
        page = MagicMock(status_code=200)
        page.json.return_value = {
            "entries": [_DATA_MODEL_ROW]
            + [{"dataModelId": f"bad-{i}"} for i in range(3)],
            "nextPage": None,
        }
        with patch.object(SigmaAPI, "_get_api_call", return_value=page):
            got = api._paginated_entries(
                "https://example.invalid/v2/dataModels",
                SigmaDataModel,
                "Unable to fetch sigma data models.",
                enumerates_entities=True,
            )

        assert len(got) == 1
        assert api.report.entity_enumeration_failed == 1
        context = _contexts(api.report.failures)
        assert "rows_dropped=3" in context
        assert "rows_parsed=1" in context

    @pytest.mark.parametrize("row", [None, 3, "x"], ids=["null", "int", "str"])
    def test_non_dict_rows_count_as_dropped(self, row: object) -> None:
        """A listing of nulls used to read as an empty tenant: dropped before
        the dropped-vs-parsed check, so the run passed and soft-deleted every
        Data Model."""
        api = _create_sigma_api()
        page = MagicMock(status_code=200)
        page.json.return_value = {"entries": [row, row, row], "nextPage": None}
        with patch.object(SigmaAPI, "_get_api_call", return_value=page):
            got = api._paginated_entries(
                "https://example.invalid/v2/dataModels",
                SigmaDataModel,
                "Unable to fetch sigma data models.",
                enumerates_entities=True,
            )

        assert got == []
        assert api.report.entity_enumeration_failed == 1
        assert "rows_dropped=3" in _contexts(api.report.failures)

    def test_an_undecodable_first_page_is_not_counted_as_read(self) -> None:
        api = _create_sigma_api()
        page = MagicMock(status_code=200)
        page.json.side_effect = requests.exceptions.JSONDecodeError("bad", "", 0)
        with patch.object(SigmaAPI, "_get_api_call", return_value=page):
            api._paginated_entries(
                "https://example.invalid/v2/dataModels",
                SigmaDataModel,
                "Unable to fetch sigma data models.",
                enumerates_entities=True,
            )

        assert "pages_read=0" in _contexts(api.report.failures)

    @pytest.mark.parametrize("model", [Workspace, Element])
    def test_a_before_validator_leaves_a_non_dict_row_to_pydantic(
        self, model: Any
    ) -> None:
        """Otherwise `.get` on the row raises AttributeError, which the
        listings read as a connector bug rather than an unparseable row."""
        with pytest.raises(ValidationError):
            model.model_validate(None)

    def test_a_null_workspace_row_is_an_unparseable_row(self) -> None:
        """The validator's `.get` on None raised AttributeError, which read as
        "Transient: re-run" for a row that fails every run."""
        api = _create_sigma_api()
        page = MagicMock(status_code=200)
        page.json.return_value = {"entries": [None], "nextPage": None}
        with patch.object(SigmaAPI, "_get_api_call", return_value=page):
            api.fill_workspaces()

        context = _contexts(api.report.failures)
        assert "cannot parse" in context
        assert "Transient" not in context

    def test_a_connector_bug_is_not_called_transient(self) -> None:
        api = _create_sigma_api()
        page = MagicMock(status_code=200)
        page.json.return_value = {
            "entries": [{"workspaceId": "ws-1"}],
            "nextPage": None,
        }
        with (
            patch.object(SigmaAPI, "_get_api_call", return_value=page),
            patch.object(Workspace, "model_validate", side_effect=TypeError("bug")),
        ):
            api.fill_workspaces()

        context = _contexts(api.report.failures)
        assert "Neither refused nor transient" in context
        assert "Transient" not in context

    def test_one_folder_is_walked_once_for_both_file_types(self) -> None:
        """entity_removing must not be part of the cache key: a folder
        holding both a data model and workbooks would be walked twice, once
        per value."""
        api = _create_sigma_api()
        page = MagicMock(status_code=200)
        page.json.return_value = {"parentId": "ws-1"}
        with patch.object(SigmaAPI, "_get_api_call", return_value=page) as call:
            first = api.get_workspace_id_from_file_path(
                "p-1", "ws/dir", entity_removing=False
            )
            second = api.get_workspace_id_from_file_path("p-1", "ws/dir")

        assert first == second == "ws-1"
        assert call.call_count == 1

    def test_an_internal_key_error_is_not_blamed_on_sigma(self) -> None:
        """A KeyError on a key this client never reads off a payload is a bug
        in this file; calling it "Sigma's response did not have the shape we
        expect" points the operator at the vendor."""
        api = _create_sigma_api()
        page = MagicMock(status_code=200)
        page.json.return_value = {
            "entries": [{"workspaceId": "ws-1"}],
            "nextPage": None,
        }
        with (
            patch.object(SigmaAPI, "_get_api_call", return_value=page),
            patch.object(
                Workspace, "model_validate", side_effect=KeyError("some_local_var")
            ),
        ):
            api.fill_workspaces()

        context = _contexts(api.report.failures)
        assert "did not have the shape" not in context
        assert "likely a bug in this connector" in context or "Transient" in context
