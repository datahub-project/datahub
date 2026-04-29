import datetime as _dt
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional
from unittest.mock import MagicMock, patch

import pytest
import requests

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sigma.config import SigmaSourceConfig, SigmaSourceReport
from datahub.ingestion.source.sigma.data_classes import (
    DatasetUpstream,
    Element,
    File,
    SheetUpstream,
    SigmaDataModel,
    SigmaDataModelElement,
    SigmaDataset,
    Workbook,
    Workspace,
)
from datahub.ingestion.source.sigma.sigma import SigmaSource
from datahub.ingestion.source.sigma.sigma_api import SigmaAPI
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

    def test_dataset_node_with_null_name_parses_with_siblings(
        self,
    ) -> None:
        # ``DatasetUpstream.name`` is ``Optional[str]`` so a null-name node
        # no longer trips ValidationError at parse time. Both nodes land
        # in the upstream map; the chart-input path in ``sigma.py`` is
        # responsible for skipping the edge and bumping
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
