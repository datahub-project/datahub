import pathlib
from typing import Any, Dict, Iterator, List
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.sink.file import write_metadata_file
from datahub.ingestion.source.redash import RedashConfig, RedashSource
from datahub.testing import mce_helpers

RESOURCES = pathlib.Path(__file__).parent
CONNECT_URI = "http://localhost:5000"
UPDATED_AT = "2021-08-13T19:14:15.288Z"

DASHBOARD_LIST = [
    {"id": 1, "name": "Sales", "slug": "sales", "is_draft": False},
    {"id": 2, "name": "Ops", "slug": "ops", "is_draft": False},
    {"id": 3, "name": "Draft dashboard", "slug": "draft", "is_draft": True},
]
DASHBOARDS: Dict[int, Dict[str, Any]] = {
    1: {
        "id": 1,
        "name": "Sales",
        "slug": "sales",
        "updated_at": UPDATED_AT,
        "changed_by": {"username": "alice"},
        "widgets": [
            {
                "text": "Sales overview",
                "options": {"position": {"col": 0, "row": 0}},
            },
            {"visualization": {"id": 10}},
            {"visualization": {"id": 11}},
        ],
    },
    2: {
        "id": 2,
        "name": "Ops",
        "slug": "ops",
        "updated_at": UPDATED_AT,
        "widgets": [{"visualization": {"id": 12}}],
    },
}
QUERY_LIST = [
    {"id": 4, "name": "Orders", "is_draft": False},
    {"id": 5, "name": "Draft query", "is_draft": True},
    {"id": 6, "name": "Cached results", "is_draft": False},
]
QUERIES: Dict[int, Dict[str, Any]] = {
    4: {
        "id": 4,
        "name": "Orders",
        "data_source_id": 1,
        "query": "SELECT order_id, amount FROM orders",
        "visualizations": [
            {
                "id": 10,
                "name": "Chart",
                "type": "CHART",
                "options": {"globalSeriesType": "pie"},
                "updated_at": UPDATED_AT,
            },
            {
                "id": 11,
                "name": "Sankey",
                "type": "SANKEY",
                "description": "Unmapped visualization type",
                "updated_at": UPDATED_AT,
            },
        ],
    },
    6: {
        "id": 6,
        "name": "Cached results",
        "data_source_id": 2,
        "query": "SELECT 1",
        "visualizations": [
            {"id": 12, "name": "Table", "type": "TABLE", "updated_at": UPDATED_AT}
        ],
    },
}
DATA_SOURCES: Dict[int, Dict[str, Any]] = {
    1: {"id": 1, "type": "pg", "syntax": "sql", "options": {"dbname": "analytics"}},
    # A "results" source without a name has no database, so its charts get no inputs.
    2: {"id": 2, "type": "results", "syntax": "sql"},
}


def _response(payload: Any) -> MagicMock:
    resp = MagicMock(status_code=200)
    resp.json.return_value = payload
    return resp


def _page(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {"count": len(items), "results": items}


def _mock_client() -> MagicMock:
    # Dashboards are processed in list order (parallelism=1): the first reads the Redash
    # version (>= 10, URL by id), the second fails to and falls back to < 10 (URL by slug).
    status_responses: Iterator[MagicMock] = iter(
        [
            _response({"version": "10.1.0"}),
            MagicMock(json=MagicMock(side_effect=ValueError)),
        ]
    )

    def get(url: str) -> MagicMock:
        if url == f"{CONNECT_URI}/api":
            return _response({})
        if url == "status.json":
            return next(status_responses)
        prefix, _, item_id = url.rpartition("/")
        if prefix == "api/dashboards":
            return _response(DASHBOARDS[int(item_id)])
        if prefix == "/api/queries":
            return _response(QUERIES[int(item_id)])
        if prefix == "/api/data_sources":
            return _response(DATA_SOURCES[int(item_id)])
        raise AssertionError(f"unexpected Redash call {url}")

    client = MagicMock()
    client._get.side_effect = get
    client.dashboards.side_effect = lambda *args, **kwargs: _page(DASHBOARD_LIST)
    client.queries.side_effect = lambda *args, **kwargs: _page(QUERY_LIST)
    return client


def test_redash_golden(pytestconfig: pytest.Config, tmp_path: pathlib.Path) -> None:
    source = RedashSource(
        ctx=PipelineContext(run_id="redash-golden-test"),
        config=RedashConfig(
            connect_uri=CONNECT_URI,
            parallelism=1,
        ),
    )
    source.client = _mock_client()

    workunits = list(source.get_workunits())

    assert not source.get_report().failures
    assert list(source.report.filtered) == ["Draft query", "Draft dashboard"]
    output_path = tmp_path / "redash.json"
    write_metadata_file(output_path, [wu.metadata for wu in workunits])
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=RESOURCES / "redash_mces_golden.json",
        golden_mces_as_mcps=True,
        fail_on_output_mces=True,
    )
