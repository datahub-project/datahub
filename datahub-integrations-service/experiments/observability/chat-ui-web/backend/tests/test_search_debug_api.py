"""Tests for search debug API routes."""

import json
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

# Mock dependency import before loading the router
with patch("api.routes.search_debug.DataHubGraph"):
    from api.routes.search_debug import router


@pytest.fixture
def mock_graph():
    """Create a mock DataHub graph client."""
    graph = MagicMock()
    graph.execute_graphql.return_value = {
        "searchAcrossEntities": {
            "start": 0,
            "count": 0,
            "total": 0,
            "searchResults": [],
        }
    }
    return graph


@pytest.fixture
def client(mock_graph):
    """Create a test client with dependency overrides."""
    from fastapi import FastAPI

    from api.dependencies import get_datahub_graph

    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_datahub_graph] = lambda: mock_graph

    with TestClient(app) as test_client:
        yield test_client

    app.dependency_overrides.clear()


def test_debug_search_rejects_invalid_function_score_json(client):
    """Malformed functionScoreOverride should return 400."""
    response = client.post(
        "/api/search/debug",
        json={
            "query": "test",
            "functionScoreOverride": "{not valid json",
        },
    )
    assert response.status_code == 400


def test_debug_search_rejects_invalid_rescore_signals_json(client):
    """Malformed rescoreSignalsOverride should return 400."""
    response = client.post(
        "/api/search/debug",
        json={
            "query": "test",
            "rescoreSignalsOverride": "{not valid json",
        },
    )
    assert response.status_code == 400


def test_debug_search_maps_legacy_rescore_override(client, mock_graph):
    """Legacy rescoreOverride should be mapped to split flags."""
    response = client.post(
        "/api/search/debug",
        json={
            "query": "test",
            "rescoreOverride": json.dumps(
                {
                    "formula": "pow(norm_bm25, 1.0)",
                    "signals": [{"name": "bm25", "boost": 1.0}],
                }
            ),
        },
    )

    assert response.status_code == 200

    call_args = mock_graph.execute_graphql.call_args
    variables = call_args[0][1]
    search_flags = variables["input"]["searchFlags"]

    assert search_flags["rescoreFormulaOverride"] == "pow(norm_bm25, 1.0)"
    assert search_flags["rescoreSignalsOverride"] == json.dumps(
        [{"name": "bm25", "boost": 1.0}]
    )
