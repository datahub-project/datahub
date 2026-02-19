"""Tests for search API routes."""

import json
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

# Mock the dependencies before importing the app
with patch("api.routes.search.DataHubGraph"):
    from api.routes.search import router


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


def test_search_request_with_rescore_enabled(client, mock_graph):
    """Test search request with rescoreEnabled flag."""
    response = client.post(
        "/api/search",
        json={
            "query": "test",
            "rescoreEnabled": True,
        },
    )

    assert response.status_code == 200

    # Verify GraphQL was called with rescoreEnabled in searchFlags
    assert mock_graph.execute_graphql.called
    call_args = mock_graph.execute_graphql.call_args
    variables = call_args[0][1]
    search_flags = variables["input"]["searchFlags"]

    assert "rescoreEnabled" in search_flags
    assert search_flags["rescoreEnabled"] is True


def test_search_request_with_rescore_disabled(client, mock_graph):
    """Test search request with rescoreEnabled=false."""
    response = client.post(
        "/api/search",
        json={
            "query": "test",
            "rescoreEnabled": False,
        },
    )

    assert response.status_code == 200

    # Verify GraphQL was called with rescoreEnabled=false
    call_args = mock_graph.execute_graphql.call_args
    variables = call_args[0][1]
    search_flags = variables["input"]["searchFlags"]

    assert "rescoreEnabled" in search_flags
    assert search_flags["rescoreEnabled"] is False


def test_search_request_with_rescore_formula_override(client, mock_graph):
    """Test search request with rescoreFormulaOverride."""
    formula = "pow(norm_bm25, 1.0) * pow(hasDesc, 1.3) * pow(hasOwners, 1.2)"

    response = client.post(
        "/api/search",
        json={
            "query": "test",
            "rescoreFormulaOverride": formula,
        },
    )

    assert response.status_code == 200

    call_args = mock_graph.execute_graphql.call_args
    variables = call_args[0][1]
    search_flags = variables["input"]["searchFlags"]

    assert search_flags["rescoreFormulaOverride"] == formula


def test_search_request_with_rescore_signals_override(client, mock_graph):
    """Test search request with rescoreSignalsOverride (must be valid JSON)."""
    signals = [{"name": "hasDesc", "boost": 1.5}]

    response = client.post(
        "/api/search",
        json={
            "query": "test",
            "rescoreSignalsOverride": json.dumps(signals),
        },
    )

    assert response.status_code == 200

    call_args = mock_graph.execute_graphql.call_args
    variables = call_args[0][1]
    search_flags = variables["input"]["searchFlags"]

    assert search_flags["rescoreSignalsOverride"] == json.dumps(signals)


def test_search_request_with_invalid_signals_json(client):
    """Test search request rejects invalid JSON in rescoreSignalsOverride."""
    response = client.post(
        "/api/search",
        json={
            "query": "test",
            "rescoreSignalsOverride": "{not valid json",
        },
    )

    assert response.status_code == 400


def test_search_request_without_rescore_params(client, mock_graph):
    """Test search request without rescore parameters uses defaults."""
    response = client.post(
        "/api/search",
        json={
            "query": "test",
        },
    )

    assert response.status_code == 200

    call_args = mock_graph.execute_graphql.call_args
    variables = call_args[0][1]
    search_flags = variables["input"]["searchFlags"]

    assert "rescoreEnabled" not in search_flags or search_flags["rescoreEnabled"] is None
    assert "rescoreFormulaOverride" not in search_flags
    assert "rescoreSignalsOverride" not in search_flags


def test_search_request_with_function_score_override(client, mock_graph):
    """Test search request with functionScoreOverride (Stage 1)."""
    function_score = {
        "functions": [{"filter": {"term": {"active": True}}, "weight": 5.0}],
        "score_mode": "sum",
        "boost_mode": "sum",
    }

    response = client.post(
        "/api/search",
        json={
            "query": "test",
            "functionScoreOverride": json.dumps(function_score),
        },
    )

    assert response.status_code == 200

    call_args = mock_graph.execute_graphql.call_args
    variables = call_args[0][1]
    search_flags = variables["input"]["searchFlags"]

    assert "functionScoreOverride" in search_flags
    assert search_flags["functionScoreOverride"] == json.dumps(function_score)


def test_search_request_with_invalid_function_score_json(client):
    """Test search request rejects invalid JSON in functionScoreOverride."""
    response = client.post(
        "/api/search",
        json={
            "query": "test",
            "functionScoreOverride": "not valid json {",
        },
    )

    assert response.status_code == 400


def test_search_request_with_all_overrides(client, mock_graph):
    """Test search request with Stage 1 and Stage 2 overrides together."""
    function_score = {
        "functions": [{"filter": {"term": {"active": True}}, "weight": 5.0}],
        "score_mode": "sum",
        "boost_mode": "sum",
    }
    formula = "pow(norm_bm25, 1.0) * pow(hasDesc, 1.3)"
    signals = [{"name": "hasDesc", "boost": 1.3}]

    response = client.post(
        "/api/search",
        json={
            "query": "test",
            "functionScoreOverride": json.dumps(function_score),
            "rescoreEnabled": True,
            "rescoreFormulaOverride": formula,
            "rescoreSignalsOverride": json.dumps(signals),
        },
    )

    assert response.status_code == 200

    call_args = mock_graph.execute_graphql.call_args
    variables = call_args[0][1]
    search_flags = variables["input"]["searchFlags"]

    assert search_flags["functionScoreOverride"] == json.dumps(function_score)
    assert search_flags["rescoreEnabled"] is True
    assert search_flags["rescoreFormulaOverride"] == formula
    assert search_flags["rescoreSignalsOverride"] == json.dumps(signals)


def test_search_request_model_validation():
    """Test SearchRequest model validates rescore parameters."""
    from api.models import SearchRequest

    # Valid request with rescore parameters
    request = SearchRequest(
        query="test",
        rescoreEnabled=True,
        rescoreFormulaOverride="pow(norm_bm25, 1.0)",
        rescoreSignalsOverride='[{"name": "hasDesc", "boost": 1.3}]',
    )

    assert request.query == "test"
    assert request.rescoreEnabled is True
    assert request.rescoreFormulaOverride == "pow(norm_bm25, 1.0)"
    assert request.rescoreSignalsOverride == '[{"name": "hasDesc", "boost": 1.3}]'

    # Valid request without rescore parameters
    request2 = SearchRequest(query="test")
    assert request2.rescoreEnabled is None
    assert request2.rescoreFormulaOverride is None
    assert request2.rescoreSignalsOverride is None
