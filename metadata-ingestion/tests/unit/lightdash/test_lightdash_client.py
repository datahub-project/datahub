"""Tests for LightdashClient — auth header, JSON envelope, error mapping."""

from __future__ import annotations

import pytest
import responses

from datahub.ingestion.source.lightdash.client import LightdashAPIError, LightdashClient


@pytest.fixture
def client():
    return LightdashClient(
        base_url="https://ld.example.com",
        personal_access_token="ldpat_secret",
        max_retries=0,
    )


@responses.activate
def test_auth_header_is_apikey_not_bearer(client):
    """Regression: Lightdash rejects ``Bearer`` tokens — the client MUST send ``ApiKey``."""

    def cb(request):
        # Assert in the request handler so a 401 from a stub doesn't mask the bug.
        assert request.headers["Authorization"] == "ApiKey ldpat_secret"
        return (
            200,
            {},
            '{"status":"ok","results":{"organizationUuid":"o","name":"n"}}',
        )

    responses.add_callback(
        responses.GET, "https://ld.example.com/api/v1/org", callback=cb
    )

    org = client.get_organization()
    assert org.organization_uuid == "o"
    assert org.name == "n"


@responses.activate
def test_unwraps_results_envelope(client):
    responses.add(
        responses.GET,
        "https://ld.example.com/api/v1/org/projects",
        json={
            "status": "ok",
            "results": [
                {"projectUuid": "p1", "name": "First", "warehouseType": "clickhouse"},
                {"projectUuid": "p2", "name": "Second", "warehouseType": "snowflake"},
            ],
        },
    )
    projects = client.list_projects()
    assert [p.project_uuid for p in projects] == ["p1", "p2"]
    assert projects[0].warehouse_type == "clickhouse"


@responses.activate
def test_401_raises_lightdash_error_with_hint(client):
    responses.add(
        responses.GET,
        "https://ld.example.com/api/v1/org",
        body="Unauthorized",
        status=401,
    )
    with pytest.raises(LightdashAPIError) as excinfo:
        client.get_organization()
    msg = str(excinfo.value)
    assert "Unauthorized" in msg
    assert "ApiKey" in msg  # Hint surfaces the correct header format.


@responses.activate
def test_non_ok_envelope_is_rejected(client):
    responses.add(
        responses.GET,
        "https://ld.example.com/api/v1/org",
        json={"status": "error", "error": {"name": "bad"}},
    )
    with pytest.raises(LightdashAPIError):
        client.get_organization()


@responses.activate
def test_health_does_not_require_envelope(client):
    # Lightdash /api/v1/health uses the same envelope, but the client tolerates
    # both wrapped and unwrapped — that's its raison d'être.
    responses.add(
        responses.GET,
        "https://ld.example.com/api/v1/health",
        json={"results": {"healthy": True, "version": "0.2925.2"}, "status": "ok"},
    )
    h = client.health()
    assert h["healthy"] is True
    assert h["version"] == "0.2925.2"


@responses.activate
def test_get_chart_parses_table_name_and_explore(client):
    responses.add(
        responses.GET,
        "https://ld.example.com/api/v1/saved/c1",
        json={
            "status": "ok",
            "results": {
                "uuid": "c1",
                "projectUuid": "p1",
                "name": "Revenue",
                "tableName": "orders_enriched",
                "metricQuery": {"exploreName": "orders_enriched"},
                "updatedAt": "2026-05-12T10:28:57.408Z",
            },
        },
    )
    chart = client.get_chart("c1")
    assert chart.table_name == "orders_enriched"
    assert chart.metric_query.explore_name == "orders_enriched"


@responses.activate
def test_get_explore_parses_joined_tables(client):
    responses.add(
        responses.GET,
        "https://ld.example.com/api/v1/projects/p1/explores/orders",
        json={
            "status": "ok",
            "results": {
                "name": "orders",
                "baseTable": "orders",
                "joinedTables": [{"table": "customers"}],
                "tables": {
                    "orders": {
                        "name": "orders",
                        "schema": "analytics",
                        "sqlTable": "`analytics`.`orders`",
                    },
                    "customers": {
                        "name": "customers",
                        "schema": "analytics",
                        "sqlTable": "`analytics`.`customers`",
                    },
                },
            },
        },
    )
    explore = client.get_explore("p1", "orders")
    assert explore.base_table == "orders"
    assert set(explore.tables) == {"orders", "customers"}
    assert explore.tables["orders"].schema_ == "analytics"
    assert explore.tables["customers"].sql_table == "`analytics`.`customers`"
