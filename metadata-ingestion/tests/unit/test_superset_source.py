import json
from typing import List, Optional, Set

import pytest
import requests
import requests_mock as rm

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mce_builder import make_chart_urn
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.superset import (
    SupersetConfig,
    SupersetSource,
    _extract_chart_id_with_status,
    get_filter_name,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import DashboardInfoClass
from datahub.sql_parsing.sqlglot_lineage import create_lineage_sql_parsed_result


def _list_api_dashboard(dashboard_id: int) -> dict:
    """Build a minimal dashboard payload mirroring the Preset list API.

    The list endpoint omits ``position_json`` (the detail endpoint adds it),
    matching what ``_process_dashboard`` receives upstream. Used as the
    starting point for end-to-end dashboard ingestion tests.
    """
    return {
        "id": dashboard_id,
        "dashboard_title": f"dashboard_{dashboard_id}",
        "url": f"/dashboard/{dashboard_id}",
        "changed_on_utc": "2020-04-14T07:00:00.000000+0000",
        "changed_by": {"id": 1},
        "owners": [],
        "tags": [],
        "status": "published",
        "published": True,
        "certified_by": "",
        "certification_details": "",
    }


def test_default_values():
    config = SupersetConfig.model_validate({})

    assert config.connect_uri == "http://localhost:8088"
    assert config.display_uri == "http://localhost:8088"
    assert config.provider == "db"
    assert config.env == "PROD"
    assert config.username is None
    assert config.password is None
    assert config.dataset_pattern == AllowDenyPattern.allow_all()
    assert config.chart_pattern == AllowDenyPattern.allow_all()
    assert config.dashboard_pattern == AllowDenyPattern.allow_all()
    assert config.database_pattern == AllowDenyPattern.allow_all()


def test_set_display_uri():
    display_uri = "some_host:1234"

    config = SupersetConfig.model_validate({"display_uri": display_uri})

    assert config.connect_uri == "http://localhost:8088"
    assert config.display_uri == display_uri


def test_superset_login(requests_mock):
    login_url = "http://localhost:8088/api/v1/security/login"
    requests_mock.post(login_url, json={"access_token": "dummy_token"}, status_code=200)

    dashboard_url = "http://localhost:8088/api/v1/dashboard/"
    requests_mock.get(dashboard_url, json={}, status_code=200)

    for entity in ["dataset", "dashboard", "chart"]:
        requests_mock.get(
            f"http://localhost:8088/api/v1/{entity}/related/owners",
            json={},
            status_code=200,
        )

    source = SupersetSource(
        ctx=PipelineContext(run_id="superset-source-test"), config=SupersetConfig()
    )
    assert source.platform == "superset"


def test_superset_build_owners_info(requests_mock):
    login_url = "http://localhost:8088/api/v1/security/login"
    requests_mock.post(login_url, json={"access_token": "dummy_token"}, status_code=200)

    dashboard_url = "http://localhost:8088/api/v1/dashboard/"
    requests_mock.get(dashboard_url, json={}, status_code=200)

    for entity in ["dataset", "dashboard", "chart"]:
        requests_mock.get(
            f"http://localhost:8088/api/v1/{entity}/related/owners",
            json={
                "count": 2,
                "result": [
                    {
                        "extra": {"active": "false", "email": "test_user1@example.com"},
                        "text": "Test User1",
                        "value": 1,
                    },
                    {
                        "extra": {"active": "false", "email": "test_user2@example.com"},
                        "text": "Test User2",
                        "value": 2,
                    },
                ],
            },
            status_code=200,
        )

    source = SupersetSource(
        ctx=PipelineContext(run_id="superset-source-owner-info-test"),
        config=SupersetConfig(),
    )
    assert source.owner_info == {
        1: "test_user1@example.com",
        2: "test_user2@example.com",
    }


def test_column_level_lineage(requests_mock):
    login_url = "http://localhost:8088/api/v1/security/login"
    requests_mock.post(login_url, json={"access_token": "dummy_token"}, status_code=200)

    dashboard_url = "http://localhost:8088/api/v1/dashboard/"
    requests_mock.get(dashboard_url, json={}, status_code=200)

    for entity in ["dataset", "dashboard", "chart"]:
        requests_mock.get(
            f"http://localhost:8088/api/v1/{entity}/related/owners",
            json={
                "count": 2,
                "result": [
                    {
                        "extra": {"active": "false", "email": "test_user1@example.com"},
                        "text": "Test User1",
                        "value": 1,
                    },
                    {
                        "extra": {"active": "false", "email": "test_user2@example.com"},
                        "text": "Test User2",
                        "value": 2,
                    },
                ],
            },
            status_code=200,
        )

    sql = """
    SELECT tt2.id, tt2.name, tt2.description, db.database_name 
    FROM test_table2 tt2
    JOIN databases db ON tt2.database_id = db.id
    WHERE tt2.kind = 'virtual'
    ORDER BY tt2.id DESC;
    """

    source = SupersetSource(
        ctx=PipelineContext(run_id="superset-source-owner-info-test"),
        config=SupersetConfig(),
    )

    parsed_query_object = create_lineage_sql_parsed_result(
        query=sql,
        default_db="test-db",
        platform="postgres",
        platform_instance=None,
        env="TEST",
    )

    virtual_lineage = source.generate_virtual_dataset_lineage(
        parsed_query_object=parsed_query_object,
        datasource_urn="urn:li:dataset:(urn:li:dataPlatform:superset,test_database_name.test_schema_name.test_table_name,PROD)",
    )

    assert any(
        "urn:li:dataset:(urn:li:dataPlatform:postgres,test-db.databases,TEST)"
        in virtual_lineage.upstreams[i].get("dataset")
        for i in range(len(virtual_lineage.upstreams))
    )


def test_construct_chart_cll_aggregate_mode(requests_mock):
    login_url = "http://localhost:8088/api/v1/security/login"
    requests_mock.post(login_url, json={"access_token": "dummy_token"}, status_code=200)
    requests_mock.get(
        "http://localhost:8088/api/v1/dashboard/", json={}, status_code=200
    )

    for entity in ["dataset", "dashboard", "chart"]:
        requests_mock.get(
            f"http://localhost:8088/api/v1/{entity}/related/owners",
            json={},
            status_code=200,
        )

    requests_mock.get(
        "http://localhost:8088/api/v1/dataset/1",
        json={
            "result": {
                "columns": [
                    {
                        "column_name": "time",
                        "type": "TIMESTAMP",
                        "description": "Event time",
                    },
                    {
                        "column_name": "network",
                        "type": "STRING",
                        "description": "Network used",
                    },
                    {
                        "column_name": "amount_usd",
                        "type": "NUMERIC",
                        "description": "USD amount",
                    },
                    {
                        "column_name": "creation_time",
                        "type": "TIMESTAMP",
                        "description": "Creation time",
                    },
                ],
                "metrics": [
                    {
                        "metric_name": "sum_usd",
                        "expression": "SUM(amount_usd)",
                        "metric_type": "SUM",
                        "description": "Total USD volume",
                    },
                    {
                        "metric_name": "total_requests",
                        "expression": "COUNT(DISTINCT request_id)",
                        "metric_type": "COUNT_DISTINCT",
                    },
                ],
                "schema": "test_schema",
                "table_name": "test_table",
            }
        },
        status_code=200,
    )

    source = SupersetSource(ctx=PipelineContext(run_id="test"), config=SupersetConfig())

    # Realistic chart data based on provided examples
    chart_data = {
        "form_data": {
            "query_mode": "aggregate",
            "viz_type": "ag-grid-table",
            "groupby": [
                "network",
                {
                    "sqlExpression": "CASE WHEN amount_usd > 1000 THEN 'Large' ELSE 'Small' END",
                    "label": "transaction_size",
                },
            ],
            "x_axis": "creation_time",
            "metrics": [
                "sum_usd",
                {
                    "aggregate": "SUM",
                    "column": {"column_name": "amount_usd", "type": "NUMERIC"},
                    "expressionType": "SIMPLE",
                    "label": "Total Amount",
                },
                {
                    "expressionType": "SQL",
                    "sqlExpression": "AVG(CASE WHEN network = 'Test Network' THEN amount_usd * 1.5 ELSE amount_usd END)",
                    "label": "Weighted Average",
                },
            ],
            "adhoc_filters": [
                {
                    "clause": "WHERE",
                    "comparator": "Test Network",
                    "expressionType": "SIMPLE",
                    "operator": "==",
                    "subject": "network",
                }
            ],
        },
        "datasource_type": "table",
        "datasource_id": 1,
    }

    datasource_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:superset,test_schema.test_table,PROD)"
    )
    input_fields = source.construct_chart_cll(chart_data, datasource_urn, 1)

    assert len(input_fields) == 4

    field_types = {}
    for f in input_fields:
        if f.schemaField:
            field_types[f.schemaField.fieldPath] = f.schemaField.nativeDataType

    assert field_types["network"] == "STRING"
    assert field_types["sum_usd"] == "SUM"
    assert field_types["amount_usd"] == "NUMERIC"
    assert field_types["creation_time"] == "TIMESTAMP"


class TestDatabaseAlias:
    """Tests for database_alias configuration that transforms database names in URNs."""

    def test_apply_database_alias_transforms_urn(self, requests_mock):
        """Test that _apply_database_alias_to_urn transforms database names correctly."""
        login_url = "http://localhost:8088/api/v1/security/login"
        requests_mock.post(
            login_url, json={"access_token": "dummy_token"}, status_code=200
        )
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/", json={}, status_code=200
        )
        for entity in ["dataset", "dashboard", "chart"]:
            requests_mock.get(
                f"http://localhost:8088/api/v1/{entity}/related/owners",
                json={},
                status_code=200,
            )

        config = SupersetConfig.model_validate(
            {"database_alias": {"ClickHouse Cloud": "fingerprint"}}
        )
        source = SupersetSource(
            ctx=PipelineContext(run_id="test-database-alias"), config=config
        )

        # Test exact case match
        urn = "urn:li:dataset:(urn:li:dataPlatform:clickhouse,ClickHouse Cloud.schema.table,PROD)"
        transformed = source._apply_database_alias_to_urn(urn)
        assert (
            transformed
            == "urn:li:dataset:(urn:li:dataPlatform:clickhouse,fingerprint.schema.table,PROD)"
        )

        # Test lowercase match (SQL parser may lowercase names)
        urn_lower = "urn:li:dataset:(urn:li:dataPlatform:clickhouse,clickhouse cloud.schema.table,PROD)"
        transformed_lower = source._apply_database_alias_to_urn(urn_lower)
        assert (
            transformed_lower
            == "urn:li:dataset:(urn:li:dataPlatform:clickhouse,fingerprint.schema.table,PROD)"
        )

    def test_apply_database_alias_no_match_returns_unchanged(self, requests_mock):
        """Test that URNs without matching database names are returned unchanged."""
        login_url = "http://localhost:8088/api/v1/security/login"
        requests_mock.post(
            login_url, json={"access_token": "dummy_token"}, status_code=200
        )
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/", json={}, status_code=200
        )
        for entity in ["dataset", "dashboard", "chart"]:
            requests_mock.get(
                f"http://localhost:8088/api/v1/{entity}/related/owners",
                json={},
                status_code=200,
            )

        config = SupersetConfig.model_validate(
            {"database_alias": {"ClickHouse Cloud": "fingerprint"}}
        )
        source = SupersetSource(
            ctx=PipelineContext(run_id="test-database-alias"), config=config
        )

        urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,other_db.schema.table,PROD)"
        transformed = source._apply_database_alias_to_urn(urn)
        assert transformed == urn

    def test_apply_database_alias_empty_config(self, requests_mock):
        """Test that empty database_alias config returns URN unchanged."""
        login_url = "http://localhost:8088/api/v1/security/login"
        requests_mock.post(
            login_url, json={"access_token": "dummy_token"}, status_code=200
        )
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/", json={}, status_code=200
        )
        for entity in ["dataset", "dashboard", "chart"]:
            requests_mock.get(
                f"http://localhost:8088/api/v1/{entity}/related/owners",
                json={},
                status_code=200,
            )

        source = SupersetSource(
            ctx=PipelineContext(run_id="test-database-alias"),
            config=SupersetConfig(),
        )

        urn = "urn:li:dataset:(urn:li:dataPlatform:clickhouse,ClickHouse Cloud.schema.table,PROD)"
        transformed = source._apply_database_alias_to_urn(urn)
        assert transformed == urn

    def test_database_alias_in_virtual_dataset_lineage(self, requests_mock):
        """Test that database_alias is applied to SQL-parsed upstream URNs."""
        login_url = "http://localhost:8088/api/v1/security/login"
        requests_mock.post(
            login_url, json={"access_token": "dummy_token"}, status_code=200
        )
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/", json={}, status_code=200
        )
        for entity in ["dataset", "dashboard", "chart"]:
            requests_mock.get(
                f"http://localhost:8088/api/v1/{entity}/related/owners",
                json={},
                status_code=200,
            )

        config = SupersetConfig.model_validate(
            {"database_alias": {"ClickHouse Cloud": "fingerprint"}}
        )
        source = SupersetSource(
            ctx=PipelineContext(run_id="test-database-alias"), config=config
        )

        sql = "SELECT id, name FROM source_table"
        parsed_query_object = create_lineage_sql_parsed_result(
            query=sql,
            default_db="ClickHouse Cloud",
            platform="clickhouse",
            platform_instance=None,
            env="PROD",
        )

        datasource_urn = "urn:li:dataset:(urn:li:dataPlatform:preset,fingerprint.schema.my_dataset,PROD)"
        lineage = source.generate_virtual_dataset_lineage(
            parsed_query_object=parsed_query_object,
            datasource_urn=datasource_urn,
        )

        # Verify upstream URNs have been transformed
        upstream_urns = [u.dataset for u in lineage.upstreams]
        assert len(upstream_urns) == 1
        # The URN should have "fingerprint" not "ClickHouse Cloud" or "clickhouse cloud"
        assert "fingerprint" in upstream_urns[0]
        assert "ClickHouse Cloud" not in upstream_urns[0]
        assert "clickhouse cloud" not in upstream_urns[0]

    def test_get_datasource_urn_applies_database_alias(self, requests_mock):
        """Test that get_datasource_urn_from_id applies database_alias."""
        login_url = "http://localhost:8088/api/v1/security/login"
        requests_mock.post(
            login_url, json={"access_token": "dummy_token"}, status_code=200
        )
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/", json={}, status_code=200
        )
        for entity in ["dataset", "dashboard", "chart"]:
            requests_mock.get(
                f"http://localhost:8088/api/v1/{entity}/related/owners",
                json={},
                status_code=200,
            )

        config = SupersetConfig.model_validate(
            {"database_alias": {"ClickHouse Cloud": "fingerprint"}}
        )
        source = SupersetSource(
            ctx=PipelineContext(run_id="test-database-alias"), config=config
        )

        dataset_response = {
            "result": {
                "schema": "analytics",
                "table_name": "metrics",
                "database": {"id": 1, "database_name": "ClickHouse Cloud"},
            }
        }

        urn = source.get_datasource_urn_from_id(dataset_response, "clickhouse")
        # URN should contain "fingerprint" not "ClickHouse Cloud"
        assert "fingerprint" in urn
        assert "ClickHouse Cloud" not in urn
        assert "analytics.metrics" in urn


def _build_source(
    requests_mock: rm.Mocker,
    *,
    config: Optional[SupersetConfig] = None,
) -> SupersetSource:
    """Helper that wires up just enough mock endpoints to instantiate the source."""
    if config is None:
        config = SupersetConfig()
    login_url = "http://localhost:8088/api/v1/security/login"
    requests_mock.post(login_url, json={"access_token": "dummy_token"}, status_code=200)
    requests_mock.get(
        "http://localhost:8088/api/v1/dashboard/", json={}, status_code=200
    )
    for entity in ["dataset", "dashboard", "chart"]:
        requests_mock.get(
            f"http://localhost:8088/api/v1/{entity}/related/owners",
            json={},
            status_code=200,
        )
    return SupersetSource(
        ctx=PipelineContext(run_id="position-json-test"), config=config
    )


def _seed_processed_chart(
    source: SupersetSource,
    chart_id: int,
    *,
    database_name: Optional[str] = None,
    is_filtered: bool = False,
) -> None:
    """Seed ``processed_charts`` so dashboard processing finds the chart
    as already-ingested. Centralised so the int-key contract has one
    locator if it ever changes."""
    source.processed_charts[chart_id] = (database_name, is_filtered)


def _mock_charts_endpoint(
    requests_mock: rm.Mocker,
    dashboard_id: int,
    *,
    chart_ids: Optional[List[int]] = None,
    status_code: int = 200,
) -> rm.adapter._Matcher:
    """Explicitly register the /charts fallback for a dashboard.

    Tests must opt in: ``_build_source`` does not register a catch-all so an
    unmocked ``/charts`` call raises ``NoMockAddress`` and surfaces as a
    test failure rather than being silently absorbed. Pass ``chart_ids=[]``
    to register a successful empty response.
    """
    if status_code == 200:
        body = {"result": [{"id": cid} for cid in (chart_ids or [])]}
        return requests_mock.get(
            f"http://localhost:8088/api/v1/dashboard/{dashboard_id}/charts",
            json=body,
            status_code=200,
        )
    return requests_mock.get(
        f"http://localhost:8088/api/v1/dashboard/{dashboard_id}/charts",
        text="error",
        status_code=status_code,
    )


def _context_field_values(
    entries: list, *, title: str, field: str
) -> List[Optional[object]]:
    """Extract a single field from JSON-serialised report-entry contexts.

    Report entries store ``context`` as a JSON string produced by
    ``_warning_context``; tests parse it back instead of substring-matching
    so they don't couple to JSON whitespace/key-ordering. Returns the list
    of values seen for the given field across all matching entries.
    """
    values: List[Optional[object]] = []
    for entry in entries:
        if entry.title != title:
            continue
        ctxs = entry.context if isinstance(entry.context, list) else [entry.context]
        for ctx in ctxs:
            if not ctx:
                continue
            try:
                values.append(json.loads(ctx).get(field))
            except (TypeError, ValueError):
                continue
    return values


class TestParsePositionJson:
    """Unit tests for ``_parse_position_json``.

    The Preset/Superset dashboard list API does not return ``position_json``;
    only the dashboard detail API does. When the detail API fails or returns
    an empty/invalid value, the connector must keep going (rather than
    crashing in ``json.loads`` and dropping the dashboard) and emit a
    structured warning so the failure is visible in the ingestion summary.
    These tests pin the defensive parsing path.
    """

    def test_valid_position_json_returns_parsed_dict(
        self, requests_mock: rm.Mocker
    ) -> None:
        source = _build_source(requests_mock)
        result = source._parse_position_json(
            {
                "id": 1,
                "dashboard_title": "Test Dashboard",
                "position_json": '{"CHART-abc": {"meta": {"chartId": "10"}}}',
            }
        )
        assert result == {"CHART-abc": {"meta": {"chartId": "10"}}}
        assert source.report.num_dashboards_invalid_position_json == 0

    def test_missing_or_falsy_position_json_returns_empty_dict(
        self, requests_mock: rm.Mocker
    ) -> None:
        # Covers the three falsy cases that all collapse to ``or "{}"``: missing
        # key, ``None`` value, and empty string. None of these are "errors" — the
        # source data is benignly empty — so no warning and no counter.
        source = _build_source(requests_mock)
        for variant in (
            {"id": 1, "dashboard_title": "no_key"},
            {"id": 2, "dashboard_title": "explicit_none", "position_json": None},
            {"id": 3, "dashboard_title": "empty_str", "position_json": ""},
        ):
            assert source._parse_position_json(variant) == {}
        assert source.report.num_dashboards_invalid_position_json == 0
        assert not any(
            w.title == "Invalid position_json" for w in source.report.warnings
        )

    def test_invalid_json_emits_warning_and_returns_empty(
        self, requests_mock: rm.Mocker
    ) -> None:
        source = _build_source(requests_mock)
        assert (
            source._parse_position_json(
                {
                    "id": 99,
                    "dashboard_title": "Garbage Dashboard",
                    "position_json": "{not valid json",
                }
            )
            == {}
        )
        assert source.report.num_dashboards_invalid_position_json == 1
        assert any(w.title == "Invalid position_json" for w in source.report.warnings)

    def test_non_object_json_emits_warning_and_returns_empty(
        self, requests_mock: rm.Mocker
    ) -> None:
        # A JSON array would break ``position_data.items()`` downstream;
        # warn so it's diagnosable rather than silently swallowed.
        source = _build_source(requests_mock)
        assert (
            source._parse_position_json(
                {"id": 1, "dashboard_title": "list", "position_json": "[1, 2, 3]"}
            )
            == {}
        )
        assert source.report.num_dashboards_invalid_position_json == 1
        assert any(w.title == "Invalid position_json" for w in source.report.warnings)

    def test_already_parsed_dict_is_returned_directly(
        self, requests_mock: rm.Mocker
    ) -> None:
        # Some API clients pre-deserialise JSON fields; if position_json is
        # already a dict, accept it as-is rather than crashing on
        # json.loads(dict) which raises TypeError.
        source = _build_source(requests_mock)
        layout = {"CHART-1": {"meta": {"chartId": 42}}}
        result = source._parse_position_json(
            {"id": 1, "dashboard_title": "pre-parsed", "position_json": layout}
        )
        assert result == layout
        assert source.report.num_dashboards_invalid_position_json == 0

    def test_non_string_non_dict_position_json_emits_warning(
        self, requests_mock: rm.Mocker
    ) -> None:
        # A truthy non-string that isn't a dict (e.g. a list from an API
        # client that partially deserialised the field) must be caught and
        # reported rather than raising TypeError from json.loads.
        source = _build_source(requests_mock)
        result = source._parse_position_json(
            {"id": 1, "dashboard_title": "bad type", "position_json": [1, 2, 3]}
        )
        assert result == {}
        assert source.report.num_dashboards_invalid_position_json == 1
        assert any(w.title == "Invalid position_json" for w in source.report.warnings)


class TestDashboardLineageDefensive:
    """End-to-end tests for ``_process_dashboard`` covering the position_json
    failure modes that produce missing chart-to-dashboard lineage."""

    def test_detail_api_failure_yields_dashboard_without_crash(
        self, requests_mock: rm.Mocker
    ) -> None:
        # Detail API HTTP failure → ``get_dashboard_info`` returns ``{}`` and
        # emits the "Dashboard detail API failed" warning + counter at fetch time.
        # ``_process_dashboard`` should NOT additionally double-warn about the
        # missing position_json — that warning is reserved for the case where the
        # API call succeeded but returned no position data.
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            text="boom",
            status_code=500,
        )
        _mock_charts_endpoint(requests_mock, 42, chart_ids=[])

        work_units = list(source._process_dashboard(_list_api_dashboard(42)))

        assert len(work_units) == 1, (
            "Dashboard must be ingested even when detail API fails"
        )
        assert source.report.num_dashboards_detail_api_failures == 1
        assert source.report.num_dashboards_missing_position_json == 0
        assert source.report.num_dashboards_with_no_charts == 1
        titles = {w.title for w in source.report.warnings}
        assert "Dashboard detail API failed" in titles
        assert "Missing position_json" not in titles
        assert "Dashboard Construction Failed" not in titles

    def test_detail_api_failure_is_not_cached(self, requests_mock: rm.Mocker) -> None:
        # Failed responses must not be memoised, so a transient 5xx
        # doesn't poison subsequent calls for the same dashboard.
        source = _build_source(requests_mock)
        matcher = requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            text="boom",
            status_code=500,
        )

        source.get_dashboard_info(42)
        source.get_dashboard_info(42)

        assert matcher.call_count == 2
        assert source.report.num_dashboards_detail_api_failures == 2

    def test_detail_api_success_is_cached(self, requests_mock: rm.Mocker) -> None:
        source = _build_source(requests_mock)
        matcher = requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            json={"id": 42, "result": {"id": 42, "position_json": "{}"}},
            status_code=200,
        )

        source.get_dashboard_info(42)
        source.get_dashboard_info(42)

        assert matcher.call_count == 1, "Successful responses should be memoised"

    def test_detail_api_returns_empty_position_json_does_not_drop_dashboard(
        self, requests_mock: rm.Mocker
    ) -> None:
        # Detail API returns ``position_json=""``. Must produce a dashboard
        # with empty chart lineage and a ``Missing position_json`` warning
        # (NOT ``Dashboard detail API failed``, because the call succeeded).
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            json={"id": 42, "result": {"id": 42, "position_json": ""}},
            status_code=200,
        )
        _mock_charts_endpoint(requests_mock, 42, chart_ids=[])

        work_units = list(source._process_dashboard(_list_api_dashboard(42)))

        assert len(work_units) == 1
        assert source.report.num_dashboards_missing_position_json == 1
        assert source.report.num_dashboards_detail_api_failures == 0
        assert source.report.num_dashboards_invalid_position_json == 0
        assert source.report.num_dashboards_with_no_charts == 1
        titles = {w.title for w in source.report.warnings}
        assert "Missing position_json" in titles
        assert "Dashboard detail API failed" not in titles
        assert "Dashboard Construction Failed" not in titles

    def test_detail_api_returns_invalid_position_json_does_not_drop_dashboard(
        self, requests_mock: rm.Mocker
    ) -> None:
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            json={
                "id": 42,
                "result": {"id": 42, "position_json": "{not valid"},
            },
            status_code=200,
        )
        _mock_charts_endpoint(requests_mock, 42, chart_ids=[])

        work_units = list(source._process_dashboard(_list_api_dashboard(42)))

        assert len(work_units) == 1
        assert source.report.num_dashboards_invalid_position_json == 1
        assert source.report.num_dashboards_with_no_charts == 1
        titles = {w.title for w in source.report.warnings}
        assert "Invalid position_json" in titles
        assert "Dashboard Construction Failed" not in titles

    def test_detail_api_returns_null_position_json(
        self, requests_mock: rm.Mocker
    ) -> None:
        # ``None`` from the detail API result is filtered out by the
        # ``if value is not None`` merge guard, so ``position_json`` ends up
        # missing from ``dashboard_data`` rather than being set to None. Same
        # external behaviour as "key absent": no chart lineage, missing-position
        # warning, no invalid-JSON counter.
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            json={"id": 42, "result": {"id": 42, "position_json": None}},
            status_code=200,
        )
        _mock_charts_endpoint(requests_mock, 42, chart_ids=[])

        work_units = list(source._process_dashboard(_list_api_dashboard(42)))

        assert len(work_units) == 1
        assert source.report.num_dashboards_missing_position_json == 1
        assert source.report.num_dashboards_invalid_position_json == 0
        assert source.report.num_dashboards_with_no_charts == 1

    def test_position_json_with_no_chart_keys_is_counted(
        self, requests_mock: rm.Mocker
    ) -> None:
        # position_json is parseable JSON but has no CHART-* entries (e.g. a
        # dashboard built from rows/markdown only). The dashboard should
        # ingest cleanly with no error counters, but the
        # ``num_dashboards_with_no_charts`` summary counter still ticks.
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            json={
                "id": 42,
                "result": {
                    "id": 42,
                    "position_json": '{"GRID_ID": {"type": "GRID"}, "ROOT_ID": {"type": "ROOT"}}',
                },
            },
            status_code=200,
        )
        _mock_charts_endpoint(requests_mock, 42, chart_ids=[])

        work_units = list(source._process_dashboard(_list_api_dashboard(42)))

        assert len(work_units) == 1
        assert source.report.num_dashboards_with_no_charts == 1
        assert source.report.num_dashboards_missing_position_json == 0
        assert source.report.num_dashboards_invalid_position_json == 0
        assert source.report.num_dashboards_detail_api_failures == 0

    def test_detail_api_returns_valid_position_json_extracts_chart_urns(
        self, requests_mock: rm.Mocker
    ) -> None:
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            json={
                "id": 42,
                "result": {
                    "id": 42,
                    "position_json": (
                        '{"CHART-a": {"meta": {"chartId": "100"}}, '
                        '"CHART-b": {"meta": {"chartId": "101"}}}'
                    ),
                },
            },
            status_code=200,
        )

        work_units = list(source._process_dashboard(_list_api_dashboard(42)))

        assert len(work_units) == 1
        assert source.report.num_dashboards_missing_position_json == 0
        assert source.report.num_dashboards_with_no_charts == 0

        mce = work_units[0].metadata
        assert isinstance(mce, MetadataChangeEvent)
        dashboard_info = next(
            a for a in mce.proposedSnapshot.aspects if isinstance(a, DashboardInfoClass)
        )
        expected = {
            make_chart_urn(platform="superset", name=str(cid)) for cid in ("100", "101")
        }
        assert set(dashboard_info.charts) == expected

    def test_database_pattern_filtered_chart_emits_incomplete_ingestion_warning(
        self, requests_mock: rm.Mocker
    ) -> None:
        # When ``database_pattern`` is non-default the source pre-flights the
        # parsed ``position_data`` to find charts whose datasets belong to a
        # filtered database, and emits an "Incomplete Ingestion" warning. The
        # parse path must still go through ``_parse_position_json`` (otherwise
        # an empty position_json would crash here too). This test pins both:
        # the warning is emitted, AND the dashboard still ingests.
        config = SupersetConfig.model_validate(
            {"database_pattern": {"deny": ["bad_db"]}}
        )
        source = _build_source(requests_mock, config=config)
        # Seed the per-source filter map. The ``processed_charts`` mapping is
        # normally populated while emitting charts; here we inject directly so
        # the dashboard-side branch can be exercised in isolation.
        _seed_processed_chart(source, 100, database_name="bad_db", is_filtered=True)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            json={
                "id": 42,
                "result": {
                    "id": 42,
                    "position_json": '{"CHART-a": {"meta": {"chartId": 100}}}',
                },
            },
            status_code=200,
        )

        work_units = list(source._process_dashboard(_list_api_dashboard(42)))

        assert len(work_units) == 1
        titles = {w.title for w in source.report.warnings}
        assert "Incomplete Ingestion" in titles

    def test_outer_exception_increments_dropped_counter(
        self, requests_mock: rm.Mocker
    ) -> None:
        # The outer ``except Exception`` in ``_process_dashboard`` is a
        # last-resort safety net. When it fires, the dropped-counter must
        # tick so the failure is visible in the report. A malformed
        # ``changed_on_utc`` raises inside ``construct_dashboard_from_api_data``
        # after the position_json paths complete, exercising this branch.
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            json={
                "id": 42,
                "result": {
                    "id": 42,
                    "position_json": '{"CHART-a": {"meta": {"chartId": "1"}}}',
                },
            },
            status_code=200,
        )
        list_data = _list_api_dashboard(42)
        list_data["changed_on_utc"] = "definitely-not-a-timestamp"
        # The detail merge will overwrite changed_on_utc only if the detail
        # value is not None — leave it absent so the bad value survives.

        work_units = list(source._process_dashboard(list_data))

        assert len(work_units) == 0
        assert source.report.num_dashboards_dropped_unexpected_error == 1
        titles = {w.title for w in source.report.warnings}
        assert "Dashboard Construction Failed" in titles


class TestChartsFallback:
    """Tests for the ``/api/v1/dashboard/{id}/charts`` fallback.

    When ``position_json`` yields no chart references (because it is missing,
    empty, malformed, or just doesn't include the chart layout) the connector
    falls back to the dedicated charts endpoint. The fallback re-derives the
    chart-to-dashboard linkage; it does not re-emit chart aspects (those come
    from the chart-listing path)."""

    def _assert_chart_urns(
        self, work_units: list, expected_chart_ids: Set[str]
    ) -> None:
        mce = work_units[0].metadata
        assert isinstance(mce, MetadataChangeEvent)
        dashboard_info = next(
            a for a in mce.proposedSnapshot.aspects if isinstance(a, DashboardInfoClass)
        )
        expected = {
            make_chart_urn(platform="superset", name=cid) for cid in expected_chart_ids
        }
        assert set(dashboard_info.charts) == expected

    def test_fallback_recovers_when_position_json_missing(
        self, requests_mock: rm.Mocker
    ) -> None:
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            json={"id": 42, "result": {"id": 42}},
            status_code=200,
        )
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42/charts",
            json={"result": [{"id": 957}, {"id": 958}]},
            status_code=200,
        )

        work_units = list(source._process_dashboard(_list_api_dashboard(42)))

        assert len(work_units) == 1
        assert source.report.num_dashboards_recovered_via_charts_endpoint == 1
        assert source.report.num_dashboards_with_no_charts == 0
        assert source.report.num_dashboards_charts_api_failures == 0
        self._assert_chart_urns(work_units, {"957", "958"})

    def test_fallback_recovers_when_position_json_empty_string(
        self, requests_mock: rm.Mocker
    ) -> None:
        # Empty position_json + /charts returning charts → lineage recovered.
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            json={"id": 42, "result": {"id": 42, "position_json": ""}},
            status_code=200,
        )
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42/charts",
            json={"result": [{"id": 957}]},
            status_code=200,
        )

        work_units = list(source._process_dashboard(_list_api_dashboard(42)))

        assert source.report.num_dashboards_recovered_via_charts_endpoint == 1
        assert source.report.num_dashboards_with_no_charts == 0
        # The "missing position_json" warning still fires because the underlying
        # source data IS missing — the fallback doesn't suppress diagnostics.
        assert source.report.num_dashboards_missing_position_json == 1
        self._assert_chart_urns(work_units, {"957"})

    def test_fallback_recovers_when_position_json_invalid(
        self, requests_mock: rm.Mocker
    ) -> None:
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            json={"id": 42, "result": {"id": 42, "position_json": "{not valid"}},
            status_code=200,
        )
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42/charts",
            json={"result": [{"id": 1}, {"id": 2}, {"id": 3}]},
            status_code=200,
        )

        work_units = list(source._process_dashboard(_list_api_dashboard(42)))

        assert source.report.num_dashboards_invalid_position_json == 1
        assert source.report.num_dashboards_recovered_via_charts_endpoint == 1
        assert source.report.num_dashboards_with_no_charts == 0
        self._assert_chart_urns(work_units, {"1", "2", "3"})

    def test_fallback_recovers_when_position_json_has_no_chart_keys(
        self, requests_mock: rm.Mocker
    ) -> None:
        # The dashboard layout JSON parses fine but has only structural keys
        # (GRID_ID, ROOT_ID, DASHBOARD_VERSION_KEY) and no CHART-* entries —
        # a known Superset quirk for dashboards last edited via the legacy
        # API. The fallback recovers the actual attached charts.
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            json={
                "id": 42,
                "result": {
                    "id": 42,
                    "position_json": '{"GRID_ID": {}, "ROOT_ID": {}}',
                },
            },
            status_code=200,
        )
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42/charts",
            json={"result": [{"id": 100}]},
            status_code=200,
        )

        work_units = list(source._process_dashboard(_list_api_dashboard(42)))

        assert source.report.num_dashboards_recovered_via_charts_endpoint == 1
        assert source.report.num_dashboards_with_no_charts == 0
        self._assert_chart_urns(work_units, {"100"})

    def test_fallback_recovers_preset_v2_layout_without_chart_entries(
        self, requests_mock: rm.Mocker
    ) -> None:
        # Preset v2 layouts can have only structural keys with no CHART-* entries even when charts are attached.
        position_json = json.dumps(
            {
                "DASHBOARD_VERSION_KEY": "v2",
                "GRID_ID": {
                    "children": [],
                    "id": "GRID_ID",
                    "parents": ["ROOT_ID"],
                    "type": "GRID",
                },
                "HEADER_ID": {
                    "id": "HEADER_ID",
                    "meta": {"text": "Emulator detection improvements"},
                    "type": "HEADER",
                },
                "ROOT_ID": {
                    "children": ["GRID_ID"],
                    "id": "ROOT_ID",
                    "type": "ROOT",
                },
            }
        )
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/108",
            json={"id": 108, "result": {"id": 108, "position_json": position_json}},
            status_code=200,
        )
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/108/charts",
            json={"result": [{"id": 957}, {"id": 958}, {"id": 959}]},
            status_code=200,
        )

        work_units = list(source._process_dashboard(_list_api_dashboard(108)))

        assert source.report.num_dashboards_recovered_via_charts_endpoint == 1
        assert source.report.num_dashboards_with_no_charts == 0
        assert source.report.num_dashboards_invalid_position_json == 0
        assert source.report.num_dashboards_missing_position_json == 0
        self._assert_chart_urns(work_units, {"957", "958", "959"})

    def test_fallback_does_not_fire_when_position_json_has_charts(
        self, requests_mock: rm.Mocker
    ) -> None:
        # When position_json already yields chart IDs we MUST NOT call the
        # fallback endpoint — that would double the HTTP load on every working
        # dashboard. Use a strict matcher that fails the test if hit.
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            json={
                "id": 42,
                "result": {
                    "id": 42,
                    "position_json": '{"CHART-a": {"meta": {"chartId": "100"}}}',
                },
            },
            status_code=200,
        )
        fallback_matcher = requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42/charts",
            json={"result": [{"id": 999}]},
            status_code=200,
        )

        work_units = list(source._process_dashboard(_list_api_dashboard(42)))

        assert fallback_matcher.call_count == 0, (
            "Fallback endpoint must not be called when position_json yields charts"
        )
        assert source.report.num_dashboards_recovered_via_charts_endpoint == 0
        self._assert_chart_urns(work_units, {"100"})

    def test_fallback_failure_is_reported_and_does_not_crash(
        self, requests_mock: rm.Mocker
    ) -> None:
        # /charts returning 5xx must increment the failure counter, emit a
        # warning, and let the dashboard ingest with empty lineage. The
        # ``num_dashboards_with_no_charts`` counter still ticks because lineage
        # really IS missing.
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            json={"id": 42, "result": {"id": 42, "position_json": ""}},
            status_code=200,
        )
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42/charts",
            text="boom",
            status_code=500,
        )

        work_units = list(source._process_dashboard(_list_api_dashboard(42)))

        assert len(work_units) == 1
        assert source.report.num_dashboards_charts_api_failures == 1
        assert source.report.num_dashboards_recovered_via_charts_endpoint == 0
        assert source.report.num_dashboards_with_no_charts == 1
        titles = {w.title for w in source.report.warnings}
        assert "Dashboard charts API failed" in titles

    def test_fallback_failure_is_not_cached(self, requests_mock: rm.Mocker) -> None:
        # /charts fallback contract: a transient 5xx must not be memoised,
        # so subsequent calls retry on the network.
        source = _build_source(requests_mock)
        matcher = requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42/charts",
            text="boom",
            status_code=500,
        )

        source.get_dashboard_charts(42)
        source.get_dashboard_charts(42)

        assert matcher.call_count == 2
        assert source.report.num_dashboards_charts_api_failures == 2

    def test_fallback_success_is_cached(self, requests_mock: rm.Mocker) -> None:
        source = _build_source(requests_mock)
        matcher = requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42/charts",
            json={"result": [{"id": 1}]},
            status_code=200,
        )

        source.get_dashboard_charts(42)
        source.get_dashboard_charts(42)

        assert matcher.call_count == 1, (
            "Successful /charts responses should be memoised"
        )

    def test_fallback_skips_entries_without_chart_id(
        self, requests_mock: rm.Mocker
    ) -> None:
        # Defensive: malformed entries (missing id, non-dict, etc.) must not
        # break the recovery path; valid IDs in the same response should still
        # land.
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            json={"id": 42, "result": {"id": 42, "position_json": ""}},
            status_code=200,
        )
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42/charts",
            json={
                "result": [
                    {"id": 100},
                    {"slice_name": "no id here"},
                    "not a dict at all",
                    {"id": 101},
                ]
            },
            status_code=200,
        )

        work_units = list(source._process_dashboard(_list_api_dashboard(42)))

        assert source.report.num_dashboards_recovered_via_charts_endpoint == 1
        self._assert_chart_urns(work_units, {"100", "101"})


class TestChartsFallbackEdgeCases:
    """Additional fallback coverage: aggressive mode, type-contract,
    database_pattern interaction, malformed responses, and joint failures."""

    def _chart_urn(self, chart_id: str) -> str:
        return make_chart_urn(platform="superset", name=chart_id)

    def _dashboard_info(self, work_units: list) -> DashboardInfoClass:
        mce = work_units[0].metadata
        assert isinstance(mce, MetadataChangeEvent)
        return next(
            a for a in mce.proposedSnapshot.aspects if isinstance(a, DashboardInfoClass)
        )

    def test_fallback_does_not_call_charts_endpoint_when_position_json_has_charts(
        self, requests_mock: rm.Mocker
    ) -> None:
        # When position_json already yields chart IDs, the /charts
        # fallback must not fire — it would cost an extra HTTP round
        # trip per dashboard and could introduce chart IDs that
        # position_json deliberately omitted (e.g. soft-deleted slices).
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            json={
                "id": 42,
                "result": {
                    "id": 42,
                    "position_json": '{"CHART-a": {"meta": {"chartId": "100"}}}',
                },
            },
            status_code=200,
        )
        charts_matcher = _mock_charts_endpoint(requests_mock, 42, chart_ids=[200, 300])

        work_units = list(source._process_dashboard(_list_api_dashboard(42)))

        assert charts_matcher.call_count == 0, (
            "Fallback must not fire when position_json yields charts"
        )
        info = self._dashboard_info(work_units)
        assert info.charts == [self._chart_urn("100")]
        assert source.report.num_dashboards_recovered_via_charts_endpoint == 0

    def test_fallback_chart_unknown_to_chart_listing_warns_under_database_pattern(
        self, requests_mock: rm.Mocker
    ) -> None:
        # The /charts endpoint can return chart IDs that the chart-listing
        # path never saw (e.g. visibility/permission differences). Under a
        # non-default database_pattern we surface this as a distinct warning
        # so operators can see the lineage edge will dangle.
        config = SupersetConfig.model_validate(
            {"database_pattern": {"deny": ["bad_db"]}}
        )
        source = _build_source(requests_mock, config=config)
        # Chart 200 has been seen by the chart-listing path; chart 300 has not.
        _seed_processed_chart(source, 200, database_name="good_db", is_filtered=False)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            json={"id": 42, "result": {"id": 42, "position_json": ""}},
            status_code=200,
        )
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42/charts",
            json={"result": [{"id": 200}, {"id": 300}]},
            status_code=200,
        )

        work_units = list(source._process_dashboard(_list_api_dashboard(42)))

        assert len(work_units) == 1
        # Chart 300 is unknown → unknown counter ticks AND warning emitted.
        assert source.report.num_dashboards_charts_unknown == 1
        titles = {w.title for w in source.report.warnings}
        assert "Fallback chart not ingested" in titles
        # Both chart URNs still land on the dashboard — we don't drop the
        # edge, we just flag it. Operators can act on the warning.
        info = self._dashboard_info(work_units)
        assert set(info.charts) == {self._chart_urn("200"), self._chart_urn("300")}

    def test_fallback_with_database_pattern_filtered_chart_emits_filter_warning(
        self, requests_mock: rm.Mocker
    ) -> None:
        # Fallback-recovered chart that IS known to processed_charts AND is
        # filtered → still emits the existing "Incomplete Ingestion" warning.
        # Pins that the database_pattern path remains exercised for fallback
        # IDs (locking the chart_id type contract).
        config = SupersetConfig.model_validate(
            {"database_pattern": {"deny": ["bad_db"]}}
        )
        source = _build_source(requests_mock, config=config)
        _seed_processed_chart(source, 100, database_name="bad_db", is_filtered=True)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            json={"id": 42, "result": {"id": 42, "position_json": ""}},
            status_code=200,
        )
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42/charts",
            json={"result": [{"id": 100}]},
            status_code=200,
        )

        list(source._process_dashboard(_list_api_dashboard(42)))

        titles = {w.title for w in source.report.warnings}
        assert "Incomplete Ingestion" in titles
        assert "Fallback chart not ingested" not in titles
        assert source.report.num_dashboards_charts_unknown == 0

    def test_both_apis_failing_yields_dashboard_with_no_charts(
        self, requests_mock: rm.Mocker
    ) -> None:
        # If detail API and /charts both fail, the dashboard ingests with no
        # charts and BOTH per-endpoint failure counters tick. The
        # ``num_dashboards_with_no_charts`` summary counter also ticks because
        # lineage really IS missing.
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            text="boom",
            status_code=500,
        )
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42/charts",
            text="boom",
            status_code=503,
        )

        work_units = list(source._process_dashboard(_list_api_dashboard(42)))

        assert len(work_units) == 1
        assert source.report.num_dashboards_detail_api_failures == 1
        assert source.report.num_dashboards_charts_api_failures == 1
        assert source.report.num_dashboards_with_no_charts == 1
        titles = {w.title for w in source.report.warnings}
        assert "Dashboard detail API failed" in titles
        assert "Dashboard charts API failed" in titles

    def test_dashboard_with_no_id_drops_with_dedicated_counter(
        self, requests_mock: rm.Mocker
    ) -> None:
        # A malformed list-API entry without an ``id`` field is dropped with
        # its own counter and warning rather than being routed into
        # ``num_dashboards_detail_api_failures`` via a 404 on /dashboard/None
        # (which would mask real detail-API failures).
        source = _build_source(requests_mock)
        list_data = {
            "dashboard_title": "no id",
            "changed_on_utc": "2020-04-14T07:00:00.000000+0000",
        }

        work_units = list(source._process_dashboard(list_data))

        assert len(work_units) == 0
        assert source.report.num_dashboards_missing_id == 1
        assert source.report.num_dashboards_dropped_unexpected_error == 0
        assert source.report.num_dashboards_detail_api_failures == 0
        titles = {w.title for w in source.report.warnings}
        assert "Dashboard missing id" in titles

    def test_charts_endpoint_returning_null_result_is_handled(
        self, requests_mock: rm.Mocker
    ) -> None:
        # ``{"result": null}`` is an API contract violation, not an empty
        # dashboard. Must produce a warning and tick the failure counter so
        # operators can distinguish it from genuinely empty dashboards.
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            json={"id": 42, "result": {"id": 42, "position_json": ""}},
            status_code=200,
        )
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42/charts",
            json={"result": None},
            status_code=200,
        )

        work_units = list(source._process_dashboard(_list_api_dashboard(42)))

        assert len(work_units) == 1
        assert source.report.num_dashboards_recovered_via_charts_endpoint == 0
        assert source.report.num_dashboards_with_no_charts == 1
        assert source.report.num_dashboards_charts_api_failures == 1
        titles = {w.title for w in source.report.warnings}
        assert "Dashboard charts API malformed result" in titles

    def test_charts_endpoint_returning_non_list_result_is_handled(
        self, requests_mock: rm.Mocker
    ) -> None:
        # Same contract as the null case: a non-list result is treated as a
        # malformed response (warning + failure counter), not silently
        # collapsed to an empty dashboard.
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            json={"id": 42, "result": {"id": 42, "position_json": ""}},
            status_code=200,
        )
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42/charts",
            json={"result": "unexpected string"},
            status_code=200,
        )

        work_units = list(source._process_dashboard(_list_api_dashboard(42)))

        assert len(work_units) == 1
        assert source.report.num_dashboards_with_no_charts == 1
        assert source.report.num_dashboards_charts_api_failures == 1

    def test_malformed_chart_entries_increment_counter(
        self, requests_mock: rm.Mocker
    ) -> None:
        # /charts returning a valid list but with malformed entries must tick
        # a per-entry counter so operators can spot API contract changes.
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            json={"id": 42, "result": {"id": 42, "position_json": ""}},
            status_code=200,
        )
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42/charts",
            json={
                "result": [
                    {"id": 100},
                    {"slice_name": "no id"},
                    "bare string",
                    None,
                    {"id": 101},
                ]
            },
            status_code=200,
        )

        list(source._process_dashboard(_list_api_dashboard(42)))

        # Three malformed entries: missing-id dict, bare string, None.
        assert source.report.num_dashboards_charts_malformed_entries == 3
        assert source.report.num_dashboards_recovered_via_charts_endpoint == 1

    def test_chart_id_zero_is_not_silently_dropped(
        self, requests_mock: rm.Mocker
    ) -> None:
        # Pin the ``raw_chart_id is None`` semantics: chart_id=0 is a valid
        # Preset chart ID and must reach the database_pattern check rather
        # than being filtered out by a truthiness test.
        config = SupersetConfig.model_validate(
            {"database_pattern": {"deny": ["bad_db"]}}
        )
        source = _build_source(requests_mock, config=config)
        _seed_processed_chart(source, 0, database_name="bad_db", is_filtered=True)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            json={
                "id": 42,
                "result": {
                    "id": 42,
                    "position_json": '{"CHART-zero": {"meta": {"chartId": 0}}}',
                },
            },
            status_code=200,
        )

        list(source._process_dashboard(_list_api_dashboard(42)))

        # Filter warning fires → chart_id=0 was actually inspected, not skipped.
        titles = {w.title for w in source.report.warnings}
        assert "Incomplete Ingestion" in titles


class TestNetworkErrorHandling:
    """Network and decode failures must increment the per-endpoint
    ``api_failures`` counter and emit a structured warning, not be lumped
    under ``num_dashboards_dropped_unexpected_error``."""

    def test_detail_api_request_exception_is_reported(
        self, requests_mock: rm.Mocker
    ) -> None:
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            exc=requests.ConnectionError("DNS lookup failed"),
        )

        result = source.get_dashboard_info(42)

        assert result == {}
        assert source.report.num_dashboards_detail_api_failures == 1
        titles = {w.title for w in source.report.warnings}
        assert "Dashboard detail API network error" in titles

    def test_detail_api_json_decode_error_is_reported(
        self, requests_mock: rm.Mocker
    ) -> None:
        # 200 OK with non-JSON body — e.g. an HTML proxy error page that
        # snuck through with the wrong content-type header.
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            text="<html>oops</html>",
            status_code=200,
            headers={"Content-Type": "application/json"},
        )

        result = source.get_dashboard_info(42)

        assert result == {}
        assert source.report.num_dashboards_detail_api_failures == 1
        titles = {w.title for w in source.report.warnings}
        assert "Dashboard detail API decode error" in titles

    def test_detail_api_with_non_dict_result_is_not_cached(
        self, requests_mock: rm.Mocker
    ) -> None:
        # ``result: null`` (or any non-dict shape) must be reported and
        # not cached, so a single malformed response doesn't keep
        # returning a poisoned cached entry.
        source = _build_source(requests_mock)
        matcher = requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            json={"id": 42, "result": None},
            status_code=200,
        )

        result1 = source.get_dashboard_info(42)
        result2 = source.get_dashboard_info(42)

        assert result1 == {} == result2
        # Both calls hit the network so a transient malformed payload
        # doesn't poison the rest of the run.
        assert matcher.call_count == 2
        assert source.report.num_dashboards_detail_api_failures == 2
        titles = {w.title for w in source.report.warnings}
        assert "Dashboard detail API malformed payload" in titles

    def test_charts_api_request_exception_is_reported(
        self, requests_mock: rm.Mocker
    ) -> None:
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42/charts",
            exc=requests.Timeout("read timeout"),
        )

        result = source.get_dashboard_charts(42)

        assert result == []
        assert source.report.num_dashboards_charts_api_failures == 1
        titles = {w.title for w in source.report.warnings}
        assert "Dashboard charts API network error" in titles

    def _warning_context_strings(self, source: SupersetSource, title: str) -> list:
        # ``StructuredLogs`` aggregates contexts under (title, message), so
        # ``w.context`` is a list of context strings (one per call), not a
        # single string. Concatenate them all for inspection.
        out: list = []
        for w in source.report.warnings:
            if w.title == title:
                ctx = w.context
                if isinstance(ctx, list):
                    out.extend(ctx)
                elif ctx is not None:
                    out.append(ctx)
        return out

    def test_safe_body_preview_redacts_non_json(self, requests_mock: rm.Mocker) -> None:
        # Cookies in HTML auth pages must not leak into warning context.
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            text="<html>Set-Cookie: session=SECRET_TOKEN_DO_NOT_LEAK</html>",
            status_code=302,
            headers={"Content-Type": "text/html"},
        )

        source.get_dashboard_info(42)

        ctxs = self._warning_context_strings(source, "Dashboard detail API failed")
        assert ctxs, "Detail API failure warning must be emitted"
        for ctx in ctxs:
            assert "SECRET_TOKEN_DO_NOT_LEAK" not in ctx
            assert "non-json body" in ctx

    def test_warning_context_is_valid_json(self, requests_mock: rm.Mocker) -> None:
        # Pin that warning context is JSON-parseable, not Python repr —
        # downstream tooling (validation scripts, log shippers) can rely on
        # this contract.
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            text="oops",
            status_code=500,
        )

        source.get_dashboard_info(42)

        ctxs = self._warning_context_strings(source, "Dashboard detail API failed")
        assert ctxs, "Detail API failure warning must be emitted"
        parsed = json.loads(ctxs[0])
        assert parsed["dashboard_id"] == 42
        assert parsed["status_code"] == 500

    def test_safe_body_preview_redacts_html_with_spoofed_json_content_type(
        self, requests_mock: rm.Mocker
    ) -> None:
        # Defence-in-depth: a server returning an HTML auth page with a
        # spoofed ``Content-Type: application/json`` header must still be
        # redacted. ``_safe_body_preview`` falls back to body-shape heuristics
        # so the secret never lands in a structured warning.
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            text="<html><body>Set-Cookie: session=SECRET_SPOOFED_TOKEN</body></html>",
            status_code=401,
            headers={"Content-Type": "application/json"},
        )

        source.get_dashboard_info(42)

        ctxs = self._warning_context_strings(source, "Dashboard detail API failed")
        assert ctxs, "Detail API failure warning must be emitted"
        for ctx in ctxs:
            assert "SECRET_SPOOFED_TOKEN" not in ctx
            assert "non-json body" in ctx

    def test_charts_api_malformed_200_is_not_cached(
        self, requests_mock: rm.Mocker
    ) -> None:
        # /charts contract: an HTTP-200 with a non-list ``result`` must
        # be treated as a contract violation and not cached.
        source = _build_source(requests_mock)
        matcher = requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42/charts",
            json={"result": None},
            status_code=200,
        )

        result1 = source.get_dashboard_charts(42)
        result2 = source.get_dashboard_charts(42)

        assert result1 == [] == result2
        assert matcher.call_count == 2, "Malformed /charts response must NOT be cached"
        assert source.report.num_dashboards_charts_api_failures == 2
        titles = {w.title for w in source.report.warnings}
        assert "Dashboard charts API malformed result" in titles

    def test_charts_api_json_decode_error_is_reported(
        self, requests_mock: rm.Mocker
    ) -> None:
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42/charts",
            text="not json",
            status_code=200,
            headers={"Content-Type": "application/json"},
        )

        source.get_dashboard_charts(42)

        assert source.report.num_dashboards_charts_api_failures == 1
        titles = {w.title for w in source.report.warnings}
        assert "Dashboard charts API decode error" in titles
        assert "Dashboard charts API network error" not in titles

    def test_fetch_api_response_raises_on_unknown_counter_attr(
        self, requests_mock: rm.Mocker
    ) -> None:
        # A typo in failure_counter_attr would silently increment the wrong
        # counter (or raise AttributeError swallowed by the outer except).
        # The guard raises immediately so the bug surfaces at development time.
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            exc=requests.ConnectionError("refused"),
        )

        with pytest.raises(AttributeError, match="no counter"):
            source._fetch_api_response(
                "http://localhost:8088/api/v1/dashboard/42",
                failure_counter_attr="num_nonexistent_counter",
                base_context={"dashboard_id": 42},
                endpoint="Dashboard detail API",
            )


class TestDatasetInfoCaching:
    """``get_dataset_info`` contract: cache only successful 2xx responses
    whose payload contains a dict ``result``. HTTP failure, network error,
    JSON-decode error, and non-dict ``result`` each return ``{}`` with a
    structured warning and increment ``num_datasets_detail_api_failures``;
    failure is never memoised."""

    def test_success_is_cached(self, requests_mock: rm.Mocker) -> None:
        source = _build_source(requests_mock)
        matcher = requests_mock.get(
            "http://localhost:8088/api/v1/dataset/7",
            json={"id": 7, "result": {"id": 7, "table_name": "t"}},
            status_code=200,
        )

        source.get_dataset_info(7)
        source.get_dataset_info(7)

        assert matcher.call_count == 1

    def test_http_failure_is_not_cached(self, requests_mock: rm.Mocker) -> None:
        source = _build_source(requests_mock)
        matcher = requests_mock.get(
            "http://localhost:8088/api/v1/dataset/7",
            text="boom",
            status_code=500,
        )

        result1 = source.get_dataset_info(7)
        result2 = source.get_dataset_info(7)

        assert result1 == {} == result2
        assert matcher.call_count == 2
        assert source.report.num_datasets_detail_api_failures == 2
        titles = {w.title for w in source.report.warnings}
        assert "Dataset detail API failed" in titles

    def test_network_error_is_reported(self, requests_mock: rm.Mocker) -> None:
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dataset/7",
            exc=requests.ConnectionError("dns"),
        )

        result = source.get_dataset_info(7)

        assert result == {}
        assert source.report.num_datasets_detail_api_failures == 1
        titles = {w.title for w in source.report.warnings}
        assert "Dataset detail API network error" in titles

    def test_decode_error_is_reported(self, requests_mock: rm.Mocker) -> None:
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dataset/7",
            text="not json",
            status_code=200,
        )

        result = source.get_dataset_info(7)

        assert result == {}
        assert source.report.num_datasets_detail_api_failures == 1
        titles = {w.title for w in source.report.warnings}
        assert "Dataset detail API decode error" in titles

    def test_missing_id_drops_with_dedicated_counter(
        self, requests_mock: rm.Mocker
    ) -> None:
        # ``get_dataset_info(None)`` must not request /api/v1/dataset/None;
        # the dedicated counter keeps it distinguishable from real HTTP
        # failures.
        source = _build_source(requests_mock)

        result = source.get_dataset_info(None)

        assert result == {}
        assert source.report.num_datasets_missing_id == 1
        assert source.report.num_datasets_detail_api_failures == 0
        titles = {w.title for w in source.report.warnings}
        assert "Dataset missing id" in titles
        for req in requests_mock.request_history:
            assert "/api/v1/dataset/None" not in req.url
            assert not req.url.endswith("/api/v1/dataset/")

    def test_non_dict_result_is_not_cached(self, requests_mock: rm.Mocker) -> None:
        # ``{"result": null}`` would crash callers that do
        # dataset_response["result"] on every cache hit; must not be cached.
        source = _build_source(requests_mock)
        matcher = requests_mock.get(
            "http://localhost:8088/api/v1/dataset/7",
            json={"id": 7, "result": None},
            status_code=200,
        )

        r1 = source.get_dataset_info(7)
        r2 = source.get_dataset_info(7)

        assert r1 == {} == r2
        assert matcher.call_count == 2
        assert source.report.num_datasets_detail_api_failures == 2
        titles = {w.title for w in source.report.warnings}
        assert "Dataset detail API malformed payload" in titles


class TestExtractChartIdRejection:
    """``_extract_chart_id`` must reject ``False``/``""`` even though
    ``isinstance(False, int)`` is True; otherwise we'd mint
    ``urn:li:chart:(preset,False)`` URNs."""

    def test_extracts_int_chart_id(self) -> None:
        chart_id, was_uncoercible = _extract_chart_id_with_status(
            {"meta": {"chartId": 42}}
        )
        assert chart_id == 42
        assert was_uncoercible is False

    def test_extracts_str_chart_id(self) -> None:
        chart_id, was_uncoercible = _extract_chart_id_with_status(
            {"meta": {"chartId": "42"}}
        )
        assert chart_id == "42"
        assert was_uncoercible is False

    def test_accepts_chart_id_zero(self) -> None:
        chart_id, was_uncoercible = _extract_chart_id_with_status(
            {"meta": {"chartId": 0}}
        )
        assert chart_id == 0
        assert was_uncoercible is False

    def test_rejects_bool_chart_id(self) -> None:
        for value in (True, False):
            chart_id, was_uncoercible = _extract_chart_id_with_status(
                {"meta": {"chartId": value}}
            )
            assert chart_id is None, f"bool {value!r} must be rejected"
            assert was_uncoercible is True

    def test_rejects_empty_string_chart_id(self) -> None:
        chart_id, was_uncoercible = _extract_chart_id_with_status(
            {"meta": {"chartId": ""}}
        )
        assert chart_id is None
        assert was_uncoercible is True

    def test_rejects_whitespace_only_string_chart_id(self) -> None:
        # Whitespace-only strings would create URNs with embedded spaces
        # (e.g. ``urn:li:chart:(preset,   )``); drop them as uncoercible.
        chart_id, was_uncoercible = _extract_chart_id_with_status(
            {"meta": {"chartId": "   "}}
        )
        assert chart_id is None
        assert was_uncoercible is True

    def test_rejects_collection_chart_id(self) -> None:
        for value in ([1, 2], {"a": "b"}):
            chart_id, was_uncoercible = _extract_chart_id_with_status(
                {"meta": {"chartId": value}}
            )
            assert chart_id is None
            assert was_uncoercible is True

    def test_absence_is_not_uncoercible(self) -> None:
        # Missing chartId, missing meta, non-dict: all return (None, False).
        cases: tuple = ({"meta": {}}, {"meta": "string"}, {}, "not a dict")
        for position_value in cases:
            chart_id, was_uncoercible = _extract_chart_id_with_status(position_value)
            assert chart_id is None
            assert was_uncoercible is False, position_value

    def test_uncoercible_counter_ticks_during_dashboard_processing(
        self, requests_mock: rm.Mocker
    ) -> None:
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            json={
                "id": 42,
                "result": {
                    "id": 42,
                    "position_json": json.dumps(
                        {
                            "CHART-good": {"meta": {"chartId": 100}},
                            "CHART-bool": {"meta": {"chartId": False}},
                            "CHART-empty": {"meta": {"chartId": ""}},
                        }
                    ),
                },
            },
            status_code=200,
        )

        list(source._process_dashboard(_list_api_dashboard(42)))

        assert source.report.num_chart_ids_uncoercible == 2


class TestMalformedChartsAggregatedWarning:
    """``get_dashboard_charts`` must emit exactly one aggregated warning
    per dashboard when the /charts response contains malformed entries
    (so operators can locate which dashboard had bad entries) and zero
    warnings when all entries are valid."""

    def test_aggregated_warning_emitted_with_dashboard_id(
        self, requests_mock: rm.Mocker
    ) -> None:
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            json={"id": 42, "result": {"id": 42, "position_json": ""}},
            status_code=200,
        )
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42/charts",
            json={
                "result": [
                    {"id": 100},
                    {"slice_name": "no id"},
                    "bare string",
                    None,
                ]
            },
            status_code=200,
        )

        list(source._process_dashboard(_list_api_dashboard(42)))

        assert source.report.num_dashboards_charts_malformed_entries == 3
        warnings = [
            w
            for w in source.report.warnings
            if w.title == "Dashboard charts API malformed entries"
        ]
        assert len(warnings) == 1, (
            "Exactly one aggregated warning per dashboard, not one per bad entry"
        )
        ctx_str = (warnings[0].context or [""])[0]
        assert '"dashboard_id": 42' in ctx_str
        assert '"malformed_count": 3' in ctx_str

    def test_no_warning_when_all_entries_valid(self, requests_mock: rm.Mocker) -> None:
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            json={"id": 42, "result": {"id": 42, "position_json": ""}},
            status_code=200,
        )
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42/charts",
            json={"result": [{"id": 100}, {"id": 200}]},
            status_code=200,
        )

        list(source._process_dashboard(_list_api_dashboard(42)))

        assert source.report.num_dashboards_charts_malformed_entries == 0
        titles = {w.title for w in source.report.warnings}
        assert "Dashboard charts API malformed entries" not in titles

    def test_invalid_id_types_are_rejected_as_malformed(
        self, requests_mock: rm.Mocker
    ) -> None:
        # bool, str, dict, and list ids must all be treated as malformed so
        # they cannot be str()-ified into synthetic chart URNs.
        # isinstance(False, int) is True in Python so bool needs an explicit
        # guard before the int check.
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42/charts",
            json={
                "result": [
                    {"id": 100},  # valid — must be returned
                    {"id": False},  # bool — rejected
                    {"id": "99"},  # str — rejected
                    {"id": {}},  # dict — rejected
                    {"id": []},  # list — rejected
                ]
            },
            status_code=200,
        )

        result = source.get_dashboard_charts(42)

        assert result == [100]
        assert source.report.num_dashboards_charts_malformed_entries == 4


class TestSafeBodyPreviewEmailRedaction:
    """Preset error responses can include the requesting user's email;
    the body preview must redact common email shapes. The method is called
    from inside exception handlers so it must not raise secondary exceptions."""

    def test_undecodable_binary_body_returns_safe_fallback(
        self, requests_mock: rm.Mocker
    ) -> None:
        # A binary / improperly-encoded response body causes response.text to
        # raise UnicodeDecodeError. _safe_body_preview must catch this and
        # return a safe fallback rather than masking the original HTTP error.
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            content=b"\xff\xfe binary garbage \x00\x01",
            status_code=500,
            headers={"Content-Type": "application/json"},
        )

        # Must not raise; the outer warning must still be emitted.
        source.get_dashboard_info(42)

        titles = {w.title for w in source.report.warnings}
        assert "Dashboard detail API failed" in titles

    def test_email_in_json_body_is_redacted(self, requests_mock: rm.Mocker) -> None:
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            json={"message": "User alice.smith@example.com is not authorized"},
            status_code=403,
            headers={"Content-Type": "application/json"},
        )

        source.get_dashboard_info(42)

        ctxs = [
            ctx
            for w in source.report.warnings
            if w.title == "Dashboard detail API failed"
            for ctx in (w.context if isinstance(w.context, list) else [w.context])
            if ctx
        ]
        assert ctxs
        for ctx in ctxs:
            assert "alice.smith@example.com" not in ctx
            assert "<email>" in ctx


class TestDanglingFallbackWarningAlwaysFires:
    """The "Fallback chart not ingested" warning + the
    ``num_dashboards_charts_unknown`` counter must fire whenever the
    /charts fallback recovers a chart that wasn't ingested as an entity,
    regardless of ``database_pattern`` configuration."""

    def test_default_pattern_warns_for_dangling_fallback_chart(
        self, requests_mock: rm.Mocker
    ) -> None:
        source = _build_source(requests_mock)
        # Chart 200 was processed; chart 999 was not (dangling).
        _seed_processed_chart(source, 200)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            json={"id": 42, "result": {"id": 42, "position_json": ""}},
            status_code=200,
        )
        _mock_charts_endpoint(requests_mock, 42, chart_ids=[200, 999])

        list(source._process_dashboard(_list_api_dashboard(42)))

        assert source.report.num_dashboards_charts_unknown == 1
        titles = {w.title for w in source.report.warnings}
        assert "Fallback chart not ingested" in titles

    def test_no_warning_when_fallback_did_not_fire(
        self, requests_mock: rm.Mocker
    ) -> None:
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            json={
                "id": 42,
                "result": {
                    "id": 42,
                    "position_json": '{"CHART-a": {"meta": {"chartId": 100}}}',
                },
            },
            status_code=200,
        )

        list(source._process_dashboard(_list_api_dashboard(42)))

        assert source.report.num_dashboards_charts_unknown == 0
        titles = {w.title for w in source.report.warnings}
        assert "Fallback chart not ingested" not in titles


class TestFetchDatasetColumnsMissingId:
    """``_fetch_dataset_columns`` must tick ``num_charts_missing_datasource_id_for_cll``
    and emit a structured warning when called with a falsy datasource_id,
    so operators can locate which charts lost CLL."""

    def test_missing_datasource_id_increments_counter(
        self, requests_mock: rm.Mocker
    ) -> None:
        source = _build_source(requests_mock)

        result = source._fetch_dataset_columns(None)

        assert result == []
        assert source.report.num_charts_missing_datasource_id_for_cll == 1
        titles = {w.title for w in source.report.warnings}
        assert "Chart missing datasource_id" in titles

    def test_zero_datasource_id_treated_as_missing(
        self, requests_mock: rm.Mocker
    ) -> None:
        # ``if not datasource_id`` rejects 0 by design (Superset autoincrement
        # never assigns 0; treat as "no id").
        source = _build_source(requests_mock)

        source._fetch_dataset_columns(0)

        assert source.report.num_charts_missing_datasource_id_for_cll == 1


class TestExtractColumnsFromSqlNarrowException:
    """``_extract_columns_from_sql`` must catch only ``SqlglotError``,
    emit a structured warning with the SQL in context (not the message),
    and tick a counter. Non-sqlglot exceptions must propagate."""

    def test_parse_error_increments_counter_and_warns(
        self, requests_mock: rm.Mocker
    ) -> None:
        source = _build_source(requests_mock)

        result = source._extract_columns_from_sql("SELECT FROM WHERE BAD SQL ((")

        assert result == []
        assert source.report.num_metrics_sql_parse_errors == 1
        warnings = [
            w for w in source.report.warnings if w.title == "Metric SQL parse error"
        ]
        assert len(warnings) == 1

    def test_valid_sql_returns_columns_no_warning(
        self, requests_mock: rm.Mocker
    ) -> None:
        source = _build_source(requests_mock)

        result = source._extract_columns_from_sql("SELECT a, b FROM t")

        assert set(result) >= {"a", "b"}
        assert source.report.num_metrics_sql_parse_errors == 0

    def test_non_sqlglot_error_propagates(self, requests_mock: rm.Mocker) -> None:
        # Contract: only ``SqlglotError`` is swallowed. Any other
        # exception (e.g. TypeError from a non-string input) must
        # propagate so genuine bugs surface.
        source = _build_source(requests_mock)

        with pytest.raises(TypeError):
            source._extract_columns_from_sql(123)  # type: ignore[arg-type]


class TestFetchDatasetColumnsDetailFailure:
    """When the dataset detail API failed (or the chart's dataset has no
    payload), CLL must be skipped with a chart-level counter so operators
    can locate which chart lost CLL — the underlying dataset failure is
    already counted, but the chart-level impact is not."""

    def test_dataset_detail_unavailable_increments_chart_counter(
        self, requests_mock: rm.Mocker
    ) -> None:
        source = _build_source(requests_mock)
        # Dataset detail returns HTTP 500 → get_dataset_info returns {}.
        requests_mock.get(
            "http://localhost:8088/api/v1/dataset/77",
            text="boom",
            status_code=500,
        )

        result = source._fetch_dataset_columns(77, chart_id=42)

        assert result == []
        assert source.report.num_charts_missing_dataset_detail_for_cll == 1
        # The structured info entry must include the chart_id so the
        # CLL impact is locatable.
        chart_ids = _context_field_values(
            source.report.infos,
            title="Chart CLL skipped (no dataset detail)",
            field="chart_id",
        )
        assert 42 in chart_ids

    def test_zero_datasource_id_is_treated_as_missing_with_locator(
        self, requests_mock: rm.Mocker
    ) -> None:
        # ``0`` is not a valid Superset auto-increment id; treat as
        # "missing" but include the chart_id locator in context.
        source = _build_source(requests_mock)

        source._fetch_dataset_columns(0, chart_id=99)

        assert source.report.num_charts_missing_datasource_id_for_cll == 1
        chart_ids = _context_field_values(
            source.report.warnings,
            title="Chart missing datasource_id",
            field="chart_id",
        )
        assert 99 in chart_ids


class TestFetchDatasetColumnsMalformedEntries:
    """Columns / metrics with empty name or type must be dropped from
    CLL with per-failure-mode counters and structured info entries
    so operators can locate the affected chart."""

    def test_malformed_column_increments_counter(
        self, requests_mock: rm.Mocker
    ) -> None:
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dataset/77",
            json={
                "result": {
                    "columns": [
                        {"column_name": "good", "type": "VARCHAR"},
                        {"column_name": "", "type": "INT"},  # empty name
                        {"column_name": "no_type", "type": ""},
                    ],
                    "metrics": [],
                }
            },
            status_code=200,
        )

        result = source._fetch_dataset_columns(77, chart_id=42)

        assert result == [("good", "VARCHAR", "")]
        assert source.report.num_datasets_columns_dropped_malformed == 2

    def test_malformed_metric_increments_counter(
        self, requests_mock: rm.Mocker
    ) -> None:
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dataset/77",
            json={
                "result": {
                    "columns": [],
                    "metrics": [
                        {"metric_name": "good", "metric_type": "count"},
                        {"metric_name": "", "metric_type": "count"},
                    ],
                }
            },
            status_code=200,
        )

        result = source._fetch_dataset_columns(77, chart_id=42)

        # Malformed metric must not leak into output; only the well-formed
        # one survives.
        assert result == [("good", "count", "")]
        assert source.report.num_datasets_metrics_dropped_malformed == 1


class TestEmailRedactionInHtmlBody:
    """Email patterns in non-JSON HTML bodies are redacted by the
    content-type-aware fallback (the body itself is replaced by a
    type+length placeholder), so the email never reaches log context."""

    def test_email_in_html_body_does_not_leak(self, requests_mock: rm.Mocker) -> None:
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            text=(
                "<html><body>Login required for alice.smith@example.com</body></html>"
            ),
            status_code=403,
            headers={"Content-Type": "text/html"},
        )

        source.get_dashboard_info(42)

        ctxs = [
            ctx
            for w in source.report.warnings
            if w.title == "Dashboard detail API failed"
            for ctx in (w.context if isinstance(w.context, list) else [w.context])
            if ctx
        ]
        assert ctxs
        for ctx in ctxs:
            assert "alice.smith@example.com" not in ctx
            assert "<non-json body" in ctx


class TestDatasetPayloadMalformed:
    """Dataset detail returning a non-list ``columns`` or ``metrics`` payload
    must be treated as empty for CLL purposes (rather than crashing on
    iteration) and surface a structured warning + per-payload counter so
    operators can detect upstream API contract drift."""

    def test_non_list_columns_payload_does_not_crash(
        self, requests_mock: rm.Mocker
    ) -> None:
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dataset/77",
            json={"result": {"columns": "oops", "metrics": []}},
            status_code=200,
        )

        result = source._fetch_dataset_columns(77, chart_id=42)

        assert result == []
        assert source.report.num_datasets_columns_payload_malformed == 1
        assert source.report.num_datasets_metrics_payload_malformed == 0

    def test_non_list_metrics_payload_does_not_crash(
        self, requests_mock: rm.Mocker
    ) -> None:
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dataset/77",
            json={
                "result": {
                    "columns": [{"column_name": "good", "type": "VARCHAR"}],
                    "metrics": {"not": "a list"},
                }
            },
            status_code=200,
        )

        result = source._fetch_dataset_columns(77, chart_id=42)

        # Columns still ingest even when metrics are malformed.
        assert result == [("good", "VARCHAR", "")]
        assert source.report.num_datasets_metrics_payload_malformed == 1
        assert source.report.num_datasets_columns_payload_malformed == 0


class TestChartUrnDatasourceMissing:
    """``construct_chart_from_chart_data`` must use the same falsy-check
    contract as the CLL path so a chart with ``datasource_id`` of ``None``
    or ``0`` ticks ``num_charts_without_datasource_for_urn`` with a chart_id
    locator. Some chart types (markdown, header) legitimately have no
    datasource, so this is reported as info."""

    def _chart_payload(
        self, *, chart_id: int = 42, datasource_id: Optional[int] = None
    ) -> dict:
        return {
            "id": chart_id,
            "slice_name": "Markdown",
            "url": f"/chart/{chart_id}",
            "viz_type": "markdown",
            "changed_on_utc": "2020-04-14T07:00:00.000000+0000",
            "datasource_id": datasource_id,
            "params": "{}",
            "owners": [],
            "tags": [],
        }

    def test_none_datasource_id_increments_counter_with_locator(
        self, requests_mock: rm.Mocker
    ) -> None:
        source = _build_source(requests_mock)

        list(source.construct_chart_from_chart_data(self._chart_payload(chart_id=42)))

        assert source.report.num_charts_without_datasource_for_urn == 1
        chart_ids = _context_field_values(
            source.report.infos,
            title="Chart missing datasource_id (URN)",
            field="chart_id",
        )
        assert 42 in chart_ids

    def test_zero_datasource_id_treated_same_as_none(
        self, requests_mock: rm.Mocker
    ) -> None:
        # Mirrors the CLL-path semantics for ``0`` so operators can
        # reconcile both counters per chart.
        source = _build_source(requests_mock)

        list(
            source.construct_chart_from_chart_data(
                self._chart_payload(chart_id=99, datasource_id=0)
            )
        )

        assert source.report.num_charts_without_datasource_for_urn == 1
        chart_ids = _context_field_values(
            source.report.infos,
            title="Chart missing datasource_id (URN)",
            field="chart_id",
        )
        assert 99 in chart_ids


class TestChartParamsInvalidJson:
    """Malformed ``params`` JSON on a chart must be reported with a
    structured warning + counter; the chart itself still ingests with
    empty params-derived custom properties."""

    def _chart_payload(self, *, params: str) -> dict:
        return {
            "id": 7,
            "slice_name": "Bad Params",
            "url": "/chart/7",
            "viz_type": "table",
            "changed_on_utc": "2020-04-14T07:00:00.000000+0000",
            "datasource_id": None,
            "params": params,
            "owners": [],
            "tags": [],
        }

    def test_invalid_params_does_not_drop_chart(self, requests_mock: rm.Mocker) -> None:
        source = _build_source(requests_mock)

        work_units = list(
            source.construct_chart_from_chart_data(
                self._chart_payload(params="{not valid json")
            )
        )

        # Chart still ingests; params-derived properties are empty.
        assert work_units, "Chart must still emit when params is malformed"
        assert source.report.num_charts_invalid_params_json == 1
        titles = {w.title for w in source.report.warnings}
        assert "Chart params invalid JSON" in titles

    def test_valid_params_does_not_increment_counter(
        self, requests_mock: rm.Mocker
    ) -> None:
        # Happy-path counterpart: counter must NOT fire on valid params.
        source = _build_source(requests_mock)
        list(
            source.construct_chart_from_chart_data(
                self._chart_payload(params='{"metrics": [], "adhoc_filters": []}')
            )
        )
        assert source.report.num_charts_invalid_params_json == 0

    def test_non_dict_params_does_not_crash(self, requests_mock: rm.Mocker) -> None:
        # JSON scalar/array would crash ``params.get(...)``; the chart
        # must still ingest with empty params-derived properties.
        source = _build_source(requests_mock)

        work_units = list(
            source.construct_chart_from_chart_data(
                self._chart_payload(params="[1, 2, 3]")
            )
        )

        assert work_units


class TestParamsMetricsUnexpectedShape:
    """``params.metrics`` returning a non-list (dict, string) must skip
    derivation with a counter and info entry rather than crashing."""

    def _chart_payload(self) -> dict:
        return {
            "id": 11,
            "slice_name": "Bad Metrics",
            "url": "/chart/11",
            "viz_type": "table",
            "changed_on_utc": "2020-04-14T07:00:00.000000+0000",
            "datasource_id": None,
            "params": '{"metrics": {"not": "a list"}}',
            "owners": [],
            "tags": [],
        }

    def test_non_list_metrics_emits_info_and_skips(
        self, requests_mock: rm.Mocker
    ) -> None:
        source = _build_source(requests_mock)

        work_units = list(source.construct_chart_from_chart_data(self._chart_payload()))

        assert work_units
        assert source.report.num_charts_invalid_params_metrics_shape == 1
        titles = {entry.title for entry in source.report.infos}
        assert "Chart params.metrics unexpected shape" in titles

    def test_malformed_metric_entries_are_skipped_not_crash(
        self, requests_mock: rm.Mocker
    ) -> None:
        # Valid JSON with non-str/non-dict metric entries (e.g. ints) must
        # be silently skipped; the chart must still ingest.
        source = _build_source(requests_mock)
        payload = {
            "id": 11,
            "slice_name": "Mixed Metrics",
            "url": "/chart/11",
            "viz_type": "table",
            "changed_on_utc": "2020-04-14T07:00:00.000000+0000",
            "datasource_id": None,
            "params": '{"metrics": [123, "valid_metric", {"label": "agg_metric"}, []]}',
            "owners": [],
            "tags": [],
        }

        work_units = list(source.construct_chart_from_chart_data(payload))

        assert work_units, "Chart must still emit when metric entries are malformed"

    def test_malformed_adhoc_filters_are_skipped_not_crash(
        self, requests_mock: rm.Mocker
    ) -> None:
        # Non-dict filter entries (strings, ints) must be skipped rather than
        # crashing on filter_obj.get() with AttributeError.
        source = _build_source(requests_mock)
        payload = {
            "id": 12,
            "slice_name": "Bad Filters",
            "url": "/chart/12",
            "viz_type": "table",
            "changed_on_utc": "2020-04-14T07:00:00.000000+0000",
            "datasource_id": None,
            "params": '{"adhoc_filters": ["bad_string", 42, {"sqlExpression": "x > 1"}]}',
            "owners": [],
            "tags": [],
        }

        work_units = list(source.construct_chart_from_chart_data(payload))

        assert work_units, "Chart must still emit when filter entries are malformed"

    def test_non_list_adhoc_filters_is_handled(self, requests_mock: rm.Mocker) -> None:
        # adhoc_filters as a non-list (string) must not crash by iterating
        # over characters.
        source = _build_source(requests_mock)
        payload = {
            "id": 13,
            "slice_name": "String Filters",
            "url": "/chart/13",
            "viz_type": "table",
            "changed_on_utc": "2020-04-14T07:00:00.000000+0000",
            "datasource_id": None,
            "params": '{"adhoc_filters": "not a list"}',
            "owners": [],
            "tags": [],
        }

        work_units = list(source.construct_chart_from_chart_data(payload))

        assert work_units, "Chart must still emit when adhoc_filters is not a list"

    def test_filter_with_missing_clause_fields_returns_empty_not_none_string(
        self,
    ) -> None:
        # A filter missing clause/subject/operator must return "" not
        # "None None = None" — the latter is truthy and leaks garbage into
        # chart custom properties visible to end users.
        assert get_filter_name({"comparator": "2023-01-01"}) == ""
        assert get_filter_name({"clause": "WHERE", "comparator": "x"}) == ""
        assert get_filter_name({"subject": "col", "operator": "=="}) == ""


class TestDatasetColumnsNonDictItems:
    """Per-item shape guard: dataset detail returning ``columns`` /
    ``metrics`` lists whose items are not dicts must increment the
    drop counter rather than crash on ``column.get(...)``."""

    def test_non_dict_column_entries_are_dropped(
        self, requests_mock: rm.Mocker
    ) -> None:
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dataset/77",
            json={
                "result": {
                    "columns": [
                        "just_a_string",
                        {"column_name": "good", "type": "VARCHAR"},
                        42,
                    ],
                    "metrics": [],
                }
            },
            status_code=200,
        )

        result = source._fetch_dataset_columns(77, chart_id=42)

        assert result == [("good", "VARCHAR", "")]
        assert source.report.num_datasets_columns_dropped_malformed == 2

    def test_non_dict_metric_entries_are_dropped(
        self, requests_mock: rm.Mocker
    ) -> None:
        source = _build_source(requests_mock)
        requests_mock.get(
            "http://localhost:8088/api/v1/dataset/77",
            json={
                "result": {
                    "columns": [],
                    "metrics": [
                        {"metric_name": "good", "metric_type": "count"},
                        "bare_string",
                    ],
                }
            },
            status_code=200,
        )

        result = source._fetch_dataset_columns(77, chart_id=42)

        assert result == [("good", "count", "")]
        assert source.report.num_datasets_metrics_dropped_malformed == 1


class TestChartDatasourceUrnFailure:
    """When the dataset detail payload is structurally valid but missing
    fields needed to build a URN (table_name / database_id), the chart
    must still ingest with no input dataset and a dedicated counter so
    the failure isn't lumped into the generic dataset-API counter."""

    def _chart_payload(self) -> dict:
        return {
            "id": 13,
            "slice_name": "Chart with broken dataset",
            "url": "/chart/13",
            "viz_type": "table",
            "changed_on_utc": "2020-04-14T07:00:00.000000+0000",
            "datasource_id": 99,
            "params": "{}",
            "owners": [],
            "tags": [],
        }

    def test_urn_construction_failure_emits_counter(
        self, requests_mock: rm.Mocker
    ) -> None:
        source = _build_source(requests_mock)
        # Detail payload has a result but no table_name/database_id —
        # ``get_datasource_urn_from_id`` raises ValueError.
        requests_mock.get(
            "http://localhost:8088/api/v1/dataset/99",
            json={"result": {"id": 99}},
            status_code=200,
        )

        work_units = list(source.construct_chart_from_chart_data(self._chart_payload()))

        assert work_units
        assert source.report.num_charts_dropped_datasource_urn_failed == 1
        titles = {w.title for w in source.report.warnings}
        assert "Chart datasource URN unavailable" in titles


class TestUncoercibleChartIdLocator:
    """When ``_extract_chart_id_with_status`` flags an uncoercible
    chartId in a dashboard's position_json, the report must include a
    locator (dashboard_id + position_key) so operators can find the
    offending source-system entry."""

    def test_uncoercible_chart_id_emits_locator_info(
        self, requests_mock: rm.Mocker
    ) -> None:
        source = _build_source(requests_mock)
        position_json = '{"CHART-bad": {"meta": {"chartId": [1, 2]}}}'
        requests_mock.get(
            "http://localhost:8088/api/v1/dashboard/42",
            json={"id": 42, "result": {"id": 42, "position_json": position_json}},
            status_code=200,
        )
        _mock_charts_endpoint(requests_mock, 42, chart_ids=[])

        list(source._process_dashboard(_list_api_dashboard(42)))

        assert source.report.num_chart_ids_uncoercible == 1
        dashboard_ids = _context_field_values(
            source.report.infos,
            title="Uncoercible chart_id in position_json",
            field="dashboard_id",
        )
        assert 42 in dashboard_ids
        position_keys = _context_field_values(
            source.report.infos,
            title="Uncoercible chart_id in position_json",
            field="position_key",
        )
        assert "CHART-bad" in position_keys


class TestDatabasePatternIndeterminate:
    """When ``database_pattern`` is set but the chart's dataset has no
    ``database_name``, the filter check is effectively skipped — the
    chart is still recorded as not-filtered to keep dashboards complete,
    but operators must see this so they don't assume the pattern was
    honoured for the chart."""

    def _chart_payload(self) -> dict:
        return {
            "id": 21,
            "slice_name": "Filter-indeterminate Chart",
            "url": "/chart/21",
            "viz_type": "table",
            "changed_on_utc": "2020-04-14T07:00:00.000000+0000",
            "datasource_id": 88,
            "params": "{}",
            "owners": [],
            "tags": [],
        }

    def test_missing_database_name_emits_indeterminate_counter(
        self, requests_mock: rm.Mocker
    ) -> None:
        config = SupersetConfig.model_validate(
            {"database_pattern": {"deny": ["bad_db"]}}
        )
        source = _build_source(requests_mock, config=config)
        # Dataset detail has no database.database_name — filter cannot
        # be evaluated.
        requests_mock.get(
            "http://localhost:8088/api/v1/dataset/88",
            json={
                "result": {
                    "id": 88,
                    "table_name": "t",
                    "database": {"id": 1},
                }
            },
            status_code=200,
        )

        list(source._process_chart(self._chart_payload()))

        assert source.report.num_charts_database_pattern_indeterminate == 1
        chart_ids = _context_field_values(
            source.report.infos,
            title="database_pattern indeterminate",
            field="chart_id",
        )
        assert 21 in chart_ids


class TestTagsDefensiveShape:
    """``_extract_and_map_tags`` must tolerate ``tags: null`` and per-item
    non-dict shapes the Superset API has been observed to return,
    rather than crashing under the broad outer ``except``."""

    def test_non_list_tags_returns_none(self, requests_mock: rm.Mocker) -> None:
        source = _build_source(requests_mock)
        assert source._extract_and_map_tags(None) is None
        assert source._extract_and_map_tags("oops") is None

    def test_non_dict_items_are_skipped(self, requests_mock: rm.Mocker) -> None:
        source = _build_source(requests_mock)
        # Mix of valid + non-dict items; the valid one survives.
        result = source._extract_and_map_tags(
            [
                "bare_string",
                {"name": "good", "type": 1},
                42,
                {"name": "bad_type", "type": 2},
            ]
        )
        assert result is not None
        tag_names = [tag.tag.split(":")[-1] for tag in result.tags]
        assert tag_names == ["good"]
