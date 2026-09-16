import time
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import requests

from datahub.ingestion.source.hex.api import (
    HexApi,
    HexApiConnection,
    HexApiProjectApiResource,
    HexApiProjectsListResponse,
    HexApiReport,
    _extract_connection_defaults,
    _RateLimiter,
)
from datahub.ingestion.source.hex.model import (
    Component,
    Project,
)
from tests.unit.hex.conftest import load_json_data


class TestHexAPI:
    token = "test-token"
    base_url = "https://test.hex.tech/api/v1"
    page_size = 8  # Small page size to test pagination

    def setup_method(self) -> None:
        self.report = HexApiReport()

    def test_fetch_projects_pagination(self):
        page1_data = load_json_data("hex_projects_page1.json")
        page2_data = load_json_data("hex_projects_page2.json")

        mock_response1 = MagicMock()
        mock_response1.json.return_value = page1_data
        mock_response2 = MagicMock()
        mock_response2.json.return_value = page2_data

        hex_api = HexApi(
            token=self.token,
            report=self.report,
            base_url=self.base_url,
            page_size=self.page_size,
        )

        # Mock the session.get method after the session is created
        with patch.object(
            hex_api.session,
            "get",
            side_effect=[mock_response1, mock_response2],
        ) as mock_get:
            results = list(hex_api.fetch_projects())

        # check pagination
        assert mock_get.call_count == 2
        assert self.report.api_calls.get("list_projects.pages", 0) == 2
        assert self.report.api_projects_items == len(
            mock_response1.json()["values"]
        ) + len(mock_response2.json()["values"])

        # some random validations on the results

        assert len(results) == len(mock_response1.json()["values"]) + len(
            mock_response2.json()["values"]
        )
        assert all(isinstance(item, (Project, Component)) for item in results)
        assert {
            (item.id, item.title) for item in results if isinstance(item, Project)
        } == {
            ("827ea1f2-ed9a-425f-8d48-0ecc491c7c7c", "Welcome to Hex!-3"),
            ("e9d940fe-34ad-415b-ad12-cb4c201650dc", "Welcome to Hex!-4"),
            ("d73da67d-c87b-4dd8-9e7f-b79cb7f822cf", "PlayNotebook"),
            ("d05b0d81-6d00-4798-8967-6587b6731c0a", "Welcome to Hex!-6"),
            ("2ef730de-25ec-4131-94af-3517e743a738", "Welcome to Hex!"),
            ("c8f815c8-88c2-4dea-981f-69f544d6165d", "Welcome to Hex!-0"),
            ("89e64571-42d9-44ac-bf47-320a7440eb57", "Welcome to Hex!-5"),
            ("dd0f1e20-7586-4b8e-89ae-bfe3c924625b", "Welcome to Hex!-2"),
        }
        assert {
            (item.id, item.title) for item in results if isinstance(item, Component)
        } == {
            ("0496a2c2-8656-475d-9946-6402320779e2", "Pet Profiles"),
            ("4759f33c-1ab9-403d-92e8-9bef48de00c4", "Cancelled Orders"),
        }

    def test_map_data_project(self):
        # Test mapping of a project
        project_data = {
            "id": "project1",
            "title": "Test Project",
            "description": "A test project",
            "type": "PROJECT",
            "createdAt": "2022-01-01T12:00:00.000Z",
            "lastEditedAt": "2022-01-02T12:00:00.000Z",
            "status": {"name": "Published"},
            "categories": [{"name": "Category1", "description": "A category"}],
            "sharing": {"collections": [{"collection": {"name": "Collection1"}}]},
            "creator": {"email": "creator@example.com"},
            "owner": {"email": "owner@example.com"},
            "analytics": {
                "appViews": {
                    "allTime": 100,
                    "lastSevenDays": 10,
                    "lastFourteenDays": 20,
                    "lastThirtyDays": 30,
                },
                "lastViewedAt": "2022-01-03T12:00:00.000Z",
            },
        }

        hex_api = HexApi(
            token=self.token,
            report=self.report,
            base_url=self.base_url,
        )

        hex_api_project = HexApiProjectApiResource.model_validate(project_data)
        result = hex_api._map_data_from_model(hex_api_project)

        # Verify the result
        assert isinstance(result, Project)
        assert result.id == "project1"
        assert result.title == "Test Project"
        assert result.description == "A test project"
        assert result.created_at == datetime(2022, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        assert result.last_edited_at == datetime(
            2022, 1, 2, 12, 0, 0, tzinfo=timezone.utc
        )
        assert result.status and result.status.name == "Published"
        assert (
            result.categories
            and len(result.categories) == 1
            and result.categories[0].name == "Category1"
        )
        assert (
            result.collections
            and len(result.collections) == 1
            and result.collections[0].name == "Collection1"
        )
        assert result.creator and result.creator.email == "creator@example.com"
        assert result.owner and result.owner.email == "owner@example.com"
        assert (
            result.analytics
            and result.analytics.appviews_all_time == 100
            and result.analytics.last_viewed_at
            == datetime(2022, 1, 3, 12, 0, 0, tzinfo=timezone.utc)
        )

    def test_map_data_component(self):
        # Test mapping of a component
        component_data = {
            "id": "component1",
            "title": "Test Component",
            "description": "A test component",
            "type": "COMPONENT",
            "createdAt": "2022-02-01T12:00:00.000Z",
            "lastEditedAt": "2022-02-02T12:00:00.000Z",
            "status": {"name": "Draft"},
            "categories": [{"name": "Category2"}],
            "sharing": {"collections": [{"collection": {"name": "Collection2"}}]},
            "creator": {"email": "creator@example.com"},
            "owner": {"email": "owner@example.com"},
            "analytics": {
                "appViews": {
                    "allTime": 50,
                    "lastSevenDays": 5,
                    "lastFourteenDays": 10,
                    "lastThirtyDays": 15,
                },
                "lastViewedAt": "2022-02-03T12:00:00.000Z",
            },
        }

        hex_api = HexApi(
            token=self.token,
            report=self.report,
            base_url=self.base_url,
        )

        hex_api_component = HexApiProjectApiResource.model_validate(component_data)
        result = hex_api._map_data_from_model(hex_api_component)

        # Verify the result
        assert isinstance(result, Component)
        assert result.id == "component1"
        assert result.title == "Test Component"
        assert result.description == "A test component"
        assert result.created_at == datetime(2022, 2, 1, 12, 0, 0, tzinfo=timezone.utc)
        assert result.last_edited_at == datetime(
            2022, 2, 2, 12, 0, 0, tzinfo=timezone.utc
        )
        assert result.status and result.status.name == "Draft"
        assert (
            result.categories
            and len(result.categories) == 1
            and result.categories[0].name == "Category2"
        )
        assert (
            result.collections
            and len(result.collections) == 1
            and result.collections[0].name == "Collection2"
        )
        assert result.creator and result.creator.email == "creator@example.com"
        assert result.owner and result.owner.email == "owner@example.com"
        assert (
            result.analytics
            and result.analytics.appviews_all_time == 50
            and result.analytics.last_viewed_at
            == datetime(2022, 2, 3, 12, 0, 0, tzinfo=timezone.utc)
        )

    def test_fetch_projects_failure_http_error(self):
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "500 Server Error: Internal Server Error"
        )

        hex_api = HexApi(
            token=self.token,
            report=self.report,
            base_url=self.base_url,
        )

        # Mock the session.get method after the session is created
        with patch.object(hex_api.session, "get", return_value=mock_response):
            # No exception should be raised; gracefully finish with no results and proper error reporting
            results = list(hex_api.fetch_projects())

        # Verify results are empty and error was reported
        assert len(results) == 0
        assert self.report.api_calls.get("list_projects.pages", 0) == 1
        failures = list(self.report.failures)
        assert len(failures) == 1
        assert (
            failures[0].title
            and failures[0].title == "Listing Projects and Components API request error"
        )
        assert (
            failures[0].message
            and failures[0].message
            == "Error fetching Projects and Components and halting metadata ingestion"
        )
        assert failures[0].context

    @patch("datahub.ingestion.source.hex.api.HexApiProjectsListResponse.model_validate")
    def test_fetch_projects_failure_response_validation(self, mock_parse_obj):
        # Create a dummy http response
        mock_response = MagicMock()
        mock_response.json.return_value = {"whatever": "json"}
        # and simulate ValidationError when parsing the response
        mock_parse_obj.side_effect = lambda _: HexApiProjectApiResource.model_validate(
            {}
        )  # will raise ValidationError

        hex_api = HexApi(
            token=self.token,
            report=self.report,
            base_url=self.base_url,
        )

        # Mock the session.get method after the session is created
        with patch.object(hex_api.session, "get", return_value=mock_response):
            # No exception should be raised; gracefully finish with no results and proper error reporting
            results = list(hex_api.fetch_projects())

        # Verify results are empty and error was reported
        assert len(results) == 0
        assert self.report.api_calls.get("list_projects.pages", 0) == 1
        failures = list(self.report.failures)
        assert len(failures) == 1
        assert (
            failures[0].title
            and failures[0].title
            == "Listing Projects and Components API response parsing error"
        )
        assert (
            failures[0].message
            and failures[0].message
            == "Error parsing API response and halting metadata ingestion"
        )
        assert failures[0].context

    @patch("datahub.ingestion.source.hex.api.HexApiProjectsListResponse.model_validate")
    @patch("datahub.ingestion.source.hex.api.HexApi._map_data_from_model")
    def test_fetch_projects_warning_model_mapping(
        self, mock_map_data_from_model, mock_parse_obj
    ):
        # Create a dummy http response
        mock_get_response = MagicMock()
        mock_get_response.json.return_value = {"values": [{"whatever": "json"}]}
        # create a couple of dummy project items
        mock_parse_obj.return_value = HexApiProjectsListResponse(
            values=[
                HexApiProjectApiResource(
                    id="problem_item", title="Problem Item", type="PROJECT"
                ),
                HexApiProjectApiResource(
                    id="valid_item", title="Valid Item", type="PROJECT"
                ),
            ]
        )

        # and simulate an Error when mapping the response to a model
        def parse_side_effect(item_data):
            assert isinstance(item_data, HexApiProjectApiResource)
            if item_data.id == "problem_item":
                raise ValueError("Invalid data structure for problem_item")
            else:
                valid_item = MagicMock()
                valid_item.id = "valid_item"
                valid_item.title = "Valid Item"
                valid_item.type = "PROJECT"
                valid_item.description = "A valid project"
                valid_item.created_at = None
                valid_item.last_edited_at = None
                valid_item.status = None
                valid_item.categories = []
                valid_item.sharing = MagicMock(collections=[])
                valid_item.creator = None
                valid_item.owner = None
                valid_item.analytics = None
                return valid_item

        mock_map_data_from_model.side_effect = parse_side_effect

        hex_api = HexApi(
            token=self.token,
            report=self.report,
            base_url=self.base_url,
        )

        # Mock the session.get method after the session is created
        with patch.object(hex_api.session, "get", return_value=mock_get_response):
            # Should not raise exception, but log warning
            results = list(hex_api.fetch_projects())

        # We should still get the valid item but skip the problematic one
        assert len(results) == 1
        assert results[0].id == "valid_item"

        assert self.report.api_calls.get("list_projects.pages", 0) == 1
        warnings = list(self.report.warnings)
        assert len(warnings) == 1
        assert warnings[0].title and warnings[0].title == "Incomplete metadata"
        assert (
            warnings[0].message
            and warnings[0].message
            == "Incomplete metadata because of error mapping item"
        )
        assert warnings[0].context


class TestRateLimiter:
    def test_allows_calls_within_limit(self):
        limiter = _RateLimiter(max_calls=5, period=60.0)
        start = time.monotonic()
        for _ in range(5):
            limiter.acquire()
        # 5 calls within limit should be near-instant
        assert time.monotonic() - start < 1.0

    def test_sleeps_when_limit_exceeded(self):
        # 3 calls in 0.2s window — 4th should sleep
        limiter = _RateLimiter(max_calls=3, period=0.2)
        for _ in range(3):
            limiter.acquire()
        start = time.monotonic()
        limiter.acquire()  # this one should sleep ~0.2s
        elapsed = time.monotonic() - start
        assert elapsed >= 0.1  # at least waited some time


class TestExtractConnectionDefaults:
    """`_extract_connection_defaults` translates Hex's per-type
    `connectionDetails` into (default_database, default_schema) for sqlglot.
    Each test covers a distinct rule, not a distinct platform.
    """

    def test_extracts_db_with_non_database_key(self):
        # Happy path + non-obvious key: BigQuery uses `projectId`, not
        # `database`. Guards against a CONNECTION_TYPE_DEFAULTS row being
        # mis-keyed.
        db, schema = _extract_connection_defaults(
            "bigquery", {"projectId": "my-gcp-project"}
        )
        assert db == "my-gcp-project"
        assert schema is None

    def test_schema_fallback_applies_when_db_resolved(self):
        db, schema = _extract_connection_defaults("postgres", {"database": "warehouse"})
        assert db == "warehouse"
        assert schema == "public"

    def test_schema_fallback_suppressed_when_db_missing(self):
        # without a db, applying schema=public would yield an incomplete `.public.t` URN. Skip the fallback.
        db, schema = _extract_connection_defaults("postgres", {})
        assert db is None
        assert schema is None

    def test_two_part_platform_database_maps_to_schema_slot(self):
        # MySQL/MariaDB/Clickhouse: Hex's `database` field is the schema slot,
        # not the catalog slot. db MUST stay None to avoid a wrong 3-part URN.
        db, schema = _extract_connection_defaults("mysql", {"database": "app_db"})
        assert db is None
        assert schema == "app_db"

    def test_unknown_type_returns_nones(self):
        db, schema = _extract_connection_defaults("vertica", {"database": "x"})
        assert db is None
        assert schema is None


class TestFetchConnectionsParsing:
    """End-to-end: HTTP response → HexApiConnection. Verifies that
    `connectionDetails.<type>` is correctly unpacked into the API connection's
    default_database / default_schema fields.
    """

    def _hex_api(self) -> HexApi:
        return HexApi(
            token="t",
            report=HexApiReport(),
            base_url="https://test.hex.tech/api/v1",
            page_size=10,
        )

    def _mock_response(self, payload: dict) -> MagicMock:
        resp = MagicMock()
        resp.json.return_value = payload
        resp.raise_for_status = MagicMock()
        return resp

    def test_extracts_defaults_from_connection_details(self):
        api = self._hex_api()
        payload = {
            "values": [
                {
                    "id": "conn-sf",
                    "name": "Analytics",
                    "type": "snowflake",
                    "connectionDetails": {
                        "snowflake": {"database": "ANALYTICS", "schema": "PUBLIC"}
                    },
                },
                {
                    "id": "conn-bq",
                    "name": "BQ",
                    "type": "bigquery",
                    "connectionDetails": {"bigquery": {"projectId": "my-proj"}},
                },
            ]
        }
        with patch.object(
            api.session, "get", return_value=self._mock_response(payload)
        ):
            result = api.fetch_connections()

        sf = result["conn-sf"]
        assert isinstance(sf, HexApiConnection)
        assert sf.name == "Analytics"
        assert sf.type == "snowflake"
        assert sf.default_database == "ANALYTICS"
        assert sf.default_schema == "PUBLIC"

        bq = result["conn-bq"]
        assert bq.default_database == "my-proj"
        assert bq.default_schema is None

    def test_missing_connection_details_yields_none_defaults(self):
        api = self._hex_api()
        payload = {"values": [{"id": "conn-sf", "name": "A", "type": "snowflake"}]}
        with patch.object(
            api.session, "get", return_value=self._mock_response(payload)
        ):
            result = api.fetch_connections()
        assert result["conn-sf"].default_database is None
        assert result["conn-sf"].default_schema is None

    def test_unknown_type_yields_none_defaults_even_with_details(self):
        api = self._hex_api()
        payload = {
            "values": [
                {
                    "id": "conn-vt",
                    "name": "Vertica",
                    "type": "vertica",
                    "connectionDetails": {"vertica": {"database": "x"}},
                }
            ]
        }
        with patch.object(
            api.session, "get", return_value=self._mock_response(payload)
        ):
            result = api.fetch_connections()
        # Type not in CONNECTION_TYPE_DEFAULTS → no extraction even though
        # details exist. User must supply defaults via connection override.
        assert result["conn-vt"].default_database is None
        assert result["conn-vt"].default_schema is None
