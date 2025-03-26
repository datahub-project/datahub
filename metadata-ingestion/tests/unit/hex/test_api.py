import json
import os
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import requests
from pydantic import ValidationError

from datahub.ingestion.source.hex.api import (
    HexApi,
    HexApiProjectApiResource,
    HexApiProjectsListResponse,
    HexApiReport,
)
from datahub.ingestion.source.hex.model import (
    Component,
    Project,
)


# Helper to load test data from JSON files
def load_json_data(filename):
    test_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    file_path = test_dir / "test_data" / filename
    with open(file_path, "r") as f:
        return json.load(f)


class TestHexAPI(unittest.TestCase):
    def setUp(self):
        self.token = "test-token"
        self.report = HexApiReport()
        self.base_url = "https://test.hex.tech/api/v1"
        self.page_size = 8  # Small page size to test pagination

    @patch("datahub.ingestion.source.hex.api.requests.get")
    def test_fetch_projects_pagination(self, mock_get):
        page1_data = load_json_data("hex_projects_page1.json")
        page2_data = load_json_data("hex_projects_page2.json")

        mock_response1 = MagicMock()
        mock_response1.json.return_value = page1_data
        mock_response2 = MagicMock()
        mock_response2.json.return_value = page2_data

        mock_get.side_effect = [mock_response1, mock_response2]

        hex_api = HexApi(
            token=self.token,
            report=self.report,
            base_url=self.base_url,
            page_size=self.page_size,
        )

        results = list(hex_api.fetch_projects())

        # check pagination

        assert mock_get.call_count == 2
        assert self.report.fetch_projects_page_calls == 2
        assert self.report.fetch_projects_page_items == len(
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

    @patch("datahub.ingestion.source.hex.api.requests.get")
    def test_map_data_project(self, mock_get):
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

        hex_api_project = HexApiProjectApiResource.parse_obj(project_data)
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

    @patch("datahub.ingestion.source.hex.api.requests.get")
    def test_map_data_component(self, mock_get):
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

        hex_api_component = HexApiProjectApiResource.parse_obj(component_data)
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

    @patch("datahub.ingestion.source.hex.api.requests.get")
    def test_fetch_projects_failure_http_error(self, mock_get):
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "500 Server Error: Internal Server Error"
        )
        mock_get.return_value = mock_response

        hex_api = HexApi(
            token=self.token,
            report=self.report,
            base_url=self.base_url,
        )

        # No exception should be raised; gracefully finish with no results and proper error reporting
        results = list(hex_api.fetch_projects())

        # Verify results are empty and error was reported
        assert len(results) == 0
        assert self.report.fetch_projects_page_calls == 1
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

    @patch("datahub.ingestion.source.hex.api.requests.get")
    @patch("datahub.ingestion.source.hex.api.HexApiProjectsListResponse.parse_obj")
    def test_fetch_projects_failure_response_validation(self, mock_parse_obj, mock_get):
        # Create a dummy http response
        mock_response = MagicMock()
        mock_response.json.return_value = {"whatever": "json"}
        mock_get.return_value = mock_response
        # and simulate ValidationError when parsing the response
        mock_parse_obj.side_effect = ValidationError([], model=HexApiProjectApiResource)

        hex_api = HexApi(
            token=self.token,
            report=self.report,
            base_url=self.base_url,
        )

        # No exception should be raised; gracefully finish with no results and proper error reporting
        results = list(hex_api.fetch_projects())

        # Verify results are empty and error was reported
        assert len(results) == 0
        assert self.report.fetch_projects_page_calls == 1
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

    @patch("datahub.ingestion.source.hex.api.requests.get")
    @patch("datahub.ingestion.source.hex.api.HexApiProjectsListResponse.parse_obj")
    @patch("datahub.ingestion.source.hex.api.HexApi._map_data_from_model")
    def test_fetch_projects_warning_model_mapping(
        self, mock_map_data_from_model, mock_parse_obj, mock_get
    ):
        # Create a dummy http response
        mock_get_response = MagicMock()
        mock_get_response.json.return_value = {"values": [{"whatever": "json"}]}
        mock_get.return_value = mock_get_response
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

        # Should not raise exception, but log warning
        results = list(hex_api.fetch_projects())

        # We should still get the valid item but skip the problematic one
        assert len(results) == 1
        assert results[0].id == "valid_item"

        assert self.report.fetch_projects_page_calls == 1
        warnings = list(self.report.warnings)
        assert len(warnings) == 1
        assert warnings[0].title and warnings[0].title == "Incomplete metadata"
        assert (
            warnings[0].message
            and warnings[0].message
            == "Incomplete metadata because of error mapping item"
        )
        assert warnings[0].context
