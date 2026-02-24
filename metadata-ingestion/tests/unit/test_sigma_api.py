from unittest.mock import MagicMock, patch

import pytest
import requests

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.sigma.config import (
    Constant,
    SigmaSourceConfig,
    SigmaSourceReport,
)
from datahub.ingestion.source.sigma.data_classes import SigmaDataset
from datahub.ingestion.source.sigma.sigma_api import SigmaAPI


def _make_response(json_data, status_code=200):
    response = MagicMock()
    response.json.return_value = json_data
    response.status_code = status_code
    response.raise_for_status.return_value = None
    return response


def _make_error_response(status_code):
    response = MagicMock()
    response.status_code = status_code
    http_error = requests.exceptions.HTTPError(
        f"{status_code} Server Error", response=response
    )
    response.raise_for_status.side_effect = http_error
    return response


def _make_token_response():
    return _make_response(
        {
            Constant.ACCESS_TOKEN: "test-access-token",
            Constant.REFRESH_TOKEN: "test-refresh-token",
        }
    )


def _make_file_entry(file_id, name, parent_id, path=None, badge=None):
    return {
        Constant.ID: file_id,
        Constant.NAME: name,
        Constant.PARENTID: parent_id,
        Constant.PATH: path or name,
        Constant.TYPE: Constant.DATASET,
        Constant.BADGE: badge,
    }


def _make_dataset_entry(dataset_id, name, description=""):
    return {
        "datasetId": dataset_id,
        "name": name,
        "description": description,
        "createdBy": "user1",
        "createdAt": "2024-01-01T00:00:00Z",
        "updatedAt": "2024-01-01T00:00:00Z",
        "url": f"https://sigma.test/dataset/{dataset_id}",
    }


def _make_workspace_entry(workspace_id, name):
    return {
        "workspaceId": workspace_id,
        "name": name,
        "createdBy": "user1",
        "createdAt": "2024-01-01T00:00:00Z",
        "updatedAt": "2024-01-01T00:00:00Z",
    }


def _make_paginated_response(entries, next_page=None):
    return _make_response(
        {
            Constant.ENTRIES: entries,
            Constant.NEXTPAGE: next_page,
        }
    )


@pytest.fixture
def mock_session():
    with patch("requests.Session") as session_cls:
        mock = session_cls.return_value
        mock.headers = MagicMock()
        mock.post.return_value = _make_token_response()
        yield mock


@pytest.fixture
def api_client(mock_session):
    config = SigmaSourceConfig(
        client_id="test-client-id",
        client_secret="test-client-secret",
        api_url="https://api.sigma.test/v2",
    )
    report = SigmaSourceReport()
    return SigmaAPI(config, report)


class TestGetSigmaDatasets:
    def test_returns_dataset_with_valid_workspace(self, api_client, mock_session):
        mock_session.get.side_effect = [
            # _get_files_metadata: GET /files?...&typeFilters=dataset
            _make_paginated_response(
                [_make_file_entry("ds1", "Dataset 1", parent_id="ws1", badge="Official")]
            ),
            # get_sigma_datasets: GET /datasets
            _make_paginated_response([_make_dataset_entry("ds1", "Dataset 1")]),
            # get_workspace: GET /workspaces/ws1
            _make_response(_make_workspace_entry("ws1", "Test Workspace")),
        ]

        datasets = api_client.get_sigma_datasets()

        assert len(datasets) == 1
        assert isinstance(datasets[0], SigmaDataset)
        assert datasets[0].datasetId == "ds1"
        assert datasets[0].name == "Dataset 1"
        assert datasets[0].workspaceId == "ws1"
        assert datasets[0].path == "Dataset 1"
        assert datasets[0].badge == "Official"

    def test_paginates_through_datasets(self, api_client, mock_session):
        mock_session.get.side_effect = [
            # _get_files_metadata: both files in one page
            _make_paginated_response(
                [
                    _make_file_entry("ds1", "Dataset 1", parent_id="ws1"),
                    _make_file_entry("ds2", "Dataset 2", parent_id="ws1"),
                ]
            ),
            # datasets page 1
            _make_paginated_response(
                [_make_dataset_entry("ds1", "Dataset 1")],
                next_page="page2",
            ),
            # get_workspace for ds1 (ws1 gets cached for ds2)
            _make_response(_make_workspace_entry("ws1", "Test Workspace")),
            # datasets page 2
            _make_paginated_response([_make_dataset_entry("ds2", "Dataset 2")]),
        ]

        datasets = api_client.get_sigma_datasets()

        assert len(datasets) == 2
        assert datasets[0].datasetId == "ds1"
        assert datasets[1].datasetId == "ds2"

    def test_drops_dataset_missing_file_metadata(self, api_client, mock_session):
        mock_session.get.side_effect = [
            # _get_files_metadata: no files returned
            _make_paginated_response([]),
            # GET /datasets returns a dataset with no matching file metadata
            _make_paginated_response([_make_dataset_entry("ds1", "Dataset 1")]),
        ]

        datasets = api_client.get_sigma_datasets()

        assert len(datasets) == 0

    def test_drops_dataset_when_workspace_pattern_denied(
        self, api_client, mock_session
    ):
        api_client.config.workspace_pattern = AllowDenyPattern(
            deny=["Denied Workspace"],
        )
        mock_session.get.side_effect = [
            _make_paginated_response(
                [_make_file_entry("ds1", "Dataset 1", parent_id="ws1")]
            ),
            _make_paginated_response([_make_dataset_entry("ds1", "Dataset 1")]),
            _make_response(_make_workspace_entry("ws1", "Denied Workspace")),
        ]

        datasets = api_client.get_sigma_datasets()

        assert len(datasets) == 0

    def test_includes_shared_entity_when_enabled(self, api_client, mock_session):
        api_client.config.ingest_shared_entities = True
        mock_session.get.side_effect = [
            _make_paginated_response(
                [_make_file_entry("ds1", "Dataset 1", parent_id="ws1")]
            ),
            _make_paginated_response([_make_dataset_entry("ds1", "Dataset 1")]),
            # workspace returns 403 (not accessible)
            _make_response({"error": "forbidden"}, status_code=403),
        ]

        datasets = api_client.get_sigma_datasets()

        assert len(datasets) == 1

    def test_drops_shared_entity_when_disabled(self, api_client, mock_session):
        api_client.config.ingest_shared_entities = False
        mock_session.get.side_effect = [
            _make_paginated_response(
                [_make_file_entry("ds1", "Dataset 1", parent_id="ws1")]
            ),
            _make_paginated_response([_make_dataset_entry("ds1", "Dataset 1")]),
            # workspace returns 403 (not accessible)
            _make_response({"error": "forbidden"}, status_code=403),
        ]

        datasets = api_client.get_sigma_datasets()

        assert len(datasets) == 0

    def test_returns_empty_on_http_error(self, api_client, mock_session):
        mock_session.get.side_effect = [
            _make_paginated_response(
                [_make_file_entry("ds1", "Dataset 1", parent_id="ws1")]
            ),
            # datasets endpoint returns 500
            _make_error_response(500),
        ]

        datasets = api_client.get_sigma_datasets()

        assert len(datasets) == 0
