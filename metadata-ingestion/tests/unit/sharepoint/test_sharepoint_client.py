from datetime import datetime, timezone
from typing import Dict, List
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.source.sharepoint.sharepoint_client import (
    SharePointClient,
    SharePointClientError,
    SharePointDrive,
    SharePointItem,
    SharePointPage,
    SharePointSite,
)
from datahub.ingestion.source.sharepoint.sharepoint_config import SharePointAuthConfig


@pytest.fixture
def auth_config() -> SharePointAuthConfig:
    return SharePointAuthConfig.model_validate(
        {
            "tenant_id": "tenant-abc",
            "client_id": "app-123",
            "client_secret": "secret-xyz",
        }
    )


@pytest.fixture
def client(auth_config: SharePointAuthConfig) -> SharePointClient:
    return SharePointClient(auth=auth_config, hostname="myorg.sharepoint.com")


def _make_mock_msal_app(access_token: str = "test-token") -> MagicMock:
    mock_app = MagicMock()
    mock_app.acquire_token_for_client.return_value = {"access_token": access_token}
    return mock_app


def test_sharepoint_site_from_graph_full() -> None:
    raw = {
        "id": "site-1",
        "name": "Engineering",
        "displayName": "Engineering Hub",
        "webUrl": "https://myorg.sharepoint.com/sites/Engineering",
        "serverRelativeUrl": "/sites/Engineering",
    }
    site = SharePointSite.from_graph(raw)
    assert site.id == "site-1"
    assert site.name == "Engineering"
    assert site.display_name == "Engineering Hub"
    assert site.path == "/sites/Engineering"


def test_sharepoint_site_path_normalises_missing_slash() -> None:
    raw = {
        "id": "site-2",
        "name": "Docs",
        "displayName": "Documentation",
        "webUrl": "https://myorg.sharepoint.com/sites/Docs",
        "serverRelativeUrl": "sites/Docs",
    }
    site = SharePointSite.from_graph(raw)
    assert site.path == "/sites/Docs"


def test_sharepoint_site_derives_path_from_url_when_missing() -> None:
    raw = {
        "id": "root",
        "name": "",
        "displayName": "Root",
        "webUrl": "https://myorg.sharepoint.com",
        "serverRelativeUrl": "",
    }
    site = SharePointSite.from_graph(raw)
    assert site.path.startswith("/")


def test_sharepoint_drive_from_graph() -> None:
    raw = {
        "id": "drive-1",
        "name": "Documents",
        "driveType": "documentLibrary",
        "webUrl": "https://myorg.sharepoint.com/sites/Eng/Shared%20Documents",
    }
    drive = SharePointDrive.from_graph(raw)
    assert drive.is_document_library
    assert drive.name == "Documents"


def test_sharepoint_drive_not_document_library() -> None:
    raw = {
        "id": "drive-2",
        "name": "OneDrive",
        "driveType": "personal",
        "webUrl": "https://myorg-my.sharepoint.com/personal/user",
    }
    drive = SharePointDrive.from_graph(raw)
    assert not drive.is_document_library


def _item_raw(name: str = "report.csv", is_folder: bool = False) -> Dict:
    raw: Dict = {
        "id": "item-abc",
        "name": name,
        "webUrl": f"https://myorg.sharepoint.com/sites/Eng/Docs/{name}",
        "size": 1024,
        "lastModifiedDateTime": "2024-03-15T12:00:00Z",
        "createdDateTime": "2024-01-01T00:00:00Z",
        "parentReference": {"path": "/drives/xyz/root:/subdir"},
    }
    if is_folder:
        raw["folder"] = {"childCount": 3}
    else:
        raw["file"] = {"mimeType": "text/csv"}
        raw["@microsoft.graph.downloadUrl"] = "https://download.example.com/file"
    return raw


def test_sharepoint_item_file_extension() -> None:
    item = SharePointItem.from_graph(_item_raw("data.parquet"))
    assert item.extension == "parquet"
    assert not item.is_folder


def test_sharepoint_item_no_extension() -> None:
    item = SharePointItem.from_graph(_item_raw("README"))
    assert item.extension == ""


def test_sharepoint_item_is_folder() -> None:
    item = SharePointItem.from_graph(_item_raw("reports", is_folder=True))
    assert item.is_folder
    assert item.extension == ""


def test_sharepoint_item_full_path_with_parent() -> None:
    raw = _item_raw("sales.csv")
    raw["parentReference"]["path"] = "/drives/xyz/root:/Analytics/Q1"
    item = SharePointItem.from_graph(raw)
    path = item.full_path("Documents")
    assert "Documents" in path
    assert "sales.csv" in path


def test_sharepoint_item_datetime_parsed_as_utc() -> None:
    item = SharePointItem.from_graph(_item_raw())
    assert item.last_modified.tzinfo is not None
    assert item.created.tzinfo is not None


def test_sharepoint_page_from_graph() -> None:
    raw = {
        "id": "page-1",
        "title": "Welcome Page",
        "webUrl": "https://myorg.sharepoint.com/sites/Eng/SitePages/Welcome.aspx",
        "lastModifiedDateTime": "2024-06-01T09:30:00Z",
        "createdDateTime": "2024-05-01T08:00:00Z",
    }
    page = SharePointPage.from_graph(raw)
    assert page.title == "Welcome Page"
    assert page.id == "page-1"
    assert page.last_modified == datetime(2024, 6, 1, 9, 30, 0, tzinfo=timezone.utc)


def test_acquire_token_success(client: SharePointClient) -> None:
    mock_app = _make_mock_msal_app("good-token")
    with patch("msal.ConfidentialClientApplication", return_value=mock_app):
        token = client._acquire_token()
    assert token == "good-token"


def test_acquire_token_failure_raises(client: SharePointClient) -> None:
    mock_app = MagicMock()
    mock_app.acquire_token_for_client.return_value = {
        "error": "invalid_client",
        "error_description": "AADSTS70011: Invalid client",
    }
    with (
        patch("msal.ConfidentialClientApplication", return_value=mock_app),
        pytest.raises(SharePointClientError) as exc_info,
    ):
        client._acquire_token()
    assert "Failed to acquire token" in str(exc_info.value)


def test_get_raises_on_http_error(client: SharePointClient) -> None:
    mock_app = _make_mock_msal_app()
    mock_response = MagicMock()
    mock_response.ok = False
    mock_response.status_code = 403
    mock_response.content = b'{"error": {"message": "Access denied"}}'
    mock_response.json.return_value = {"error": {"message": "Access denied"}}

    with (
        patch("msal.ConfidentialClientApplication", return_value=mock_app),
        patch.object(client._session, "get", return_value=mock_response),
        pytest.raises(SharePointClientError) as exc_info,
    ):
        client._get("/sites/myorg.sharepoint.com")

    assert exc_info.value.status_code == 403


def test_get_retries_on_401(client: SharePointClient) -> None:
    mock_app = _make_mock_msal_app()

    first_response = MagicMock()
    first_response.ok = False
    first_response.status_code = 401
    first_response.content = b"{}"
    first_response.json.return_value = {}

    success_response = MagicMock()
    success_response.ok = True
    success_response.status_code = 200
    success_response.json.return_value = {"id": "site-1", "webUrl": "https://x"}

    with (
        patch("msal.ConfidentialClientApplication", return_value=mock_app),
        patch.object(
            client._session, "get", side_effect=[first_response, success_response]
        ),
    ):
        result = client._get("/sites/myorg.sharepoint.com")

    assert result == {"id": "site-1", "webUrl": "https://x"}


def test_paginate_follows_next_link(client: SharePointClient) -> None:
    mock_app = _make_mock_msal_app()

    page1 = MagicMock()
    page1.ok = True
    page1.json.return_value = {
        "value": [{"id": "item-1"}],
        "@odata.nextLink": "https://graph.microsoft.com/v1.0/next",
    }

    page2 = MagicMock()
    page2.ok = True
    page2.json.return_value = {"value": [{"id": "item-2"}]}

    with (
        patch("msal.ConfidentialClientApplication", return_value=mock_app),
        patch.object(client._session, "get", side_effect=[page1, page2]),
    ):
        results = list(client._paginate("/sites/foo/drives"))

    assert [r["id"] for r in results] == ["item-1", "item-2"]


def test_list_sites_filters_by_hostname(client: SharePointClient) -> None:
    mock_app = _make_mock_msal_app()

    sites_raw = [
        {
            "id": "s1",
            "name": "Engineering",
            "displayName": "Engineering",
            "webUrl": "https://myorg.sharepoint.com/sites/Engineering",
            "serverRelativeUrl": "/sites/Engineering",
        },
        {
            "id": "s2",
            "name": "HR",
            "displayName": "HR",
            "webUrl": "https://other.sharepoint.com/sites/HR",
            "serverRelativeUrl": "/sites/HR",
        },
    ]

    with (
        patch("msal.ConfidentialClientApplication", return_value=mock_app),
        patch.object(client, "_paginate", return_value=iter(sites_raw)),
    ):
        sites = list(client.list_sites())

    assert len(sites) == 1
    assert sites[0].name == "Engineering"


def test_list_items_recursive_skips_folders(client: SharePointClient) -> None:
    mock_app = _make_mock_msal_app()

    root_children = [
        _item_raw("data.csv"),
        _item_raw("reports", is_folder=True),
    ]
    nested_children = [
        _item_raw("summary.xlsx"),
    ]

    def fake_list_root(site_id: str, drive_id: str) -> List[SharePointItem]:
        return [SharePointItem.from_graph(r) for r in root_children]

    def fake_list_folder(
        drive_id: str, item_id: str, parent_path: str = ""
    ) -> List[SharePointItem]:
        return [SharePointItem.from_graph(r) for r in nested_children]

    with (
        patch("msal.ConfidentialClientApplication", return_value=mock_app),
        patch.object(client, "list_root_items", side_effect=fake_list_root),
        patch.object(client, "list_folder_children", side_effect=fake_list_folder),
    ):
        items = list(client.list_items_recursive("site-1", "drive-1", "Documents"))

    names = {item.name for item in items}
    assert "data.csv" in names
    assert "summary.xlsx" in names
    assert "reports" not in names


def test_get_page_html_extracts_inner_html(client: SharePointClient) -> None:
    mock_app = _make_mock_msal_app()

    canvas_data = {
        "canvasLayout": {
            "horizontalSections": [
                {
                    "columns": [
                        {
                            "webparts": [
                                {"innerHtml": "<p>Hello world</p>"},
                                {"innerHtml": "<p>Second section</p>"},
                            ]
                        }
                    ]
                }
            ]
        }
    }

    with (
        patch("msal.ConfidentialClientApplication", return_value=mock_app),
        patch.object(client, "_get", return_value=canvas_data),
    ):
        html = client.get_page_html("site-1", "page-1")

    assert "Hello world" in html
    assert "Second section" in html


def test_get_page_html_returns_empty_on_error(client: SharePointClient) -> None:
    mock_app = _make_mock_msal_app()

    with (
        patch("msal.ConfidentialClientApplication", return_value=mock_app),
        patch.object(
            client, "_get", side_effect=SharePointClientError(404, "Not found")
        ),
    ):
        html = client.get_page_html("site-1", "page-1")

    assert html == ""


def test_test_connectivity_success(client: SharePointClient) -> None:
    mock_app = _make_mock_msal_app()
    with (
        patch("msal.ConfidentialClientApplication", return_value=mock_app),
        patch.object(client, "_get", return_value={"id": "root"}),
    ):
        client.test_connectivity()  # Should not raise


def test_test_connectivity_raises_on_error(client: SharePointClient) -> None:
    mock_app = _make_mock_msal_app()
    with (
        patch("msal.ConfidentialClientApplication", return_value=mock_app),
        patch.object(
            client, "_get", side_effect=SharePointClientError(401, "Unauthorized")
        ),
        pytest.raises(SharePointClientError),
    ):
        client.test_connectivity()
