"""A small fake Sigma tenant for the probe tests: one object per rule
ingestion applies, so a parity run exercises each.

- Workspaces: "Sales", "Finance", and one user's personal space (the API's
  "User Folder", which ingestion names "My documents").
- Workbooks: one per workspace, one whose workspace refuses the lookup (a
  shared entity), and one /files does not list.
- Data Models: one per workspace, and one whose workspace refuses the lookup.
"""

from typing import Any, Dict, List, Optional

API = "https://aws-api.sigmacomputing.com/v2"

SALES_WS = "11111111-0000-0000-0000-000000000001"
FINANCE_WS = "11111111-0000-0000-0000-000000000002"
PERSONAL_WS = "11111111-0000-0000-0000-000000000003"
# Named by /files, but answers 403: the objects under it are shared entities.
HIDDEN_WS = "11111111-0000-0000-0000-000000000004"

OWNER = "member-0001"
_STAMP = {
    "createdBy": OWNER,
    "updatedBy": OWNER,
    "createdAt": "2024-01-01T00:00:00.000Z",
    "updatedAt": "2024-01-02T00:00:00.000Z",
}

WORKBOOKS = {
    # id: (name, workspace id or None when /files omits it, path)
    "22222222-0000-0000-0000-000000000001": ("Revenue Overview", SALES_WS, "Sales"),
    "22222222-0000-0000-0000-000000000002": ("Budget Plan", FINANCE_WS, "Finance"),
    "22222222-0000-0000-0000-000000000003": (
        "Scratch Notes",
        PERSONAL_WS,
        "My Documents",
    ),
    "22222222-0000-0000-0000-000000000004": ("Shared Report", HIDDEN_WS, "Shared"),
    "22222222-0000-0000-0000-000000000005": ("Unfiled Report", None, "Sales"),
}

DATA_MODELS = {
    "33333333-0000-0000-0000-000000000001": ("Sales Model", SALES_WS),
    "33333333-0000-0000-0000-000000000002": ("Finance Model", FINANCE_WS),
    "33333333-0000-0000-0000-000000000003": ("Private Model", PERSONAL_WS),
    "33333333-0000-0000-0000-000000000004": ("Shared Model", HIDDEN_WS),
}


def _page(entries: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {"entries": entries, "total": len(entries), "nextPage": None}


def _workspace(workspace_id: str, name: str) -> Dict[str, Any]:
    return {"workspaceId": workspace_id, "name": name, **_STAMP}


def _file(
    object_id: str, name: str, file_type: str, parent: str, path: str
) -> Dict[str, Any]:
    return {
        "id": object_id,
        "urlId": f"url-{object_id[-4:]}",
        "name": name,
        "type": file_type,
        "parentId": parent,
        "path": path,
        "badge": None,
        **_STAMP,
    }


def register_tenant(
    requests_mock: Any, *, overrides: Optional[Dict[str, Dict[str, Any]]] = None
) -> None:
    """Every endpoint ingestion and the probe read for this tenant. An
    override maps a URL to requests_mock keyword arguments."""
    urls: Dict[str, Dict[str, Any]] = {
        f"{API}/workspaces?limit=50": {
            "json": _page(
                [
                    _workspace(SALES_WS, "Sales"),
                    _workspace(FINANCE_WS, "Finance"),
                    _workspace(PERSONAL_WS, "User Folder"),
                ]
            )
        },
        f"{API}/workspaces/{HIDDEN_WS}": {"status_code": 403, "json": {}},
        f"{API}/members?limit=50": {"json": _page([])},
        f"{API}/connections": {"json": _page([])},
        f"{API}/datasets": {"json": _page([])},
        f"{API}/files?permissionFilter=view&typeFilters=dataset": {"json": _page([])},
        f"{API}/files?permissionFilter=view&typeFilters=workbook": {
            "json": _page(
                [
                    _file(wb_id, name, "workbook", workspace, path)
                    for wb_id, (name, workspace, path) in WORKBOOKS.items()
                    if workspace is not None
                ]
            )
        },
        f"{API}/files?permissionFilter=view&typeFilters=data-model": {
            "json": _page(
                [
                    _file(dm_id, name, "data-model", workspace, "x")
                    for dm_id, (name, workspace) in DATA_MODELS.items()
                ]
            )
        },
        f"{API}/workbooks": {
            "json": _page(
                [
                    {
                        "workbookId": wb_id,
                        "workbookUrlId": f"url-{wb_id[-4:]}",
                        "ownerId": OWNER,
                        "name": name,
                        "url": f"https://app.example.com/org/workbook/{wb_id}",
                        "path": path,
                        "latestVersion": 1,
                        "isArchived": False,
                        **_STAMP,
                    }
                    for wb_id, (name, _, path) in WORKBOOKS.items()
                ]
            )
        },
        f"{API}/dataModels": {
            "json": _page(
                [
                    {
                        "dataModelId": dm_id,
                        "urlId": f"url-{dm_id[-4:]}",
                        "name": name,
                        "url": f"https://app.example.com/org/dm/{dm_id}",
                        "latestVersion": 1,
                        "workspaceId": workspace,
                        "path": "x",
                        **_STAMP,
                    }
                    for dm_id, (name, workspace) in DATA_MODELS.items()
                ]
            )
        },
    }
    for wb_id in WORKBOOKS:
        urls[f"{API}/workbooks/{wb_id}/pages"] = {"json": _page([])}
        urls[f"{API}/workbooks/{wb_id}/lineage"] = {"json": _page([])}
        urls[f"{API}/workbooks/{wb_id}/columns"] = {"json": _page([])}
    for dm_id in DATA_MODELS:
        for detail in ("elements", "columns", "lineage"):
            urls[f"{API}/dataModels/{dm_id}/{detail}"] = {"json": _page([])}
    urls.update(overrides or {})

    requests_mock.post(
        f"{API}/auth/token",
        json={
            "access_token": "fake-access-token",
            "refresh_token": "fake-refresh-token",
            "token_type": "bearer",
            "expires_in": 3599,
        },
    )
    for url, response in urls.items():
        requests_mock.get(url, **{"status_code": 200, **response})


def recipe(**overrides: object) -> Dict[str, object]:
    return {
        "client_id": "fake-client-id",
        "client_secret": "fake-client-secret",
        **overrides,
    }
