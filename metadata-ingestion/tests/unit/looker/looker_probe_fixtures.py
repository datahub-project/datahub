"""One fake Looker instance, served to `looker` ingestion and to the probe
through the same mocked SDK client, so a parity test compares two readers of
identical content. Placeholder names only."""

from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional
from unittest import mock

from looker_sdk.error import SDKError
from looker_sdk.sdk.api40.models import (
    Dashboard,
    DashboardElement,
    FolderBase,
    Look,
    LookmlModel,
    LookmlModelExplore,
    LookmlModelExploreField,
    LookmlModelExploreFieldset,
    LookmlModelNavExplore,
    LookWithQuery,
    PermissionSet,
    Query,
    Role,
    User,
)

CLIENT_ID = "probe-client-id"
CLIENT_SECRET = "probe-client-secret-value"
API_USER_EMAIL = "api-user@example.com"
PERSONAL_FOLDER_NAME = "Some Person"
GRANTED_PERMISSIONS = ["access_data", "explore", "see_lookml", "see_looks"]


def recipe(**overrides: Any) -> Dict[str, Any]:
    config: Dict[str, Any] = {
        "base_url": "https://looker.example.com",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "extract_usage_history": False,
        "extract_owners": False,
        "max_threads": 1,
    }
    config.update(overrides)
    return config


def sdk_error(status: int, message: str = "request failed") -> SDKError:
    # The shape looker_sdk raises: the status lives in the documentation URL.
    return SDKError(
        message,
        documentation_url=(
            f"https://cloud.google.com/looker/docs/r/err/4.0/{status}/get/placeholder"
        ),
    )


SHARED = FolderBase(id="f-shared", name="Shared")
SALES = FolderBase(id="f-sales", name="Sales", parent_id="f-shared")
ARCHIVE = FolderBase(id="f-archive", name="Archive", parent_id="f-shared")
USERS = FolderBase(id="f-users", name="Users")
PERSONAL = FolderBase(
    id="f-personal",
    name=PERSONAL_FOLDER_NAME,
    parent_id="f-users",
    is_personal=True,
    is_personal_descendant=True,
)
ANCESTORS: Dict[str, List[FolderBase]] = {
    "f-shared": [],
    "f-sales": [SHARED],
    "f-archive": [SHARED],
    "f-users": [],
    "f-personal": [USERS],
}


def _vis(element_id: str, explore: str) -> DashboardElement:
    return DashboardElement(
        id=element_id,
        type="vis",
        title=f"chart {element_id}",
        query=Query(model="sales", view=explore, fields=[f"{explore}.id"]),
    )


def _dashboard(
    dashboard_id: str,
    title: str,
    folder: Optional[FolderBase],
    elements: List[DashboardElement],
    deleted: bool = False,
) -> Dashboard:
    return Dashboard(
        id=dashboard_id,
        title=title,
        folder=folder,
        dashboard_elements=elements,
        deleted=deleted,
    )


LIVE_DASHBOARDS: List[Dashboard] = [
    _dashboard(
        "1",
        "Revenue",
        SALES,
        [
            _vis("11", "orders"),
            DashboardElement(id="12", type="text", title="notes"),
            _vis("14", "orders"),
        ],
    ),
    _dashboard("2", "Denied by id", SALES, [_vis("21", "orders")]),
    _dashboard("3", "Personal", PERSONAL, [_vis("31", "orders")]),
    # The only user of the `archived` explore: ingestion records an explore as
    # used before folder_path_pattern drops the dashboard, so it is emitted
    # even when Shared/Archive is denied.
    _dashboard("5", "Archived", ARCHIVE, [_vis("51", "archived")]),
    _dashboard(
        "6",
        "No folder",
        None,
        [
            _vis("61", "customers"),
            # Saved look 105 placed on this dashboard: ingestion emits it as
            # this dashboard's chart, never as a standalone look.
            DashboardElement(
                id="62",
                type="vis",
                title="chart 62",
                look_id="105",
                look=LookWithQuery(
                    query=Query(
                        model="sales", view="customers", fields=["customers.id"]
                    )
                ),
            ),
        ],
    ),
]
DELETED_DASHBOARDS: List[Dashboard] = [
    _dashboard("4", "Deleted", SALES, [_vis("41", "orders")], deleted=True)
]
LIVE_LOOKS: List[Look] = [
    Look(id="101", title="Shared look", query_id="q-101", folder=SALES),
    Look(id="102", title="Personal look", query_id="q-102", folder=PERSONAL),
    Look(id="103", title="No query", query_id=None, folder=SALES),
    Look(id="105", title="Also on a dashboard", query_id="q-105", folder=SALES),
    # Has a query id, but its look reads back without a query, so ingestion's
    # _get_looker_dashboard_element yields nothing for it.
    Look(id="106", title="Query unreadable", query_id="q-106", folder=SALES),
]
_LOOKS_WITHOUT_QUERY = {"106"}
DELETED_LOOKS: List[Look] = [
    Look(id="104", title="Deleted look", query_id="q-104", folder=SALES, deleted=True)
]
MODELS: List[LookmlModel] = [
    LookmlModel(
        name="sales",
        project_name="proj",
        explores=[
            LookmlModelNavExplore(name="orders"),
            LookmlModelNavExplore(name="customers"),
            LookmlModelNavExplore(name="archived"),
            LookmlModelNavExplore(name="unused", hidden=True),
        ],
    ),
    # An unnamed explore, which list_all_explores skips: still no explores.
    LookmlModel(
        name="empty", project_name="proj", explores=[LookmlModelNavExplore(name=None)]
    ),
]


def _explore(model: str, explore: str) -> LookmlModelExplore:
    # The minimal explore tests/integration/looker's setup_mock_explore uses,
    # named per call so each (model, explore) gets its own dataset urn.
    return LookmlModelExplore(
        id=f"{model}::{explore}",
        name=explore,
        label=explore,
        description="placeholder",
        view_name=explore,
        project_name="proj",
        fields=LookmlModelExploreFieldset(
            dimensions=[
                LookmlModelExploreField(
                    name="dim1",
                    type="string",
                    dimension_group=None,
                    description="placeholder",
                    label_short="Dim One",
                )
            ]
        ),
        source_file="placeholder.lkml",
    )


def install(client: mock.MagicMock) -> mock.MagicMock:
    by_id = {d.id: d for d in [*LIVE_DASHBOARDS, *DELETED_DASHBOARDS]}

    def dashboard(
        dashboard_id: str, fields: Optional[str] = None, transport_options: Any = None
    ) -> Dashboard:
        if dashboard_id not in by_id:
            raise sdk_error(404, "Not found")
        return by_id[dashboard_id]

    def search_dashboards(
        fields: Optional[str] = None,
        deleted: Optional[str] = None,
        transport_options: Any = None,
    ) -> List[Dashboard]:
        return list(DELETED_DASHBOARDS) if deleted == "true" else []

    def search_looks(
        fields: Optional[str] = None,
        deleted: Optional[bool] = None,
        transport_options: Any = None,
    ) -> List[Look]:
        return list(DELETED_LOOKS) if deleted else []

    client.me.return_value = User(id="7", email=API_USER_EMAIL)
    client.all_dashboards.side_effect = lambda fields=None, transport_options=None: (
        list(LIVE_DASHBOARDS)
    )
    client.dashboard.side_effect = dashboard
    client.search_dashboards.side_effect = search_dashboards
    client.folder_ancestors.side_effect = (
        lambda folder_id, fields=None, transport_options=None: list(
            ANCESTORS[folder_id]
        )
    )
    client.all_looks.side_effect = lambda fields=None, transport_options=None: list(
        LIVE_LOOKS
    )
    client.search_looks.side_effect = search_looks

    def look(
        look_id: str, fields: Optional[str] = None, transport_options: Any = None
    ) -> LookWithQuery:
        if look_id in _LOOKS_WITHOUT_QUERY:
            return LookWithQuery(query=None)
        return LookWithQuery(
            query=Query(
                id=f"q-{look_id}", model="sales", view="orders", fields=["orders.id"]
            )
        )

    client.look.side_effect = look
    client.all_lookml_models.side_effect = lambda transport_options=None: list(MODELS)
    client.lookml_model.side_effect = (
        lambda model_name, fields=None, transport_options=None: LookmlModel(
            name=model_name, project_name="proj"
        )
    )
    client.lookml_model_explore.side_effect = (
        lambda model, explore_name, fields=None, transport_options=None: _explore(
            model, explore_name
        )
    )
    client.all_users.return_value = []
    client.user_roles.return_value = [
        Role(permission_set=PermissionSet(permissions=list(GRANTED_PERMISSIONS)))
    ]
    return client


@contextmanager
def fake_looker() -> Iterator[mock.MagicMock]:
    client = install(mock.MagicMock())
    with mock.patch("looker_sdk.init40", return_value=client):
        yield client
