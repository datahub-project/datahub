"""Shared fixtures and helpers for the Tableau unit tests.

``default_config`` is the recipe every test starts from. The ``site_source``
fixture builds a ``TableauSiteSource`` on a mocked ``tableauserverclient``
server, ``tableau_source`` the multi-site ``TableauSource`` wrapper around it.
The remaining helpers build Metadata API payloads and pull aspects out of the
work units a source emits.
"""

from typing import Any, Callable, Dict, Iterable, List, Optional, Type, TypeVar, cast
from unittest import mock

import pytest
from tableauserverclient import GroupItem, SiteItem
from tableauserverclient.models import UserItem

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.tableau import tableau_constant as c
from datahub.ingestion.source.tableau.tableau import (
    SiteIdContentUrl,
    TableauConfig,
    TableauProject,
    TableauSiteSource,
    TableauSource,
    TableauSourceReport,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent

T = TypeVar("T")

GMS_PORT = 8080
GMS_SERVER = f"http://localhost:{GMS_PORT}"

default_config: Dict[str, Any] = {
    "username": "username",
    "password": "pass`",
    "connect_uri": "https://do-not-connect",
    "site": "acryl",
    "projects": ["default", "Project 2", "Samples"],
    "extract_project_hierarchy": False,
    "page_size": 1000,
    "workbook_page_size": None,
    "ingest_tags": True,
    "ingest_owner": True,
    "ingest_tables_external": True,
    "default_schema_map": {
        "dvdrental": "public",
        "someotherdb": "schema",
    },
    "platform_instance_map": {"postgres": "demo_postgres_instance"},
    "extract_usage_stats": True,
    "stateful_ingestion": {
        "enabled": True,
        "remove_stale_metadata": True,
        "fail_safe_threshold": 100.0,
        "state_provider": {
            "type": "datahub",
            "config": {"datahub_api": {"server": GMS_SERVER}},
        },
    },
}

SITE_ID = "site-1"
SITE_CONTENT_URL = "site-a"
PROJECT_ID = "project-1"
WORKBOOK: Dict[str, Any] = {
    c.ID: "wb-1",
    c.NAME: "Sales WB",
    c.LUID: "wb-luid-1",
    c.PROJECT_NAME: "default",
    c.OWNER: {c.USERNAME: "alice", c.EMAIL: "alice@example.com"},
    c.TAGS: [{c.NAME: "workbook-tag"}],
}
# A postgres table under default_config's platform_instance_map.
ORDERS_URN = make_dataset_urn_with_platform_instance(
    "postgres", "dvdrental.public.orders", "demo_postgres_instance", "PROD"
)
STAGING_URN = make_dataset_urn_with_platform_instance(
    "postgres", "dvdrental.public.staging", "demo_postgres_instance", "PROD"
)


def tableau_urn(name: str) -> str:
    return make_dataset_urn_with_platform_instance("tableau", name, None, "PROD")


def registry_project(
    project_id: str = PROJECT_ID, path: Optional[List[str]] = None
) -> TableauProject:
    path = path or ["default"]
    return TableauProject(
        id=project_id,
        name=path[-1],
        description="",
        parent_id=None,
        parent_name=None,
        path=path,
    )


def mock_tableau_server() -> mock.MagicMock:
    """A tableauserverclient ``Server`` whose signed-in user passes the role check."""
    server = mock.MagicMock()
    server.user_id = "user-1"
    server.site_id = SITE_ID
    server.users.get_by_id.return_value = UserItem(
        name="user", site_role=UserItem.Roles.SiteAdministratorExplorer
    )
    return server


def mock_server_of(source: TableauSiteSource) -> mock.MagicMock:
    return cast(mock.MagicMock, source.server)


def query_mock(source: TableauSiteSource) -> mock.MagicMock:
    """The Metadata API ``query`` mock behind ``get_connection_object_page``."""
    return mock_server_of(source).metadata.query


def tsc_group(group_id: Optional[str], name: str) -> GroupItem:
    group = GroupItem(name)
    group._id = group_id
    return group


def tsc_site(
    name: str = "Acryl",
    site_id: str = SITE_ID,
    content_url: str = SITE_CONTENT_URL,
    state: Optional[str] = None,
) -> SiteItem:
    site = SiteItem(name=name, content_url=content_url)
    # SiteItem only exposes these through private attributes initialised to None.
    site_attrs = cast(Any, site)
    site_attrs._id = site_id
    if state is not None:
        site_attrs._state = state
    return site


SiteSourceFactory = Callable[..., TableauSiteSource]


@pytest.fixture
def site_source() -> SiteSourceFactory:
    """Factory for a ``TableauSiteSource`` on a mocked server.

    Keyword arguments override ``default_config``. ``site_item`` swaps the
    ``SiteIdContentUrl`` for a REST ``SiteItem``; ``seed_project`` registers
    ``PROJECT_ID`` and maps ``WORKBOOK`` and the published datasource
    ``ds-luid-1`` to it, which is what ``emit_*`` needs to place assets.
    """

    def build(
        *,
        site_item: Optional[SiteItem] = None,
        seed_project: bool = False,
        **config_overrides: object,
    ) -> TableauSiteSource:
        source = TableauSiteSource(
            config=TableauConfig.model_validate({**default_config, **config_overrides}),
            ctx=PipelineContext(run_id="test"),
            platform="tableau",
            site=site_item
            or SiteIdContentUrl(site_id=SITE_ID, site_content_url=SITE_CONTENT_URL),
            report=TableauSourceReport(),
            server=mock_tableau_server(),
        )
        if seed_project:
            source.tableau_project_registry = {PROJECT_ID: registry_project()}
            source.workbook_project_map = {WORKBOOK[c.LUID]: PROJECT_ID}
            source.datasource_project_map = {"ds-luid-1": PROJECT_ID}
        return source

    return build


TableauSourceFactory = Callable[..., TableauSource]


@pytest.fixture
def tableau_source() -> TableauSourceFactory:
    """Factory for the multi-site ``TableauSource`` signed in to ``server``.

    Stateful ingestion is dropped so no graph client is needed; the role check
    is patched out because the mocked server cannot answer it.
    """

    def build(server: mock.MagicMock, **config_overrides: object) -> TableauSource:
        config = {**default_config, **config_overrides}
        config.pop("stateful_ingestion", None)
        with (
            mock.patch.object(
                TableauConfig, "make_tableau_client", autospec=True, return_value=server
            ),
            mock.patch(
                "datahub.ingestion.source.tableau.tableau.report_user_role",
                autospec=True,
            ),
        ):
            return TableauSource(
                TableauConfig.model_validate(config), PipelineContext(run_id="test")
            )

    return build


def graphql_page(
    nodes: List[dict],
    *,
    connection: str = c.WORKBOOKS_CONNECTION,
    end_cursor: Optional[str] = None,
    has_next_page: bool = False,
    errors: Optional[List[dict]] = None,
) -> dict:
    """One Metadata API response page, optionally carrying an ``errors`` list."""
    payload: dict = {
        c.DATA: {
            connection: {
                c.NODES: nodes,
                c.PAGE_INFO: {c.HAS_NEXT_PAGE: has_next_page, "endCursor": end_cursor},
            }
        }
    }
    if errors is not None:
        payload[c.ERRORS] = errors
    return payload


def graphql_warning(code: Optional[str] = None, message: str = "warning") -> dict:
    extensions: Dict[str, str] = {c.SEVERITY: c.WARNING}
    if code:
        extensions["code"] = code
    return {"message": message, c.EXTENSIONS: extensions}


def graphql_error(message: str) -> dict:
    return {"message": message, c.EXTENSIONS: {c.SEVERITY: "ERROR"}}


def snapshot_aspects(
    work_units: Iterable[MetadataWorkUnit], aspect_type: Type[T]
) -> List[T]:
    """Aspects of ``aspect_type`` inside MCE snapshots, in emission order."""
    return [
        aspect
        for wu in work_units
        if isinstance(wu.metadata, MetadataChangeEvent)
        for aspect in wu.metadata.proposedSnapshot.aspects
        if isinstance(aspect, aspect_type)
    ]


def snapshot_aspect(work_units: Iterable[MetadataWorkUnit], aspect_type: Type[T]) -> T:
    aspects = snapshot_aspects(work_units, aspect_type)
    assert aspects, f"no {aspect_type.__name__} aspect in snapshot"
    return aspects[0]


def mcp_aspects(
    work_units: Iterable[MetadataWorkUnit], aspect_type: Type[T]
) -> List[T]:
    """Aspects of ``aspect_type`` emitted as MCPs, in emission order."""
    return [
        wu.metadata.aspect
        for wu in work_units
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, aspect_type)
    ]
