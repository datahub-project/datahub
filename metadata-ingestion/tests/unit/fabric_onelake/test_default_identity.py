"""Default identity on Fabric workspace items is emitted as the owner."""

from unittest.mock import MagicMock

from datahub.emitter.mce_builder import make_user_urn
from datahub.ingestion.source.fabric.common.core_client import FabricCoreClient
from datahub.ingestion.source.fabric.common.models import FabricWorkspace
from datahub.ingestion.source.fabric.onelake.client import OneLakeClient
from datahub.ingestion.source.fabric.onelake.models import FabricLakehouse
from datahub.ingestion.source.fabric.onelake.notebooks import FabricNotebook
from datahub.ingestion.source.fabric.onelake.source import FabricOneLakeSource


def _json_response(payload: dict) -> MagicMock:
    response = MagicMock()
    response.json.return_value = payload
    return response


def test_list_lakehouses_reads_default_identity_upn() -> None:
    client = OneLakeClient.__new__(OneLakeClient)

    def get(endpoint: str, params=None):
        if endpoint == "workspaces/ws-1/lakehouses":
            return _json_response({"value": [{"id": "lh-1", "displayName": "Sales"}]})
        assert endpoint == "workspaces/ws-1/items/lh-1"
        assert params == {"include": "DefaultIdentity"}
        return _json_response(
            {
                "defaultIdentity": {
                    "userDetails": {"userPrincipalName": "jane.doe@contoso.com"}
                }
            }
        )

    client.get = get  # type: ignore[method-assign]

    lakehouses = list(client.list_lakehouses("ws-1"))

    assert len(lakehouses) == 1
    assert lakehouses[0].owner_user_principal_name == "jane.doe@contoso.com"


def test_list_notebooks_reads_default_identity_upn() -> None:
    client = OneLakeClient.__new__(OneLakeClient)

    def get(endpoint: str, params=None):
        if endpoint == "workspaces/ws-1/notebooks":
            return _json_response(
                {"value": [{"id": "nb-1", "displayName": "Explore", "folderId": None}]}
            )
        assert endpoint == "workspaces/ws-1/items/nb-1"
        assert params == {"include": "DefaultIdentity"}
        return _json_response(
            {
                "defaultIdentity": {
                    "userDetails": {"userPrincipalName": "jane.doe@contoso.com"}
                }
            }
        )

    client.get = get  # type: ignore[method-assign]

    notebooks = list(client.list_notebooks("ws-1"))

    assert notebooks[0].owner_user_principal_name == "jane.doe@contoso.com"


def test_list_items_reads_default_identity_upn() -> None:
    client = FabricCoreClient.__new__(FabricCoreClient)

    def get(endpoint: str, params=None):
        if endpoint == "workspaces/ws-1/items":
            return _json_response(
                {
                    "value": [
                        {"id": "pl-1", "displayName": "ETL", "type": "DataPipeline"}
                    ]
                }
            )
        assert endpoint == "workspaces/ws-1/items/pl-1"
        assert params == {"include": "DefaultIdentity"}
        return _json_response(
            {
                "defaultIdentity": {
                    "userDetails": {"userPrincipalName": "jane.doe@contoso.com"}
                }
            }
        )

    client.get = get  # type: ignore[method-assign]

    items = list(client.list_items("ws-1", item_type="DataPipeline"))

    assert items[0].owner_user_principal_name == "jane.doe@contoso.com"


def test_failed_default_identity_lookup_still_lists_the_item() -> None:
    client = OneLakeClient.__new__(OneLakeClient)

    def get(endpoint: str, params=None):
        if endpoint == "workspaces/ws-1/lakehouses":
            return _json_response({"value": [{"id": "lh-1", "displayName": "Sales"}]})
        raise RuntimeError("identity lookup failed")

    client.get = get  # type: ignore[method-assign]

    lakehouses = list(client.list_lakehouses("ws-1"))

    assert lakehouses[0].name == "Sales"
    assert lakehouses[0].owner_user_principal_name is None


def test_lakehouse_container_owner_is_user_principal_name() -> None:
    source = FabricOneLakeSource.__new__(FabricOneLakeSource)
    source.config = MagicMock()
    source.config.platform_instance = None
    source.config.env = "PROD"
    source.config.extract_views = False
    source._create_schema_client = MagicMock(return_value=None)  # type: ignore[method-assign]
    source._fetch_schema_map = MagicMock(return_value={})  # type: ignore[method-assign]
    source._process_item_tables = MagicMock(return_value=iter(()))  # type: ignore[method-assign]
    source._load_lakehouse_shortcuts = MagicMock(return_value={})  # type: ignore[method-assign]
    source._extract_item_usage = MagicMock()  # type: ignore[method-assign]
    source._profile_item = MagicMock(return_value=iter(()))  # type: ignore[method-assign]

    emitted = list(
        source._process_lakehouse(
            FabricWorkspace(id="ws-1", name="Sales"),
            FabricLakehouse(
                id="lh-1",
                name="Sales",
                type="Lakehouse",
                workspace_id="ws-1",
                owner_user_principal_name="jane.doe@contoso.com",
            ),
        )
    )

    assert emitted[0].owners is not None
    assert emitted[0].owners[0].owner == make_user_urn("jane.doe@contoso.com")


def test_notebook_dataset_owner_is_user_principal_name() -> None:
    source = FabricOneLakeSource.__new__(FabricOneLakeSource)
    source.config = MagicMock()
    source.config.platform_instance = None
    source.config.env = "PROD"

    dataset = source._create_notebook_dataset(
        FabricWorkspace(id="ws-1", name="Sales"),
        FabricNotebook(
            id="nb-1",
            display_name="Explore",
            workspace_id="ws-1",
            owner_user_principal_name="jane.doe@contoso.com",
        ),
        path="/Explore",
        content=None,
        language=None,
    )

    assert dataset.owners is not None
    assert dataset.owners[0].owner == make_user_urn("jane.doe@contoso.com")
