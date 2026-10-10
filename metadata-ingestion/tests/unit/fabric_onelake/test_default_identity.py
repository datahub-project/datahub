"""Default identity on Fabric workspace items is emitted as the owner."""

from unittest.mock import MagicMock

from datahub.emitter.mce_builder import make_user_urn
from datahub.ingestion.source.fabric.common.core_client import FabricCoreClient
from datahub.ingestion.source.fabric.common.models import FabricItem, FabricWorkspace
from datahub.ingestion.source.fabric.data_factory.models import PipelineActivity
from datahub.ingestion.source.fabric.data_factory.source import FabricDataFactorySource
from datahub.ingestion.source.fabric.onelake.client import OneLakeClient
from datahub.ingestion.source.fabric.onelake.models import FabricLakehouse, FabricTable
from datahub.ingestion.source.fabric.onelake.notebooks import FabricNotebook
from datahub.ingestion.source.fabric.onelake.source import FabricOneLakeSource
from datahub.metadata.schema_classes import OwnershipTypeClass


def _json_response(payload: dict) -> MagicMock:
    response = MagicMock()
    response.json.return_value = payload
    return response


def test_list_lakehouses_reads_default_identity_upn() -> None:
    client = OneLakeClient.__new__(OneLakeClient)
    client.include_owners = True

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
    client.include_owners = True

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
    client.include_owners = True

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
    client.include_owners = True

    def get(endpoint: str, params=None):
        if endpoint == "workspaces/ws-1/lakehouses":
            return _json_response({"value": [{"id": "lh-1", "displayName": "Sales"}]})
        raise RuntimeError("identity lookup failed")

    client.get = get  # type: ignore[method-assign]

    lakehouses = list(client.list_lakehouses("ws-1"))

    assert lakehouses[0].name == "Sales"
    assert lakehouses[0].owner_user_principal_name is None


def test_owner_lookup_skipped_when_disabled() -> None:
    client = OneLakeClient.__new__(OneLakeClient)
    client.include_owners = False
    called: list[str] = []

    def get(endpoint: str, params=None):
        called.append(endpoint)
        return _json_response({"value": [{"id": "lh-1", "displayName": "Sales"}]})

    client.get = get  # type: ignore[method-assign]

    lakehouses = list(client.list_lakehouses("ws-1"))

    assert called == ["workspaces/ws-1/lakehouses"]
    assert lakehouses[0].owner_user_principal_name is None


def test_lakehouse_container_owner_is_user_principal_name() -> None:
    source = FabricOneLakeSource.__new__(FabricOneLakeSource)
    source.config = MagicMock()
    source.config.platform_instance = None
    source.config.env = "PROD"
    source.config.extract_owners.enabled = True
    source.config.extract_owners.ownership_type = "TECHNICAL_OWNER"
    source.config.extract_owners.cascade_ownership = True
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
    assert emitted[0].owners[0].type == OwnershipTypeClass.TECHNICAL_OWNER


def test_notebook_dataset_owner_is_user_principal_name() -> None:
    source = FabricOneLakeSource.__new__(FabricOneLakeSource)
    source.config = MagicMock()
    source.config.platform_instance = None
    source.config.env = "PROD"
    source.config.extract_owners.enabled = True
    source.config.extract_owners.ownership_type = "BUSINESS_OWNER"
    source.config.extract_owners.cascade_ownership = True

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
    assert dataset.owners[0].type == OwnershipTypeClass.BUSINESS_OWNER


def _source_for_tables(
    *, cascade: bool, ownership_type: str, enabled: bool = True
) -> FabricOneLakeSource:
    source = FabricOneLakeSource.__new__(FabricOneLakeSource)
    source.config = MagicMock()
    source.config.platform_instance = None
    source.config.env = "PROD"
    source.config.extract_schemas = True
    source.config.convert_urns_to_lowercase = False
    source.config.extract_owners.enabled = enabled
    source.config.extract_owners.ownership_type = ownership_type
    source.config.extract_owners.cascade_ownership = cascade
    source.config.schema_pattern.allowed.return_value = True
    source.config.table_pattern.allowed.return_value = True
    source.config.is_profiling_enabled.return_value = False
    source.report = MagicMock()
    source._ingested_dataset_names = set()
    source._ingested_columns = {}
    source.client = MagicMock()
    source.client.list_lakehouse_tables.return_value = [
        FabricTable(
            name="orders",
            schema_name="dbo",
            item_id="lh-1",
            workspace_id="ws-1",
        )
    ]
    return source


def test_schema_and_table_inherit_item_owner() -> None:
    source = _source_for_tables(cascade=True, ownership_type="DATA_STEWARD")

    emitted = list(
        source._process_item_tables(
            FabricWorkspace(id="ws-1", name="Sales"),
            "lh-1",
            "Lakehouse",
            MagicMock(),
            item_display_name="Sales",
            schema_map={},
            emitted_schemas=set(),
            profile_targets=[],
            owner_user_principal_name="jane.doe@contoso.com",
        )
    )

    assert len(emitted) == 2
    for entity in emitted:
        assert entity.owners is not None
        assert entity.owners[0].owner == make_user_urn("jane.doe@contoso.com")
        assert entity.owners[0].type == OwnershipTypeClass.DATA_STEWARD


def test_schema_and_table_stay_unowned_when_cascade_is_off() -> None:
    source = _source_for_tables(cascade=False, ownership_type="DATA_STEWARD")

    emitted = list(
        source._process_item_tables(
            FabricWorkspace(id="ws-1", name="Sales"),
            "lh-1",
            "Lakehouse",
            MagicMock(),
            item_display_name="Sales",
            schema_map={},
            emitted_schemas=set(),
            profile_targets=[],
            owner_user_principal_name="jane.doe@contoso.com",
        )
    )

    assert len(emitted) == 2
    assert all(entity.owners is None for entity in emitted)


def test_schema_and_table_stay_unowned_when_ownership_is_disabled() -> None:
    source = _source_for_tables(
        cascade=True, ownership_type="DATA_STEWARD", enabled=False
    )

    emitted = list(
        source._process_item_tables(
            FabricWorkspace(id="ws-1", name="Sales"),
            "lh-1",
            "Lakehouse",
            MagicMock(),
            item_display_name="Sales",
            schema_map={},
            emitted_schemas=set(),
            profile_targets=[],
            owner_user_principal_name="jane.doe@contoso.com",
        )
    )

    assert len(emitted) == 2
    assert all(entity.owners is None for entity in emitted)


def test_activity_inherits_pipeline_owner() -> None:
    from datahub.ingestion.source.fabric.common.models import WorkspaceKey
    from datahub.sdk.dataflow import DataFlow

    source = FabricDataFactorySource.__new__(FabricDataFactorySource)
    source.config = MagicMock()
    source.config.extract_owners.enabled = True
    source.config.extract_owners.ownership_type = "BUSINESS_OWNER"
    source.config.extract_owners.cascade_ownership = True
    source.report = MagicMock()
    source._pipeline_activities_cache = {
        ("ws-1", "pl-1"): [PipelineActivity(name="Copy orders", type="Copy")]
    }
    source._resolve_upstream_edges = MagicMock(return_value=[])  # type: ignore[method-assign]
    source._extract_activity_lineage = MagicMock(return_value=([], []))  # type: ignore[method-assign]
    source._merge_cross_pipeline_info = MagicMock()  # type: ignore[method-assign]

    emitted = list(
        source._emit_pipeline_activities(
            FabricItem(
                id="pl-1",
                name="ETL",
                type="DataPipeline",
                workspace_id="ws-1",
                owner_user_principal_name="jane.doe@contoso.com",
            ),
            DataFlow(platform="fabric-data-factory", name="flow"),
            WorkspaceKey(workspace_id="ws-1"),
            {},
        )
    )

    assert emitted[0].owners is not None
    assert emitted[0].owners[0].owner == make_user_urn("jane.doe@contoso.com")
    assert emitted[0].owners[0].type == OwnershipTypeClass.BUSINESS_OWNER
