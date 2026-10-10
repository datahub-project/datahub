"""Sensitivity label id lookup, Graph resolution, and tag or glossary emission."""

from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from datahub.ingestion.source.azure.azure_auth import AzureCredentialConfig
from datahub.ingestion.source.fabric.common.auth import GRAPH_API_SCOPE
from datahub.ingestion.source.fabric.common.models import (
    FabricWorkspace,
    sensitivity_label_id,
)
from datahub.ingestion.source.fabric.common.sensitivity import (
    GRAPH_SENSITIVITY_LABELS_URL,
    SensitivityLabel,
    SensitivityLabelEmitter,
    SensitivityLabelResolver,
    sensitivity_label_filter,
)
from datahub.ingestion.source.fabric.data_factory.config import (
    FabricDataFactorySourceConfig,
)
from datahub.ingestion.source.fabric.onelake.client import OneLakeClient
from datahub.ingestion.source.fabric.onelake.config import FabricOneLakeSourceConfig
from datahub.ingestion.source.fabric.onelake.models import FabricLakehouse
from datahub.ingestion.source.fabric.onelake.source import FabricOneLakeSource
from datahub.sdk.container import Container
from datahub.sdk.glossary_term import GlossaryTerm
from datahub.sdk.tag import Tag


def _json_response(payload: dict) -> MagicMock:
    response = MagicMock()
    response.json.return_value = payload
    return response


def test_sensitivity_label_id_reads_guid() -> None:
    assert (
        sensitivity_label_id({"sensitivityLabel": {"id": "label-guid"}}) == "label-guid"
    )
    assert sensitivity_label_id({}) is None
    assert sensitivity_label_id({"sensitivityLabel": {}}) is None


def test_sensitivity_label_filter_escapes_quotes() -> None:
    assert sensitivity_label_filter("label-guid") == "id in ('label-guid')"
    assert sensitivity_label_filter("o'brien") == "id in ('o''brien')"


def test_resolver_requests_graph_filter_and_caches() -> None:
    auth = MagicMock()
    auth.get_authorization_header.return_value = "Bearer token"
    resolver = SensitivityLabelResolver(auth, timeout=30)
    response = _json_response(
        {
            "value": [
                {
                    "id": "label-guid",
                    "name": "Confidential",
                    "description": "Secret data",
                }
            ]
        }
    )

    with patch(
        "datahub.ingestion.source.fabric.common.sensitivity.requests.get",
        return_value=response,
    ) as get:
        first = resolver.resolve("label-guid")
        second = resolver.resolve("label-guid")

    assert first == second
    assert first is not None
    assert first.name == "Confidential"
    assert first.description == "Secret data"
    assert get.call_count == 1
    assert get.call_args.args[0] == GRAPH_SENSITIVITY_LABELS_URL
    assert get.call_args.kwargs["params"] == {"$filter": "id in ('label-guid')"}
    assert get.call_args.kwargs["headers"]["Authorization"] == "Bearer token"
    auth.get_authorization_header.assert_called_once_with(scope=GRAPH_API_SCOPE)


def test_emitter_glossary_term_uses_label_description() -> None:
    emitter = SensitivityLabelEmitter(
        enabled=True,
        output="glossary_term",
        auth_helper=MagicMock(),
        timeout=30,
    )
    emitter._resolver.resolve = lambda label_id: SensitivityLabel(  # type: ignore[union-attr]
        id=label_id,
        name="Confidential",
        description="Secret data",
    )
    entity = MagicMock()

    created = emitter.attach(entity, "label-guid")
    again = emitter.attach(entity, "label-guid")

    assert len(created) == 1
    term = created[0]
    assert isinstance(term, GlossaryTerm)
    assert term.id == "label-guid"
    assert term.display_name == "Confidential"
    assert term.definition == "Secret data"
    assert again == []
    assert entity.add_term.call_count == 2
    entity.add_term.assert_called_with(term.urn)


def test_emitter_tag_uses_label_name_and_description() -> None:
    emitter = SensitivityLabelEmitter(
        enabled=True,
        output="tag",
        auth_helper=MagicMock(),
        timeout=30,
    )
    emitter._resolver.resolve = lambda label_id: SensitivityLabel(  # type: ignore[union-attr]
        id=label_id,
        name="Highly Confidential",
        description="Restricted",
    )
    entity = MagicMock()

    created = emitter.attach(entity, "label-guid")

    assert len(created) == 1
    tag = created[0]
    assert isinstance(tag, Tag)
    assert tag.name == "Highly Confidential"
    assert tag.description == "Restricted"
    entity.add_tag.assert_called_once_with(tag.urn)


def test_emitter_disabled_does_not_resolve() -> None:
    emitter = SensitivityLabelEmitter(
        enabled=False,
        output="glossary_term",
        auth_helper=MagicMock(),
        timeout=30,
    )

    assert emitter.attach(MagicMock(), "label-guid") == []
    assert emitter._resolver is None


def test_extract_sensitivity_label_config() -> None:
    for config_cls in (FabricOneLakeSourceConfig, FabricDataFactorySourceConfig):
        config = config_cls(
            credential=AzureCredentialConfig(),
            extract_sensitivity_label={"enabled": True, "output": "glossary_term"},
        )
        assert config.extract_sensitivity_label.enabled is True
        assert config.extract_sensitivity_label.output == "glossary_term"

        default = config_cls(credential=AzureCredentialConfig())
        assert default.extract_sensitivity_label.enabled is False
        assert default.extract_sensitivity_label.output == "tag"

        with pytest.raises(ValidationError):
            config_cls(
                credential=AzureCredentialConfig(),
                extract_sensitivity_label={"output": "owner"},
            )


def test_list_lakehouses_reads_sensitivity_label_from_items_get() -> None:
    client = OneLakeClient.__new__(OneLakeClient)
    client.include_owners = False
    client.include_sensitivity_labels = True
    called: list[tuple[str, object]] = []

    def get(endpoint: str, params=None):
        called.append((endpoint, params))
        if endpoint == "workspaces/ws-1/lakehouses":
            return _json_response({"value": [{"id": "lh-1", "displayName": "Sales"}]})
        assert endpoint == "workspaces/ws-1/items/lh-1"
        assert params is None
        return _json_response({"sensitivityLabel": {"id": "label-guid"}})

    client.get = get  # type: ignore[method-assign]

    lakehouses = list(client.list_lakehouses("ws-1"))

    assert lakehouses[0].sensitivity_label_id == "label-guid"
    assert called == [
        ("workspaces/ws-1/lakehouses", None),
        ("workspaces/ws-1/items/lh-1", None),
    ]


def test_warehouse_without_label_uses_items_get() -> None:
    client = OneLakeClient.__new__(OneLakeClient)
    client.include_owners = True
    client.include_sensitivity_labels = True

    def get(endpoint: str, params=None):
        if endpoint == "workspaces/ws-1/warehouses":
            return _json_response({"value": [{"id": "wh-1", "displayName": "Sales"}]})
        if endpoint == "workspaces/ws-1/warehouses/wh-1":
            assert params is None
            return _json_response(
                {
                    "defaultIdentity": {
                        "userDetails": {"userPrincipalName": "jane.doe@contoso.com"}
                    }
                }
            )
        assert endpoint == "workspaces/ws-1/items/wh-1"
        assert params is None
        return _json_response({"sensitivityLabel": {"id": "label-guid"}})

    client.get = get  # type: ignore[method-assign]

    warehouses = list(client.list_warehouses("ws-1"))

    assert warehouses[0].owner_user_principal_name == "jane.doe@contoso.com"
    assert warehouses[0].sensitivity_label_id == "label-guid"


def test_lakehouse_glossary_term_description_comes_from_the_label() -> None:
    source = FabricOneLakeSource.__new__(FabricOneLakeSource)
    source.config = MagicMock()
    source.config.platform_instance = None
    source.config.env = "PROD"
    source.config.extract_owners.enabled = False
    source.config.extract_views = False
    source._create_schema_client = MagicMock(return_value=None)  # type: ignore[method-assign]
    source._fetch_schema_map = MagicMock(return_value={})  # type: ignore[method-assign]
    source._process_item_tables = MagicMock(return_value=iter(()))  # type: ignore[method-assign]
    source._load_lakehouse_shortcuts = MagicMock(return_value={})  # type: ignore[method-assign]
    source._extract_item_usage = MagicMock()  # type: ignore[method-assign]
    source._profile_item = MagicMock(return_value=iter(()))  # type: ignore[method-assign]
    emitter = SensitivityLabelEmitter(
        enabled=True,
        output="glossary_term",
        auth_helper=MagicMock(),
        timeout=30,
    )
    emitter._resolver.resolve = lambda label_id: SensitivityLabel(  # type: ignore[union-attr]
        id=label_id,
        name="Confidential",
        description="Secret data",
    )
    source._labels = emitter  # type: ignore[attr-defined]

    emitted = list(
        source._process_lakehouse(
            FabricWorkspace(id="ws-1", name="Sales"),
            FabricLakehouse(
                id="lh-1",
                name="Sales",
                type="Lakehouse",
                workspace_id="ws-1",
                sensitivity_label_id="label-guid",
            ),
        )
    )

    term = emitted[0]
    container = emitted[1]
    assert isinstance(term, GlossaryTerm)
    assert term.definition == "Secret data"
    assert term.display_name == "Confidential"
    assert isinstance(container, Container)
    assert container.terms is not None
    assert container.terms[0].urn == str(term.urn)
