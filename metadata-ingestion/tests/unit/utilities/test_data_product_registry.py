from typing import Any, Dict

import pytest

from datahub.metadata.schema_classes import DataProductPropertiesClass
from datahub.utilities.registries.data_product_registry import DataProductRegistry
from tests.test_helpers.graph_helpers import MockDataHubGraph

PARENT_URN = "urn:li:dataProduct:north_america_finance"


@pytest.fixture
def mock_graph() -> MockDataHubGraph:
    entity_graph: Dict[str, Dict[str, Any]] = {
        PARENT_URN: {
            "dataProductProperties": DataProductPropertiesClass(
                name="North-America-Finance-Data-Platform",
            )
        }
    }
    return MockDataHubGraph(entity_graph=entity_graph)


def test_resolves_display_name_with_four_hyphens(mock_graph: MockDataHubGraph) -> None:
    registry = DataProductRegistry(
        cached_data_products=["North-America-Finance-Data-Platform"],
        graph=mock_graph,
    )
    assert (
        registry.get_data_product_urn("North-America-Finance-Data-Platform")
        == PARENT_URN
    )


def test_uuid_identifier_skips_server_resolution() -> None:
    uuid_id = "ec428203-ce86-4db3-985d-5a8ee6df32ba"
    registry = DataProductRegistry(cached_data_products=[uuid_id], graph=None)
    assert registry.get_data_product_urn(uuid_id) == uuid_id


def test_unprovisioned_name_raises(mock_graph: MockDataHubGraph) -> None:
    with pytest.raises(ValueError, match="doesn't seem to be provisioned"):
        DataProductRegistry(cached_data_products=["Missing Product"], graph=mock_graph)


def test_name_without_graph_raises() -> None:
    with pytest.raises(ValueError, match="need server-side resolution"):
        DataProductRegistry(cached_data_products=["Vendor Equities"], graph=None)
