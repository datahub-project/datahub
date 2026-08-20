import logging
import os
import tempfile
import uuid
from typing import Any, Dict, List, Optional

import pytest

from conftest import _ingest_cleanup_data_impl
from datahub.emitter.mce_builder import datahub_guid, make_dataset_urn, make_user_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.sink import NoopWriteCallback
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.sink.file import FileSink, FileSinkConfig
from datahub.metadata.schema_classes import (
    DataProductAssociationClass,
    DataProductPropertiesClass,
    DatasetPropertiesClass,
    DomainPropertiesClass,
    GlossaryTermInfoClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)
from datahub.specific.dataproduct import DataProductPatchBuilder
from datahub.utilities.urns.urn import Urn
from tests.utilities.metadata_operations import add_term
from tests.utils import wait_for_writes_to_sync, with_test_retry

logger = logging.getLogger(__name__)

TEST_ID = uuid.uuid4().hex[:8]
DOMAIN_URN = Urn("domain", [datahub_guid({"name": f"Marketplace Domain {TEST_ID}"})])
TERM_URN = f"urn:li:glossaryTerm:marketplace_customer_data_{TEST_ID}"
OWNER_URN = make_user_urn("datahub")

DATASET_URNS = [
    make_dataset_urn("snowflake", f"marketplace_{TEST_ID}.public.customers"),
    make_dataset_urn("snowflake", f"marketplace_{TEST_ID}.public.orders"),
    make_dataset_urn("snowflake", f"marketplace_{TEST_ID}.public.internal_staging"),
]
OUTPUT_PORT_URNS = DATASET_URNS[:2]

CREATE_DATA_PRODUCT = """
mutation createDataProduct($input: CreateDataProductInput!) {
  createDataProduct(input: $input) {
    urn
    properties { name description }
  }
}
"""

DELETE_DATA_PRODUCT = """
mutation deleteDataProduct($urn: String!) {
  deleteDataProduct(urn: $urn)
}
"""

GET_ROOT_DATA_PRODUCTS = """
query getRootDataProducts($input: GetRootEntitiesInput!) {
  getRootDataProducts(input: $input) {
    total
    start
    count
    dataProducts {
      urn
      type
      properties { name description numAssets }
      domain { domain { urn } }
      childDataProducts(input: { count: 0, query: "*" }) { total }
      ownership { owners { owner { ... on CorpUser { urn } } } }
      glossaryTerms { terms { term { urn } } }
    }
  }
}
"""

SCROLL_ROOT_DATA_PRODUCTS = """
query scrollRootDataProducts($input: ScrollAcrossEntitiesInput!) {
  scrollAcrossEntities(input: $input) {
    total
    searchResults {
      entity {
        urn
        ... on DataProduct {
          properties { name description numAssets }
          domain { domain { urn } }
          childDataProducts(input: { count: 0, query: "*" }) { total }
          ownership { owners { owner { ... on CorpUser { urn } } } }
          glossaryTerms { terms { term { urn } } }
        }
      }
    }
  }
}
"""

GET_DATA_PRODUCT_PROFILE = """
query getDataProduct($urn: String!) {
  dataProduct(urn: $urn) {
    urn
    properties {
      name
      description
      numAssets
      parentDataProduct { urn }
    }
    domain { domain { urn } }
    ownership { owners { owner { ... on CorpUser { urn } } } }
    glossaryTerms { terms { term { urn } } }
    childDataProducts(input: { count: 10, query: "*" }) {
      total
      searchResults {
        entity {
          urn
          ... on DataProduct {
            properties {
              name
              parentDataProduct { urn }
            }
          }
        }
      }
    }
  }
}
"""

LIST_DATA_PRODUCT_ASSETS = """
query listDataProductAssets($urn: String!, $input: SearchAcrossEntitiesInput!) {
  listDataProductAssets(urn: $urn, input: $input) {
    total
    searchResults {
      entity { urn type }
    }
  }
}
"""


class FileEmitter:
    def __init__(self, filename: str) -> None:
        self.sink: FileSink = FileSink(
            ctx=PipelineContext(run_id="marketplace_demo_seed"),
            config=FileSinkConfig(filename=filename),
        )

    def emit(self, event: MetadataChangeProposalWrapper) -> None:
        self.sink.write_record_async(
            record_envelope=RecordEnvelope(record=event, metadata={}),
            write_callback=NoopWriteCallback(),
        )

    def close(self) -> None:
        self.sink.close()


def create_seed_file(filename: str) -> None:
    events: List[MetadataChangeProposalWrapper] = [
        MetadataChangeProposalWrapper(
            entityUrn=str(DOMAIN_URN),
            aspect=DomainPropertiesClass(
                name=f"Marketplace Domain {TEST_ID}",
                description="Domain for Data Product Marketplace smoke demo",
            ),
        ),
        MetadataChangeProposalWrapper(
            entityUrn=TERM_URN,
            aspect=GlossaryTermInfoClass(
                name=f"Customer Data {TEST_ID}",
                definition="Customer-facing data assets for marketplace demo",
                termSource="INTERNAL",
            ),
        ),
    ]
    for dataset_urn in DATASET_URNS:
        events.append(
            MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=DatasetPropertiesClass(
                    name=dataset_urn.split(",")[1],
                    description=f"Marketplace demo asset {dataset_urn}",
                ),
            )
        )

    emitter = FileEmitter(filename)
    for event in events:
        emitter.emit(event)
    emitter.close()


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client):
    _, filename = tempfile.mkstemp(suffix=".json")
    try:
        create_seed_file(filename)
        yield from _ingest_cleanup_data_impl(
            auth_session, graph_client, filename, "marketplace"
        )
    finally:
        os.remove(filename)


def _create_data_product(
    graph_client: DataHubGraph,
    name: str,
    description: str,
    parent_urn: Optional[str] = None,
) -> str:
    properties: Dict[str, Any] = {"name": name, "description": description}
    if parent_urn:
        properties["parentDataProduct"] = parent_urn
    result = graph_client.execute_graphql(
        CREATE_DATA_PRODUCT,
        {
            "input": {
                "domainUrn": str(DOMAIN_URN),
                "properties": properties,
            }
        },
    )
    assert "createDataProduct" in result
    return result["createDataProduct"]["urn"]


def _attach_assets_with_output_ports(
    graph_client: DataHubGraph, data_product_urn: str
) -> None:
    associations = [
        DataProductAssociationClass(
            destinationUrn=urn, outputPort=urn in OUTPUT_PORT_URNS
        )
        for urn in DATASET_URNS
    ]
    for mcp in (
        DataProductPatchBuilder(data_product_urn)
        .set_assets(associations)
        .add_owner(OwnerClass(owner=OWNER_URN, type=OwnershipTypeClass.TECHNICAL_OWNER))
        .build()
    ):
        graph_client.emit(mcp)


def _delete_data_product(graph_client: DataHubGraph, urn: str) -> None:
    graph_client.execute_graphql(DELETE_DATA_PRODUCT, {"urn": urn})


@pytest.fixture(scope="module")
def marketplace_hierarchy(graph_client, auth_session, ingest_cleanup_data):
    parent_urn = _create_data_product(
        graph_client,
        name=f"Customer 360 Bundle {TEST_ID}",
        description="Parent data product with child products and output ports",
    )
    child_a = _create_data_product(
        graph_client,
        name=f"Customer Profiles {TEST_ID}",
        description="Child product for customer profiles",
        parent_urn=parent_urn,
    )
    child_b = _create_data_product(
        graph_client,
        name=f"Customer Orders {TEST_ID}",
        description="Child product for customer orders",
        parent_urn=parent_urn,
    )

    _attach_assets_with_output_ports(graph_client, parent_urn)
    assert add_term(auth_session, parent_urn, TERM_URN)

    wait_for_writes_to_sync()

    yield {
        "parent": parent_urn,
        "children": [child_a, child_b],
    }

    for urn in [child_a, child_b, parent_urn]:
        try:
            _delete_data_product(graph_client, urn)
        except Exception:
            logger.exception("Failed to delete data product %s", urn)
    wait_for_writes_to_sync()


@with_test_retry()
def test_marketplace_roots_and_children(graph_client, marketplace_hierarchy):
    parent_urn = marketplace_hierarchy["parent"]
    child_urns = set(marketplace_hierarchy["children"])

    # Landing-page path: getRootDataProducts with full field hydration
    roots = graph_client.execute_graphql(
        GET_ROOT_DATA_PRODUCTS, {"input": {"start": 0, "count": 50, "query": TEST_ID}}
    )["getRootDataProducts"]

    root_by_urn = {dp["urn"]: dp for dp in roots["dataProducts"]}
    assert parent_urn in root_by_urn, (
        "Parent product should appear in marketplace roots"
    )
    assert set(root_by_urn).isdisjoint(child_urns), (
        "Child products must not appear as roots"
    )

    parent_root = root_by_urn[parent_urn]
    assert parent_root["properties"] is not None
    assert parent_root["properties"]["name"].startswith("Customer 360 Bundle")
    assert parent_root["domain"]["domain"]["urn"] == str(DOMAIN_URN)
    assert parent_root["childDataProducts"]["total"] == 2

    owner_urns = {
        o["owner"]["urn"] for o in parent_root["ownership"]["owners"] if o.get("owner")
    }
    assert OWNER_URN in owner_urns

    term_urns = {
        t["term"]["urn"]
        for t in (parent_root.get("glossaryTerms") or {}).get("terms") or []
    }
    assert TERM_URN in term_urns

    # Sidebar path: scrollAcrossEntities + hasParentDataProduct=false
    scroll = graph_client.execute_graphql(
        SCROLL_ROOT_DATA_PRODUCTS,
        {
            "input": {
                "query": TEST_ID,
                "types": ["DATA_PRODUCT"],
                "count": 50,
                "orFilters": [
                    {"and": [{"field": "hasParentDataProduct", "values": ["false"]}]}
                ],
                "searchFlags": {"skipCache": True},
            }
        },
    )["scrollAcrossEntities"]

    scroll_urns = {
        r["entity"]["urn"]
        for r in scroll["searchResults"]
        if r.get("entity") and r["entity"].get("urn")
    }
    assert parent_urn in scroll_urns
    assert scroll_urns.isdisjoint(child_urns)


@with_test_retry()
def test_data_product_profile_and_output_ports(graph_client, marketplace_hierarchy):
    parent_urn = marketplace_hierarchy["parent"]
    child_urns = set(marketplace_hierarchy["children"])

    profile = graph_client.execute_graphql(
        GET_DATA_PRODUCT_PROFILE, {"urn": parent_urn}
    )["dataProduct"]

    assert profile["urn"] == parent_urn
    assert profile["domain"]["domain"]["urn"] == str(DOMAIN_URN)
    assert profile["properties"]["numAssets"] == len(DATASET_URNS)
    assert profile["properties"].get("parentDataProduct") is None

    owner_urns = {
        o["owner"]["urn"] for o in profile["ownership"]["owners"] if o.get("owner")
    }
    assert OWNER_URN in owner_urns

    term_urns = {
        t["term"]["urn"] for t in profile["glossaryTerms"]["terms"] if t.get("term")
    }
    assert TERM_URN in term_urns

    children = profile["childDataProducts"]
    assert children["total"] == 2
    found_children = {r["entity"]["urn"] for r in children["searchResults"]}
    assert found_children == child_urns
    for result in children["searchResults"]:
        parent_ref = result["entity"]["properties"]["parentDataProduct"]
        assert parent_ref["urn"] == parent_urn

    # Profile-style Output Ports listing (isOutputPort=true)
    output_ports = graph_client.execute_graphql(
        LIST_DATA_PRODUCT_ASSETS,
        {
            "urn": parent_urn,
            "input": {
                "query": "*",
                "start": 0,
                "count": 20,
                "filters": [{"field": "isOutputPort", "values": ["true"]}],
            },
        },
    )["listDataProductAssets"]
    output_urns = {r["entity"]["urn"] for r in output_ports["searchResults"]}
    assert output_urns == set(OUTPUT_PORT_URNS)
    assert output_ports["total"] == len(OUTPUT_PORT_URNS)

    # All assets (no output-port filter)
    all_assets = graph_client.execute_graphql(
        LIST_DATA_PRODUCT_ASSETS,
        {
            "urn": parent_urn,
            "input": {"query": "*", "start": 0, "count": 20},
        },
    )["listDataProductAssets"]
    all_urns = {r["entity"]["urn"] for r in all_assets["searchResults"]}
    assert all_urns == set(DATASET_URNS)


def test_seeded_aspects_on_parent(graph_client, marketplace_hierarchy):
    parent_urn = marketplace_hierarchy["parent"]
    props = graph_client.get_aspect(parent_urn, DataProductPropertiesClass)
    assert props is not None
    assert props.assets is not None
    assert len(props.assets) == len(DATASET_URNS)
    flagged = {
        a.destinationUrn for a in props.assets if getattr(a, "outputPort", False)
    }
    assert flagged == set(OUTPUT_PORT_URNS)

    ownership = graph_client.get_aspect(parent_urn, OwnershipClass)
    assert ownership is not None
    assert any(o.owner == OWNER_URN for o in ownership.owners)
