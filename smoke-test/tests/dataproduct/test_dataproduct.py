import logging
import os
import tempfile
from random import randint
from typing import List

import pytest

from conftest import _ingest_cleanup_data_impl
from datahub.emitter.mce_builder import (
    datahub_guid,
    make_chart_urn,
    make_container_urn,
    make_dashboard_urn,
    make_dataset_urn,
    make_ml_model_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.sink import NoopWriteCallback
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.sink.file import FileSink, FileSinkConfig
from datahub.metadata.schema_classes import (
    ChangeAuditStampsClass,
    ChartInfoClass,
    ContainerPropertiesClass,
    DashboardInfoClass,
    DataProductPropertiesClass,
    DatasetPropertiesClass,
    DomainPropertiesClass,
    DomainsClass,
    MLModelPropertiesClass,
)
from datahub.utilities.urns.urn import Urn
from tests.utilities.domains import Domain
from tests.utils import wait_for_writes_to_sync, with_test_retry

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.domain(Domain.CATALOG)


start_index = randint(10, 10000)
dataset_urns = [
    make_dataset_urn("snowflake", f"table_foo_{i}")
    for i in range(start_index, start_index + 10)
]

# One asset of each type, used to exercise the batch data product mutations
# with a heterogeneous set of resources rather than only datasets.
heterogeneous_asset_urns = [
    make_dataset_urn("snowflake", f"table_batch_{start_index}"),
    make_dashboard_urn("looker", f"dashboard_batch_{start_index}"),
    make_chart_urn("looker", f"chart_batch_{start_index}"),
    make_container_urn(datahub_guid({"name": f"container_batch_{start_index}"})),
    make_ml_model_urn("sagemaker", f"model_batch_{start_index}", "PROD"),
]

BATCH_ADD_TO_DATA_PRODUCTS_QUERY = """
mutation batchAddToDataProducts($dataProductUrns: [String!]!, $resourceUrns: [String!]!) {
  batchAddToDataProducts(input: { dataProductUrns: $dataProductUrns, resourceUrns: $resourceUrns })
}
"""

BATCH_REMOVE_FROM_DATA_PRODUCTS_QUERY = """
mutation batchRemoveFromDataProducts($dataProductUrns: [String!]!, $resourceUrns: [String!]!) {
  batchRemoveFromDataProducts(input: { dataProductUrns: $dataProductUrns, resourceUrns: $resourceUrns })
}
"""


class FileEmitter:
    def __init__(self, filename: str) -> None:
        self.sink: FileSink = FileSink(
            ctx=PipelineContext(run_id="create_test_data"),
            config=FileSinkConfig(filename=filename),
        )

    def emit(self, event):
        self.sink.write_record_async(
            record_envelope=RecordEnvelope(record=event, metadata={}),
            write_callback=NoopWriteCallback(),
        )

    def close(self):
        self.sink.close()


def create_test_data(filename: str):
    domain_urn = Urn("domain", [datahub_guid({"name": "Marketing"})])

    domain_mcp = MetadataChangeProposalWrapper(
        entityUrn=str(domain_urn),
        aspect=DomainPropertiesClass(
            name="Marketing", description="The marketing domain"
        ),
    )

    dataset_mcps = [
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DatasetPropertiesClass(
                name=f"Test Dataset ({dataset_urn})", description=dataset_urn
            ),
        )
        for dataset_urn in dataset_urns
    ]

    (
        heterogeneous_dataset_urn,
        dashboard_urn,
        chart_urn,
        container_urn,
        ml_model_urn,
    ) = heterogeneous_asset_urns
    heterogeneous_asset_mcps = [
        MetadataChangeProposalWrapper(
            entityUrn=heterogeneous_dataset_urn,
            aspect=DatasetPropertiesClass(
                name=f"Test Dataset ({heterogeneous_dataset_urn})",
                description=heterogeneous_dataset_urn,
            ),
        ),
        MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn,
            aspect=DashboardInfoClass(
                title="Test Dashboard",
                description=dashboard_urn,
                lastModified=ChangeAuditStampsClass(),
            ),
        ),
        MetadataChangeProposalWrapper(
            entityUrn=chart_urn,
            aspect=ChartInfoClass(
                title="Test Chart",
                description=chart_urn,
                lastModified=ChangeAuditStampsClass(),
            ),
        ),
        MetadataChangeProposalWrapper(
            entityUrn=container_urn,
            aspect=ContainerPropertiesClass(name="Test Container"),
        ),
        MetadataChangeProposalWrapper(
            entityUrn=ml_model_urn,
            aspect=MLModelPropertiesClass(name="Test ML Model"),
        ),
    ]

    file_emitter = FileEmitter(filename)
    for mcps in [domain_mcp] + dataset_mcps + heterogeneous_asset_mcps:
        file_emitter.emit(mcps)

    file_emitter.close()


@pytest.fixture(scope="module", autouse=False)
def ingest_cleanup_data(auth_session, graph_client):
    _, filename = tempfile.mkstemp(suffix=".json")
    try:
        create_test_data(filename)
        yield from _ingest_cleanup_data_impl(
            auth_session, graph_client, filename, "data_products"
        )
    finally:
        os.remove(filename)


def get_gql_query(filename: str) -> str:
    with open(filename) as fp:
        return fp.read()


def validate_listing(
    graph_client: DataHubGraph, data_product_urn: str, dataset_urns: List[str]
) -> None:
    # Validate listing
    result = graph_client.execute_graphql(
        get_gql_query("tests/dataproduct/queries/list_dataproduct_assets.graphql"),
        {"urn": data_product_urn, "input": {"query": "*", "start": 0, "count": 20}},
    )
    assert "listDataProductAssets" in result
    assert "searchResults" in result["listDataProductAssets"]
    search_results = [
        res["entity"]["urn"] for res in result["listDataProductAssets"]["searchResults"]
    ]
    assert set(search_results) == set(dataset_urns)


def validate_relationships(
    graph_client: DataHubGraph, data_product_urn: str, dataset_urns: List[str]
) -> None:
    # Validate relationships
    urn_match = {k: False for k in dataset_urns}
    for dataset_urn in dataset_urns:
        for e in graph_client.get_related_entities(
            dataset_urn,
            relationship_types=["DataProductContains"],
            direction=DataHubGraph.RelationshipDirection.INCOMING,
        ):
            if e.urn == data_product_urn:
                urn_match[dataset_urn] = True

    urns_missing = [k for k in urn_match if urn_match[k] is False]
    assert urns_missing == [], (
        "All dataset urns should have a DataProductContains relationship to the data product"
    )

    dataset_urns_matched = set()
    for e in graph_client.get_related_entities(
        data_product_urn,
        relationship_types=["DataProductContains"],
        direction=DataHubGraph.RelationshipDirection.OUTGOING,
    ):
        dataset_urns_matched.add(e.urn)

    assert set(dataset_urns) == dataset_urns_matched, (
        "All dataset urns should be navigable from the data product"
    )


@with_test_retry()
def test_create_data_product(graph_client, ingest_cleanup_data):
    domain_urn = Urn("domain", [datahub_guid({"name": "Marketing"})])

    result = graph_client.execute_graphql(
        get_gql_query("tests/dataproduct/queries/add_dataproduct.graphql"),
        {
            "domainUrn": str(domain_urn),
            "name": "Test Data Product",
            "description": "Test Description",
        },
    )
    assert "createDataProduct" in result
    data_product_urn = result["createDataProduct"]["urn"]
    # Data Product Properties
    data_product_props = graph_client.get_aspect(
        data_product_urn, DataProductPropertiesClass
    )
    assert data_product_props is not None
    assert data_product_props.description == "Test Description"
    assert data_product_props.name == "Test Data Product"
    # Domain assignment
    domains = graph_client.get_aspect(data_product_urn, DomainsClass)
    assert domains and domains.domains[0] == str(domain_urn)

    # Add assets
    result = graph_client.execute_graphql(
        get_gql_query("tests/dataproduct/queries/setassets_dataproduct.graphql"),
        {"dataProductUrn": data_product_urn, "resourceUrns": dataset_urns},
    )
    assert "batchSetDataProduct" in result
    assert result["batchSetDataProduct"] is True
    data_product_props = graph_client.get_aspect(
        data_product_urn, DataProductPropertiesClass
    )
    assert data_product_props is not None
    assert data_product_props.assets is not None
    assert data_product_props.description == "Test Description"
    assert data_product_props.name == "Test Data Product"
    assert len(data_product_props.assets) == len(dataset_urns)
    assert set([asset.destinationUrn for asset in data_product_props.assets]) == set(
        dataset_urns
    )

    wait_for_writes_to_sync(mae_only=True)

    validate_listing(graph_client, data_product_urn, dataset_urns)
    validate_relationships(graph_client, data_product_urn, dataset_urns)

    # Update name and description
    result = graph_client.execute_graphql(
        get_gql_query("tests/dataproduct/queries/update_dataproduct.graphql"),
        {
            "urn": data_product_urn,
            "name": "New Test Data Product",
            "description": "New Description",
        },
    )
    wait_for_writes_to_sync(mae_only=True)

    # Data Product Properties
    data_product_props = graph_client.get_aspect(
        data_product_urn, DataProductPropertiesClass
    )
    assert data_product_props is not None
    assert data_product_props.description == "New Description"
    assert data_product_props.name == "New Test Data Product"
    assert data_product_props.assets is not None
    assert len(data_product_props.assets) == len(dataset_urns)

    validate_listing(graph_client, data_product_urn, dataset_urns)
    validate_relationships(graph_client, data_product_urn, dataset_urns)

    # delete dataproduct
    result = graph_client.execute_graphql(
        get_gql_query("tests/dataproduct/queries/delete_dataproduct.graphql"),
        {"urn": data_product_urn},
    )
    wait_for_writes_to_sync()
    assert graph_client.exists(data_product_urn) is False

    # Validate relationships are removed
    urn_match = {k: False for k in dataset_urns}
    for dataset_urn in dataset_urns:
        for e in graph_client.get_related_entities(
            dataset_urn,
            relationship_types=["DataProductContains"],
            direction=DataHubGraph.RelationshipDirection.INCOMING,
        ):
            if e.urn == data_product_urn:
                urn_match[dataset_urn] = True

    urns_missing = [k for k in urn_match if urn_match[k] is False]
    assert set(urns_missing) == set(dataset_urns), (
        f"All dataset urns should no longer have a DataProductContains relationship to the data product {data_product_urn}"
    )


def create_data_product(graph_client, name: str) -> str:
    domain_urn = Urn("domain", [datahub_guid({"name": "Marketing"})])
    result = graph_client.execute_graphql(
        get_gql_query("tests/dataproduct/queries/add_dataproduct.graphql"),
        {"domainUrn": str(domain_urn), "name": name, "description": "Test Description"},
    )
    assert "createDataProduct" in result
    return result["createDataProduct"]["urn"]


def delete_data_product(graph_client, data_product_urn: str) -> None:
    graph_client.execute_graphql(
        get_gql_query("tests/dataproduct/queries/delete_dataproduct.graphql"),
        {"urn": data_product_urn},
    )


def get_data_product_asset_urns(graph_client, data_product_urn: str) -> set:
    data_product_props = graph_client.get_aspect(
        data_product_urn, DataProductPropertiesClass
    )
    assert data_product_props is not None
    if data_product_props.assets is None:
        return set()
    return {asset.destinationUrn for asset in data_product_props.assets}


@with_test_retry()
def test_batch_add_to_data_products(graph_client, ingest_cleanup_data):
    data_product_urn_1 = create_data_product(
        graph_client, "Test Batch Add Data Product 1"
    )
    data_product_urn_2 = create_data_product(
        graph_client, "Test Batch Add Data Product 2"
    )
    try:
        result = graph_client.execute_graphql(
            BATCH_ADD_TO_DATA_PRODUCTS_QUERY,
            {
                "dataProductUrns": [data_product_urn_1, data_product_urn_2],
                "resourceUrns": heterogeneous_asset_urns,
            },
        )
        assert result["batchAddToDataProducts"] is True

        wait_for_writes_to_sync()

        assert get_data_product_asset_urns(graph_client, data_product_urn_1) == set(
            heterogeneous_asset_urns
        )
        assert get_data_product_asset_urns(graph_client, data_product_urn_2) == set(
            heterogeneous_asset_urns
        )
    finally:
        delete_data_product(graph_client, data_product_urn_1)
        delete_data_product(graph_client, data_product_urn_2)


@with_test_retry()
def test_batch_remove_from_data_products(graph_client, ingest_cleanup_data):
    data_product_urn = create_data_product(
        graph_client, "Test Batch Remove Data Product"
    )
    try:
        graph_client.execute_graphql(
            get_gql_query("tests/dataproduct/queries/setassets_dataproduct.graphql"),
            {
                "dataProductUrn": data_product_urn,
                "resourceUrns": heterogeneous_asset_urns,
            },
        )
        wait_for_writes_to_sync()
        assert get_data_product_asset_urns(graph_client, data_product_urn) == set(
            heterogeneous_asset_urns
        )

        result = graph_client.execute_graphql(
            BATCH_REMOVE_FROM_DATA_PRODUCTS_QUERY,
            {
                "dataProductUrns": [data_product_urn],
                "resourceUrns": heterogeneous_asset_urns,
            },
        )
        assert result["batchRemoveFromDataProducts"] is True

        wait_for_writes_to_sync()

        assert get_data_product_asset_urns(graph_client, data_product_urn) == set()
    finally:
        delete_data_product(graph_client, data_product_urn)
