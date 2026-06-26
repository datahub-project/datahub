import logging
from random import randint

import pytest

from datahub.emitter.mce_builder import datahub_guid, make_data_platform_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    ApplicationsClass,
    DataObjectPropertiesClass,
    DataProductPropertiesClass,
    DomainPropertiesClass,
    SubTypesClass,
)
from datahub.utilities.urns.urn import Urn
from tests.utils import wait_for_writes_to_sync, with_test_retry

logger = logging.getLogger(__name__)

# A dataObject URN mirrors a dataset URN: (platform, name, env). The unique suffix
# keeps each test run idempotent so stale data from a previous run cannot mask a
# real ingestion/indexing regression.
_UNIQUE = randint(10, 1000000)
_PLATFORM = "s3"
_OBJECT_NAME = f"test-bucket/smoke-data-object-{_UNIQUE}.parquet"
_DATA_OBJECT_URN = (
    f"urn:li:dataObject:({make_data_platform_urn(_PLATFORM)},{_OBJECT_NAME},PROD)"
)
_DISPLAY_NAME = f"Smoke Data Object {_UNIQUE}"
_SUBTYPE = "File"


def _read_query(filename: str) -> str:
    with open(filename) as fp:
        return fp.read()


def _ingest_data_object(graph_client: DataHubGraph) -> None:
    """Emit a dataObject with the minimum aspects needed to make it governable
    and searchable: properties (name) and a subtype."""
    for aspect in (
        DataObjectPropertiesClass(
            name=_DISPLAY_NAME,
            description="A smoke-test data object cataloged by reference.",
        ),
        SubTypesClass(typeNames=[_SUBTYPE]),
    ):
        graph_client.emit(
            MetadataChangeProposalWrapper(entityUrn=_DATA_OBJECT_URN, aspect=aspect)
        )


@pytest.fixture(scope="module", autouse=False)
def ingest_cleanup_data_object(graph_client):
    # Pre-clean for idempotency in case a prior run aborted before teardown.
    graph_client.hard_delete_entity(_DATA_OBJECT_URN)
    _ingest_data_object(graph_client)
    wait_for_writes_to_sync()
    yield
    logger.info("removing dataObject smoke-test data")
    graph_client.hard_delete_entity(_DATA_OBJECT_URN)
    wait_for_writes_to_sync()


@with_test_retry()
def _assert_retrievable_by_urn(graph_client: DataHubGraph) -> None:
    result = graph_client.execute_graphql(
        _read_query("tests/dataobject/queries/get_dataobject.graphql"),
        {"urn": _DATA_OBJECT_URN},
    )
    assert "dataObject" in result
    data_object = result["dataObject"]
    assert data_object is not None, (
        f"dataObject query returned null for {_DATA_OBJECT_URN}"
    )
    assert data_object["urn"] == _DATA_OBJECT_URN
    assert data_object["type"] == "DATA_OBJECT"
    assert data_object["exists"] is True
    assert data_object["properties"]["name"] == _DISPLAY_NAME
    assert data_object["subTypes"]["typeNames"] == [_SUBTYPE]
    assert data_object["platform"]["urn"] == make_data_platform_urn(_PLATFORM)


@with_test_retry()
def _assert_searchable(graph_client: DataHubGraph) -> None:
    result = graph_client.execute_graphql(
        _read_query("tests/dataobject/queries/search_dataobject.graphql"),
        {
            "input": {
                "types": ["DATA_OBJECT"],
                "query": _DISPLAY_NAME,
                "start": 0,
                "count": 20,
            }
        },
    )
    urns = [
        res["entity"]["urn"] for res in result["searchAcrossEntities"]["searchResults"]
    ]
    assert _DATA_OBJECT_URN in urns, (
        f"dataObject {_DATA_OBJECT_URN} not found in search results for "
        f"'{_DISPLAY_NAME}' (indexing/search regression). Got: {urns}"
    )


def test_data_object_ingest_query_and_governance(
    graph_client, ingest_cleanup_data_object
):
    # (b) Retrievable by URN with name + subtype.
    _assert_retrievable_by_urn(graph_client)

    # (c) Indexed and searchable.
    _assert_searchable(graph_client)

    # (d) Governance write #1 — data product membership.
    # batchSetDataProduct with the dataObject as the asset is the exact path that
    # regresses when an entity declares membership capability the backend can't
    # actually back. We create a throwaway data product, attach the dataObject,
    # and assert the membership landed on the data product's assets.
    # createDataProduct requires a domain, so stand one up first.
    domain_urn = str(Urn("domain", [datahub_guid({"name": f"smoke-do-{_UNIQUE}"})]))
    graph_client.emit(
        MetadataChangeProposalWrapper(
            entityUrn=domain_urn,
            aspect=DomainPropertiesClass(name=f"Smoke DataObject Domain {_UNIQUE}"),
        )
    )
    create_result = graph_client.execute_graphql(
        """
        mutation($name: String!, $domainUrn: String!) {
          createDataProduct(
            input: { properties: { name: $name }, domainUrn: $domainUrn }
          ) {
            urn
          }
        }
        """,
        {"name": f"Smoke DataObject DP {_UNIQUE}", "domainUrn": domain_urn},
    )
    data_product_urn = create_result["createDataProduct"]["urn"]
    try:
        set_dp_result = graph_client.execute_graphql(
            """
            mutation($dataProductUrn: String!, $resourceUrns: [String!]!) {
              batchSetDataProduct(
                input: {
                  dataProductUrn: $dataProductUrn
                  resourceUrns: $resourceUrns
                }
              )
            }
            """,
            {
                "dataProductUrn": data_product_urn,
                "resourceUrns": [_DATA_OBJECT_URN],
            },
        )
        assert set_dp_result["batchSetDataProduct"] is True
        wait_for_writes_to_sync()

        dp_props = graph_client.get_aspect(data_product_urn, DataProductPropertiesClass)
        assert dp_props is not None and dp_props.assets is not None
        assert _DATA_OBJECT_URN in {
            asset.destinationUrn for asset in dp_props.assets
        }, "dataObject was not added to the data product's assets"
    finally:
        graph_client.execute_graphql(
            "mutation($urn: String!) { deleteDataProduct(urn: $urn) }",
            {"urn": data_product_urn},
        )
        graph_client.hard_delete_entity(domain_urn)

    # (d) Governance write #2 — application membership.
    # Application membership is stored on the *asset's* `applications` aspect, so
    # this directly proves the dataObject entity accepts the applications aspect
    # end-to-end (the other half of the original membership gap).
    create_app_result = graph_client.execute_graphql(
        """
        mutation($name: String!) {
          createApplication(input: { properties: { name: $name } }) {
            urn
          }
        }
        """,
        {"name": f"Smoke DataObject App {_UNIQUE}"},
    )
    application_urn = create_app_result["createApplication"]["urn"]
    try:
        set_app_result = graph_client.execute_graphql(
            """
            mutation($applicationUrn: String!, $resourceUrns: [String!]!) {
              batchSetApplication(
                input: {
                  applicationUrn: $applicationUrn
                  resourceUrns: $resourceUrns
                }
              )
            }
            """,
            {
                "applicationUrn": application_urn,
                "resourceUrns": [_DATA_OBJECT_URN],
            },
        )
        assert set_app_result["batchSetApplication"] is True
        wait_for_writes_to_sync()

        applications = graph_client.get_aspect(_DATA_OBJECT_URN, ApplicationsClass)
        assert applications is not None, (
            "dataObject has no applications aspect after batchSetApplication"
        )
        assert application_urn in applications.applications, (
            "application was not recorded on the dataObject's applications aspect"
        )
    finally:
        graph_client.execute_graphql(
            "mutation($urn: String!) { deleteApplication(urn: $urn) }",
            {"urn": application_urn},
        )
