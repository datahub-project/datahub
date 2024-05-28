import logging
import time
from typing import Any, Generator

import pytest

# import tenacity
from datahub.api.entities.dataset.dataset import (
    Dataset,
    SchemaFieldSpecification,
    SchemaSpecification,
)
from datahub.emitter.mce_builder import make_dataset_urn, make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import AuditStampClass

from tests.utils import get_gms_url, wait_for_healthcheck_util

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def wait_for_healthchecks():
    wait_for_healthcheck_util()
    yield


@pytest.mark.dependency()
def test_healthchecks(wait_for_healthchecks):
    # Call to wait_for_healthchecks fixture will do the actual functionality.
    pass


@pytest.fixture(scope="session")
def graph_client() -> DataHubGraph:
    return DataHubGraph(config=DatahubClientConfig(server=get_gms_url()))


# make 2 datasets with 2 schema fields each and connect them via column level
# lineage


@pytest.fixture(scope="module")
def create_test_data(graph_client: DataHubGraph) -> Generator[Any, Any, Any]:
    # Create datasets
    dataset_urns = [make_dataset_urn("snowflake", f"table_foo_{i}") for i in range(2)]
    for i, dataset_urn in enumerate(dataset_urns):
        dataset = Dataset(
            id=f"table_foo_{i}",
            platform="snowflake",
            name=f"table_foo_{i}",
            description=f"this is table foo {i}",
            subtype=None,
            subtypes=None,
            downstreams=None,
            properties=None,
            urn=dataset_urn,
            schema=SchemaSpecification(
                file=None,
                fields=[
                    SchemaFieldSpecification(
                        id="column_1",
                        type="string",
                        description="this is column 1" if i == 0 else None,
                        urn=make_schema_field_urn(dataset_urn, "column_1"),
                    ),
                    SchemaFieldSpecification(
                        id="column_2",
                        type="string",
                        urn=make_schema_field_urn(dataset_urn, "column_2"),
                        description="this is column 2" if i == 0 else None,
                    ),
                ],
            ),
        )
        for mcp in dataset.generate_mcp():
            graph_client.emit_mcp(mcp)

    # Emit lineage
    downstream_dataset_urn = dataset_urns[1]
    upstream_dataset_urn = dataset_urns[0]
    downstream_schema_field_urns = [
        make_schema_field_urn(downstream_dataset_urn, "column_1"),
        make_schema_field_urn(downstream_dataset_urn, "column_2"),
    ]
    upstream_schema_field_urns = [
        make_schema_field_urn(upstream_dataset_urn, "column_1"),
        make_schema_field_urn(upstream_dataset_urn, "column_2"),
    ]
    from datahub.metadata.schema_classes import (
        FineGrainedLineageClass,
        FineGrainedLineageDownstreamTypeClass,
        FineGrainedLineageUpstreamTypeClass,
        UpstreamClass,
        UpstreamLineageClass,
    )

    upstream_lineage_aspect = UpstreamLineageClass(
        upstreams=[
            UpstreamClass(
                dataset=upstream_dataset_urn,
                type="COPY",
            )
        ],
        fineGrainedLineages=[
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD_SET,
                upstreams=[upstream_schema_field_urn],
                downstreams=[downstream_schema_field_urn],
            )
            for upstream_schema_field_urn, downstream_schema_field_urn in zip(
                upstream_schema_field_urns, downstream_schema_field_urns
            )
        ],
    )
    graph_client.emit(
        MetadataChangeProposalWrapper(
            entityUrn=downstream_dataset_urn,
            aspect=upstream_lineage_aspect,
        )
    )
    yield
    # Clean up
    for urn in dataset_urns:
        graph_client.delete_entity(urn, hard=True)
    for urn in downstream_schema_field_urns:
        graph_client.delete_entity(urn, hard=True)
    for urn in upstream_schema_field_urns:
        graph_client.delete_entity(urn, hard=True)


def test_docs_propagation(graph_client: DataHubGraph, create_test_data) -> None:
    # Wait for the writes to sync

    from time import sleep

    sleep(10)
    # wait_for_writes_to_sync()

    # edit the description of the upstream schema field
    upstream_dataset_urn = make_dataset_urn("snowflake", "table_foo_0")
    upstream_schema_field_urn = make_schema_field_urn(upstream_dataset_urn, "column_1")

    import datetime

    from datahub.metadata.schema_classes import (
        DocumentationClass,
        EditableSchemaFieldInfoClass,
        EditableSchemaMetadataClass,
    )

    # get human readable timestamp
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    editable_schema_metadata_aspect = EditableSchemaMetadataClass(
        editableSchemaFieldInfo=[
            EditableSchemaFieldInfoClass(
                fieldPath="column_1",
                description="this is the updated description as of {}".format(
                    timestamp
                ),
            ),
        ],
        created=AuditStampClass(
            time=int(datetime.datetime.now().timestamp() * 1000),
            actor="urn:li:corpuser:sdas@acryl.io",
        ),
        lastModified=AuditStampClass(
            time=int(datetime.datetime.now().timestamp() * 1000),
            actor="urn:li:corpuser:sdas@acryl.io",
        ),
    )
    assert editable_schema_metadata_aspect.validate()
    graph_client.emit(
        MetadataChangeProposalWrapper(
            entityUrn=upstream_dataset_urn,
            aspect=editable_schema_metadata_aspect,
        )
    )

    # wait_for_writes_to_sync()
    time.sleep(10)

    # get the downstream schema field and check if the description has been
    # propagated
    downstream_schema_field_urn = make_schema_field_urn(
        make_dataset_urn("snowflake", "table_foo_1"), "column_1"
    )

    documentation_aspect = graph_client.get_aspect(
        downstream_schema_field_urn, DocumentationClass
    )
    assert documentation_aspect is not None
    if documentation_aspect:
        first_element = documentation_aspect.documentations[0]
        assert (
            first_element.documentation
            == "this is the updated description as of {}".format(timestamp)
        )
        assert first_element.attribution
        assert first_element.attribution.sourceDetail
        assert (
            first_element.attribution.sourceDetail.get("origin")
            == upstream_schema_field_urn
        )
        assert first_element.attribution.sourceDetail.get("via") is None
        assert first_element.attribution.sourceDetail.get("propagated") == "true"
        assert first_element.attribution.actor == "urn:li:corpuser:__datahub_system"
        # assert first_element.attribution.source == "urn:li:corpuser:__system"
    pass
