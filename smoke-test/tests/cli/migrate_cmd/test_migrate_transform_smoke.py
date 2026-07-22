import logging
from random import randint

import pytest

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    SubTypesClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from tests.consistency_utils import wait_for_writes_to_sync
from tests.utils import delete_urns, run_datahub_cmd

logger = logging.getLogger(__name__)

# Unique platform so `migrate transform` discovery only touches this dataset.
PLATFORM = f"lctest{randint(10, 100000)}"
ENV = "PROD"
mixed = make_dataset_urn_with_platform_instance(
    PLATFORM, "MyDb.MySchema.LcTbl", None, ENV
)
lower = make_dataset_urn_with_platform_instance(
    PLATFORM, "mydb.myschema.lctbl", None, ENV
)


@pytest.fixture(scope="module")
def seeded(graph_client: DataHubGraph):
    all_urns = [mixed, lower]
    delete_urns(graph_client, all_urns)
    wait_for_writes_to_sync()
    for mcp in [
        MetadataChangeProposalWrapper(
            entityUrn=mixed,
            aspect=SchemaMetadataClass(
                schemaName="s",
                platform=make_data_platform_urn(PLATFORM),
                version=0,
                hash="",
                platformSchema=OtherSchemaClass(rawSchema=""),
                fields=[
                    SchemaFieldClass(
                        fieldPath="col_a",
                        type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                        nativeDataType="STRING",
                    )
                ],
            ),
        ),
        MetadataChangeProposalWrapper(
            entityUrn=mixed, aspect=SubTypesClass(typeNames=["View"])
        ),
        # Self-referential column-level lineage: must be lowercased with the URN.
        MetadataChangeProposalWrapper(
            entityUrn=mixed,
            aspect=UpstreamLineageClass(
                upstreams=[
                    UpstreamClass(
                        dataset=mixed, type=DatasetLineageTypeClass.TRANSFORMED
                    )
                ],
                fineGrainedLineages=[
                    FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                        upstreams=[make_schema_field_urn(mixed, "col_a")],
                        downstreams=[make_schema_field_urn(mixed, "col_a")],
                    )
                ],
            ),
        ),
    ]:
        graph_client.emit_mcp(mcp)
    wait_for_writes_to_sync()
    try:
        yield
    finally:
        delete_urns(graph_client, all_urns)
        wait_for_writes_to_sync()


def test_transform_lowercase(graph_client: DataHubGraph, seeded) -> None:
    result = run_datahub_cmd(
        [
            "migrate",
            "transform",
            "--platform",
            PLATFORM,
            "--converter",
            "lowercase",
            "--env",
            ENV,
            "--force",
            "--keep",
        ]
    )
    assert result.exit_code == 0, result.output
    wait_for_writes_to_sync()

    assert graph_client.exists(lower)
    assert graph_client.get_aspect(lower, SubTypesClass) is not None

    lineage = graph_client.get_aspect(lower, UpstreamLineageClass)
    assert lineage is not None and lineage.fineGrainedLineages
    # The self-referential column-level lineage is lowercased with the URN.
    assert lineage.fineGrainedLineages[0].downstreams == [
        make_schema_field_urn(lower, "col_a")
    ]
