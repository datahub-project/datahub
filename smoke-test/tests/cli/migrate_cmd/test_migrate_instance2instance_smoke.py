import logging
from random import randint

import pytest

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    EmbedClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    GlobalTagsClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    SiblingsClass,
    StringTypeClass,
    SubTypesClass,
    TagAssociationClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from tests.consistency_utils import wait_for_writes_to_sync
from tests.utils import delete_urns, run_datahub_cmd

logger = logging.getLogger(__name__)

PLATFORM = "snowflake"
ENV = "PROD"
# Randomize instances so repeated runs don't collide.
_suffix = randint(10, 100000)
OLD_INSTANCE = f"mig_old_{_suffix}"
NEW_INSTANCE = f"mig_new_{_suffix}"
KEEP_INSTANCE = f"mig_keep_{_suffix}"
TABLE = "my_db.my_schema.events_agg"
UPSTREAM_TABLE = "my_db.my_schema.events_raw"

old_down = make_dataset_urn_with_platform_instance(PLATFORM, TABLE, OLD_INSTANCE, ENV)
new_down = make_dataset_urn_with_platform_instance(PLATFORM, TABLE, NEW_INSTANCE, ENV)
upstream = make_dataset_urn_with_platform_instance(
    PLATFORM, UPSTREAM_TABLE, KEEP_INSTANCE, ENV
)
# The URN that the old (buggy) builder produced by doubling the instance prefix.
double_prefixed = make_dataset_urn_with_platform_instance(
    PLATFORM, f"{NEW_INSTANCE}.{TABLE}", NEW_INSTANCE, ENV
)

# Separate instances for the incoming-reference test so the two migrations don't
# interfere.
OLD_INSTANCE_INC = f"mig_iold_{_suffix}"
NEW_INSTANCE_INC = f"mig_inew_{_suffix}"
up_src_old = make_dataset_urn_with_platform_instance(
    PLATFORM, "my_db.my_schema.up_src", OLD_INSTANCE_INC, ENV
)
up_src_new = make_dataset_urn_with_platform_instance(
    PLATFORM, "my_db.my_schema.up_src", NEW_INSTANCE_INC, ENV
)
# A referencing entity on a non-migrated instance.
ref_down = make_dataset_urn_with_platform_instance(
    PLATFORM, "my_db.my_schema.ref_down", KEEP_INSTANCE, ENV
)

# Merge-mode scenario: the target already exists, so migrate merges into it.
OLD_INSTANCE_MRG = f"mig_mold_{_suffix}"
NEW_INSTANCE_MRG = f"mig_mnew_{_suffix}"
mrg_src = make_dataset_urn_with_platform_instance(
    PLATFORM, "my_db.my_schema.mrg_tbl", OLD_INSTANCE_MRG, ENV
)
mrg_tgt = make_dataset_urn_with_platform_instance(
    PLATFORM, "my_db.my_schema.mrg_tbl", NEW_INSTANCE_MRG, ENV
)
mrg_up = make_dataset_urn_with_platform_instance(
    PLATFORM, "my_db.my_schema.mrg_up", KEEP_INSTANCE, ENV
)


def _schema(field_name: str) -> SchemaMetadataClass:
    return SchemaMetadataClass(
        schemaName="s",
        platform=make_data_platform_urn(PLATFORM),
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=[
            SchemaFieldClass(
                fieldPath=field_name,
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="STRING",
            )
        ],
    )


def _instance(name: str) -> DataPlatformInstanceClass:
    return DataPlatformInstanceClass(
        platform=make_data_platform_urn(PLATFORM),
        instance=make_dataplatform_instance_urn(PLATFORM, name),
    )


def _seed(graph_client: DataHubGraph) -> None:
    mcps = [
        MetadataChangeProposalWrapper(entityUrn=upstream, aspect=_schema("id")),
        MetadataChangeProposalWrapper(
            entityUrn=upstream, aspect=_instance(KEEP_INSTANCE)
        ),
        MetadataChangeProposalWrapper(
            entityUrn=old_down, aspect=_schema("event_count")
        ),
        MetadataChangeProposalWrapper(
            entityUrn=old_down, aspect=_instance(OLD_INSTANCE)
        ),
        MetadataChangeProposalWrapper(
            entityUrn=old_down,
            aspect=DatasetPropertiesClass(description="downstream aggregate"),
        ),
        MetadataChangeProposalWrapper(
            entityUrn=old_down, aspect=SubTypesClass(typeNames=["View"])
        ),
        # embed was absent from the old hardcoded aspect list; it must now be
        # migrated (guards the registry-driven aspect selection).
        MetadataChangeProposalWrapper(
            entityUrn=old_down, aspect=EmbedClass(renderUrl="https://example.com/e")
        ),
        # siblings is a relationship aspect and must be carried over.
        MetadataChangeProposalWrapper(
            entityUrn=old_down,
            aspect=SiblingsClass(siblings=[upstream], primary=True),
        ),
        MetadataChangeProposalWrapper(
            entityUrn=old_down,
            aspect=GlobalTagsClass(
                tags=[TagAssociationClass(tag="urn:li:tag:mig_smoke")]
            ),
        ),
        MetadataChangeProposalWrapper(
            entityUrn=old_down,
            aspect=UpstreamLineageClass(
                upstreams=[
                    UpstreamClass(
                        dataset=upstream, type=DatasetLineageTypeClass.TRANSFORMED
                    )
                ],
                fineGrainedLineages=[
                    FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                        upstreams=[make_schema_field_urn(upstream, "id")],
                        downstreams=[make_schema_field_urn(old_down, "event_count")],
                    )
                ],
            ),
        ),
    ]
    for mcp in mcps:
        graph_client.emit_mcp(mcp)
    wait_for_writes_to_sync()


@pytest.fixture(scope="module")
def seeded(graph_client: DataHubGraph):
    all_urns = [old_down, new_down, upstream, double_prefixed]
    delete_urns(graph_client, all_urns)
    wait_for_writes_to_sync()
    _seed(graph_client)
    try:
        yield
    finally:
        delete_urns(graph_client, all_urns)
        wait_for_writes_to_sync()


def test_instance2instance_rewrites_lineage_and_migrates_aspects(
    graph_client: DataHubGraph, seeded
) -> None:
    result = run_datahub_cmd(
        [
            "migrate",
            "instance2instance",
            "--platform",
            PLATFORM,
            "--old-instance",
            OLD_INSTANCE,
            "--new-instance",
            NEW_INSTANCE,
            "--env",
            ENV,
            "--entity-types",
            "dataset",
            "--force",
            "--keep",
        ]
    )
    assert result.exit_code == 0, result.output
    wait_for_writes_to_sync()

    # The migrated URN exists with a single instance prefix, and the doubled
    # variant produced by the old builder bug does not.
    assert graph_client.exists(new_down)
    assert not graph_client.exists(double_prefixed)

    lineage = graph_client.get_aspect(new_down, UpstreamLineageClass)
    assert lineage is not None
    fgl = lineage.fineGrainedLineages
    assert fgl is not None

    # Column-level downstream is rewritten to the new URN; the cross-instance
    # upstream (table- and column-level) is left untouched.
    assert fgl[0].downstreams == [make_schema_field_urn(new_down, "event_count")]
    assert fgl[0].upstreams == [make_schema_field_urn(upstream, "id")]
    assert [u.dataset for u in lineage.upstreams] == [upstream]

    # Aspects beyond lineage are carried over.
    assert graph_client.get_aspect(new_down, SubTypesClass) is not None
    assert graph_client.get_aspect(new_down, DatasetPropertiesClass) is not None
    assert graph_client.get_aspect(new_down, GlobalTagsClass) is not None

    # An aspect absent from the old hardcoded list is now migrated.
    assert graph_client.get_aspect(new_down, EmbedClass) is not None

    # siblings is a relationship aspect and is carried over (pointing at the
    # un-migrated counterpart), not dropped.
    siblings = graph_client.get_aspect(new_down, SiblingsClass)
    assert siblings is not None
    assert siblings.siblings == [upstream]


@pytest.fixture(scope="module")
def seeded_incoming(graph_client: DataHubGraph):
    all_urns = [up_src_old, up_src_new, ref_down]
    delete_urns(graph_client, all_urns)
    wait_for_writes_to_sync()
    for mcp in [
        MetadataChangeProposalWrapper(entityUrn=up_src_old, aspect=_schema("id")),
        MetadataChangeProposalWrapper(
            entityUrn=up_src_old, aspect=_instance(OLD_INSTANCE_INC)
        ),
        MetadataChangeProposalWrapper(entityUrn=ref_down, aspect=_schema("cnt")),
        MetadataChangeProposalWrapper(
            entityUrn=ref_down, aspect=_instance(KEEP_INSTANCE)
        ),
        MetadataChangeProposalWrapper(
            entityUrn=ref_down,
            aspect=UpstreamLineageClass(
                upstreams=[
                    UpstreamClass(
                        dataset=up_src_old, type=DatasetLineageTypeClass.TRANSFORMED
                    )
                ],
                fineGrainedLineages=[
                    FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                        upstreams=[make_schema_field_urn(up_src_old, "id")],
                        downstreams=[make_schema_field_urn(ref_down, "cnt")],
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


def test_incoming_references_are_rewritten(
    graph_client: DataHubGraph, seeded_incoming
) -> None:
    # Migrate the UPSTREAM; a non-migrated downstream references it.
    result = run_datahub_cmd(
        [
            "migrate",
            "instance2instance",
            "--platform",
            PLATFORM,
            "--old-instance",
            OLD_INSTANCE_INC,
            "--new-instance",
            NEW_INSTANCE_INC,
            "--env",
            ENV,
            "--entity-types",
            "dataset",
            "--force",
            "--keep",
        ]
    )
    assert result.exit_code == 0, result.output
    wait_for_writes_to_sync()

    lineage = graph_client.get_aspect(ref_down, UpstreamLineageClass)
    assert lineage is not None
    fgl = lineage.fineGrainedLineages
    assert fgl is not None

    # The reference to the migrated upstream is rewritten both table-level and
    # column-level. The column-level rewrite is the case the old per-aspect
    # modifier never handled.
    assert [u.dataset for u in lineage.upstreams] == [up_src_new]
    assert fgl[0].upstreams == [make_schema_field_urn(up_src_new, "id")]
    # ref_down itself was not migrated, so its own downstream field is unchanged.
    assert fgl[0].downstreams == [make_schema_field_urn(ref_down, "cnt")]


@pytest.fixture(scope="module")
def seeded_merge(graph_client: DataHubGraph):
    all_urns = [mrg_src, mrg_tgt, mrg_up]
    delete_urns(graph_client, all_urns)
    wait_for_writes_to_sync()
    for mcp in [
        MetadataChangeProposalWrapper(entityUrn=mrg_up, aspect=_schema("id")),
        MetadataChangeProposalWrapper(
            entityUrn=mrg_up, aspect=_instance(KEEP_INSTANCE)
        ),
        MetadataChangeProposalWrapper(entityUrn=mrg_src, aspect=_schema("cnt")),
        MetadataChangeProposalWrapper(
            entityUrn=mrg_src, aspect=_instance(OLD_INSTANCE_MRG)
        ),
        MetadataChangeProposalWrapper(
            entityUrn=mrg_src,
            aspect=UpstreamLineageClass(
                upstreams=[
                    UpstreamClass(
                        dataset=mrg_up, type=DatasetLineageTypeClass.TRANSFORMED
                    )
                ],
                fineGrainedLineages=[
                    FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                        upstreams=[make_schema_field_urn(mrg_up, "id")],
                        downstreams=[make_schema_field_urn(mrg_src, "cnt")],
                    )
                ],
            ),
        ),
        # Pre-create the target so the migration merges into it.
        MetadataChangeProposalWrapper(entityUrn=mrg_tgt, aspect=_schema("cnt")),
        MetadataChangeProposalWrapper(
            entityUrn=mrg_tgt, aspect=_instance(NEW_INSTANCE_MRG)
        ),
    ]:
        graph_client.emit_mcp(mcp)
    wait_for_writes_to_sync()
    try:
        yield
    finally:
        delete_urns(graph_client, all_urns)
        wait_for_writes_to_sync()


def test_merge_mode_carries_and_rewrites_fine_grained_lineage(
    graph_client: DataHubGraph, seeded_merge
) -> None:
    result = run_datahub_cmd(
        [
            "migrate",
            "instance2instance",
            "--platform",
            PLATFORM,
            "--old-instance",
            OLD_INSTANCE_MRG,
            "--new-instance",
            NEW_INSTANCE_MRG,
            "--env",
            ENV,
            "--entity-types",
            "dataset",
            "--on-conflict",
            "overwrite",
            "--force",
            "--keep",
        ]
    )
    assert result.exit_code == 0, result.output
    wait_for_writes_to_sync()

    lineage = graph_client.get_aspect(mrg_tgt, UpstreamLineageClass)
    assert lineage is not None
    fgl = lineage.fineGrainedLineages
    # Column-level lineage is carried into the pre-existing target (not dropped),
    # with the self-downstream rewritten to the target URN.
    assert fgl is not None and len(fgl) >= 1
    downstreams = fgl[0].downstreams or []
    upstreams = fgl[0].upstreams or []
    assert make_schema_field_urn(mrg_tgt, "cnt") in downstreams
    assert make_schema_field_urn(mrg_up, "id") in upstreams
