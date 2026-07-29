import json
import logging
from random import randint

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionTypeClass,
    DataPlatformInstanceClass,
    DatasetAssertionInfoClass,
    DatasetAssertionScopeClass,
    DatasetPropertiesClass,
    GlobalTagsClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    TagAssociationClass,
)
from tests.consistency_utils import wait_for_writes_to_sync
from tests.utils import delete_urns, run_datahub_cmd

logger = logging.getLogger(__name__)

PLATFORM = "snowflake"
ENV = "PROD"
_suffix = randint(10, 100000)

# --- platform2instance scenario ---
P2I_INSTANCE = f"mig_p2i_{_suffix}"
P2I_TABLE = "my_db.my_schema.p2i_tbl"
p2i_src = make_dataset_urn_with_platform_instance(PLATFORM, P2I_TABLE, None, ENV)
p2i_dst = make_dataset_urn_with_platform_instance(
    PLATFORM, P2I_TABLE, P2I_INSTANCE, ENV
)

# --- preserve scenario ---
PRE_OLD = f"mig_pold_{_suffix}"
PRE_NEW = f"mig_pnew_{_suffix}"
pre_src = make_dataset_urn_with_platform_instance(
    PLATFORM, "my_db.my_schema.pre_tbl", PRE_OLD, ENV
)
pre_tgt = make_dataset_urn_with_platform_instance(
    PLATFORM, "my_db.my_schema.pre_tbl", PRE_NEW, ENV
)

# --- urns-mapping scenario ---
UM_OLD = f"mig_umold_{_suffix}"
UM_NEW = f"mig_umnew_{_suffix}"
um_src = make_dataset_urn_with_platform_instance(
    PLATFORM, "my_db.my_schema.um_tbl", UM_OLD, ENV
)
um_tgt = make_dataset_urn_with_platform_instance(
    PLATFORM, "my_db.my_schema.um_tbl", UM_NEW, ENV
)

# --- assertion incoming-reference scenario ---
ASRT_OLD = f"mig_aold_{_suffix}"
ASRT_NEW = f"mig_anew_{_suffix}"
asrt_src = make_dataset_urn_with_platform_instance(
    PLATFORM, "my_db.my_schema.asrt_tbl", ASRT_OLD, ENV
)
asrt_dst = make_dataset_urn_with_platform_instance(
    PLATFORM, "my_db.my_schema.asrt_tbl", ASRT_NEW, ENV
)
assertion_urn = f"urn:li:assertion:mig_smoke_{_suffix}"


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


def test_dataplatform2instance_assigns_instance_and_carries_aspects(
    graph_client: DataHubGraph,
) -> None:
    all_urns = [p2i_src, p2i_dst]
    delete_urns(graph_client, all_urns)
    wait_for_writes_to_sync()
    for mcp in [
        MetadataChangeProposalWrapper(entityUrn=p2i_src, aspect=_schema("id")),
        MetadataChangeProposalWrapper(
            entityUrn=p2i_src,
            aspect=DatasetPropertiesClass(description="p2i source"),
        ),
        MetadataChangeProposalWrapper(
            entityUrn=p2i_src,
            aspect=GlobalTagsClass(tags=[TagAssociationClass(tag="urn:li:tag:p2i")]),
        ),
    ]:
        graph_client.emit_mcp(mcp)
    wait_for_writes_to_sync()

    try:
        result = run_datahub_cmd(
            [
                "migrate",
                "dataplatform2instance",
                "--platform",
                PLATFORM,
                "--instance",
                P2I_INSTANCE,
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

        assert graph_client.exists(p2i_dst)
        assert graph_client.get_aspect(p2i_dst, DatasetPropertiesClass) is not None
        assert graph_client.get_aspect(p2i_dst, GlobalTagsClass) is not None
        instance = graph_client.get_aspect(p2i_dst, DataPlatformInstanceClass)
        assert instance is not None and instance.instance == (
            make_dataplatform_instance_urn(PLATFORM, P2I_INSTANCE)
        )
    finally:
        delete_urns(graph_client, all_urns)
        wait_for_writes_to_sync()


def test_preserve_leaves_existing_target_untouched(
    graph_client: DataHubGraph,
) -> None:
    all_urns = [pre_src, pre_tgt]
    delete_urns(graph_client, all_urns)
    wait_for_writes_to_sync()
    for mcp in [
        MetadataChangeProposalWrapper(entityUrn=pre_src, aspect=_schema("id")),
        MetadataChangeProposalWrapper(entityUrn=pre_src, aspect=_instance(PRE_OLD)),
        MetadataChangeProposalWrapper(
            entityUrn=pre_src, aspect=DatasetPropertiesClass(description="from source")
        ),
        # Pre-existing target with its own description that must survive.
        MetadataChangeProposalWrapper(entityUrn=pre_tgt, aspect=_schema("id")),
        MetadataChangeProposalWrapper(entityUrn=pre_tgt, aspect=_instance(PRE_NEW)),
        MetadataChangeProposalWrapper(
            entityUrn=pre_tgt,
            aspect=DatasetPropertiesClass(description="existing target"),
        ),
    ]:
        graph_client.emit_mcp(mcp)
    wait_for_writes_to_sync()

    try:
        result = run_datahub_cmd(
            [
                "migrate",
                "instance2instance",
                "--platform",
                PLATFORM,
                "--old-instance",
                PRE_OLD,
                "--new-instance",
                PRE_NEW,
                "--env",
                ENV,
                "--entity-types",
                "dataset",
                "--on-conflict",
                "preserve",
                "--force",
                "--keep",
            ]
        )
        assert result.exit_code == 0, result.output
        wait_for_writes_to_sync()

        # The pre-existing target keeps its own description (source did not win).
        props = graph_client.get_aspect(pre_tgt, DatasetPropertiesClass)
        assert props is not None and props.description == "existing target"
    finally:
        delete_urns(graph_client, all_urns)
        wait_for_writes_to_sync()


def test_urns_mapping_migrates_explicit_pairs(
    graph_client: DataHubGraph, tmp_path
) -> None:
    all_urns = [um_src, um_tgt]
    delete_urns(graph_client, all_urns)
    wait_for_writes_to_sync()
    for mcp in [
        MetadataChangeProposalWrapper(entityUrn=um_src, aspect=_schema("id")),
        MetadataChangeProposalWrapper(
            entityUrn=um_src, aspect=DatasetPropertiesClass(description="um source")
        ),
    ]:
        graph_client.emit_mcp(mcp)
    wait_for_writes_to_sync()

    mapping_file = tmp_path / "mapping.json"
    mapping_file.write_text(json.dumps([{"source": um_src, "target": um_tgt}]))

    try:
        result = run_datahub_cmd(
            [
                "migrate",
                "urns-mapping",
                "--mapping-file",
                str(mapping_file),
                "--force",
                "--keep",
            ]
        )
        assert result.exit_code == 0, result.output
        wait_for_writes_to_sync()

        assert graph_client.exists(um_tgt)
        props = graph_client.get_aspect(um_tgt, DatasetPropertiesClass)
        assert props is not None and props.description == "um source"
    finally:
        delete_urns(graph_client, all_urns)
        wait_for_writes_to_sync()


def test_assertion_reference_is_rewritten(graph_client: DataHubGraph) -> None:
    all_urns = [asrt_src, asrt_dst, assertion_urn]
    delete_urns(graph_client, all_urns)
    wait_for_writes_to_sync()
    for mcp in [
        MetadataChangeProposalWrapper(entityUrn=asrt_src, aspect=_schema("id")),
        MetadataChangeProposalWrapper(entityUrn=asrt_src, aspect=_instance(ASRT_OLD)),
        # An assertion whose "Asserts" relationship points at the migrated dataset.
        MetadataChangeProposalWrapper(
            entityUrn=assertion_urn,
            aspect=AssertionInfoClass(
                type=AssertionTypeClass.DATASET,
                datasetAssertion=DatasetAssertionInfoClass(
                    dataset=asrt_src,
                    scope=DatasetAssertionScopeClass.DATASET_ROWS,
                    operator="_NATIVE_",  # type: ignore[arg-type]
                ),
            ),
        ),
    ]:
        graph_client.emit_mcp(mcp)
    wait_for_writes_to_sync()

    try:
        result = run_datahub_cmd(
            [
                "migrate",
                "instance2instance",
                "--platform",
                PLATFORM,
                "--old-instance",
                ASRT_OLD,
                "--new-instance",
                ASRT_NEW,
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

        # The assertion now targets the migrated dataset. This is the case the old
        # hardcoded relationship-type list (missing "Asserts") left dangling.
        info = graph_client.get_aspect(assertion_urn, AssertionInfoClass)
        assert info is not None and info.datasetAssertion is not None
        assert info.datasetAssertion.dataset == asrt_dst
    finally:
        delete_urns(graph_client, all_urns)
        wait_for_writes_to_sync()
