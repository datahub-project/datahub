import json
import logging
import time
from typing import Optional

import pytest

from datahub.cli.migration_utils import get_incoming_relationships
from datahub.emitter.mce_builder import (
    make_dashboard_urn,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import DatabaseKey
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionTypeClass,
    AuditStampClass,
    ChangeAuditStampsClass,
    ContainerPropertiesClass,
    DashboardInfoClass,
    DataPlatformInstanceClass,
    DatasetAssertionInfoClass,
    DatasetAssertionScopeClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    GlobalTagsClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    OtherSchemaClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    StructuredPropertiesClass,
    StructuredPropertyDefinitionClass,
    StructuredPropertyValueAssignmentClass,
    SubTypesClass,
    TagAssociationClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from tests.consistency_utils import wait_for_writes_to_sync
from tests.utilities.domains import Domain
from tests.utils import delete_urns, get_sleep_info, run_datahub_cmd, unique_suffix

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.domain(Domain.INGESTION)

PLATFORM = "snowflake"
ENV = "PROD"
_suffix = unique_suffix()

# --- platform2instance scenario ---
# Unique platform so catalog-wide dataplatform2instance cannot rewrite other
# xdist workers' snowflake datasets (e.g. lineage smoke tests).
P2I_PLATFORM = f"migp2i_{_suffix}"
P2I_INSTANCE = f"mig_p2i_{_suffix}"
P2I_TABLE = f"my_db.my_schema.p2i_tbl_{_suffix}"
p2i_src = make_dataset_urn_with_platform_instance(P2I_PLATFORM, P2I_TABLE, None, ENV)
p2i_dst = make_dataset_urn_with_platform_instance(
    P2I_PLATFORM, P2I_TABLE, P2I_INSTANCE, ENV
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

# --- urns-mapping scenario (a full multi-entity, multi-aspect case) ---
DASH_TOOL = "looker"
UM_OLD = f"mig_umold_{_suffix}"
UM_NEW = f"mig_umnew_{_suffix}"
# Upstream dataset that is NOT migrated; the migrated dataset keeps pointing at it.
um_up = make_dataset_urn_with_platform_instance(
    PLATFORM, "my_db.my_schema.um_up", f"mig_umkeep_{_suffix}", ENV
)
um_ds_src = make_dataset_urn_with_platform_instance(
    PLATFORM, "my_db.my_schema.um_events", UM_OLD, ENV
)
um_ds_tgt = make_dataset_urn_with_platform_instance(
    PLATFORM, "my_db.my_schema.um_events", UM_NEW, ENV
)
um_dash_src = make_dashboard_urn(DASH_TOOL, f"{UM_OLD}.um_dash")
um_dash_tgt = make_dashboard_urn(DASH_TOOL, f"{UM_NEW}.um_dash")
um_assert1 = f"urn:li:assertion:um_a1_{_suffix}"
um_assert2 = f"urn:li:assertion:um_a2_{_suffix}"
# A structured-property definition must exist before values can be assigned.
um_prop = f"urn:li:structuredProperty:um_tier_{_suffix}"

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

# --- container migration scenario ---
CT_OLD = f"mig_ctold_{_suffix}"
CT_NEW = f"mig_ctnew_{_suffix}"
_ct_src_key = DatabaseKey(
    platform=PLATFORM, instance=CT_OLD, env=ENV, database=f"mig_ctdb_{_suffix}"
)
_ct_props = _ct_src_key.model_dump(by_alias=True, exclude_none=True)
ct_src = f"urn:li:container:{_ct_src_key.guid()}"
# The migrated container's URN is a hash of the same key with the new instance —
# computed exactly as _migrate_containers does (parse props → reseat instance → guid).
_ct_dst_key = DatabaseKey.model_validate(dict(_ct_props))
_ct_dst_key.instance = CT_NEW
ct_dst = f"urn:li:container:{_ct_dst_key.guid()}"

# --- non-dataset additive-merge scenarios (urns-mapping --on-conflict patch) ---
# A no-builder container pair. urns-mapping routes containers through the normal
# entity merge (not the i2i/p2i legacy container path), so a pre-seeded target
# exercises _merge_generic_entity: the union must keep both sides' owners/tags/terms,
# reseat containerProperties, and keep the target on a conflicting scalar.
NDP_OLD = f"mig_ndpold_{_suffix}"
NDP_NEW = f"mig_ndpnew_{_suffix}"
_ndp_src_key = DatabaseKey(
    platform=PLATFORM, instance=NDP_OLD, env=ENV, database=f"mig_ndpdb_{_suffix}"
)
ndp_src = f"urn:li:container:{_ndp_src_key.guid()}"
_ndp_dst_key = DatabaseKey(
    platform=PLATFORM, instance=NDP_NEW, env=ENV, database=f"mig_ndpdb_{_suffix}"
)
ndp_dst = f"urn:li:container:{_ndp_dst_key.guid()}"
ndp_prop = f"urn:li:structuredProperty:ndp_tier_{_suffix}"

# A separate container pair for the overwrite (full-replace) case.
NDO_OLD = f"mig_ndoold_{_suffix}"
NDO_NEW = f"mig_ndonew_{_suffix}"
_ndo_src_key = DatabaseKey(
    platform=PLATFORM, instance=NDO_OLD, env=ENV, database=f"mig_ndodb_{_suffix}"
)
ndo_src = f"urn:li:container:{_ndo_src_key.guid()}"
_ndo_dst_key = DatabaseKey(
    platform=PLATFORM, instance=NDO_NEW, env=ENV, database=f"mig_ndodb_{_suffix}"
)
ndo_dst = f"urn:li:container:{_ndo_dst_key.guid()}"

# schemaField pair — the motivating cascade case (col_a → Col_A on the same dataset).
_ndp_ds = make_dataset_urn_with_platform_instance(
    PLATFORM, f"my_db.my_schema.ndp_sf_{_suffix}", None, ENV
)
ndp_sf_src = make_schema_field_urn(_ndp_ds, "col_a")
ndp_sf_dst = make_schema_field_urn(_ndp_ds, "Col_A")


def _schema(field_name: str, platform: str = PLATFORM) -> SchemaMetadataClass:
    return SchemaMetadataClass(
        schemaName="s",
        platform=make_data_platform_urn(platform),
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
        MetadataChangeProposalWrapper(
            entityUrn=p2i_src, aspect=_schema("id", P2I_PLATFORM)
        ),
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
                P2I_PLATFORM,
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
            make_dataplatform_instance_urn(P2I_PLATFORM, P2I_INSTANCE)
        )
    finally:
        delete_urns(graph_client, all_urns)
        wait_for_writes_to_sync()


@pytest.mark.p0
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


def _audit(actor: str) -> ChangeAuditStampsClass:
    stamp = AuditStampClass(time=0, actor=actor)
    return ChangeAuditStampsClass(created=stamp, lastModified=stamp)


def _seed_urns_mapping_scenario(graph_client: DataHubGraph) -> None:
    mcps = [
        # Structured-property definition (GMS rejects value assignments otherwise).
        MetadataChangeProposalWrapper(
            entityUrn=um_prop,
            aspect=StructuredPropertyDefinitionClass(
                qualifiedName=f"um_tier_{_suffix}",
                valueType="urn:li:dataType:datahub.string",
                entityTypes=["urn:li:entityType:datahub.dataset"],
                displayName="UM Tier",
            ),
        ),
        # Upstream (not migrated).
        MetadataChangeProposalWrapper(entityUrn=um_up, aspect=_schema("id")),
        # Dataset with the full spread of user aspects + table/column lineage.
        MetadataChangeProposalWrapper(entityUrn=um_ds_src, aspect=_schema("amt")),
        MetadataChangeProposalWrapper(entityUrn=um_ds_src, aspect=_instance(UM_OLD)),
        MetadataChangeProposalWrapper(
            entityUrn=um_ds_src,
            aspect=DatasetPropertiesClass(description="events source"),
        ),
        MetadataChangeProposalWrapper(
            entityUrn=um_ds_src,
            aspect=OwnershipClass(
                owners=[
                    OwnerClass(
                        owner="urn:li:corpuser:um_alice",
                        type=OwnershipTypeClass.DATAOWNER,
                    )
                ]
            ),
        ),
        MetadataChangeProposalWrapper(
            entityUrn=um_ds_src,
            aspect=GlobalTagsClass(tags=[TagAssociationClass(tag="urn:li:tag:um_pii")]),
        ),
        MetadataChangeProposalWrapper(
            entityUrn=um_ds_src,
            aspect=GlossaryTermsClass(
                terms=[
                    GlossaryTermAssociationClass(urn="urn:li:glossaryTerm:um_Revenue")
                ],
                auditStamp=AuditStampClass(time=0, actor="urn:li:corpuser:um_alice"),
            ),
        ),
        MetadataChangeProposalWrapper(
            entityUrn=um_ds_src,
            aspect=StructuredPropertiesClass(
                properties=[
                    StructuredPropertyValueAssignmentClass(
                        propertyUrn=um_prop,
                        values=["gold"],
                    )
                ]
            ),
        ),
        MetadataChangeProposalWrapper(
            entityUrn=um_ds_src,
            aspect=UpstreamLineageClass(
                upstreams=[
                    UpstreamClass(
                        dataset=um_up, type=DatasetLineageTypeClass.TRANSFORMED
                    )
                ],
                fineGrainedLineages=[
                    FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                        upstreams=[make_schema_field_urn(um_up, "id")],
                        downstreams=[make_schema_field_urn(um_ds_src, "amt")],
                    )
                ],
            ),
        ),
        # Dashboard that consumes the dataset (relationship on datasets[]).
        MetadataChangeProposalWrapper(
            entityUrn=um_dash_src,
            aspect=DashboardInfoClass(
                title="Sales",
                description="Sales dashboard",
                lastModified=_audit("urn:li:corpuser:um_bob"),
                datasets=[um_ds_src],
            ),
        ),
        MetadataChangeProposalWrapper(
            entityUrn=um_dash_src,
            aspect=OwnershipClass(
                owners=[
                    OwnerClass(
                        owner="urn:li:corpuser:um_bob",
                        type=OwnershipTypeClass.DATAOWNER,
                    )
                ]
            ),
        ),
        # Two assertions targeting the dataset (Asserts relationship).
        MetadataChangeProposalWrapper(
            entityUrn=um_assert1,
            aspect=AssertionInfoClass(
                type=AssertionTypeClass.DATASET,
                datasetAssertion=DatasetAssertionInfoClass(
                    dataset=um_ds_src,
                    scope=DatasetAssertionScopeClass.DATASET_ROWS,
                    operator="_NATIVE_",  # type: ignore[arg-type]
                ),
            ),
        ),
        MetadataChangeProposalWrapper(
            entityUrn=um_assert2,
            aspect=AssertionInfoClass(
                type=AssertionTypeClass.DATASET,
                datasetAssertion=DatasetAssertionInfoClass(
                    dataset=um_ds_src,
                    scope=DatasetAssertionScopeClass.DATASET_COLUMN,
                    operator="_NATIVE_",  # type: ignore[arg-type]
                ),
            ),
        ),
    ]
    for mcp in mcps:
        graph_client.emit_mcp(mcp)
    wait_for_writes_to_sync()


def _wait_for_incoming_asserts(
    graph: DataHubGraph, dataset_urn: str, assertion_urns: list[str]
) -> None:
    """Poll until assertion URNs are incoming Asserts edges on the dataset."""
    expected = set(assertion_urns)
    sleep_sec, sleep_times = get_sleep_info()
    found: set[str] = set()
    for attempt in range(sleep_times):
        found = {
            rel.urn
            for rel in get_incoming_relationships(dataset_urn, graph=graph)
            if rel.relationship_type == "Asserts"
        }
        if expected <= found:
            return
        if attempt < sleep_times - 1:
            time.sleep(sleep_sec)
    raise AssertionError(
        f"Incoming Asserts on {dataset_urn} did not include {sorted(expected)}; "
        f"found {sorted(found)}"
    )


def test_urns_mapping_full_scenario(graph_client: DataHubGraph, tmp_path) -> None:
    """A multi-entity urns-mapping migration (dataset + dashboard) carries every
    user aspect, rewrites the dataset's own column-level lineage, and repoints the
    dashboard's dataset reference plus two assertions to the new dataset URN."""
    all_urns = [
        um_prop,
        um_up,
        um_ds_src,
        um_ds_tgt,
        um_dash_src,
        um_dash_tgt,
        um_assert1,
        um_assert2,
    ]
    delete_urns(graph_client, all_urns)
    wait_for_writes_to_sync()
    _seed_urns_mapping_scenario(graph_client)
    _wait_for_incoming_asserts(graph_client, um_ds_src, [um_assert1, um_assert2])

    # Migrate the referenced entity (dataset) before the referrer (dashboard):
    # the dataset's incoming-reference pass repoints the dashboard in the primary
    # store, and the dashboard's own clone then reads that already-updated
    # reference. The reverse order would rely on the relationship index having
    # caught up with the just-cloned dashboard within the same run, which is not
    # guaranteed under eventual consistency.
    mapping_file = tmp_path / "mapping.json"
    mapping_file.write_text(
        json.dumps(
            [
                {"source": um_ds_src, "target": um_ds_tgt},
                {"source": um_dash_src, "target": um_dash_tgt},
            ]
        )
    )

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

        # Dataset: created with all user aspects carried over.
        assert graph_client.exists(um_ds_tgt)
        props = graph_client.get_aspect(um_ds_tgt, DatasetPropertiesClass)
        assert props is not None and props.description == "events source"
        assert graph_client.get_aspect(um_ds_tgt, OwnershipClass) is not None
        assert graph_client.get_aspect(um_ds_tgt, GlobalTagsClass) is not None
        assert graph_client.get_aspect(um_ds_tgt, GlossaryTermsClass) is not None
        assert graph_client.get_aspect(um_ds_tgt, StructuredPropertiesClass) is not None

        # Dataset lineage: cross-instance upstream preserved; column-level
        # downstream rewritten to the new dataset field.
        lineage = graph_client.get_aspect(um_ds_tgt, UpstreamLineageClass)
        assert lineage is not None
        assert [u.dataset for u in lineage.upstreams] == [um_up]
        assert lineage.fineGrainedLineages is not None
        assert lineage.fineGrainedLineages[0].downstreams == [
            make_schema_field_urn(um_ds_tgt, "amt")
        ]
        assert lineage.fineGrainedLineages[0].upstreams == [
            make_schema_field_urn(um_up, "id")
        ]

        # Dashboard: created with its own aspects, and its dataset reference points
        # at the migrated dataset.
        assert graph_client.exists(um_dash_tgt)
        dash_info = graph_client.get_aspect(um_dash_tgt, DashboardInfoClass)
        assert dash_info is not None
        assert dash_info.datasets == [um_ds_tgt]
        assert graph_client.get_aspect(um_dash_tgt, OwnershipClass) is not None

        # Both assertions now target the migrated dataset.
        for assertion_urn in (um_assert1, um_assert2):
            info = graph_client.get_aspect(assertion_urn, AssertionInfoClass)
            assert info is not None and info.datasetAssertion is not None
            assert info.datasetAssertion.dataset == um_ds_tgt
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
    _wait_for_incoming_asserts(graph_client, asrt_src, [assertion_urn])

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


def test_container_migration_regenerates_instance(graph_client: DataHubGraph) -> None:
    """instance2instance migrates containers and stamps the *new* instance on the
    migrated container. dataPlatformInstance is excluded from the clone, so the
    migration must regenerate it — without this the container would have no instance
    aspect and drop out of instance-scoped search."""
    all_urns = [ct_src, ct_dst]
    delete_urns(graph_client, all_urns)
    wait_for_writes_to_sync()
    for mcp in [
        MetadataChangeProposalWrapper(
            entityUrn=ct_src, aspect=SubTypesClass(typeNames=["Database"])
        ),
        MetadataChangeProposalWrapper(
            entityUrn=ct_src,
            aspect=ContainerPropertiesClass(name="mig_ct", customProperties=_ct_props),
        ),
        MetadataChangeProposalWrapper(entityUrn=ct_src, aspect=_instance(CT_OLD)),
    ]:
        graph_client.emit_mcp(mcp)
    wait_for_writes_to_sync()

    try:
        # No dataset entities on this instance; the run still migrates containers.
        result = run_datahub_cmd(
            [
                "migrate",
                "instance2instance",
                "--platform",
                PLATFORM,
                "--old-instance",
                CT_OLD,
                "--new-instance",
                CT_NEW,
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

        assert graph_client.exists(ct_dst)
        instance = graph_client.get_aspect(ct_dst, DataPlatformInstanceClass)
        assert instance is not None and instance.instance == (
            make_dataplatform_instance_urn(PLATFORM, CT_NEW)
        )
    finally:
        delete_urns(graph_client, all_urns)
        wait_for_writes_to_sync()


def _seed_container(
    graph_client: DataHubGraph,
    urn: str,
    props: dict,
    name: str,
    tag: str,
    owner: str,
    term: str,
    subtype: str,
    prop_urn: Optional[str] = None,
    prop_value: Optional[str] = None,
) -> None:
    mcps = [
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=ContainerPropertiesClass(name=name, customProperties=props),
        ),
        MetadataChangeProposalWrapper(
            entityUrn=urn, aspect=SubTypesClass(typeNames=[subtype])
        ),
        MetadataChangeProposalWrapper(
            entityUrn=urn, aspect=GlobalTagsClass(tags=[TagAssociationClass(tag=tag)])
        ),
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=OwnershipClass(
                owners=[OwnerClass(owner=owner, type=OwnershipTypeClass.DATAOWNER)]
            ),
        ),
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=GlossaryTermsClass(
                terms=[GlossaryTermAssociationClass(urn=term)],
                auditStamp=AuditStampClass(time=0, actor="urn:li:corpuser:datahub"),
            ),
        ),
    ]
    if prop_urn is not None:
        mcps.append(
            MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=StructuredPropertiesClass(
                    properties=[
                        StructuredPropertyValueAssignmentClass(
                            propertyUrn=prop_urn, values=[prop_value]
                        )
                    ]
                ),
            )
        )
    for mcp in mcps:
        graph_client.emit_mcp(mcp)


def test_urns_mapping_patch_unions_nondataset_target(
    graph_client: DataHubGraph, tmp_path
) -> None:
    """urns-mapping --on-conflict patch on a pre-seeded container (a no-builder type,
    so it runs _merge_generic_entity) unions ownership/tags/terms from both sides,
    reseats containerProperties, keeps a target-only structured property, and keeps
    the target on a conflicting scalar. This is the server-side union a MagicMock
    unit test cannot demonstrate — arrayPrimaryKeys is enforced by GMS."""
    all_urns = [ndp_prop, ndp_src, ndp_dst]
    delete_urns(graph_client, all_urns)
    wait_for_writes_to_sync()
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=ndp_prop,
            aspect=StructuredPropertyDefinitionClass(
                qualifiedName=f"ndp_tier_{_suffix}",
                valueType="urn:li:dataType:datahub.string",
                entityTypes=["urn:li:entityType:datahub.container"],
                displayName="NDP Tier",
            ),
        )
    )
    _seed_container(
        graph_client,
        ndp_src,
        _ndp_src_key.model_dump(by_alias=True, exclude_none=True),
        name="src_ct",
        tag="urn:li:tag:ndp_src",
        owner="urn:li:corpuser:ndp_alice",
        term="urn:li:glossaryTerm:ndp_SrcTerm",
        subtype="Database",
    )
    # Pre-seeded target with its OWN owner/tag/term, a target-only structured
    # property, a conflicting subtype, and its own name.
    _seed_container(
        graph_client,
        ndp_dst,
        _ndp_dst_key.model_dump(by_alias=True, exclude_none=True),
        name="tgt_ct",
        tag="urn:li:tag:ndp_tgt",
        owner="urn:li:corpuser:ndp_bob",
        term="urn:li:glossaryTerm:ndp_TgtTerm",
        subtype="Schema",
        prop_urn=ndp_prop,
        prop_value="silver",
    )
    wait_for_writes_to_sync()

    mapping_file = tmp_path / "ndp_mapping.json"
    mapping_file.write_text(json.dumps([{"source": ndp_src, "target": ndp_dst}]))
    try:
        result = run_datahub_cmd(
            [
                "migrate",
                "urns-mapping",
                "--mapping-file",
                str(mapping_file),
                "--on-conflict",
                "patch",
                "--force",
                "--keep",
            ]
        )
        assert result.exit_code == 0, result.output
        wait_for_writes_to_sync()

        tags = graph_client.get_aspect(ndp_dst, GlobalTagsClass)
        assert tags is not None
        assert {t.tag for t in tags.tags} == {
            "urn:li:tag:ndp_src",
            "urn:li:tag:ndp_tgt",
        }
        owners = graph_client.get_aspect(ndp_dst, OwnershipClass)
        assert owners is not None
        assert {o.owner for o in owners.owners} == {
            "urn:li:corpuser:ndp_alice",
            "urn:li:corpuser:ndp_bob",
        }
        terms = graph_client.get_aspect(ndp_dst, GlossaryTermsClass)
        assert terms is not None
        assert {t.urn for t in terms.terms} == {
            "urn:li:glossaryTerm:ndp_SrcTerm",
            "urn:li:glossaryTerm:ndp_TgtTerm",
        }
        # The target's own structured property survives the union (arrayPrimaryKeys).
        props = graph_client.get_aspect(ndp_dst, StructuredPropertiesClass)
        assert props is not None
        assert any(p.propertyUrn == ndp_prop for p in props.properties)
        # containerProperties is always reseated from the source.
        cprops = graph_client.get_aspect(ndp_dst, ContainerPropertiesClass)
        assert cprops is not None and cprops.name == "src_ct"
        # A conflicting scalar keeps the target under patch.
        subtypes = graph_client.get_aspect(ndp_dst, SubTypesClass)
        assert subtypes is not None and subtypes.typeNames == ["Schema"]
    finally:
        delete_urns(graph_client, all_urns)
        wait_for_writes_to_sync()


def test_urns_mapping_overwrite_replaces_nondataset_target(
    graph_client: DataHubGraph, tmp_path
) -> None:
    """--on-conflict overwrite on a non-dataset fully replaces the source-carried
    aspects on the target — no union."""
    all_urns = [ndo_src, ndo_dst]
    delete_urns(graph_client, all_urns)
    wait_for_writes_to_sync()
    _seed_container(
        graph_client,
        ndo_src,
        _ndo_src_key.model_dump(by_alias=True, exclude_none=True),
        name="src_ct",
        tag="urn:li:tag:ndo_src",
        owner="urn:li:corpuser:ndo_alice",
        term="urn:li:glossaryTerm:ndo_SrcTerm",
        subtype="Database",
    )
    _seed_container(
        graph_client,
        ndo_dst,
        _ndo_dst_key.model_dump(by_alias=True, exclude_none=True),
        name="tgt_ct",
        tag="urn:li:tag:ndo_tgt",
        owner="urn:li:corpuser:ndo_bob",
        term="urn:li:glossaryTerm:ndo_TgtTerm",
        subtype="Schema",
    )
    wait_for_writes_to_sync()

    mapping_file = tmp_path / "ndo_mapping.json"
    mapping_file.write_text(json.dumps([{"source": ndo_src, "target": ndo_dst}]))
    try:
        result = run_datahub_cmd(
            [
                "migrate",
                "urns-mapping",
                "--mapping-file",
                str(mapping_file),
                "--on-conflict",
                "overwrite",
                "--force",
                "--keep",
            ]
        )
        assert result.exit_code == 0, result.output
        wait_for_writes_to_sync()

        # Overwrite replaces the source-carried aspects: the target's own tag is gone.
        tags = graph_client.get_aspect(ndo_dst, GlobalTagsClass)
        assert tags is not None
        assert {t.tag for t in tags.tags} == {"urn:li:tag:ndo_src"}
    finally:
        delete_urns(graph_client, all_urns)
        wait_for_writes_to_sync()


def test_urns_mapping_patch_unions_schema_field_target(
    graph_client: DataHubGraph, tmp_path
) -> None:
    """The motivating cascade case: a schemaField pair under --on-conflict patch
    unions tags and terms on a pre-seeded target (col_a → Col_A re-key)."""
    all_urns = [ndp_sf_src, ndp_sf_dst]
    delete_urns(graph_client, all_urns)
    wait_for_writes_to_sync()
    for urn, tag, term in (
        (ndp_sf_src, "urn:li:tag:sf_src", "urn:li:glossaryTerm:sf_SrcTerm"),
        (ndp_sf_dst, "urn:li:tag:sf_tgt", "urn:li:glossaryTerm:sf_TgtTerm"),
    ):
        graph_client.emit_mcp(
            MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=GlobalTagsClass(tags=[TagAssociationClass(tag=tag)]),
            )
        )
        graph_client.emit_mcp(
            MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=GlossaryTermsClass(
                    terms=[GlossaryTermAssociationClass(urn=term)],
                    auditStamp=AuditStampClass(time=0, actor="urn:li:corpuser:datahub"),
                ),
            )
        )
    wait_for_writes_to_sync()

    mapping_file = tmp_path / "sf_mapping.json"
    mapping_file.write_text(json.dumps([{"source": ndp_sf_src, "target": ndp_sf_dst}]))
    try:
        result = run_datahub_cmd(
            [
                "migrate",
                "urns-mapping",
                "--mapping-file",
                str(mapping_file),
                "--on-conflict",
                "patch",
                "--force",
                "--keep",
            ]
        )
        assert result.exit_code == 0, result.output
        wait_for_writes_to_sync()

        tags = graph_client.get_aspect(ndp_sf_dst, GlobalTagsClass)
        assert tags is not None
        assert {t.tag for t in tags.tags} == {
            "urn:li:tag:sf_src",
            "urn:li:tag:sf_tgt",
        }
        terms = graph_client.get_aspect(ndp_sf_dst, GlossaryTermsClass)
        assert terms is not None
        assert {t.urn for t in terms.terms} == {
            "urn:li:glossaryTerm:sf_SrcTerm",
            "urn:li:glossaryTerm:sf_TgtTerm",
        }
    finally:
        delete_urns(graph_client, all_urns)
        wait_for_writes_to_sync()
