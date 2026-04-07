import time
import uuid
from typing import Dict, Optional

import pytest

from datahub.emitter.mce_builder import make_dataset_urn, make_tag_urn, make_term_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    DocumentationAssociationClass,
    DocumentationClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    GlobalTagsClass,
    GlossaryTermAssociationClass,
    MetadataAttributionClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    TagAssociationClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.specific.dataset import DatasetPatchBuilder
from tests.patch.common_patch_tests import (
    helper_test_add_fine_grained_lineage,
    helper_test_custom_properties_patch,
    helper_test_dataset_tags_patch,
    helper_test_entity_terms_patch,
    helper_test_ownership_patch,
    helper_test_set_fine_grained_lineages,
)


@pytest.fixture(params=["graph_client", "openapi_graph_client"])
def patch_dataset(request):
    """Yields (graph_client, dataset_urn). The dataset is hard-deleted after the test."""
    graph_client = request.getfixturevalue(request.param)
    dataset_urn = make_dataset_urn(
        platform="hive", name=f"SampleHiveDataset-{uuid.uuid4()}", env="PROD"
    )
    yield graph_client, dataset_urn
    graph_client.hard_delete_entity(dataset_urn)


# Common Aspect Patch Tests
# Ownership
def test_dataset_ownership_patch(patch_dataset):
    graph_client, dataset_urn = patch_dataset
    helper_test_ownership_patch(graph_client, dataset_urn, DatasetPatchBuilder)


# Tags
def test_dataset_tags_patch(patch_dataset):
    graph_client, dataset_urn = patch_dataset
    helper_test_dataset_tags_patch(graph_client, dataset_urn, DatasetPatchBuilder)


# Terms
def test_dataset_terms_patch(patch_dataset):
    graph_client, dataset_urn = patch_dataset
    helper_test_entity_terms_patch(graph_client, dataset_urn, DatasetPatchBuilder)


def test_dataset_upstream_lineage_patch(patch_dataset):
    graph_client, dataset_urn = patch_dataset

    other_dataset_urn = make_dataset_urn(
        platform="hive", name=f"SampleHiveDataset2-{uuid.uuid4()}", env="PROD"
    )

    patch_dataset_urn = make_dataset_urn(
        platform="hive", name=f"SampleHiveDataset3-{uuid.uuid4()}", env="PROD"
    )

    upstream_lineage = UpstreamLineageClass(
        upstreams=[
            UpstreamClass(dataset=other_dataset_urn, type=DatasetLineageTypeClass.VIEW)
        ]
    )
    upstream_lineage_to_add = UpstreamClass(
        dataset=patch_dataset_urn, type=DatasetLineageTypeClass.VIEW
    )
    mcpw = MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=upstream_lineage)

    graph_client.emit_mcp(mcpw)
    upstream_lineage_read = graph_client.get_aspect_v2(
        entity_urn=dataset_urn,
        aspect_type=UpstreamLineageClass,
        aspect="upstreamLineage",
    )

    assert upstream_lineage_read is not None
    assert len(upstream_lineage_read.upstreams) > 0
    assert upstream_lineage_read.upstreams[0].dataset == other_dataset_urn

    for patch_mcp in (
        DatasetPatchBuilder(dataset_urn)
        .add_upstream_lineage(upstream_lineage_to_add)
        .build()
    ):
        graph_client.emit_mcp(patch_mcp)
        pass

    upstream_lineage_read = graph_client.get_aspect_v2(
        entity_urn=dataset_urn,
        aspect_type=UpstreamLineageClass,
        aspect="upstreamLineage",
    )

    assert upstream_lineage_read is not None
    assert len(upstream_lineage_read.upstreams) == 2
    assert upstream_lineage_read.upstreams[0].dataset == other_dataset_urn
    assert upstream_lineage_read.upstreams[1].dataset == patch_dataset_urn

    for patch_mcp in (
        DatasetPatchBuilder(dataset_urn)
        .remove_upstream_lineage(upstream_lineage_to_add.dataset)
        .build()
    ):
        graph_client.emit_mcp(patch_mcp)
        pass

    upstream_lineage_read = graph_client.get_aspect_v2(
        entity_urn=dataset_urn,
        aspect_type=UpstreamLineageClass,
        aspect="upstreamLineage",
    )

    assert upstream_lineage_read is not None
    assert len(upstream_lineage_read.upstreams) == 1
    assert upstream_lineage_read.upstreams[0].dataset == other_dataset_urn


def get_field_info(
    graph: DataHubGraph, dataset_urn: str, field_path: str
) -> Optional[EditableSchemaFieldInfoClass]:
    schema_metadata = graph.get_aspect(
        entity_urn=dataset_urn,
        aspect_type=EditableSchemaMetadataClass,
    )
    assert schema_metadata
    field_info = [
        f for f in schema_metadata.editableSchemaFieldInfo if f.fieldPath == field_path
    ]
    if len(field_info):
        return field_info[0]
    else:
        return None


def test_field_terms_patch(patch_dataset):
    graph_client, dataset_urn = patch_dataset

    field_path = "foo.bar"

    editable_field = EditableSchemaMetadataClass(
        [
            EditableSchemaFieldInfoClass(
                fieldPath=field_path, description="This is a test field"
            )
        ]
    )
    mcpw = MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=editable_field)

    graph_client.emit_mcp(mcpw)
    field_info = get_field_info(graph_client, dataset_urn, field_path)
    assert field_info
    assert field_info.description == "This is a test field"

    new_term = GlossaryTermAssociationClass(urn=make_term_urn(f"test-{uuid.uuid4()}"))
    for patch_mcp in (
        DatasetPatchBuilder(dataset_urn)
        .for_field(field_path)
        .add_term(new_term)
        .parent()
        .build()
    ):
        graph_client.emit_mcp(patch_mcp)
        pass

    field_info = get_field_info(graph_client, dataset_urn, field_path)

    assert field_info
    assert field_info.description == "This is a test field"
    assert field_info.glossaryTerms is not None
    assert len(field_info.glossaryTerms.terms) == 1
    assert field_info.glossaryTerms.terms[0].urn == new_term.urn

    for patch_mcp in (
        DatasetPatchBuilder(dataset_urn)
        .for_field(field_path)
        .remove_term(new_term.urn)
        .parent()
        .build()
    ):
        graph_client.emit_mcp(patch_mcp)
        pass

    field_info = get_field_info(graph_client, dataset_urn, field_path)

    assert field_info
    assert field_info.description == "This is a test field"
    assert field_info.glossaryTerms is not None
    assert len(field_info.glossaryTerms.terms) == 0


def test_field_tags_patch(patch_dataset):
    graph_client, dataset_urn = patch_dataset

    field_path = "foo.bar"

    editable_field = EditableSchemaMetadataClass(
        [
            EditableSchemaFieldInfoClass(
                fieldPath=field_path, description="This is a test field"
            )
        ]
    )
    mcpw = MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=editable_field)

    graph_client.emit_mcp(mcpw)
    field_info = get_field_info(graph_client, dataset_urn, field_path)
    assert field_info
    assert field_info.description == "This is a test field"

    new_tag_urn = make_tag_urn(tag=f"testTag-{uuid.uuid4()}")

    new_tag = TagAssociationClass(tag=new_tag_urn, context="test")

    for patch_mcp in (
        DatasetPatchBuilder(dataset_urn)
        .for_field(field_path)
        .add_tag(new_tag)
        .parent()
        .build()
    ):
        graph_client.emit_mcp(patch_mcp)
        pass

    field_info = get_field_info(graph_client, dataset_urn, field_path)

    assert field_info
    assert field_info.description == "This is a test field"
    assert field_info.globalTags is not None
    assert len(field_info.globalTags.tags) == 1
    assert field_info.globalTags.tags[0].tag == new_tag.tag

    # Add the same tag again and verify that it doesn't get added
    for patch_mcp in (
        DatasetPatchBuilder(dataset_urn)
        .for_field(field_path)
        .add_tag(new_tag)
        .parent()
        .build()
    ):
        graph_client.emit_mcp(patch_mcp)
        pass

    field_info = get_field_info(graph_client, dataset_urn, field_path)

    assert field_info
    assert field_info.description == "This is a test field"
    assert field_info.globalTags is not None
    assert len(field_info.globalTags.tags) == 1
    assert field_info.globalTags.tags[0].tag == new_tag.tag

    for patch_mcp in (
        DatasetPatchBuilder(dataset_urn)
        .for_field(field_path)
        .remove_tag(new_tag.tag)
        .parent()
        .build()
    ):
        graph_client.emit_mcp(patch_mcp)
        pass

    field_info = get_field_info(graph_client, dataset_urn, field_path)

    assert field_info
    assert field_info.description == "This is a test field"
    assert field_info.globalTags is not None
    assert len(field_info.globalTags.tags) == 0


def get_custom_properties(
    graph: DataHubGraph, dataset_urn: str
) -> Optional[Dict[str, str]]:
    dataset_properties = graph.get_aspect(
        entity_urn=dataset_urn,
        aspect_type=DatasetPropertiesClass,
    )
    assert dataset_properties
    return dataset_properties.customProperties


def test_custom_properties_patch(patch_dataset):
    graph_client, dataset_urn = patch_dataset
    orig_dataset_properties = DatasetPropertiesClass(
        name="test_name", description="test_description"
    )
    helper_test_custom_properties_patch(
        graph_client,
        test_entity_urn=dataset_urn,
        patch_builder_class=DatasetPatchBuilder,
        custom_properties_aspect_class=DatasetPropertiesClass,
        base_aspect=orig_dataset_properties,
    )

    # Patch custom properties along with name
    for patch_mcp in (
        DatasetPatchBuilder(dataset_urn)
        .set_description("This is a new description")
        .add_custom_property("test_description_property", "test_description_value")
        .build()
    ):
        graph_client.emit_mcp(patch_mcp)

    dataset_properties: Optional[DatasetPropertiesClass] = graph_client.get_aspect(
        dataset_urn, DatasetPropertiesClass
    )

    assert dataset_properties
    assert dataset_properties.name == orig_dataset_properties.name
    assert dataset_properties.description == "This is a new description"

    custom_properties = get_custom_properties(graph_client, dataset_urn)

    assert custom_properties is not None

    assert custom_properties["test_description_property"] == "test_description_value"


def test_dataset_add_fine_grained_lineage(patch_dataset):
    """Test that add_fine_grained_lineage works correctly for datasets."""
    graph_client, dataset_urn = patch_dataset
    helper_test_add_fine_grained_lineage(
        graph_client, dataset_urn, UpstreamLineageClass, DatasetPatchBuilder
    )


def test_dataset_set_fine_grained_lineages(patch_dataset):
    """Test setting fine-grained lineages with end-to-end DataHub integration."""
    graph_client, dataset_urn = patch_dataset
    helper_test_set_fine_grained_lineages(
        graph_client, dataset_urn, UpstreamLineageClass, DatasetPatchBuilder
    )


def test_attribution_based_tag_patch(patch_dataset):
    """Test attribution-aware tag patching via arrayPrimaryKeys compound keys.

    Two sources can independently hold the same tag URN as separate entries keyed
    by (attribution.source, tag).  Removing with attribution_source removes only
    that source's entry; the other source's entry is untouched.
    """
    graph_client, dataset_urn = patch_dataset

    source_a = "urn:li:dataHubAction:testSourceA"
    source_b = "urn:li:dataHubAction:testSourceB"
    # Both sources apply the same tag URN independently.
    shared_tag_urn = make_tag_urn(f"sharedTag-{uuid.uuid4()}")

    now_ms = int(time.time() * 1000)
    tag_from_a = TagAssociationClass(
        tag=shared_tag_urn,
        attribution=MetadataAttributionClass(
            time=now_ms,
            actor="urn:li:corpuser:datahub",
            source=source_a,
        ),
    )
    tag_from_b = TagAssociationClass(
        tag=shared_tag_urn,
        attribution=MetadataAttributionClass(
            time=now_ms,
            actor="urn:li:corpuser:datahub",
            source=source_b,
        ),
    )

    # Add the same tag URN from two different sources — both entries are stored.
    for mcp in (
        DatasetPatchBuilder(dataset_urn).add_tag(tag_from_a).add_tag(tag_from_b).build()
    ):
        graph_client.emit_mcp(mcp)

    tags_read = graph_client.get_aspect(
        entity_urn=dataset_urn, aspect_type=GlobalTagsClass
    )
    assert tags_read is not None
    stored = [
        (t.tag, t.attribution.source if t.attribution else None) for t in tags_read.tags
    ]
    assert (shared_tag_urn, source_a) in stored
    assert (shared_tag_urn, source_b) in stored

    # Remove only source_a's entry — source_b's entry must survive.
    for mcp in (
        DatasetPatchBuilder(dataset_urn)
        .remove_tag(shared_tag_urn, attribution_source=source_a)
        .build()
    ):
        graph_client.emit_mcp(mcp)

    tags_read = graph_client.get_aspect(
        entity_urn=dataset_urn, aspect_type=GlobalTagsClass
    )
    assert tags_read is not None
    stored = [
        (t.tag, t.attribution.source if t.attribution else None) for t in tags_read.tags
    ]
    assert (shared_tag_urn, source_a) not in stored, "source_a entry should be removed"
    assert (shared_tag_urn, source_b) in stored, "source_b entry must be untouched"


def _make_attributed_tag(tag_urn: str, source: str) -> TagAssociationClass:
    return TagAssociationClass(
        tag=tag_urn,
        attribution=MetadataAttributionClass(
            time=int(time.time() * 1000),
            actor="urn:li:corpuser:datahub",
            source=source,
        ),
    )


def test_attributed_duplicates_survive_unrelated_tag_patch(patch_dataset):
    """Adding/removing an unrelated tag must not disturb pre-existing attributed duplicates."""
    graph_client, dataset_urn = patch_dataset
    tag_x = make_tag_urn(f"tagX-{uuid.uuid4()}")
    tag_y = make_tag_urn(f"tagY-{uuid.uuid4()}")
    src_a = "urn:li:dataHubAction:srcA"
    src_b = "urn:li:dataHubAction:srcB"

    # Seed tag_x from two sources.
    for mcp in (
        DatasetPatchBuilder(dataset_urn)
        .add_tag(_make_attributed_tag(tag_x, src_a))
        .add_tag(_make_attributed_tag(tag_x, src_b))
        .build()
    ):
        graph_client.emit_mcp(mcp)

    # Add then remove an unrelated tag_y.
    for mcp in (
        DatasetPatchBuilder(dataset_urn).add_tag(TagAssociationClass(tag=tag_y)).build()
    ):
        graph_client.emit_mcp(mcp)
    for mcp in DatasetPatchBuilder(dataset_urn).remove_tag(tag_y).build():
        graph_client.emit_mcp(mcp)

    aspect = graph_client.get_aspect(
        entity_urn=dataset_urn, aspect_type=GlobalTagsClass
    )
    assert aspect is not None
    stored = [
        (t.tag, t.attribution.source if t.attribution else None) for t in aspect.tags
    ]
    assert (tag_x, src_a) in stored, "src_a entry for tag_x must survive"
    assert (tag_x, src_b) in stored, "src_b entry for tag_x must survive"
    assert not any(t == tag_y for t, _ in stored), "tag_y must be gone"


def test_add_attributed_tag_is_idempotent(patch_dataset):
    """Patch-adding the same (source, tag) pair twice must not create duplicate entries."""
    graph_client, dataset_urn = patch_dataset
    tag_urn = make_tag_urn(f"tag-{uuid.uuid4()}")
    source = "urn:li:dataHubAction:src"

    attributed = _make_attributed_tag(tag_urn, source)
    for _ in range(2):
        for mcp in DatasetPatchBuilder(dataset_urn).add_tag(attributed).build():
            graph_client.emit_mcp(mcp)

    aspect = graph_client.get_aspect(
        entity_urn=dataset_urn, aspect_type=GlobalTagsClass
    )
    assert aspect is not None
    matches = [t for t in aspect.tags if t.tag == tag_urn]
    assert len(matches) == 1, "duplicate add must not create two entries"


def test_unattributed_and_attributed_tag_coexist(patch_dataset):
    """A plain add and an attributed add for the same tag URN produce two distinct entries.

    Also verifies that upserting the attributed entry (changing its context) updates
    only that entry while leaving the unattributed entry completely untouched.
    """
    graph_client, dataset_urn = patch_dataset
    tag_urn = make_tag_urn(f"tag-{uuid.uuid4()}")
    source = "urn:li:dataHubAction:src"

    # Add attributed (context="v1") and plain entries for the same tag URN.
    attributed_v1 = TagAssociationClass(
        tag=tag_urn,
        context="v1",
        attribution=MetadataAttributionClass(
            time=int(time.time() * 1000),
            actor="urn:li:corpuser:datahub",
            source=source,
        ),
    )
    for mcp in DatasetPatchBuilder(dataset_urn).add_tag(attributed_v1).build():
        graph_client.emit_mcp(mcp)
    for mcp in (
        DatasetPatchBuilder(dataset_urn)
        .add_tag(TagAssociationClass(tag=tag_urn))
        .build()
    ):
        graph_client.emit_mcp(mcp)

    aspect = graph_client.get_aspect(
        entity_urn=dataset_urn, aspect_type=GlobalTagsClass
    )
    assert aspect is not None
    matches = [t for t in aspect.tags if t.tag == tag_urn]
    assert len(matches) == 2, "attributed and plain entries must both be stored"
    assert any(t.attribution and t.attribution.source == source for t in matches), (
        "attributed entry must be present"
    )
    assert any(t.attribution is None for t in matches), "plain entry must be present"

    # Upsert the attributed entry with context="v2" — same (source, tag) key.
    attributed_v2 = TagAssociationClass(
        tag=tag_urn,
        context="v2",
        attribution=MetadataAttributionClass(
            time=int(time.time() * 1000),
            actor="urn:li:corpuser:datahub",
            source=source,
        ),
    )
    for mcp in DatasetPatchBuilder(dataset_urn).add_tag(attributed_v2).build():
        graph_client.emit_mcp(mcp)

    aspect = graph_client.get_aspect(
        entity_urn=dataset_urn, aspect_type=GlobalTagsClass
    )
    assert aspect is not None
    matches = [t for t in aspect.tags if t.tag == tag_urn]
    assert len(matches) == 2, "upsert must not create a third entry"
    attributed = next(
        (t for t in matches if t.attribution and t.attribution.source == source), None
    )
    assert attributed is not None, "attributed entry must still be present"
    assert attributed.context == "v2", "upsert must have updated the context to v2"
    plain = next((t for t in matches if t.attribution is None), None)
    assert plain is not None, "plain entry must survive the upsert"


def test_remove_without_attribution_deletes_all_entries(patch_dataset):
    """remove_tag without attribution_source removes every entry for that URN
    (attributed from multiple sources and plain), while leaving other tag URNs untouched."""
    graph_client, dataset_urn = patch_dataset
    tag_urn = make_tag_urn(f"tag-{uuid.uuid4()}")
    other_tag = make_tag_urn(f"other-{uuid.uuid4()}")
    src_a = "urn:li:dataHubAction:srcA"
    src_b = "urn:li:dataHubAction:srcB"

    # Add tag_urn attributed from two sources, once plain, and an unrelated other_tag.
    for mcp in (
        DatasetPatchBuilder(dataset_urn)
        .add_tag(_make_attributed_tag(tag_urn, src_a))
        .add_tag(_make_attributed_tag(tag_urn, src_b))
        .add_tag(TagAssociationClass(tag=tag_urn))
        .add_tag(TagAssociationClass(tag=other_tag))
        .build()
    ):
        graph_client.emit_mcp(mcp)

    # Unconditional remove — must wipe all entries for tag_urn.
    for mcp in DatasetPatchBuilder(dataset_urn).remove_tag(tag_urn).build():
        graph_client.emit_mcp(mcp)

    aspect = graph_client.get_aspect(
        entity_urn=dataset_urn, aspect_type=GlobalTagsClass
    )
    assert aspect is not None
    stored = [
        (t.tag, t.attribution.source if t.attribution else None) for t in aspect.tags
    ]
    assert not any(t == tag_urn for t, _ in stored), (
        "all entries for tag_urn must be removed"
    )
    assert any(t == other_tag for t, _ in stored), "other_tag must be untouched"


def test_documentation_patch(patch_dataset):
    """Test add and remove of documentation entries via the DocumentationTemplate.

    Verifies:
    - Adding two docs from different sources produces two entries.
    - Removing one doc by source leaves only the other entry.
    """
    graph_client, dataset_urn = patch_dataset
    src_a = "urn:li:dataHubAction:docSrcA"
    src_b = "urn:li:dataHubAction:docSrcB"
    now = int(time.time() * 1000)

    def make_doc(text: str, source: str) -> DocumentationAssociationClass:
        return DocumentationAssociationClass(
            documentation=text,
            attribution=MetadataAttributionClass(
                time=now,
                actor="urn:li:corpuser:datahub",
                source=source,
            ),
        )

    # Add two documentation entries from different sources.
    for mcp in (
        DatasetPatchBuilder(dataset_urn)
        .add_documentation(make_doc("Doc from A", src_a))
        .add_documentation(make_doc("Doc from B", src_b))
        .build()
    ):
        graph_client.emit_mcp(mcp)

    aspect = graph_client.get_aspect(
        entity_urn=dataset_urn, aspect_type=DocumentationClass
    )
    assert aspect is not None
    stored = {
        d.attribution.source if d.attribution else None: d.documentation
        for d in aspect.documentations
    }
    assert stored[src_a] == "Doc from A", "src_a entry must be present"
    assert stored[src_b] == "Doc from B", "src_b entry must be present"
    assert len(stored) == 2

    # Remove only src_a's entry — src_b must survive.
    for mcp in (
        DatasetPatchBuilder(dataset_urn)
        .remove_documentation(attribution_source=src_a)
        .build()
    ):
        graph_client.emit_mcp(mcp)

    aspect = graph_client.get_aspect(
        entity_urn=dataset_urn, aspect_type=DocumentationClass
    )
    assert aspect is not None
    remaining = {
        d.attribution.source if d.attribution else None: d.documentation
        for d in aspect.documentations
    }
    assert src_a not in remaining, "src_a entry must be removed"
    assert remaining[src_b] == "Doc from B", "src_b entry must survive"
    assert len(remaining) == 1


def test_remove_all_documentation(patch_dataset):
    """remove_documentation() with no attribution removes all entries — attributed and unattributed.

    Verifies:
    - Adding an unattributed doc and two attributed docs produces three entries.
    - remove_documentation() with no argument removes every entry.
    """
    graph_client, dataset_urn = patch_dataset
    src_a = "urn:li:dataHubAction:removeSrcA"
    src_b = "urn:li:dataHubAction:removeSrcB"
    now = int(time.time() * 1000)

    def make_doc(text: str, source: str) -> DocumentationAssociationClass:
        return DocumentationAssociationClass(
            documentation=text,
            attribution=MetadataAttributionClass(
                time=now,
                actor="urn:li:corpuser:datahub",
                source=source,
            ),
        )

    # Seed: one unattributed doc and two attributed docs.
    for mcp in (
        DatasetPatchBuilder(dataset_urn)
        .add_documentation(
            DocumentationAssociationClass(documentation="Unattributed doc")
        )
        .add_documentation(make_doc("Doc from A", src_a))
        .add_documentation(make_doc("Doc from B", src_b))
        .build()
    ):
        graph_client.emit_mcp(mcp)

    aspect = graph_client.get_aspect(
        entity_urn=dataset_urn, aspect_type=DocumentationClass
    )
    assert aspect is not None
    assert len(aspect.documentations) == 3, (
        "all three entries must be present before remove"
    )

    # remove_documentation() with no argument must wipe every entry.
    for mcp in DatasetPatchBuilder(dataset_urn).remove_documentation().build():
        graph_client.emit_mcp(mcp)

    aspect = graph_client.get_aspect(
        entity_urn=dataset_urn, aspect_type=DocumentationClass
    )
    assert aspect is None or len(aspect.documentations) == 0, (
        "all documentation entries must be removed"
    )


def test_ownership_multi_type_urn_patch(patch_dataset):
    """Two owners with the same type enum but different typeUrns can coexist.

    Also tests removal:
    - by typeUrn (leaves the other typeUrn entry)
    - by type enum via wildcard (removes both entries for that type)
    """
    graph_client, dataset_urn = patch_dataset

    JDOE = "urn:li:corpuser:jdoe"
    TYPE_A = "urn:li:ownershipType:Technical"
    TYPE_B = "urn:li:ownershipType:Business"

    def make_owner(type_urn: str) -> OwnerClass:
        return OwnerClass(
            owner=JDOE,
            type=OwnershipTypeClass.DATAOWNER,
            typeUrn=type_urn,
        )

    # Add jdoe as DATAOWNER twice — once per typeUrn.  Both entries must coexist.
    for mcp in (
        DatasetPatchBuilder(dataset_urn)
        .add_owner(make_owner(TYPE_A))
        .add_owner(make_owner(TYPE_B))
        .build()
    ):
        graph_client.emit_mcp(mcp)

    aspect = graph_client.get_aspect(entity_urn=dataset_urn, aspect_type=OwnershipClass)
    assert aspect is not None
    type_urns = {o.typeUrn for o in aspect.owners}
    assert TYPE_A in type_urns, "TYPE_A entry must be stored"
    assert TYPE_B in type_urns, "TYPE_B entry must be stored"
    assert len(aspect.owners) == 2

    # Remove only the TYPE_A entry — TYPE_B must survive.
    for mcp in (
        DatasetPatchBuilder(dataset_urn).remove_owner(JDOE, type_urn=TYPE_A).build()
    ):
        graph_client.emit_mcp(mcp)

    aspect = graph_client.get_aspect(entity_urn=dataset_urn, aspect_type=OwnershipClass)
    assert aspect is not None
    type_urns = {o.typeUrn for o in aspect.owners}
    assert TYPE_A not in type_urns, "TYPE_A must be removed"
    assert TYPE_B in type_urns, "TYPE_B must survive"
    assert len(aspect.owners) == 1

    # Re-add TYPE_A, then remove by type enum (DATAOWNER/* wildcard).
    for mcp in DatasetPatchBuilder(dataset_urn).add_owner(make_owner(TYPE_A)).build():
        graph_client.emit_mcp(mcp)

    for mcp in (
        DatasetPatchBuilder(dataset_urn)
        .remove_owner(JDOE, owner_type=OwnershipTypeClass.DATAOWNER)
        .build()
    ):
        graph_client.emit_mcp(mcp)

    aspect = graph_client.get_aspect(entity_urn=dataset_urn, aspect_type=OwnershipClass)
    owners = aspect.owners if aspect else []
    assert not any(o.owner == JDOE for o in owners), (
        "all JDOE DATAOWNER entries must be removed"
    )


def test_field_tags_attribution_patch(patch_dataset):
    """Patch-add and remove field tags with and without attribution source.

    Verifies:
    - An unattributed tag and an attributed tag for the same URN coexist as separate entries.
    - remove_tag without attribution_source removes all entries for that tag URN.
    - remove_tag with attribution_source removes only that source's entry.
    """
    graph_client, dataset_urn = patch_dataset
    field_path = f"field_{uuid.uuid4().hex[:8]}"
    tag_urn = make_tag_urn(f"fieldTag-{uuid.uuid4()}")
    source = "urn:li:dataHubAction:fieldTagSource"
    now = int(time.time() * 1000)

    editable_field = EditableSchemaMetadataClass(
        [EditableSchemaFieldInfoClass(fieldPath=field_path, description="test")]
    )
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=editable_field)
    )

    # Add unattributed tag and attributed tag for the same URN.
    for mcp in (
        DatasetPatchBuilder(dataset_urn)
        .for_field(field_path)
        .add_tag(TagAssociationClass(tag=tag_urn))
        .add_tag(
            TagAssociationClass(
                tag=tag_urn,
                attribution=MetadataAttributionClass(
                    time=now, actor="urn:li:corpuser:datahub", source=source
                ),
            )
        )
        .parent()
        .build()
    ):
        graph_client.emit_mcp(mcp)

    field_info = get_field_info(graph_client, dataset_urn, field_path)
    assert field_info and field_info.globalTags is not None
    stored = [
        (t.tag, t.attribution.source if t.attribution else None)
        for t in field_info.globalTags.tags
    ]
    assert (tag_urn, None) in stored, "unattributed entry must be present"
    assert (tag_urn, source) in stored, "attributed entry must be present"
    assert len(field_info.globalTags.tags) == 2

    # Remove only the attributed entry — unattributed must survive.
    for mcp in (
        DatasetPatchBuilder(dataset_urn)
        .for_field(field_path)
        .remove_tag(tag_urn, attribution_source=source)
        .parent()
        .build()
    ):
        graph_client.emit_mcp(mcp)

    field_info = get_field_info(graph_client, dataset_urn, field_path)
    assert field_info and field_info.globalTags is not None
    remaining = [t.tag for t in field_info.globalTags.tags]
    assert remaining == [tag_urn], "unattributed entry must survive"
    assert field_info.globalTags.tags[0].attribution is None

    # Remove all remaining entries (no attribution filter).
    for mcp in (
        DatasetPatchBuilder(dataset_urn)
        .for_field(field_path)
        .remove_tag(tag_urn)
        .parent()
        .build()
    ):
        graph_client.emit_mcp(mcp)

    field_info = get_field_info(graph_client, dataset_urn, field_path)
    assert field_info
    assert not field_info.globalTags or len(field_info.globalTags.tags) == 0


def test_field_terms_attribution_patch(patch_dataset):
    """Patch-add and remove field glossary terms with and without attribution source.

    Verifies:
    - An unattributed term and an attributed term for the same URN coexist.
    - remove_term with attribution_source removes only that source's entry.
    - remove_term without attribution_source removes all entries for that term URN.
    """
    graph_client, dataset_urn = patch_dataset
    field_path = f"field_{uuid.uuid4().hex[:8]}"
    term_urn = make_term_urn(f"fieldTerm-{uuid.uuid4()}")
    source = "urn:li:dataHubAction:fieldTermSource"
    now = int(time.time() * 1000)

    editable_field = EditableSchemaMetadataClass(
        [EditableSchemaFieldInfoClass(fieldPath=field_path, description="test")]
    )
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=editable_field)
    )

    # Add unattributed term and attributed term for the same URN.
    for mcp in (
        DatasetPatchBuilder(dataset_urn)
        .for_field(field_path)
        .add_term(GlossaryTermAssociationClass(urn=term_urn))
        .add_term(
            GlossaryTermAssociationClass(
                urn=term_urn,
                attribution=MetadataAttributionClass(
                    time=now, actor="urn:li:corpuser:datahub", source=source
                ),
            )
        )
        .parent()
        .build()
    ):
        graph_client.emit_mcp(mcp)

    field_info = get_field_info(graph_client, dataset_urn, field_path)
    assert field_info and field_info.glossaryTerms is not None
    stored = [
        (t.urn, t.attribution.source if t.attribution else None)
        for t in field_info.glossaryTerms.terms
    ]
    assert (term_urn, None) in stored, "unattributed entry must be present"
    assert (term_urn, source) in stored, "attributed entry must be present"
    assert len(field_info.glossaryTerms.terms) == 2

    # Remove only the attributed entry — unattributed must survive.
    for mcp in (
        DatasetPatchBuilder(dataset_urn)
        .for_field(field_path)
        .remove_term(term_urn, attribution_source=source)
        .parent()
        .build()
    ):
        graph_client.emit_mcp(mcp)

    field_info = get_field_info(graph_client, dataset_urn, field_path)
    assert field_info and field_info.glossaryTerms is not None
    remaining = [t.urn for t in field_info.glossaryTerms.terms]
    assert remaining == [term_urn], "unattributed entry must survive"
    assert field_info.glossaryTerms.terms[0].attribution is None

    # Remove all remaining entries (no attribution filter).
    for mcp in (
        DatasetPatchBuilder(dataset_urn)
        .for_field(field_path)
        .remove_term(term_urn)
        .parent()
        .build()
    ):
        graph_client.emit_mcp(mcp)

    field_info = get_field_info(graph_client, dataset_urn, field_path)
    assert field_info
    assert not field_info.glossaryTerms or len(field_info.glossaryTerms.terms) == 0
