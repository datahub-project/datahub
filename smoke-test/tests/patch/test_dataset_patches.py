import uuid
from typing import Dict, Optional

from datahub.emitter.mce_builder import make_dataset_urn, make_tag_urn, make_term_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    GlossaryTermAssociationClass,
    TagAssociationClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.specific.dataset import DatasetPatchBuilder
from tests.patch.common_patch_tests import (
    helper_test_custom_properties_patch,
    helper_test_dataset_tags_patch,
    helper_test_entity_terms_patch,
    helper_test_ownership_patch,
)


# Common Aspect Patch Tests
# Ownership
def test_dataset_ownership_patch(graph_client):
    dataset_urn = make_dataset_urn(
        platform="hive", name=f"SampleHiveDataset{uuid.uuid4()}", env="PROD"
    )
    helper_test_ownership_patch(graph_client, dataset_urn, DatasetPatchBuilder)


# Tags
def test_dataset_tags_patch(graph_client):
    dataset_urn = make_dataset_urn(
        platform="hive", name=f"SampleHiveDataset-{uuid.uuid4()}", env="PROD"
    )
    helper_test_dataset_tags_patch(graph_client, dataset_urn, DatasetPatchBuilder)


# Terms
def test_dataset_terms_patch(graph_client):
    dataset_urn = make_dataset_urn(
        platform="hive", name=f"SampleHiveDataset-{uuid.uuid4()}", env="PROD"
    )
    helper_test_entity_terms_patch(graph_client, dataset_urn, DatasetPatchBuilder)


def test_dataset_upstream_lineage_patch(graph_client: DataHubGraph):
    dataset_urn = make_dataset_urn(
        platform="hive", name=f"SampleHiveDataset-{uuid.uuid4()}", env="PROD"
    )

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


def test_field_terms_patch(graph_client: DataHubGraph):
    dataset_urn = make_dataset_urn(
        platform="hive", name=f"SampleHiveDataset-{uuid.uuid4()}", env="PROD"
    )

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


def test_field_tags_patch(graph_client: DataHubGraph):
    dataset_urn = make_dataset_urn(
        platform="hive", name=f"SampleHiveDataset-{uuid.uuid4()}", env="PROD"
    )

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


def test_custom_properties_patch(graph_client: DataHubGraph):
    dataset_urn = make_dataset_urn(
        platform="hive", name=f"SampleHiveDataset-{uuid.uuid4()}", env="PROD"
    )
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
