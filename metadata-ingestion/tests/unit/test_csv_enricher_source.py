from typing import Dict, List, Union
from unittest import mock

from datahub.emitter import mce_builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.csv_enricher import CSVEnricherConfig, CSVEnricherSource
from datahub.metadata.schema_classes import (
    GlossaryTermAssociationClass,
    OwnerClass,
    OwnershipClass,
    OwnershipSourceClass,
    OwnershipTypeClass,
    StructuredPropertiesClass,
    StructuredPropertyValueAssignmentClass,
    TagAssociationClass,
)

DATASET_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:bigquery,test_dataset.test.Test,PROD)"
)


def create_owners_list_from_urn_list(
    owner_urns: List[str], source_type: str
) -> List[OwnerClass]:
    ownership_source_type: Union[None, OwnershipSourceClass] = None
    if source_type:
        ownership_source_type = OwnershipSourceClass(type=source_type)
    owners_list = [
        OwnerClass(
            owner=owner_urn,
            type=OwnershipTypeClass.DATAOWNER,
            source=ownership_source_type,
        )
        for owner_urn in owner_urns
    ]
    return owners_list


def create_mocked_csv_enricher_source() -> CSVEnricherSource:
    ctx = PipelineContext("test-run-id")
    graph = mock.MagicMock()
    graph.get_ownership.return_value = mce_builder.make_ownership_aspect_from_urn_list(
        ["urn:li:corpuser:olduser1"], "AUDIT"
    )
    graph.get_glossary_terms.return_value = (
        mce_builder.make_glossary_terms_aspect_from_urn_list(
            ["urn:li:glossaryTerm:oldterm1", "urn:li:glossaryTerm:oldterm2"]
        )
    )
    graph.get_tags.return_value = mce_builder.make_global_tag_aspect_with_tag_list(
        ["oldtag1", "oldtag2"]
    )
    graph.get_aspect.return_value = None
    graph.get_aspect_v2.return_value = None
    graph.get_domain.return_value = None
    ctx.graph = graph
    return CSVEnricherSource(
        CSVEnricherConfig(**create_base_csv_enricher_config()), ctx
    )


def create_base_csv_enricher_config() -> Dict:
    return dict(
        {
            "filename": "../integration/csv_enricher/csv_enricher_test_data.csv",
            "write_semantics": "PATCH",
            "delimiter": ",",
            "array_delimiter": "|",
        },
    )


def test_get_resource_glossary_terms_work_unit_no_terms():
    source = create_mocked_csv_enricher_source()
    maybe_terms_wu = source.get_resource_glossary_terms_work_unit(DATASET_URN, [])
    assert not maybe_terms_wu


def test_get_resource_glossary_terms_no_new_glossary_terms():
    source = create_mocked_csv_enricher_source()
    new_glossary_terms = [
        "urn:li:glossaryTerm:oldterm1",
        "urn:li:glossaryTerm:oldterm2",
    ]
    term_associations: List[GlossaryTermAssociationClass] = [
        GlossaryTermAssociationClass(term) for term in new_glossary_terms
    ]
    maybe_terms_wu = source.get_resource_glossary_terms_work_unit(
        DATASET_URN, term_associations
    )
    assert not maybe_terms_wu


def test_get_resource_glossary_terms_work_unit_produced():
    source = create_mocked_csv_enricher_source()
    new_glossary_terms = [
        "urn:li:glossaryTerm:newterm1",
        "urn:li:glossaryTerm:newterm2",
    ]
    term_associations: List[GlossaryTermAssociationClass] = [
        GlossaryTermAssociationClass(term) for term in new_glossary_terms
    ]
    maybe_terms_wu = source.get_resource_glossary_terms_work_unit(
        DATASET_URN, term_associations
    )
    assert maybe_terms_wu


def test_get_resource_tags_work_unit_no_tags():
    source = create_mocked_csv_enricher_source()
    maybe_tags_wu = source.get_resource_tags_work_unit(DATASET_URN, [])
    assert not maybe_tags_wu


def test_get_resource_tags_no_new_tags():
    source = create_mocked_csv_enricher_source()
    new_tags = ["urn:li:tag:oldtag1", "urn:li:tag:oldtag2"]
    tag_associations: List[TagAssociationClass] = [
        TagAssociationClass(tag) for tag in new_tags
    ]
    maybe_tags_wu = source.get_resource_tags_work_unit(DATASET_URN, tag_associations)
    assert not maybe_tags_wu


def test_get_resource_tags_work_unit_produced():
    source = create_mocked_csv_enricher_source()
    new_tags = ["urn:li:tag:newtag1", "urn:li:tag:newtag2"]
    tag_associations: List[TagAssociationClass] = [
        TagAssociationClass(tag) for tag in new_tags
    ]
    maybe_tags_wu = source.get_resource_tags_work_unit(DATASET_URN, tag_associations)
    assert maybe_tags_wu


def test_get_resource_owners_work_unit_no_terms():
    source = create_mocked_csv_enricher_source()
    maybe_owners_wu = source.get_resource_owners_work_unit(DATASET_URN, [])
    assert not maybe_owners_wu


def test_get_resource_owners_no_new_owners():
    source = create_mocked_csv_enricher_source()
    new_owners = ["urn:li:corpuser:owner1", "urn:li:corpuser:owner2"]
    owners: List[OwnerClass] = [
        OwnerClass(owner, type=OwnershipTypeClass.NONE) for owner in new_owners
    ]
    maybe_owners_wu = source.get_resource_owners_work_unit(DATASET_URN, owners)
    assert maybe_owners_wu


def test_maybe_extract_owners_ownership_type_urn():
    source = create_mocked_csv_enricher_source()
    row = {
        "resource": "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
        "owners": "urn:li:corpuser:datahub",
        "ownership_type": "TECHNICAL_OWNER",
    }
    assert source.maybe_extract_owners(row=row, is_resource_row=True) == [
        OwnerClass(
            owner="urn:li:corpuser:datahub", type=OwnershipTypeClass.TECHNICAL_OWNER
        )
    ]

    row2 = {
        "resource": "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
        "owners": "urn:li:corpuser:datahub",
        "ownership_type": "TECHNICAL_OWNER",
        "ownership_type_urn": "urn:li:ownershipType:technical_owner",
    }
    assert source.maybe_extract_owners(row=row2, is_resource_row=True) == [
        OwnerClass(
            owner="urn:li:corpuser:datahub",
            type=OwnershipTypeClass.CUSTOM,
            typeUrn="urn:li:ownershipType:technical_owner",
        )
    ]


def test_get_resource_owners_work_unit_produced():
    source = create_mocked_csv_enricher_source()
    new_owners = ["urn:li:corpuser:owner1", "urn:li:corpuser:owner2"]
    owners: List[OwnerClass] = [
        OwnerClass(owner, type=OwnershipTypeClass.NONE) for owner in new_owners
    ]
    maybe_owners_wu = source.get_resource_owners_work_unit(DATASET_URN, owners)
    assert maybe_owners_wu


def test_get_resource_description_no_description():
    source = create_mocked_csv_enricher_source()
    new_description = None
    maybe_description_wu = source.get_resource_description_work_unit(
        DATASET_URN, new_description
    )
    assert not maybe_description_wu


def test_get_resource_description_work_unit_produced():
    source = create_mocked_csv_enricher_source()
    new_description = "description"
    maybe_description_wu = source.get_resource_description_work_unit(
        DATASET_URN, new_description
    )
    assert maybe_description_wu


def test_get_resource_domain_no_domain():
    source = create_mocked_csv_enricher_source()
    new_domain = None
    maybe_domain_wu = source.get_resource_domain_work_unit(DATASET_URN, new_domain)
    assert not maybe_domain_wu


def test_get_resource_domain_work_unit_produced():
    source = create_mocked_csv_enricher_source()
    new_domain = "domain"
    maybe_domain_wu = source.get_resource_domain_work_unit(DATASET_URN, new_domain)
    assert maybe_domain_wu


def test_maybe_extract_structured_properties():
    source = create_mocked_csv_enricher_source()
    row = {
        "resource": DATASET_URN,
        "subresource": "",
        "sp__io.acryl.test.classification": "Sensitive",
        "sp.ownerTeam": "Finance",
        "sp__": "ignored",
    }

    structured_properties = source.maybe_extract_structured_properties(
        row=row,
        is_resource_row=True,
    )
    assert structured_properties == [
        StructuredPropertyValueAssignmentClass(
            propertyUrn="urn:li:structuredProperty:io.acryl.test.classification",
            values=["Sensitive"],
        ),
        StructuredPropertyValueAssignmentClass(
            propertyUrn="urn:li:structuredProperty:ownerTeam",
            values=["Finance"],
        ),
    ]


def test_get_resource_structured_properties_work_unit_produced():
    source = create_mocked_csv_enricher_source()
    structured_properties = [
        StructuredPropertyValueAssignmentClass(
            propertyUrn="urn:li:structuredProperty:io.acryl.test.classification",
            values=["Sensitive"],
        )
    ]

    maybe_wu = source.get_resource_structured_properties_work_unit(
        DATASET_URN,
        structured_properties,
    )
    assert maybe_wu


def test_get_resource_structured_properties_no_new_values():
    source = create_mocked_csv_enricher_source()
    source.ctx.graph.get_aspect.return_value = StructuredPropertiesClass(
        properties=[
            StructuredPropertyValueAssignmentClass(
                propertyUrn="urn:li:structuredProperty:io.acryl.test.classification",
                values=["Sensitive"],
            )
        ]
    )

    structured_properties = [
        StructuredPropertyValueAssignmentClass(
            propertyUrn="urn:li:structuredProperty:io.acryl.test.classification",
            values=["Sensitive"],
        )
    ]
    maybe_wu = source.get_resource_structured_properties_work_unit(
        DATASET_URN,
        structured_properties,
    )
    assert not maybe_wu


def test_get_workunits_internal_emits_structured_properties(tmp_path):
    csv_content = """resource,subresource,glossary_terms,tags,owners,ownership_type,description,domain,sp__io.acryl.test.classification,sp.ownerTeam
\"urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)\",,,,,,,,Sensitive,Finance
"""
    csv_file = tmp_path / "structured_properties.csv"
    csv_file.write_text(csv_content)

    source = CSVEnricherSource(
        CSVEnricherConfig(
            filename=str(csv_file),
            write_semantics="OVERRIDE",
            delimiter=",",
            array_delimiter="|",
        ),
        PipelineContext("test-run-id"),
    )

    workunits = list(source.get_workunits_internal())
    structured_properties_mcps = [
        wu.metadata
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, StructuredPropertiesClass)
    ]

    assert len(structured_properties_mcps) == 1
    aspect = structured_properties_mcps[0].aspect
    assert isinstance(aspect, StructuredPropertiesClass)
    assert {(prop.propertyUrn, tuple(prop.values)) for prop in aspect.properties} == {
        ("urn:li:structuredProperty:io.acryl.test.classification", ("Sensitive",)),
        ("urn:li:structuredProperty:ownerTeam", ("Finance",)),
    }


def test_get_workunits_internal_merges_resource_owners(tmp_path):
    csv_content = """resource,subresource,glossary_terms,tags,owners,ownership_type,description,domain
\"urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)\",,,,[urn:li:corpuser:datahub],TECHNICAL_OWNER,,
\"urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)\",,,,[urn:li:corpuser:jdoe],BUSINESS_OWNER,,
\"urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)\",,,,[urn:li:corpuser:datahub],TECHNICAL_OWNER,,
"""
    csv_file = tmp_path / "owners.csv"
    csv_file.write_text(csv_content)

    source = CSVEnricherSource(
        CSVEnricherConfig(
            filename=str(csv_file),
            write_semantics="OVERRIDE",
            delimiter=",",
            array_delimiter="|",
        ),
        PipelineContext("test-run-id"),
    )

    workunits = list(source.get_workunits_internal())
    ownership_mcps = [
        wu.metadata
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, OwnershipClass)
    ]

    assert len(ownership_mcps) == 1
    aspect = ownership_mcps[0].aspect
    assert isinstance(aspect, OwnershipClass)
    owners = aspect.owners
    assert len(owners) == 2
    assert {(owner.owner, owner.type) for owner in owners} == {
        ("urn:li:corpuser:datahub", OwnershipTypeClass.TECHNICAL_OWNER),
        ("urn:li:corpuser:jdoe", OwnershipTypeClass.BUSINESS_OWNER),
    }
