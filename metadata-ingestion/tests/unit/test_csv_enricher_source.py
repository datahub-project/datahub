from typing import Dict, List, Union
from unittest import mock

from datahub.emitter import mce_builder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.csv_enricher import CSVEnricherConfig, CSVEnricherSource
from datahub.metadata.schema_classes import (
    GlossaryTermAssociationClass,
    OwnerClass,
    OwnershipSourceClass,
    OwnershipTypeClass,
    TagAssociationClass,
)

DATASET_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:bigquery,test_dataset.test.Test,PROD)"
)
DATASET_ENTITY_TYPE = "dataset"


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
    maybe_terms_wu = source.get_resource_glossary_terms_work_unit(
        DATASET_URN, DATASET_ENTITY_TYPE, []
    )
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
        DATASET_URN, DATASET_ENTITY_TYPE, term_associations
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
        DATASET_URN, DATASET_ENTITY_TYPE, term_associations
    )
    assert maybe_terms_wu


def test_get_resource_tags_work_unit_no_tags():
    source = create_mocked_csv_enricher_source()
    maybe_tags_wu = source.get_resource_tags_work_unit(
        DATASET_URN, DATASET_ENTITY_TYPE, []
    )
    assert not maybe_tags_wu


def test_get_resource_tags_no_new_tags():
    source = create_mocked_csv_enricher_source()
    new_tags = ["urn:li:tag:oldtag1", "urn:li:tag:oldtag2"]
    tag_associations: List[TagAssociationClass] = [
        TagAssociationClass(tag) for tag in new_tags
    ]
    maybe_tags_wu = source.get_resource_tags_work_unit(
        DATASET_URN, DATASET_ENTITY_TYPE, tag_associations
    )
    assert not maybe_tags_wu


def test_get_resource_tags_work_unit_produced():
    source = create_mocked_csv_enricher_source()
    new_tags = ["urn:li:tag:newtag1", "urn:li:tag:newtag2"]
    tag_associations: List[TagAssociationClass] = [
        TagAssociationClass(tag) for tag in new_tags
    ]
    maybe_tags_wu = source.get_resource_tags_work_unit(
        DATASET_URN, DATASET_ENTITY_TYPE, tag_associations
    )
    assert maybe_tags_wu


def test_get_resource_owners_work_unit_no_terms():
    source = create_mocked_csv_enricher_source()
    maybe_owners_wu = source.get_resource_owners_work_unit(
        DATASET_URN, DATASET_ENTITY_TYPE, []
    )
    assert not maybe_owners_wu


def test_get_resource_owners_no_new_owners():
    source = create_mocked_csv_enricher_source()
    new_owners = ["urn:li:corpuser:owner1", "urn:li:corpuser:owner2"]
    owners: List[OwnerClass] = [
        OwnerClass(owner, type=OwnershipTypeClass.NONE) for owner in new_owners
    ]
    maybe_owners_wu = source.get_resource_owners_work_unit(
        DATASET_URN, DATASET_ENTITY_TYPE, owners
    )
    assert maybe_owners_wu


def test_get_resource_owners_work_unit_produced():
    source = create_mocked_csv_enricher_source()
    new_owners = ["urn:li:corpuser:owner1", "urn:li:corpuser:owner2"]
    owners: List[OwnerClass] = [
        OwnerClass(owner, type=OwnershipTypeClass.NONE) for owner in new_owners
    ]
    maybe_owners_wu = source.get_resource_owners_work_unit(
        DATASET_URN, DATASET_ENTITY_TYPE, owners
    )
    assert maybe_owners_wu


def test_get_resource_description_no_description():
    source = create_mocked_csv_enricher_source()
    new_description = None
    maybe_description_wu = source.get_resource_description_work_unit(
        DATASET_URN, DATASET_ENTITY_TYPE, new_description
    )
    assert not maybe_description_wu


def test_get_resource_description_work_unit_produced():
    source = create_mocked_csv_enricher_source()
    new_description = "description"
    maybe_description_wu = source.get_resource_description_work_unit(
        DATASET_URN, DATASET_ENTITY_TYPE, new_description
    )
    assert maybe_description_wu


def test_get_resource_domain_no_domain():
    source = create_mocked_csv_enricher_source()
    new_domain = None
    maybe_domain_wu = source.get_resource_domain_work_unit(
        DATASET_URN, DATASET_ENTITY_TYPE, new_domain
    )
    assert not maybe_domain_wu


def test_get_resource_domain_work_unit_produced():
    source = create_mocked_csv_enricher_source()
    new_domain = "domain"
    maybe_domain_wu = source.get_resource_domain_work_unit(
        DATASET_URN, DATASET_ENTITY_TYPE, new_domain
    )
    assert maybe_domain_wu
