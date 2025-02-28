from typing import Any, Dict

from datahub.metadata.com.linkedin.pegasus2avro.common import GlobalTags
from datahub.metadata.schema_classes import (
    DomainsClass,
    GlobalTagsClass,
    GlossaryTermsClass,
    InstitutionalMemoryClass,
    OwnerClass,
    OwnershipClass,
    OwnershipSourceTypeClass,
    OwnershipTypeClass,
)
from datahub.utilities.mapping import OperationProcessor


def get_operation_defs() -> Dict[str, Any]:
    return {
        "user_owner": {
            "match": ".*",
            "operation": "add_owner",
            "config": {"owner_type": "user"},
        },
        "user_owner_2": {
            "match": ".*",
            "operation": "add_owner",
            "config": {"owner_type": "user"},
        },
        "multi_user": {
            "match": ".*",
            "operation": "add_owner",
            "config": {"owner_type": "user"},
        },
        "group.owner": {
            "match": ".*",
            "operation": "add_owner",
            "config": {"owner_type": "group"},
        },
        "pii": {
            "match": True,
            "operation": "add_tag",
            "config": {"tag": "has_pii_test"},
        },
        "int_property": {
            "match": 1,
            "operation": "add_tag",
            "config": {"tag": "int_property"},
        },
        "double_property": {
            "match": 2.5,
            "operation": "add_term",
            "config": {"term": "double_property"},
        },
        "governance.team_owner": {
            "match": "Finan.*",
            "operation": "add_term",
            "config": {"term": "Finance.test"},
        },
        "tag": {
            "match": ".*",
            "operation": "add_tag",
            "config": {"tag": "{{ $match }}"},
        },
    }


def test_operation_processor_not_matching():
    # no property matches to the rules
    raw_props = {
        "user_owner_test": "test_user@abc.com",
        "group.owner_test": "test.group@abc.co.in",
        "governance.team_owner": "Binance",
        "pii": False,
        "int_property": 3,
        "double_property": 25,
    }
    processor = OperationProcessor(get_operation_defs())
    aspect_map = processor.process(raw_props)
    assert "add_tag" not in aspect_map
    assert "add_term" not in aspect_map
    assert "add_owner" not in aspect_map


def test_operation_processor_matching():
    raw_props = {
        "user_owner": "test_user@abc.com",
        "multi_user": "sales_member1@abc.com, sales_member2@abc.com",
        "user_owner_2": "test_user_2",
        "group.owner": "test.group@abc.co.in",
        "governance.team_owner": "Finance",
        "pii": True,
        "int_property": 1,
        "double_property": 2.5,
        "tag": "Finance",
    }

    processor = OperationProcessor(
        operation_defs=get_operation_defs(),
        owner_source_type="SOURCE_CONTROL",
        strip_owner_email_id=True,
    )
    aspect_map = processor.process(raw_props)
    assert "add_tag" in aspect_map
    assert "add_term" in aspect_map
    assert "add_owner" in aspect_map
    tag_aspect: GlobalTags = aspect_map["add_tag"]
    tags_added = [
        tag_association_class.tag for tag_association_class in tag_aspect.tags
    ]
    term_aspect: GlossaryTermsClass = aspect_map["add_term"]
    terms_added = [
        term_association_class.urn for term_association_class in term_aspect.terms
    ]
    assert (
        len(tags_added) == 3
        and "urn:li:tag:has_pii_test" in tags_added
        and "urn:li:tag:int_property" in tags_added
        and "urn:li:tag:Finance" in tags_added
    )
    assert (
        len(terms_added) == 2
        and "urn:li:glossaryTerm:Finance.test" in terms_added
        and "urn:li:glossaryTerm:double_property" in terms_added
    )

    ownership_aspect: OwnershipClass = aspect_map["add_owner"]
    assert len(ownership_aspect.owners) == 5
    owner_set = {
        "urn:li:corpuser:test_user",
        "urn:li:corpuser:test_user_2",
        "urn:li:corpGroup:test.group",
        "urn:li:corpuser:sales_member1",
        "urn:li:corpuser:sales_member2",
    }
    for single_owner in ownership_aspect.owners:
        assert single_owner.owner in owner_set
        assert (
            single_owner.source
            and single_owner.source.type == OwnershipSourceTypeClass.SOURCE_CONTROL
        )


def test_operation_processor_no_email_strip_source_type_not_null():
    raw_props = {
        "user_owner": "test_user@abc.com",
    }
    processor = OperationProcessor(
        operation_defs=get_operation_defs(),
        owner_source_type="SERVICE",
        strip_owner_email_id=False,
    )
    aspect_map = processor.process(raw_props)
    assert "add_owner" in aspect_map

    ownership_aspect: OwnershipClass = aspect_map["add_owner"]
    assert len(ownership_aspect.owners) == 1
    new_owner: OwnerClass = ownership_aspect.owners[0]
    assert new_owner.owner == "urn:li:corpuser:test_user@abc.com"
    assert new_owner.source and new_owner.source.type == "SERVICE"


def test_operation_processor_advanced_matching_owners():
    raw_props = {
        "user_owner": "@test_user@abc.com",
    }
    processor = OperationProcessor(
        operation_defs={
            "user_owner": {
                "match": "^@(.*)",
                "operation": "add_owner",
                "config": {"owner_type": "group"},
            },
        },
        owner_source_type="SOURCE_CONTROL",
    )
    aspect_map = processor.process(raw_props)
    assert "add_owner" in aspect_map

    ownership_aspect: OwnershipClass = aspect_map["add_owner"]
    assert len(ownership_aspect.owners) == 1
    new_owner: OwnerClass = ownership_aspect.owners[0]
    assert new_owner.owner == "urn:li:corpGroup:test_user@abc.com"
    assert new_owner.source and new_owner.source.type == "SOURCE_CONTROL"


def test_operation_processor_ownership_category():
    raw_props = {
        "user_owner": "@test_user",
        "business_owner": "alice,urn:li:corpGroup:biz-data-team",
        "architect": "bob",
        "producer": "urn:li:corpGroup:producer-group",
    }
    processor = OperationProcessor(
        operation_defs={
            "user_owner": {
                "match": "^@(.*)",
                "operation": "add_owner",
                "config": {
                    "owner_type": "group",
                    "owner_category": OwnershipTypeClass.DATA_STEWARD,
                },
            },
            "business_owner": {
                "match": ".*",
                "operation": "add_owner",
                "config": {
                    "owner_type": "user",
                    "owner_category": OwnershipTypeClass.BUSINESS_OWNER,
                },
            },
            "architect": {
                "match": ".*",
                "operation": "add_owner",
                "config": {
                    "owner_type": "user",
                    "owner_category": "urn:li:ownershipType:architect",
                },
            },
            "producer": {
                "match": ".*",
                "operation": "add_owner",
                "config": {
                    # Testing using full urns without any owner_type set.
                    "owner_category": OwnershipTypeClass.PRODUCER,
                },
            },
        },
        owner_source_type="SOURCE_CONTROL",
    )
    aspect_map = processor.process(raw_props)
    assert "add_owner" in aspect_map

    ownership_aspect: OwnershipClass = aspect_map["add_owner"]
    assert len(ownership_aspect.owners) == 5
    assert all(
        new_owner.source and new_owner.source.type == "SOURCE_CONTROL"
        for new_owner in ownership_aspect.owners
    )

    new_owner: OwnerClass = ownership_aspect.owners[0]
    assert new_owner.owner == "urn:li:corpGroup:biz-data-team"
    assert new_owner.type and new_owner.type == OwnershipTypeClass.BUSINESS_OWNER

    new_owner = ownership_aspect.owners[1]
    assert new_owner.owner == "urn:li:corpGroup:producer-group"
    assert new_owner.type and new_owner.type == OwnershipTypeClass.PRODUCER

    new_owner = ownership_aspect.owners[2]
    assert new_owner.owner == "urn:li:corpGroup:test_user"
    assert new_owner.type and new_owner.type == OwnershipTypeClass.DATA_STEWARD

    new_owner = ownership_aspect.owners[3]
    assert new_owner.owner == "urn:li:corpuser:alice"
    assert new_owner.type and new_owner.type == OwnershipTypeClass.BUSINESS_OWNER

    new_owner = ownership_aspect.owners[4]
    assert new_owner.owner == "urn:li:corpuser:bob"
    assert new_owner.type == OwnershipTypeClass.CUSTOM
    assert new_owner.typeUrn == "urn:li:ownershipType:architect"


def test_operation_processor_advanced_matching_tags():
    raw_props = {
        "case": "PLT-4567",
    }
    processor = OperationProcessor(
        operation_defs={
            "case": {
                "match": "^PLT-(.*)",
                "operation": "add_tag",
                "config": {"tag": "case_{{ $match }}"},
            },
        },
        owner_source_type="SOURCE_CONTROL",
    )
    aspect_map = processor.process(raw_props)
    assert "add_tag" in aspect_map

    tag_aspect: GlobalTagsClass = aspect_map["add_tag"]
    assert len(tag_aspect.tags) == 1
    assert tag_aspect.tags[0].tag == "urn:li:tag:case_4567"


def test_operation_processor_institutional_memory():
    raw_props = {
        "documentation_link": "https://test.com/documentation#ignore-this",
    }
    processor = OperationProcessor(
        operation_defs={
            "documentation_link": {
                "match": r"(?:https?)?\:\/\/\w*[^#]*",
                "operation": "add_doc_link",
                "config": {"link": "{{ $match }}", "description": "test"},
            },
        },
    )
    aspect_map = processor.process(raw_props)
    assert "add_doc_link" in aspect_map

    doc_link_aspect: InstitutionalMemoryClass = aspect_map["add_doc_link"]

    assert doc_link_aspect.elements[0].url == "https://test.com/documentation"
    assert doc_link_aspect.elements[0].description == "test"


def test_operation_processor_institutional_memory_no_description():
    raw_props = {
        "documentation_link": "test.com/documentation#ignore-this",
    }
    processor = OperationProcessor(
        operation_defs={
            "documentation_link": {
                "match": r"(?:https?)?\:\/\/\w*[^#]*",
                "operation": "add_doc_link",
                "config": {"link": "{{ $match }}"},
            },
        },
    )
    # we require a description, so this should stay empty
    aspect_map = processor.process(raw_props)
    assert aspect_map == {}


def test_operation_processor_matching_nested_props():
    raw_props = {
        "gdpr": {
            "pii": True,
        },
    }
    processor = OperationProcessor(
        operation_defs={
            "gdpr.pii": {
                "match": True,
                "operation": "add_tag",
                "config": {"tag": "pii"},
            },
        },
        owner_source_type="SOURCE_CONTROL",
        match_nested_props=True,
    )
    aspect_map = processor.process(raw_props)
    assert "add_tag" in aspect_map

    tag_aspect: GlobalTagsClass = aspect_map["add_tag"]
    assert len(tag_aspect.tags) == 1
    assert tag_aspect.tags[0].tag == "urn:li:tag:pii"


def test_operation_processor_matching_dot_props():
    raw_props = {
        "gdpr.pii": True,
    }
    processor = OperationProcessor(
        operation_defs={
            "gdpr.pii": {
                "match": True,
                "operation": "add_tag",
                "config": {"tag": "pii"},
            },
        },
        owner_source_type="SOURCE_CONTROL",
        match_nested_props=True,
    )
    aspect_map = processor.process(raw_props)
    assert "add_tag" in aspect_map

    tag_aspect: GlobalTagsClass = aspect_map["add_tag"]
    assert len(tag_aspect.tags) == 1
    assert tag_aspect.tags[0].tag == "urn:li:tag:pii"


def test_operation_processor_datahub_props():
    raw_props = {
        "datahub": {
            "tags": ["tag1", "tag2"],
            "terms": ["term1", "term2"],
            "owners": [
                "owner1",
                "urn:li:corpGroup:group1",
                {
                    "owner": "owner2",
                    "owner_type": "urn:li:ownershipType:steward",
                },
                {
                    "owner": "urn:li:corpGroup:group2",
                    "owner_type": "urn:li:ownershipType:steward",
                },
            ],
            "domain": "domain1",
        }
    }

    processor = OperationProcessor(
        operation_defs={},
        owner_source_type="SOURCE_CONTROL",
    )
    aspect_map = processor.process(raw_props)

    assert isinstance(aspect_map["add_owner"], OwnershipClass)
    assert [
        (owner.owner, owner.type, owner.typeUrn)
        for owner in aspect_map["add_owner"].owners
    ] == [
        ("urn:li:corpGroup:group1", "DATAOWNER", None),
        ("urn:li:corpGroup:group2", "CUSTOM", "urn:li:ownershipType:steward"),
        ("urn:li:corpuser:owner1", "DATAOWNER", None),
        ("urn:li:corpuser:owner2", "CUSTOM", "urn:li:ownershipType:steward"),
    ]

    assert isinstance(aspect_map["add_tag"], GlobalTagsClass)
    assert [tag_association.tag for tag_association in aspect_map["add_tag"].tags] == [
        "urn:li:tag:tag1",
        "urn:li:tag:tag2",
    ]

    assert isinstance(aspect_map["add_term"], GlossaryTermsClass)
    assert [
        term_association.urn for term_association in aspect_map["add_term"].terms
    ] == ["urn:li:glossaryTerm:term1", "urn:li:glossaryTerm:term2"]

    assert isinstance(aspect_map["add_domain"], DomainsClass)
    assert aspect_map["add_domain"].domains == ["urn:li:domain:domain1"]
