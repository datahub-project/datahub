import pathlib

import pytest

from datahub.metadata.urns import CorpUserUrn, GlossaryNodeUrn, GlossaryTermUrn
from datahub.sdk.glossary_node import GlossaryNode
from datahub.sdk.glossary_term import GlossaryTerm
from datahub.testing.sdk_v2_helpers import assert_entity_golden

GOLDEN_DIR = pathlib.Path(__file__).parent / "glossary_term_golden"


def test_glossary_term_basic() -> None:
    term = GlossaryTerm(id="a1b2c3d4")

    assert GlossaryTerm.get_urn_type() == GlossaryTermUrn
    assert isinstance(term.urn, GlossaryTermUrn)
    assert str(term.urn) == "urn:li:glossaryTerm:a1b2c3d4"
    assert str(term.urn) in repr(term)

    assert term.id == "a1b2c3d4"
    assert term.display_name is None
    assert term.definition == ""
    assert term.term_source == "INTERNAL"
    assert term.parent_node is None
    assert term.source_ref is None
    assert term.source_url is None
    assert term.custom_properties == {}
    assert term.owners is None
    assert term.links is None
    assert term.domain is None
    assert term.is_a == []
    assert term.has_a == []
    assert term.values == []
    assert term.related_terms == []

    with pytest.raises(AttributeError):
        assert term.extra_attribute  # type: ignore
    with pytest.raises(AttributeError):
        term.extra_attribute = "slots should reject extra fields"  # type: ignore

    assert_entity_golden(term, GOLDEN_DIR / "test_glossary_term_basic_golden.json")


def test_glossary_term_with_node() -> None:
    node = GlossaryNode(id="7f3d2c1a", definition="Financial terms.")

    # parent as GlossaryNode object
    term1 = GlossaryTerm(
        id="a1b2c3d4", definition="Annual recurring revenue.", parent_node=node
    )
    assert term1.parent_node == GlossaryNodeUrn("7f3d2c1a")

    # parent as GlossaryNodeUrn
    term2 = GlossaryTerm(
        id="a1b2c3d4",
        definition="Annual recurring revenue.",
        parent_node=GlossaryNodeUrn("7f3d2c1a"),
    )
    assert term2.parent_node == GlossaryNodeUrn("7f3d2c1a")

    # parent as URN string
    term3 = GlossaryTerm(
        id="a1b2c3d4",
        definition="Annual recurring revenue.",
        parent_node="urn:li:glossaryNode:7f3d2c1a",
    )
    assert term3.parent_node == GlossaryNodeUrn("7f3d2c1a")

    assert term1.parent_node == term2.parent_node == term3.parent_node


def test_glossary_term_external() -> None:
    term = GlossaryTerm(
        id="7f8a9b0c",
        definition="A financial instrument as defined by FIBO.",
        term_source="EXTERNAL",
        source_ref="FIBO",
        source_url="https://spec.edmcouncil.org/fibo/",
    )

    assert term.term_source == "EXTERNAL"
    assert term.source_ref == "FIBO"
    assert term.source_url == "https://spec.edmcouncil.org/fibo/"

    assert_entity_golden(term, GOLDEN_DIR / "test_glossary_term_external_golden.json")


def test_glossary_term_related_terms() -> None:
    term = GlossaryTerm(
        id="b2c3d4e5",
        definition="Monthly Recurring Revenue.",
        is_a=[GlossaryTermUrn("e5f6a7b8")],  # RecurringRevenue
        has_a=[GlossaryTermUrn("f6a7b8c9")],  # SubscriptionCount
        values=[
            GlossaryTermUrn("a7b8c9d0"),  # MRR.New
            GlossaryTermUrn("b8c9d0e1"),  # MRR.Expansion
        ],
        related_terms=[
            GlossaryTermUrn("a1b2c3d4"),  # ARR
            GlossaryTermUrn("c9d0e1f2"),  # Churn
        ],
    )

    assert term.is_a == [GlossaryTermUrn("e5f6a7b8")]
    assert term.has_a == [GlossaryTermUrn("f6a7b8c9")]
    assert term.values == [GlossaryTermUrn("a7b8c9d0"), GlossaryTermUrn("b8c9d0e1")]
    assert term.related_terms == [
        GlossaryTermUrn("a1b2c3d4"),
        GlossaryTermUrn("c9d0e1f2"),
    ]

    # add is_a is idempotent
    term.add_is_a(GlossaryTermUrn("e5f6a7b8"))
    assert len(term.is_a) == 1

    # add new is_a
    term.add_is_a(GlossaryTermUrn("e6f7a8b9"))  # Revenue
    assert len(term.is_a) == 2

    # remove is_a
    term.remove_is_a(GlossaryTermUrn("e6f7a8b9"))
    assert term.is_a == [GlossaryTermUrn("e5f6a7b8")]

    # add / remove value
    term.add_value(GlossaryTermUrn("a2b3c4d5"))  # MRR.Contraction
    assert len(term.values) == 3
    term.remove_value(GlossaryTermUrn("a2b3c4d5"))
    assert len(term.values) == 2

    assert_entity_golden(
        term, GOLDEN_DIR / "test_glossary_term_related_terms_golden.json"
    )


def test_glossary_term_complex() -> None:
    term = GlossaryTerm(
        id="3a4b5c6d",
        display_name="Personally Identifiable Information",
        definition="Information that can identify, contact, or locate a person.",
        parent_node=GlossaryNodeUrn("2f3a4b5c"),
        custom_properties={"sensitivity_level": "HIGH", "regulatory_framework": "GDPR"},
        owners=[CorpUserUrn("datahub")],
        links=[
            (
                "https://wiki.company.com/privacy/pii-guidelines",
                "Internal PII Guidelines",
            ),
            ("https://gdpr.eu/", "GDPR Official Documentation"),
        ],
        domain="urn:li:domain:privacy",
        is_a=[GlossaryTermUrn("c3d4e5f6")],  # SensitiveData
        related_terms=[
            GlossaryTermUrn("d4e5f6a7"),  # SSN
            GlossaryTermUrn("1a2b3c4d"),  # Email
        ],
    )

    assert term.id == "3a4b5c6d"
    assert term.display_name == "Personally Identifiable Information"
    assert (
        term.definition == "Information that can identify, contact, or locate a person."
    )
    assert term.parent_node == GlossaryNodeUrn("2f3a4b5c")
    assert term.custom_properties == {
        "sensitivity_level": "HIGH",
        "regulatory_framework": "GDPR",
    }
    assert term.owners is not None
    assert len(term.owners) == 1
    assert term.links is not None
    assert len(term.links) == 2
    assert term.is_a == [GlossaryTermUrn("c3d4e5f6")]
    assert term.related_terms == [
        GlossaryTermUrn("d4e5f6a7"),
        GlossaryTermUrn("1a2b3c4d"),
    ]

    assert_entity_golden(term, GOLDEN_DIR / "test_glossary_term_complex_golden.json")


def test_glossary_term_setters() -> None:
    term = GlossaryTerm(id="a1b2c3d4")

    term.set_display_name("Annual Recurring Revenue")
    assert term.display_name == "Annual Recurring Revenue"

    term.set_definition("Total annualized subscription revenue.")
    assert term.definition == "Total annualized subscription revenue."

    term.set_term_source("EXTERNAL")
    assert term.term_source == "EXTERNAL"

    term.set_source_ref("SomeRef")
    assert term.source_ref == "SomeRef"

    term.set_source_url("https://example.com/arr")
    assert term.source_url == "https://example.com/arr"

    term.set_parent_node(GlossaryNodeUrn("7f3d2c1a"))
    assert term.parent_node == GlossaryNodeUrn("7f3d2c1a")

    term.set_parent_node(GlossaryNode(id="2c3d4e5f"))
    assert term.parent_node == GlossaryNodeUrn("2c3d4e5f")

    term.set_custom_properties({"key": "value"})
    assert term.custom_properties == {"key": "value"}


def test_glossary_term_has_a_and_related() -> None:
    term = GlossaryTerm(id="5e6f7a8b")

    # add_has_a / remove_has_a
    term.add_has_a(GlossaryTermUrn("d0e1f2a3"))  # ZipCode
    term.add_has_a(GlossaryTermUrn("e1f2a3b4"))  # Street
    term.add_has_a(GlossaryTermUrn("d0e1f2a3"))  # idempotent
    assert term.has_a == [GlossaryTermUrn("d0e1f2a3"), GlossaryTermUrn("e1f2a3b4")]

    term.remove_has_a(GlossaryTermUrn("e1f2a3b4"))
    assert term.has_a == [GlossaryTermUrn("d0e1f2a3")]

    # add_related_term / remove_related_term
    term.add_related_term(GlossaryTermUrn("1a2b3c4d"))  # Email
    term.add_related_term(GlossaryTermUrn("f2a3b4c5"))  # Phone
    term.add_related_term(GlossaryTermUrn("1a2b3c4d"))  # idempotent
    assert term.related_terms == [
        GlossaryTermUrn("1a2b3c4d"),
        GlossaryTermUrn("f2a3b4c5"),
    ]

    term.remove_related_term(GlossaryTermUrn("f2a3b4c5"))
    assert term.related_terms == [GlossaryTermUrn("1a2b3c4d")]

    # remove on non-existent term is safe
    term.remove_has_a(GlossaryTermUrn("d6e7f8a9"))
    assert term.has_a == [GlossaryTermUrn("d0e1f2a3")]

    # add_is_a and add_value from scratch (no prior related terms) hit None-init branches
    fresh = GlossaryTerm(id="a3b4c5d6")
    fresh.add_is_a(GlossaryTermUrn("b4c5d6e7"))  # Base
    assert fresh.is_a == [GlossaryTermUrn("b4c5d6e7")]

    fresh.add_value(GlossaryTermUrn("c5d6e7f8"))  # Option1
    assert fresh.values == [GlossaryTermUrn("c5d6e7f8")]


def test_glossary_term_new_from_graph() -> None:
    import datahub.metadata.schema_classes as models

    urn = GlossaryTermUrn("3a4b5c6d")
    aspects: models.AspectBag = {
        "glossaryTermInfo": models.GlossaryTermInfoClass(
            definition="Data that identifies a person.",
            termSource="INTERNAL",
            name="PII",
            parentNode="urn:li:glossaryNode:2f3a4b5c",
        ),
        "glossaryRelatedTerms": models.GlossaryRelatedTermsClass(
            isRelatedTerms=["urn:li:glossaryTerm:c3d4e5f6"],  # SensitiveData
            hasRelatedTerms=None,
            values=None,
            relatedTerms=["urn:li:glossaryTerm:d4e5f6a7"],  # SSN
        ),
    }
    term = GlossaryTerm._new_from_graph(urn, aspects)

    assert term.id == "3a4b5c6d"
    assert term.definition == "Data that identifies a person."
    assert term.display_name == "PII"
    assert term.parent_node == GlossaryNodeUrn("2f3a4b5c")
    assert term.is_a == [GlossaryTermUrn("c3d4e5f6")]
    assert term.related_terms == [GlossaryTermUrn("d4e5f6a7")]
    assert term.has_a == []
    assert term.values == []


def test_glossary_term_structured_properties() -> None:
    term = GlossaryTerm(
        id="a1b2c3d4",
        structured_properties={
            "urn:li:structuredProperty:sp1": ["value1"],
        },
    )
    assert term.structured_properties is not None
    assert len(term.structured_properties) == 1

    term.set_structured_property("urn:li:structuredProperty:sp2", ["value2"])
    assert term.structured_properties is not None
    assert len(term.structured_properties) == 2
