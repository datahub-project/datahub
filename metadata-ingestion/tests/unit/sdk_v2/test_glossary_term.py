import pathlib

import pytest

from datahub.metadata.urns import CorpUserUrn, GlossaryNodeUrn, GlossaryTermUrn
from datahub.sdk.glossary_node import GlossaryNode
from datahub.sdk.glossary_term import GlossaryTerm
from datahub.testing.sdk_v2_helpers import assert_entity_golden

GOLDEN_DIR = pathlib.Path(__file__).parent / "glossary_term_golden"


def test_glossary_term_basic() -> None:
    term = GlossaryTerm(name="ARR")

    assert GlossaryTerm.get_urn_type() == GlossaryTermUrn
    assert isinstance(term.urn, GlossaryTermUrn)
    assert str(term.urn) == "urn:li:glossaryTerm:ARR"
    assert str(term.urn) in repr(term)

    assert term.name == "ARR"
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
    node = GlossaryNode(name="Finance", definition="Financial terms.")

    # parent as GlossaryNode object
    term1 = GlossaryTerm(
        name="ARR", definition="Annual recurring revenue.", parent_node=node
    )
    assert term1.parent_node == GlossaryNodeUrn("Finance")

    # parent as GlossaryNodeUrn
    term2 = GlossaryTerm(
        name="ARR",
        definition="Annual recurring revenue.",
        parent_node=GlossaryNodeUrn("Finance"),
    )
    assert term2.parent_node == GlossaryNodeUrn("Finance")

    # parent as URN string
    term3 = GlossaryTerm(
        name="ARR",
        definition="Annual recurring revenue.",
        parent_node="urn:li:glossaryNode:Finance",
    )
    assert term3.parent_node == GlossaryNodeUrn("Finance")

    assert term1.parent_node == term2.parent_node == term3.parent_node


def test_glossary_term_external() -> None:
    term = GlossaryTerm(
        name="FIBO.FinancialInstrument",
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
        name="MRR",
        definition="Monthly Recurring Revenue.",
        is_a=[GlossaryTermUrn("RecurringRevenue")],
        has_a=[GlossaryTermUrn("SubscriptionCount")],
        values=[GlossaryTermUrn("MRR.New"), GlossaryTermUrn("MRR.Expansion")],
        related_terms=[GlossaryTermUrn("ARR"), GlossaryTermUrn("Churn")],
    )

    assert term.is_a == [GlossaryTermUrn("RecurringRevenue")]
    assert term.has_a == [GlossaryTermUrn("SubscriptionCount")]
    assert term.values == [GlossaryTermUrn("MRR.New"), GlossaryTermUrn("MRR.Expansion")]
    assert term.related_terms == [GlossaryTermUrn("ARR"), GlossaryTermUrn("Churn")]

    # add is_a is idempotent
    term.add_is_a(GlossaryTermUrn("RecurringRevenue"))
    assert len(term.is_a) == 1

    # add new is_a
    term.add_is_a(GlossaryTermUrn("Revenue"))
    assert len(term.is_a) == 2

    # remove is_a
    term.remove_is_a(GlossaryTermUrn("Revenue"))
    assert term.is_a == [GlossaryTermUrn("RecurringRevenue")]

    # add / remove value
    term.add_value(GlossaryTermUrn("MRR.Contraction"))
    assert len(term.values) == 3
    term.remove_value(GlossaryTermUrn("MRR.Contraction"))
    assert len(term.values) == 2

    assert_entity_golden(
        term, GOLDEN_DIR / "test_glossary_term_related_terms_golden.json"
    )


def test_glossary_term_complex() -> None:
    term = GlossaryTerm(
        name="Classification.PII",
        display_name="Personally Identifiable Information",
        definition="Information that can identify, contact, or locate a person.",
        parent_node=GlossaryNodeUrn("Classification"),
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
        is_a=[GlossaryTermUrn("SensitiveData")],
        related_terms=[GlossaryTermUrn("SSN"), GlossaryTermUrn("Email")],
    )

    assert term.name == "Classification.PII"
    assert term.display_name == "Personally Identifiable Information"
    assert (
        term.definition == "Information that can identify, contact, or locate a person."
    )
    assert term.parent_node == GlossaryNodeUrn("Classification")
    assert term.custom_properties == {
        "sensitivity_level": "HIGH",
        "regulatory_framework": "GDPR",
    }
    assert term.owners is not None
    assert len(term.owners) == 1
    assert term.links is not None
    assert len(term.links) == 2
    assert term.is_a == [GlossaryTermUrn("SensitiveData")]
    assert term.related_terms == [GlossaryTermUrn("SSN"), GlossaryTermUrn("Email")]

    assert_entity_golden(term, GOLDEN_DIR / "test_glossary_term_complex_golden.json")


def test_glossary_term_setters() -> None:
    term = GlossaryTerm(name="ARR")

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

    term.set_parent_node(GlossaryNodeUrn("Finance"))
    assert term.parent_node == GlossaryNodeUrn("Finance")

    term.set_parent_node(GlossaryNode(name="TopLevel"))
    assert term.parent_node == GlossaryNodeUrn("TopLevel")

    term.set_custom_properties({"key": "value"})
    assert term.custom_properties == {"key": "value"}


def test_glossary_term_has_a_and_related() -> None:
    term = GlossaryTerm(name="Address")

    # add_has_a / remove_has_a
    term.add_has_a(GlossaryTermUrn("ZipCode"))
    term.add_has_a(GlossaryTermUrn("Street"))
    term.add_has_a(GlossaryTermUrn("ZipCode"))  # idempotent
    assert term.has_a == [GlossaryTermUrn("ZipCode"), GlossaryTermUrn("Street")]

    term.remove_has_a(GlossaryTermUrn("Street"))
    assert term.has_a == [GlossaryTermUrn("ZipCode")]

    # add_related_term / remove_related_term
    term.add_related_term(GlossaryTermUrn("Email"))
    term.add_related_term(GlossaryTermUrn("Phone"))
    term.add_related_term(GlossaryTermUrn("Email"))  # idempotent
    assert term.related_terms == [GlossaryTermUrn("Email"), GlossaryTermUrn("Phone")]

    term.remove_related_term(GlossaryTermUrn("Phone"))
    assert term.related_terms == [GlossaryTermUrn("Email")]

    # remove on non-existent term is safe
    term.remove_has_a(GlossaryTermUrn("NonExistent"))
    assert term.has_a == [GlossaryTermUrn("ZipCode")]


def test_glossary_term_new_from_graph() -> None:
    import datahub.metadata.schema_classes as models

    urn = GlossaryTermUrn("Classification.PII")
    aspects: models.AspectBag = {
        "glossaryTermInfo": models.GlossaryTermInfoClass(
            definition="Data that identifies a person.",
            termSource="INTERNAL",
            name="PII",
            parentNode="urn:li:glossaryNode:Classification",
        ),
        "glossaryRelatedTerms": models.GlossaryRelatedTermsClass(
            isRelatedTerms=["urn:li:glossaryTerm:SensitiveData"],
            hasRelatedTerms=None,
            values=None,
            relatedTerms=["urn:li:glossaryTerm:SSN"],
        ),
    }
    term = GlossaryTerm._new_from_graph(urn, aspects)

    assert term.name == "Classification.PII"
    assert term.definition == "Data that identifies a person."
    assert term.display_name == "PII"
    assert term.parent_node == GlossaryNodeUrn("Classification")
    assert term.is_a == [GlossaryTermUrn("SensitiveData")]
    assert term.related_terms == [GlossaryTermUrn("SSN")]
    assert term.has_a == []
    assert term.values == []


def test_glossary_term_structured_properties() -> None:
    term = GlossaryTerm(
        name="ARR",
        structured_properties={
            "urn:li:structuredProperty:sp1": ["value1"],
        },
    )
    assert term.structured_properties is not None
    assert len(term.structured_properties) == 1

    term.set_structured_property("urn:li:structuredProperty:sp2", ["value2"])
    assert term.structured_properties is not None
    assert len(term.structured_properties) == 2
