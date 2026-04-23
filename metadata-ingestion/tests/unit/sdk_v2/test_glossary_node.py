import pathlib

import pytest

from datahub.metadata.urns import CorpUserUrn, GlossaryNodeUrn
from datahub.sdk.glossary_node import GlossaryNode
from datahub.testing.sdk_v2_helpers import assert_entity_golden

GOLDEN_DIR = pathlib.Path(__file__).parent / "glossary_node_golden"


def test_glossary_node_basic() -> None:
    node = GlossaryNode(id="Finance")

    assert GlossaryNode.get_urn_type() == GlossaryNodeUrn
    assert isinstance(node.urn, GlossaryNodeUrn)
    assert str(node.urn) == "urn:li:glossaryNode:Finance"
    assert str(node.urn) in repr(node)

    assert node.id == "Finance"
    assert node.display_name is None
    assert node.definition == ""
    assert node.parent_node is None
    assert node.custom_properties == {}
    assert node.owners is None
    assert node.links is None

    with pytest.raises(AttributeError):
        assert node.extra_attribute  # type: ignore
    with pytest.raises(AttributeError):
        node.extra_attribute = "slots should reject extra fields"  # type: ignore

    assert_entity_golden(node, GOLDEN_DIR / "test_glossary_node_basic_golden.json")


def test_glossary_node_with_parent() -> None:
    parent = GlossaryNode(id="BusinessGlossary", definition="Top-level glossary.")

    # parent as GlossaryNode object
    child1 = GlossaryNode(id="Finance", parent_node=parent)
    assert child1.parent_node == GlossaryNodeUrn("BusinessGlossary")

    # parent as GlossaryNodeUrn
    child2 = GlossaryNode(
        id="Finance", parent_node=GlossaryNodeUrn("BusinessGlossary")
    )
    assert child2.parent_node == GlossaryNodeUrn("BusinessGlossary")

    # parent as URN string
    child3 = GlossaryNode(
        id="Finance", parent_node="urn:li:glossaryNode:BusinessGlossary"
    )
    assert child3.parent_node == GlossaryNodeUrn("BusinessGlossary")

    # All three are equivalent
    assert child1.parent_node == child2.parent_node == child3.parent_node


def test_glossary_node_complex() -> None:
    node = GlossaryNode(
        id="RevenueMetrics",
        display_name="Revenue Metrics",
        definition="Metrics related to revenue recognition and reporting.",
        parent_node=GlossaryNodeUrn("Finance"),
        custom_properties={"domain": "finance", "owner_team": "revenue"},
        owners=[CorpUserUrn("jdoe")],
        links=["https://wiki.company.com/revenue"],
    )

    assert node.id == "RevenueMetrics"
    assert node.display_name == "Revenue Metrics"
    assert node.definition == "Metrics related to revenue recognition and reporting."
    assert node.parent_node == GlossaryNodeUrn("Finance")
    assert node.custom_properties == {"domain": "finance", "owner_team": "revenue"}
    assert node.owners is not None
    assert len(node.owners) == 1
    assert node.links is not None
    assert len(node.links) == 1

    assert_entity_golden(node, GOLDEN_DIR / "test_glossary_node_complex_golden.json")


def test_glossary_node_setters() -> None:
    node = GlossaryNode(id="Finance")

    node.set_display_name("Financial Metrics")
    assert node.display_name == "Financial Metrics"

    node.set_definition("All financial and accounting-related business terms.")
    assert node.definition == "All financial and accounting-related business terms."

    node.set_parent_node(GlossaryNodeUrn("BusinessGlossary"))
    assert node.parent_node == GlossaryNodeUrn("BusinessGlossary")

    node.set_custom_properties({"key": "value"})
    assert node.custom_properties == {"key": "value"}

    node.set_parent_node(GlossaryNode(id="TopLevel"))
    assert node.parent_node == GlossaryNodeUrn("TopLevel")


def test_glossary_node_new_from_graph() -> None:
    import datahub.metadata.schema_classes as models

    urn = GlossaryNodeUrn("Finance")
    aspects: models.AspectBag = {
        "glossaryNodeInfo": models.GlossaryNodeInfoClass(
            definition="All financial terms.",
            name="Financial Metrics",
            parentNode="urn:li:glossaryNode:BusinessGlossary",
            customProperties={"domain": "finance"},
        )
    }
    node = GlossaryNode._new_from_graph(urn, aspects)

    assert node.id == "Finance"
    assert node.definition == "All financial terms."
    assert node.display_name == "Financial Metrics"
    assert node.parent_node == GlossaryNodeUrn("BusinessGlossary")
    assert node.custom_properties == {"domain": "finance"}


def test_glossary_node_structured_properties() -> None:
    node = GlossaryNode(
        id="Finance",
        structured_properties={
            "urn:li:structuredProperty:sp1": ["value1"],
            "urn:li:structuredProperty:sp2": ["value2"],
        },
    )
    assert node.structured_properties is not None
    assert len(node.structured_properties) == 2

    node.set_structured_property("urn:li:structuredProperty:sp1", ["updated"])
    sp_dict = {p.propertyUrn: p.values for p in node.structured_properties}
    assert sp_dict["urn:li:structuredProperty:sp1"] == ["updated"]
