"""Unit tests for the SAP Datasphere tags module."""

from datahub.ingestion.source.sap_datasphere.tags import (
    DIMENSION_TAG_URN,
    MEASURE_TAG_URN,
    SAP_CALENDAR_TAG_URNS,
    SAP_CURRENCY_TAG_URN,
    SAP_UNIT_TAG_URN,
    get_predefined_tag_workunits,
    sap_dimension_type_tag_urn,
)
from datahub.metadata.schema_classes import TagPropertiesClass
from tests.unit.sap_datasphere_test_helpers import aspect_as, entity_urn_of


def test_universal_bi_tag_urns_are_flat():
    """Dimension and Measure URNs intentionally collide cross-connector — they
    must NOT carry the ``sap:`` namespace prefix."""
    assert DIMENSION_TAG_URN == "urn:li:tag:Dimension"
    assert MEASURE_TAG_URN == "urn:li:tag:Measure"


def test_sap_specific_tag_urns_are_namespaced():
    """SAP-specific concepts live under ``sap:`` so they never collide with
    unrelated tags emitted by other connectors."""
    assert SAP_CURRENCY_TAG_URN == "urn:li:tag:sap:semantic:currency"
    assert SAP_UNIT_TAG_URN == "urn:li:tag:sap:semantic:unit"
    for cal_type, urn in SAP_CALENDAR_TAG_URNS.items():
        assert urn == f"urn:li:tag:sap:calendar:{cal_type}"


def test_sap_calendar_tag_urns_cover_all_six_types():
    """The mapping must cover the six SAP CDS calendar terms the EDMX parser
    surfaces via ``sap_calendar_type``."""
    assert set(SAP_CALENDAR_TAG_URNS) == {
        "year",
        "month",
        "quarter",
        "week",
        "date",
        "yearmonth",
    }


def test_sap_dimension_type_tag_urn_builds_namespaced_urn():
    """``Analytics.DimensionType`` values build URNs on the fly because the
    set of allowed values is open-ended in CDS."""
    assert sap_dimension_type_tag_urn("Time") == "urn:li:tag:sap:dimension_type:Time"
    assert (
        sap_dimension_type_tag_urn("Customer")
        == "urn:li:tag:sap:dimension_type:Customer"
    )


def test_predefined_tag_workunits_emit_one_mcp_per_tag_urn_with_name_and_description():
    """Each predefined tag URN must yield a TagPropertiesClass MCP with both a
    display name and a description so the DataHub UI renders the tag with
    proper metadata instead of a bare URN."""
    workunits = list(get_predefined_tag_workunits())
    # 10 predefined tags: Dimension, Measure, 6 calendar types, currency, unit.
    # Dynamic sap:dimension_type:* tags are NOT in this set.
    assert len(workunits) == 10

    urns = set()
    for wu in workunits:
        aspect = aspect_as(wu, TagPropertiesClass)
        entity_urn = entity_urn_of(wu)
        assert aspect.name, f"Tag URN {entity_urn} has no name"
        assert aspect.description, f"Tag URN {entity_urn} has no description"
        urns.add(entity_urn)
    assert urns == {
        DIMENSION_TAG_URN,
        MEASURE_TAG_URN,
        SAP_CURRENCY_TAG_URN,
        SAP_UNIT_TAG_URN,
        *SAP_CALENDAR_TAG_URNS.values(),
    }
