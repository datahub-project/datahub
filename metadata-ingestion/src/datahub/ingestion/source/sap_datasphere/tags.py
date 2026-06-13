"""DataHub tag URNs + descriptions for SAP Datasphere CDS semantic annotations.

The connector surfaces six kinds of CDS semantic annotations as DataHub tags so
they're searchable/filterable in DataHub Search. Universal BI concepts
(Dimension/Measure) use flat URNs that intentionally collide with Looker /
Snowflake-semantic-view / Mode / ThoughtSpot — customers get free
cross-connector pivot. SAP-specific concepts (currency, units, calendar types,
dimension types) use a ``sap:`` namespace.

Mirrors the Looker tag-emission pattern (looker_common.py:743-855) and
Snowflake's semantic-view tags.
"""

from typing import Dict, Iterable, Tuple

from datahub.emitter.mce_builder import make_tag_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import TagPropertiesClass

# Universal BI concepts — flat URNs collide intentionally across connectors so a
# single "Measure" or "Dimension" tag pivots across Looker / Snowflake semantic
# views / Mode / ThoughtSpot / Datasphere.
DIMENSION_TAG_URN = make_tag_urn("Dimension")
MEASURE_TAG_URN = make_tag_urn("Measure")

# SAP-specific semantic concepts — namespaced so they don't collide with
# unrelated tags that other connectors might emit.
SAP_CURRENCY_TAG_URN = make_tag_urn("sap:semantic:currency")
SAP_UNIT_TAG_URN = make_tag_urn("sap:semantic:unit")

# Calendar tags keyed by the value used in our existing
# ``sap_calendar_type`` custom property.
SAP_CALENDAR_TAG_URNS: Dict[str, str] = {
    "year": make_tag_urn("sap:calendar:year"),
    "month": make_tag_urn("sap:calendar:month"),
    "quarter": make_tag_urn("sap:calendar:quarter"),
    "week": make_tag_urn("sap:calendar:week"),
    "date": make_tag_urn("sap:calendar:date"),
    "yearmonth": make_tag_urn("sap:calendar:yearmonth"),
}


def sap_dimension_type_tag_urn(value: str) -> str:
    """Build a ``urn:li:tag:sap:dimension_type:<value>`` URN for an
    ``@Analytics.DimensionType`` value (e.g. 'Time', 'Customer').

    Built on the fly because we don't know upfront which DimensionType values a
    tenant uses — CDS allows arbitrary string values here.
    """
    return make_tag_urn(f"sap:dimension_type:{value}")


# Tag descriptions used when emitting standalone TagProperties MCPs.
# Tag URN → (display name, description).
_TAG_DEFINITIONS: Dict[str, Tuple[str, str]] = {
    DIMENSION_TAG_URN: (
        "Dimension",
        "A tag applied to dimension columns. Dimensions are categorical "
        "attributes used to slice/group facts.",
    ),
    MEASURE_TAG_URN: (
        "Measure",
        "A tag applied to measure (metric) columns. Measures are numeric "
        "values that are aggregated.",
    ),
    SAP_CURRENCY_TAG_URN: (
        "SAP Currency",
        "Column holds a currency amount per SAP CDS `@Common.IsCurrency`.",
    ),
    SAP_UNIT_TAG_URN: (
        "SAP Unit",
        "Column holds a unit-of-measure code per SAP CDS `@Common.IsUnit`.",
    ),
    SAP_CALENDAR_TAG_URNS["year"]: (
        "SAP Calendar: Year",
        "Column holds a calendar year (YYYY) per SAP CDS `@Common.IsCalendarYear`.",
    ),
    SAP_CALENDAR_TAG_URNS["month"]: (
        "SAP Calendar: Month",
        "Column holds a calendar month per SAP CDS `@Common.IsCalendarMonth`.",
    ),
    SAP_CALENDAR_TAG_URNS["quarter"]: (
        "SAP Calendar: Quarter",
        "Column holds a calendar quarter per SAP CDS `@Common.IsCalendarQuarter`.",
    ),
    SAP_CALENDAR_TAG_URNS["week"]: (
        "SAP Calendar: Week",
        "Column holds a calendar week per SAP CDS `@Common.IsCalendarWeek`.",
    ),
    SAP_CALENDAR_TAG_URNS["date"]: (
        "SAP Calendar: Date",
        "Column holds a calendar date per SAP CDS `@Common.IsCalendarDate`.",
    ),
    SAP_CALENDAR_TAG_URNS["yearmonth"]: (
        "SAP Calendar: Year-Month",
        "Column holds a calendar year-month per SAP CDS `@Common.IsCalendarYearMonth`.",
    ),
}


def get_predefined_tag_workunits() -> Iterable[MetadataWorkUnit]:
    """Yield one MCP per predefined SAP tag URN with TagProperties (name +
    description). Dynamic ``sap:dimension_type:*`` tags are NOT yielded here —
    they're auto-materialized by GMS when referenced because we don't know
    upfront which DimensionType values a tenant uses.

    Call once per ingestion run; the source class guards against re-emission via
    a ``_sap_tags_emitted`` flag.
    """
    for tag_urn, (display_name, description) in _TAG_DEFINITIONS.items():
        mcp = MetadataChangeProposalWrapper(
            entityUrn=tag_urn,
            aspect=TagPropertiesClass(name=display_name, description=description),
        )
        yield mcp.as_workunit()
