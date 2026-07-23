from typing import Dict, Iterable, Tuple

from datahub.emitter.mce_builder import make_tag_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sap_datasphere.constants import (
    CALENDAR_DATE,
    CALENDAR_MONTH,
    CALENDAR_QUARTER,
    CALENDAR_WEEK,
    CALENDAR_YEAR,
    CALENDAR_YEARMONTH,
)
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
    CALENDAR_YEAR: make_tag_urn("sap:calendar:year"),
    CALENDAR_MONTH: make_tag_urn("sap:calendar:month"),
    CALENDAR_QUARTER: make_tag_urn("sap:calendar:quarter"),
    CALENDAR_WEEK: make_tag_urn("sap:calendar:week"),
    CALENDAR_DATE: make_tag_urn("sap:calendar:date"),
    CALENDAR_YEARMONTH: make_tag_urn("sap:calendar:yearmonth"),
}


def sap_dimension_type_tag_urn(value: str) -> str:
    # Built on the fly because CDS allows arbitrary @Analytics.DimensionType values.
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
    SAP_CALENDAR_TAG_URNS[CALENDAR_YEAR]: (
        "SAP Calendar: Year",
        "Column holds a calendar year (YYYY) per SAP CDS `@Common.IsCalendarYear`.",
    ),
    SAP_CALENDAR_TAG_URNS[CALENDAR_MONTH]: (
        "SAP Calendar: Month",
        "Column holds a calendar month per SAP CDS `@Common.IsCalendarMonth`.",
    ),
    SAP_CALENDAR_TAG_URNS[CALENDAR_QUARTER]: (
        "SAP Calendar: Quarter",
        "Column holds a calendar quarter per SAP CDS `@Common.IsCalendarQuarter`.",
    ),
    SAP_CALENDAR_TAG_URNS[CALENDAR_WEEK]: (
        "SAP Calendar: Week",
        "Column holds a calendar week per SAP CDS `@Common.IsCalendarWeek`.",
    ),
    SAP_CALENDAR_TAG_URNS[CALENDAR_DATE]: (
        "SAP Calendar: Date",
        "Column holds a calendar date per SAP CDS `@Common.IsCalendarDate`.",
    ),
    SAP_CALENDAR_TAG_URNS[CALENDAR_YEARMONTH]: (
        "SAP Calendar: Year-Month",
        "Column holds a calendar year-month per SAP CDS `@Common.IsCalendarYearMonth`.",
    ),
}


def get_predefined_tag_workunits() -> Iterable[MetadataWorkUnit]:
    # Dynamic sap:dimension_type:* tags are omitted here — GMS auto-materializes
    # them when referenced, since a tenant's values aren't known upfront.
    for tag_urn, (display_name, description) in _TAG_DEFINITIONS.items():
        mcp = MetadataChangeProposalWrapper(
            entityUrn=tag_urn,
            aspect=TagPropertiesClass(name=display_name, description=description),
        )
        yield mcp.as_workunit()
