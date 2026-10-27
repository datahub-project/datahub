from datahub.ingestion.source.quicksight.quicksight_constants import (
    DATA_SOURCE_TYPE_TO_DIALECT,
    DATA_SOURCE_TYPE_TO_PLATFORM,
    VISUAL_TYPE_TO_CHART_TYPE,
)
from datahub.metadata.schema_classes import ChartTypeClass

_VALID_CHART_TYPES = {
    getattr(ChartTypeClass, attr)
    for attr in dir(ChartTypeClass)
    if attr.isupper() and isinstance(getattr(ChartTypeClass, attr), str)
}


def test_every_sql_platform_has_a_dialect_entry():
    # The two maps are looked up by the same QuickSight Type key during lineage
    # extraction; a platform without a corresponding dialect entry would KeyError
    # at runtime, so keep them aligned.
    assert set(DATA_SOURCE_TYPE_TO_PLATFORM) == set(DATA_SOURCE_TYPE_TO_DIALECT)


def test_visual_type_map_only_uses_valid_chart_types():
    # DataHub's ChartTypeClass has no HEATMAP/TREEMAP/MAP members; this guards
    # against a future addition mapping to a non-existent enum value.
    invalid = {
        visual: chart_type
        for visual, chart_type in VISUAL_TYPE_TO_CHART_TYPE.items()
        if chart_type not in _VALID_CHART_TYPES
    }
    assert invalid == {}
