from datahub.metadata.schema_classes import (
    ChartTypeClass,
)

CHART_TYPE_MAPPINGS = {
    "graph": ChartTypeClass.LINE,
    "timeseries": ChartTypeClass.LINE,
    "table": ChartTypeClass.TABLE,
    "stat": ChartTypeClass.TEXT,
    "gauge": ChartTypeClass.TEXT,
    "bargauge": ChartTypeClass.TEXT,
    "bar": ChartTypeClass.BAR,
    "pie": ChartTypeClass.PIE,
    "heatmap": ChartTypeClass.TABLE,
    "histogram": ChartTypeClass.BAR,
}
