from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    ChartTypeClass,
    DateTypeClass,
    NumberTypeClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    TimeTypeClass,
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


class GrafanaTypeMapper:
    """Maps Grafana types to DataHub types"""

    _TYPE_MAPPINGS = {
        "string": SchemaFieldDataTypeClass(type=StringTypeClass()),
        "number": SchemaFieldDataTypeClass(type=NumberTypeClass()),
        "integer": SchemaFieldDataTypeClass(type=NumberTypeClass()),
        "float": SchemaFieldDataTypeClass(type=NumberTypeClass()),
        "boolean": SchemaFieldDataTypeClass(type=BooleanTypeClass()),
        "time": SchemaFieldDataTypeClass(type=TimeTypeClass()),
        "timestamp": SchemaFieldDataTypeClass(type=TimeTypeClass()),
        "timeseries": SchemaFieldDataTypeClass(type=TimeTypeClass()),
        "time_series": SchemaFieldDataTypeClass(type=TimeTypeClass()),
        "datetime": SchemaFieldDataTypeClass(type=TimeTypeClass()),
        "date": SchemaFieldDataTypeClass(type=DateTypeClass()),
    }

    @classmethod
    def get_field_type(
        cls, grafana_type: str, default_type: str = "string"
    ) -> SchemaFieldDataTypeClass:
        return cls._TYPE_MAPPINGS.get(
            grafana_type.lower(),
            cls._TYPE_MAPPINGS.get(default_type, cls._TYPE_MAPPINGS["string"]),
        )

    @classmethod
    def get_native_type(cls, grafana_type: str, default_type: str = "string") -> str:
        grafana_type = grafana_type.lower()
        if grafana_type in cls._TYPE_MAPPINGS:
            return grafana_type
        return default_type
