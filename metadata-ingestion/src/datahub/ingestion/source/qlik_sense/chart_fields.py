from typing import Collection, Dict, Iterable, List, Optional

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.qlik_sense.data_classes import (
    App,
    AppField,
    AppFieldIndex,
    Chart,
    ChartInputs,
    SchemaField as QlikDatasetSchemaField,
)
from datahub.ingestion.source.qlik_sense.script_lineage import expression_names
from datahub.ingestion.source.qlik_sense.source_utils import patch_workunits
from datahub.specific.chart import ChartPatchBuilder


def gen_chart_info_patch(
    chart_urn: str, chart: Chart, custom_properties: Dict[str, str]
) -> Iterable[MetadataWorkUnit]:
    # GMS creates missing chart info from its patch template, which also
    # stamps lastModified. An empty stamp here would erase the last one.
    patch = ChartPatchBuilder(chart_urn)
    if chart.title:
        patch.set_title(chart.title)
    if chart.visualization:
        patch.set_description(chart.visualization)
    for key, value in custom_properties.items():
        patch.add_custom_property(key, value)
    yield from patch_workunits(patch, is_primary_source=True)


def chart_field_names(expression: str, app_fields: AppFieldIndex) -> List[str]:
    # A dimension is usually a bare field name, which may contain spaces.
    return [expression] if app_fields.get(expression) else expression_names(expression)


def app_field_key(name: str, app_fields: AppFieldIndex) -> Optional[str]:
    """The app field a chart name reads. A derived field such as
    Date.autoCalendar.Year reads its base field, the longest leading part
    of the name that is a field (field names can contain dots), unless a
    field of that exact name exists."""
    key = name
    while not app_fields.get(key) and "." in key:
        key = key.rsplit(".", 1)[0]
    return key if app_fields.get(key) else None


def get_chart_input_fields(chart: Chart, app_fields: AppFieldIndex) -> List[AppField]:
    matched: Dict[str, AppField] = {}
    for expression in chart.field_expressions:
        for name in chart_field_names(expression, app_fields):
            key = app_field_key(name, app_fields)
            for app_field in app_fields.get(key) if key else []:
                matched.setdefault(
                    f"{app_field.dataset_urn}.{app_field.field.name}", app_field
                )
    return list(matched.values())


def index_app_fields(
    tables: Dict[str, List[QlikDatasetSchemaField]],
    script_table_urns: Collection[str] = (),
) -> AppFieldIndex:
    index = AppFieldIndex()
    for dataset_urn, fields in tables.items():
        for field in fields:
            app_field = AppField(dataset_urn=dataset_urn, field=field)
            index.exact.setdefault(field.name, []).append(app_field)
            if dataset_urn in script_table_urns:
                index.folded.setdefault(field.name.lower(), []).append(app_field)
    return index


def tables_without_chart_path(
    app: App, chart_inputs: Dict[str, ChartInputs], table_urns: List[str]
) -> List[str]:
    """The app's tables no chart links to the app. A table reaches the app
    through a chart only if the chart's sheet is written with its charts,
    which needs all of them to have been read."""
    charts_on_complete_sheets = {
        chart.qId
        for sheet in app.sheets
        if sheet.charts_complete
        for chart in sheet.charts
    }
    charted = {
        urn
        for chart_id, inputs in chart_inputs.items()
        if chart_id in charts_on_complete_sheets
        for urn in inputs.tables
    }
    return [urn for urn in table_urns if urn not in charted]
