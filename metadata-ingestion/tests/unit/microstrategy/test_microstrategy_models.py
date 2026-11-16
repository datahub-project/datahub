from typing import Any, Dict, Optional

import pytest

from datahub.ingestion.source.microstrategy.models import (
    DashboardDefinition,
    DatasetObject,
    Datasource,
    DatasourceConnection,
    MicroStrategyObject,
    PersonalFolderResolution,
    ReportDefinition,
    extract_document_derived_metric_definitions,
    is_document_object,
    is_dossier_view_media,
    is_personal_folder_object,
)


def test_dashboard_definition_extracts_datasets_and_visualizations() -> None:
    definition = DashboardDefinition.from_api_response(
        object_id="dash-1",
        object_name="Executive Sales",
        response={
            "result": {
                "definition": {
                    "datasets": [{"id": "ds-1", "name": "Sales Cube"}],
                    "chapters": [
                        {
                            "pages": [
                                {
                                    "visualizations": [
                                        {
                                            "key": "viz-1",
                                            "name": "Revenue Trend",
                                            "datasets": [{"id": "ds-1"}],
                                        }
                                    ]
                                }
                            ]
                        }
                    ],
                }
            }
        },
    )

    assert [dataset.id for dataset in definition.datasets] == ["ds-1"]
    assert [visualization.key for visualization in definition.visualizations] == [
        "viz-1"
    ]
    assert definition.visualizations[0].datasets == ["ds-1"]
    assert definition.visualizations[0].chapter_key is None


def test_dataset_available_objects_list_is_grouped_by_type() -> None:
    definition = DashboardDefinition.from_api_response(
        object_id="dash-1",
        object_name="Executive Sales",
        response={
            "datasets": [
                {
                    "id": "ds-1",
                    "name": "Sales Cube",
                    "availableObjects": [
                        {"id": "metric-1", "name": "Revenue", "type": "metric"},
                        {
                            "id": "attribute-1",
                            "name": "Region",
                            "type": "attribute",
                        },
                    ],
                }
            ]
        },
    )

    available_objects = definition.datasets[0].available_objects

    assert available_objects["metrics"][0]["id"] == "metric-1"
    assert available_objects["attributes"][0]["id"] == "attribute-1"


def test_dashboard_definition_preserves_visualization_chapter_key() -> None:
    definition = DashboardDefinition.from_api_response(
        object_id="dash-1",
        object_name="Executive Sales",
        response={
            "chapters": [
                {
                    "key": "chapter-1",
                    "pages": [
                        {
                            "key": "page-1",
                            "visualizations": [
                                {
                                    "key": "viz-1",
                                    "name": "Revenue Trend",
                                }
                            ],
                        }
                    ],
                }
            ]
        },
    )

    visualization = definition.visualizations[0]

    assert visualization.chapter_key == "chapter-1"
    assert visualization.page_key == "page-1"


def test_datasource_extracts_source_type_and_connection() -> None:
    datasource = Datasource.model_validate(
        {
            "id": "source-1",
            "name": "Enterprise Warehouse",
            "datasourceType": "normal",
            "database": {
                "type": "snow_flake",
                "version": "snowflake_1x",
                "connection": {
                    "id": "conn-1",
                    "name": "Snowflake Connection",
                    "embedded": False,
                },
            },
            "dbms": {"name": "Snowflake"},
        }
    )

    assert datasource.database_type == "snow_flake"
    assert datasource.database_version == "snowflake_1x"
    assert datasource.dbms_name == "Snowflake"
    assert datasource.connection_id == "conn-1"
    assert datasource.connection_name == "Snowflake Connection"
    assert datasource.connection_embedded is False


def test_datasource_connection_drops_raw_connection_string_but_keeps_context() -> None:
    connection = DatasourceConnection.model_validate(
        {
            "id": "conn-1",
            "name": "Sales Warehouse Connection",
            "database": {"type": "snow_flake"},
            "connectionString": "DATABASE=SALES_DB;SCHEMA=ORDERS;UID=metadata_reader",
        }
    )

    assert connection.database_type == "snow_flake"
    assert connection.database_name == "SALES_DB"
    assert connection.schema_name == "ORDERS"
    assert connection.connection_string_present is True
    assert "connectionString" not in connection.model_dump()


def test_datasource_connection_parses_jdbc_url_query_params() -> None:
    # Snowflake JDBC connections carry db/schema as URL query parameters
    # (&db=...&schema=...), not ODBC-style ;KEY=value pairs.
    connection = DatasourceConnection.model_validate(
        {
            "id": "conn-1",
            "name": "SNOWFLAKE_DWH_JDBC_Connection",
            "database": {"type": "snow_flake"},
            "connectionString": (
                ";JDBC;DRIVER={net.snowflake.client.jdbc.SnowflakeDriver};"
                "URL={jdbc:snowflake://acme.us-east-1.snowflakecomputing.com/"
                "?AUTHENTICATOR=SNOWFLAKE_JWT&warehouse=REPORT_WH&db=MY_EDW_DB"
                "&schema=SALES_DM&role=READER_ROLE};MSTR_AUTH=standard;"
            ),
        }
    )

    assert connection.database_name == "MY_EDW_DB"
    assert connection.schema_name == "SALES_DM"
    assert "connectionString" not in connection.model_dump()


def test_dataset_preserves_source_warehouse_reference_when_present() -> None:
    dataset = DatasetObject.model_validate(
        {
            "id": "ds-1",
            "name": "Sales Cube",
            "sourceWarehouse": {
                "id": "source-1",
                "name": "Enterprise Warehouse",
                "database": {"type": "snow_flake"},
            },
        }
    )

    assert dataset.source_warehouse is not None
    assert dataset.source_warehouse.id == "source-1"
    assert dataset.source_warehouse.database_type == "snow_flake"


def test_report_definition_extracts_source_and_available_objects() -> None:
    definition = ReportDefinition.from_api_response(
        object_id="report-1",
        object_name="Sales Report",
        response={
            "result": {
                "definition": {
                    "dataSource": {"id": "cube-1", "name": "Sales Cube"},
                    "availableObjects": [
                        {"id": "metric-1", "name": "Revenue", "type": "metric"},
                        {
                            "id": "attr-1",
                            "name": "Region",
                            "type": "attribute",
                        },
                    ],
                    "prompts": [{"id": "prompt-1"}],
                    "filter": {"id": "filter-1"},
                }
            }
        },
    )

    assert definition.source_id == "cube-1"
    assert definition.source_name == "Sales Cube"
    assert definition.available_objects["metrics"][0]["id"] == "metric-1"
    assert definition.available_objects["attributes"][0]["id"] == "attr-1"
    assert definition.object_ids == ["attr-1", "metric-1"]
    assert definition.prompt_count == 1
    assert definition.has_filter is True


def _model_report_payload() -> dict:
    # Shape modelled on GET /api/model/reports/{id}: catalog metrics as plain
    # elements, report-level derived metrics with expressions, a report
    # filter and a threshold that also carry expression-like text.
    return {
        "information": {"objectId": "REPORT-1", "name": "RETAIL SALES YESTERDAY"},
        "dataSource": {
            "dataTemplate": {
                "units": [
                    {
                        "type": "metrics",
                        "elements": [
                            {
                                "id": "M-NET",
                                "name": "Net Sales Amt",
                                "subType": "metric",
                            },
                            {
                                "id": "D-SALES-PLAN",
                                "name": "SALES PLAN",
                                "subType": "derived_metric",
                                "expression": {
                                    "text": (
                                        "([Net Sales Amt]/[Store Net Sales Amt])-1"
                                    ),
                                    "tokens": [
                                        {
                                            "type": "object_reference",
                                            "target": {
                                                "objectId": "M-NET",
                                                "name": "Net Sales Amt",
                                                "subType": "metric",
                                            },
                                        }
                                    ],
                                },
                            },
                            {
                                "id": "D-AMT-VAR",
                                "name": "Amt Var PY %",
                                "subType": "metric",
                                "derived": True,
                                "definition": {
                                    "expression": {"text": "{Net Sales Amt} - 1"}
                                },
                            },
                        ],
                    }
                ]
            },
            "filter": {
                "id": "FILTER-1",
                "name": "Yesterday",
                "subType": "filter",
                "expression": {"text": "{Day} = Yesterday"},
            },
        },
        "grid": {
            "viewTemplate": {
                "columns": {
                    "units": [
                        {
                            "type": "metrics",
                            "elements": [
                                {
                                    "id": "M-NET",
                                    "name": "Net Sales Amt",
                                    "subType": "metric",
                                    "thresholds": [
                                        {
                                            "name": "Positive",
                                            "condition": {
                                                "text": "{Net Sales Amt} > 0"
                                            },
                                        }
                                    ],
                                }
                            ],
                        }
                    ]
                }
            }
        },
    }


def test_extract_embedded_metric_definitions_finds_derived_metrics_only() -> None:
    from datahub.ingestion.source.microstrategy.models import (
        extract_embedded_metric_definitions,
    )

    definitions = {
        definition.id: definition
        for definition in extract_embedded_metric_definitions(_model_report_payload())
    }

    # Catalog metrics (no expression), the filter and the threshold are not
    # metric definitions; the two derived metrics are, whichever way their
    # expression is nested.
    assert set(definitions) == {"D-SALES-PLAN", "D-AMT-VAR"}
    rtl = definitions["D-SALES-PLAN"]
    assert rtl.name == "SALES PLAN"
    assert rtl.expression_text == ("([Net Sales Amt]/[Store Net Sales Amt])-1")
    assert rtl.expression_tokens is not None
    assert "Net Sales Amt" in rtl.expression_tokens
    assert rtl.source == "report"
    assert definitions["D-AMT-VAR"].expression_text == "{Net Sales Amt} - 1"


def test_extract_embedded_metric_definitions_names_flagged_derived_without_formula() -> (
    None
):
    from datahub.ingestion.source.microstrategy.models import (
        extract_embedded_metric_definitions,
    )

    # v2 report definitions may flag a derived metric without exposing its
    # expression; the object name is still worth having.
    payload = {
        "definition": {
            "availableObjects": {
                "metrics": [
                    {"id": "M-NET", "name": "Net Sales Amt", "type": "metric"},
                    {
                        "id": "D-SALES-PLAN",
                        "name": "SALES PLAN",
                        "type": "metric",
                        "derived": True,
                    },
                ]
            }
        }
    }
    definitions = extract_embedded_metric_definitions(payload)
    assert [(d.id, d.name, d.expression_text) for d in definitions] == [
        ("D-SALES-PLAN", "SALES PLAN", None)
    ]


def test_metric_formula_references_accept_braces_and_brackets() -> None:
    from datahub.ingestion.source.microstrategy.lineage import (
        metric_formula_references,
    )

    assert metric_formula_references("([Net Sales Amt]/[Store Net Sales Amt])-1") == [
        "Net Sales Amt",
        "Store Net Sales Amt",
    ]
    assert metric_formula_references(
        "({Revenue} - {Revenue LY}) / Abs({Revenue LY})"
    ) == [
        "Revenue",
        "Revenue LY",
    ]


def _personal_report(*ancestors: Dict[str, str]) -> Dict[str, Any]:
    return {"id": "R-1", "name": "Copy of Dossier", "ancestors": list(ancestors)}


def test_personal_folder_object_by_id_ignores_names() -> None:
    resolution = PersonalFolderResolution(
        root_ids={"PROFILES-ID"}, names={"profiles"}, profiles_root_resolved=True
    )
    assert not resolution.by_name

    under_profiles = _personal_report(
        {"id": "root", "name": "Analytics"},
        {"id": "profiles-id", "name": "Perfiles"},
        {"id": "u-1", "name": "jdoe"},
        {"id": "mr-1", "name": "Mis informes"},
    )
    assert is_personal_folder_object(under_profiles, resolution)
    # A public folder that merely carries the name is not personal by id.
    named_profiles = _personal_report(
        {"id": "root", "name": "Analytics"},
        {"id": "shared", "name": "Shared Reports"},
        {"id": "f-9", "name": "Profiles"},
    )
    assert not is_personal_folder_object(named_profiles, resolution)


def test_personal_folder_names_still_apply_without_the_profiles_root() -> None:
    # Only the principal's own My Reports resolved: other users' profile
    # folders can only be recognised by name.
    resolution = PersonalFolderResolution(root_ids={"MR-SELF"}, names={"profiles"})
    other_user = _personal_report(
        {"id": "root", "name": "Analytics"},
        {"id": "profiles-id", "name": "Profiles"},
        {"id": "u-2", "name": "asmith"},
    )
    assert is_personal_folder_object(other_user, resolution)


def test_personal_folder_string_path_matches_by_name() -> None:
    resolution = PersonalFolderResolution(
        root_ids={"PROFILES-ID"}, names={"profiles"}, profiles_root_resolved=True
    )
    assert is_personal_folder_object(
        {"id": "R-1", "location": "/Analytics/Profiles/jdoe/My Reports"}, resolution
    )


def test_personal_folder_object_by_name_fallback_is_case_insensitive() -> None:
    resolution = PersonalFolderResolution(names={"profiles", "my reports"})
    assert resolution.by_name

    assert is_personal_folder_object(
        _personal_report(
            {"id": "root", "name": "Analytics"},
            {"id": "p", "name": "PROFILES"},
            {"id": "u", "name": "jdoe"},
        ),
        resolution,
    )
    assert is_personal_folder_object(
        {"id": "R-2", "name": "R", "folder": {"path": "/Analytics/jdoe/My Reports"}},
        resolution,
    )
    # Exact-name matching: "My Reports Archive" under Shared Reports stays.
    assert not is_personal_folder_object(
        _personal_report(
            {"id": "shared", "name": "Shared Reports"},
            {"id": "f", "name": "My Reports Archive"},
        ),
        resolution,
    )
    assert not is_personal_folder_object({"id": "R-3", "name": "No folder"}, resolution)
    assert not is_personal_folder_object(
        _personal_report({"id": "p", "name": "Profiles"}),
        PersonalFolderResolution.empty(),
    )


def test_dashboard_definition_annotates_chapter_and_page_names_and_positions() -> None:
    def page(key: str, name: str, viz_key: str) -> Dict[str, Any]:
        return {
            "key": key,
            "name": name,
            "visualizations": [{"key": viz_key, "name": f"Viz {viz_key}"}],
        }

    definition = DashboardDefinition.from_api_response(
        object_id="dash-1",
        object_name="Executive Sales",
        response={
            "chapters": [
                {
                    "key": "ch-1",
                    "name": "Overview",
                    "pages": [
                        page("pg-1", "Summary", "viz-a"),
                        page("pg-2", "Trend", "viz-b"),
                    ],
                },
                {
                    "key": "ch-2",
                    "name": "Detail",
                    "pages": [
                        page("pg-3", "By Region", "viz-c"),
                        page("pg-4", "By Store", "viz-d"),
                    ],
                },
            ]
        },
    )

    placements = {
        visualization.key: (
            visualization.chapter_name,
            visualization.chapter_key,
            visualization.chapter_index,
            visualization.page_name,
            visualization.page_key,
            visualization.page_index,
        )
        for visualization in definition.visualizations
    }

    assert placements == {
        "viz-a": ("Overview", "ch-1", 1, "Summary", "pg-1", 1),
        "viz-b": ("Overview", "ch-1", 1, "Trend", "pg-2", 2),
        "viz-c": ("Detail", "ch-2", 2, "By Region", "pg-3", 1),
        "viz-d": ("Detail", "ch-2", 2, "By Store", "pg-4", 2),
    }


def test_visualization_outside_any_page_has_no_placement() -> None:
    definition = DashboardDefinition.from_api_response(
        object_id="dash-1",
        object_name="Executive Sales",
        response={"visualizations": [{"key": "viz-1", "name": "Loose"}]},
    )

    visualization = definition.visualizations[0]

    assert visualization.chapter_name is None
    assert visualization.chapter_index is None
    assert visualization.page_name is None
    assert visualization.page_index is None


def test_document_definition_keeps_layout_names_in_order() -> None:
    # A Report Services document lists its layouts as chapters without pages.
    definition = DashboardDefinition.from_api_response(
        object_id="doc-1",
        object_name="Weekly Sales Report",
        response={
            "chapters": [
                {"key": "ch-1", "name": "Summary", "filters": [], "pages": []},
                {"key": "ch-2", "filters": [], "pages": []},
                {"key": "ch-3", "name": "Appendix", "filters": [], "pages": []},
            ],
            "datasets": [{"id": "ds-1", "name": "Sales Report"}],
        },
    )

    assert definition.layouts == ["Summary", "Appendix"]
    assert definition.visualizations == []


def test_dossier_definition_has_no_layouts() -> None:
    definition = DashboardDefinition.from_api_response(
        object_id="dash-1",
        object_name="Executive Sales",
        response={
            "chapters": [
                {"key": "ch-1", "name": "Overview", "pages": []},
                {
                    "key": "ch-2",
                    "name": "Detail",
                    "pages": [
                        {
                            "key": "pg-1",
                            "name": "By Region",
                            "visualizations": [{"key": "viz-1", "name": "Grid"}],
                        }
                    ],
                },
            ]
        },
    )

    assert definition.layouts == []
    assert [visualization.key for visualization in definition.visualizations] == [
        "viz-1"
    ]


def test_extract_embedded_metric_definitions_follow_data_template_order() -> None:
    from datahub.ingestion.source.microstrategy.models import (
        extract_embedded_metric_definitions,
    )

    def derived(object_id: str, name: str) -> Dict[str, Any]:
        return {
            "id": object_id,
            "name": name,
            "subType": "derived_metric",
            "expression": {"text": f"{{{name}}} - 1"},
        }

    # The grid precedes the data template in this payload's key order; the
    # template's element order (the Report Objects order) still wins.
    payload = {
        "grid": {
            "viewTemplate": {
                "columns": {
                    "units": [{"type": "metrics", "elements": [derived("D-B", "Beta")]}]
                }
            }
        },
        "dataSource": {
            "dataTemplate": {
                "units": [
                    {
                        "type": "metrics",
                        "elements": [
                            {"id": "M-1", "name": "Net", "subType": "metric"},
                            derived("D-A", "Alpha"),
                            derived("D-B", "Beta"),
                        ],
                    }
                ]
            }
        },
    }

    definitions = extract_embedded_metric_definitions(payload)

    assert [definition.id for definition in definitions] == ["D-A", "D-B"]


def test_document_derived_metrics_keyed_by_defining_dataset() -> None:
    by_dataset = extract_document_derived_metric_definitions(
        {
            "datasets": [
                {
                    "information": {"objectId": "ds-a"},
                    "derivedMetrics": [
                        {
                            "information": {"objectId": "1" * 32, "name": "Forecast"},
                            "expression": {"text": "[A]*[B]"},
                        },
                        # Neither an expression nor a derived flag: not a
                        # definition the walker can attribute, so skipped.
                        {"information": {"objectId": "2" * 32, "name": "Spare"}},
                    ],
                    # Attribute-form expressions must never become metrics.
                    "derivedAttributes": [
                        {
                            "information": {"objectId": "3" * 32, "name": "Band"},
                            "forms": [
                                {
                                    "id": "4" * 32,
                                    "expressions": [{"expression": {"text": "x"}}],
                                }
                            ],
                        }
                    ],
                },
                {"information": {"objectId": "ds-b"}, "derivedMetrics": []},
                {"derivedMetrics": [{"information": {"objectId": "5" * 32}}]},
            ]
        }
    )

    # Keyed by normalized dataset id; empty and id-less datasets drop out.
    assert list(by_dataset) == ["DS-A"]
    names = {d.name: d for d in by_dataset["DS-A"]}
    assert set(names) == {"Forecast"}
    assert names["Forecast"].expression_text == "[A]*[B]"
    assert names["Forecast"].source == "document"
    assert names["Forecast"].endpoint == "model_document"
    assert "Band" not in names


def test_document_derived_metrics_tolerate_missing_datasets() -> None:
    assert extract_document_derived_metric_definitions({}) == {}
    assert extract_document_derived_metric_definitions({"datasets": "x"}) == {}


@pytest.mark.parametrize(
    ("subtype", "view_media", "expected"),
    [
        # Object-info responses and older fixtures carry no viewMedia: the
        # subtype alone decides, as it did before viewMedia was read.
        ("14081", None, True),
        ("14081", 1073741827, True),
        # A dossier search hit has the same subtype; bits 30 and 29 of its
        # viewMedia (with 31 and 27 clear) mark it as a dossier.
        ("14081", 1879072805, False),
        ("14081", 0x6000_0000, False),
        ("14081", 0xE000_0000, True),
        ("14080", 1073741827, False),
        ("14080", None, False),
        (None, 1073741827, False),
    ],
)
def test_is_document_object_needs_the_subtype_and_a_non_dossier_view_media(
    subtype: Optional[str], view_media: Optional[int], expected: bool
) -> None:
    mstr_object = MicroStrategyObject.model_validate(
        {
            "id": "o-1",
            "name": "Object",
            "type": "55",
            "subtype": subtype,
            "viewMedia": view_media,
        }
    )
    assert is_document_object(mstr_object) is expected


def test_view_media_is_read_from_search_results() -> None:
    mstr_object = MicroStrategyObject.model_validate(
        {"id": "o-1", "name": "Object", "viewMedia": "1879072805"}
    )
    assert mstr_object.view_media == 1879072805
    assert is_dossier_view_media(1879072805)
    assert not is_dossier_view_media(1073741827)
