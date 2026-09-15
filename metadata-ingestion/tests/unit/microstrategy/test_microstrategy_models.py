import json
from typing import Any, Dict

from datahub.ingestion.source.microstrategy.models import (
    DashboardDefinition,
    DatasetObject,
    Datasource,
    DatasourceConnection,
    PersonalFolderResolution,
    ReportDefinition,
    first_derived_node_skeleton,
    is_personal_folder_object,
    payload_key_skeleton,
    payload_type_vocabulary,
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
                                "name": "Net Sales Retail Amt",
                                "subType": "metric",
                            },
                            {
                                "id": "D-RTL-PLN",
                                "name": "RTL PLN",
                                "subType": "derived_metric",
                                "expression": {
                                    "text": (
                                        "([Net Sales Retail Amt]/"
                                        "[Salon Merch OPR Net Sales Retail Amt])-1"
                                    ),
                                    "tokens": [
                                        {
                                            "type": "object_reference",
                                            "target": {
                                                "objectId": "M-NET",
                                                "name": "Net Sales Retail Amt",
                                                "subType": "metric",
                                            },
                                        }
                                    ],
                                },
                            },
                            {
                                "id": "D-AMT-VAR",
                                "name": "Amt Var LYS %",
                                "subType": "metric",
                                "derived": True,
                                "definition": {
                                    "expression": {"text": "{Net Sales Retail Amt} - 1"}
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
                                    "name": "Net Sales Retail Amt",
                                    "subType": "metric",
                                    "thresholds": [
                                        {
                                            "name": "Positive",
                                            "condition": {
                                                "text": "{Net Sales Retail Amt} > 0"
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
    assert set(definitions) == {"D-RTL-PLN", "D-AMT-VAR"}
    rtl = definitions["D-RTL-PLN"]
    assert rtl.name == "RTL PLN"
    assert rtl.expression_text == (
        "([Net Sales Retail Amt]/[Salon Merch OPR Net Sales Retail Amt])-1"
    )
    assert rtl.expression_tokens is not None
    assert "Net Sales Retail Amt" in rtl.expression_tokens
    assert rtl.source == "report"
    assert definitions["D-AMT-VAR"].expression_text == "{Net Sales Retail Amt} - 1"


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
                    {"id": "M-NET", "name": "Net Sales Retail Amt", "type": "metric"},
                    {
                        "id": "D-RTL-PLN",
                        "name": "RTL PLN",
                        "type": "metric",
                        "derived": True,
                    },
                ]
            }
        }
    }
    definitions = extract_embedded_metric_definitions(payload)
    assert [(d.id, d.name, d.expression_text) for d in definitions] == [
        ("D-RTL-PLN", "RTL PLN", None)
    ]


def test_metric_formula_references_accept_braces_and_brackets() -> None:
    from datahub.ingestion.source.microstrategy.lineage import (
        metric_formula_references,
    )

    assert metric_formula_references(
        "([Net Sales Retail Amt]/[Salon Merch OPR Net Sales Retail Amt])-1"
    ) == ["Net Sales Retail Amt", "Salon Merch OPR Net Sales Retail Amt"]
    assert metric_formula_references(
        "({Revenue} - {Revenue LY}) / Abs({Revenue LY})"
    ) == [
        "Revenue",
        "Revenue LY",
    ]


def test_payload_key_skeleton_keeps_keys_and_types_but_never_values() -> None:
    payload = {
        "information": {"objectId": "ABC123", "name": "RTL PLN", "hidden": False},
        "elements": [
            {"id": "D-1", "derived": True, "expression": {"text": "[A]/[B]"}},
            {"id": "D-2", "derived": True, "expression": None},
        ],
        "count": 2,
        "ratio": 0.5,
        "empty": [],
    }

    rendered = payload_key_skeleton(payload)
    assert json.loads(rendered) == {
        "information": {"objectId": "str", "name": "str", "hidden": "bool"},
        "elements": {
            "$len": 2,
            # Second item's null expression is visible alongside the first's dict.
            "$item": {
                "id": "str",
                "derived": "bool",
                "expression": '{"text":"str"}|null',
            },
        },
        "count": "int",
        "ratio": "float",
        "empty": "list[0]",
    }
    for leaked in ("ABC123", "RTL PLN", "D-1", "[A]/[B]"):
        assert leaked not in rendered


def test_payload_key_skeleton_unions_heterogeneous_list_items() -> None:
    # A report template mixes attribute units with one metrics unit whose
    # elements are catalog metrics and derived metrics; only the minority
    # items carry the keys that matter, so they must survive the rendering.
    payload = {
        "units": [
            {"id": "A1", "name": "Region", "type": "attribute"},
            {"id": "A2", "name": "District", "type": "attribute"},
            {
                "type": "metrics",
                "elements": [
                    {"id": "M1", "name": "Net Sales", "subType": "metric"},
                    {
                        "id": "D1",
                        "name": "RTL PLN",
                        "subType": "derived_metric",
                        "expression": {"tokens": [{"value": "x"}]},
                    },
                ],
            },
        ]
    }

    assert json.loads(payload_key_skeleton(payload)) == {
        "units": {
            "$len": 3,
            "$item": {
                "id": "str",
                "name": "str",
                "type": "str",
                "elements": {
                    "$len": 2,
                    "$item": {
                        "id": "str",
                        "name": "str",
                        "subType": "str",
                        "expression": {
                            "tokens": {"$len": 1, "$item": {"value": "str"}}
                        },
                    },
                },
            },
        }
    }
    assert payload_type_vocabulary(payload) == [
        "attribute",
        "derived_metric",
        "metric",
        "metrics",
    ]
    # The cap bounds traversal, so it keeps the first values met, not the
    # alphabetically first: the unit-level types come before element subtypes.
    assert payload_type_vocabulary(payload, max_values=2) == [
        "attribute",
        "metrics",
    ]
    assert payload_type_vocabulary({"type": 7, "items": [{"subType": "x"}]}) == ["x"]


def test_payload_key_skeleton_is_depth_limited() -> None:
    payload = {"a": {"b": {"c": {"d": [1, 2, 3]}}}}

    assert json.loads(payload_key_skeleton(payload, max_depth=2)) == {
        "a": {"b": "dict[1]"}
    }
    assert json.loads(payload_key_skeleton(payload, max_depth=3)) == {
        "a": {"b": {"c": "dict[1]"}}
    }
    assert json.loads(payload_key_skeleton(payload)) == {
        "a": {"b": {"c": {"d": {"$len": 3, "$item": "int"}}}}
    }


def test_payload_key_skeleton_is_size_capped() -> None:
    payload = {f"key_{index:04d}": "value" for index in range(500)}

    rendered = payload_key_skeleton(payload, max_chars=200)
    assert len(rendered) == 200
    assert rendered.endswith("...")
    assert payload_key_skeleton("scalar") == '"str"'
    assert payload_key_skeleton(None) == '"null"'


def test_first_derived_node_skeleton_describes_node_identity_and_parent() -> None:
    payload = {
        "dataSource": {
            "units": [
                {
                    "type": "metrics",
                    "elements": [
                        {"id": "M-1", "name": "Net Sales"},
                        {
                            "id": "D-1",
                            "name": "RTL PLN",
                            "derived": True,
                            "definition": {"formulaText": "[A]/[B]"},
                        },
                    ],
                }
            ]
        }
    }

    rendered = first_derived_node_skeleton(payload)
    assert rendered is not None
    assert json.loads(rendered) == {
        "node": {
            "id": "str",
            "name": "str",
            "derived": "bool",
            "definition": {"formulaText": "str"},
        },
        "identity": {
            "id": "str",
            "name": "str",
            "derived": "bool",
            "definition": {"formulaText": "str"},
        },
        "parent_keys": ["elements", "type"],
    }
    assert "RTL PLN" not in rendered and "D-1" not in rendered


def test_first_derived_node_skeleton_uses_parent_identity_and_handles_absence() -> None:
    # A `definition` sub-object flagged derived borrows its parent's identity.
    payload = {
        "metrics": [{"id": "D-1", "name": "RTL PLN", "definition": {"isDerived": True}}]
    }
    rendered = first_derived_node_skeleton(payload)
    assert rendered is not None
    assert json.loads(rendered) == {
        "node": {"isDerived": "bool"},
        "identity": {"id": "str", "name": "str", "definition": {"isDerived": "bool"}},
        "parent_keys": ["definition", "id", "name"],
    }
    assert first_derived_node_skeleton({"metrics": [{"id": "M-1"}]}) is None


def _personal_report(*ancestors: Dict[str, str]) -> Dict[str, Any]:
    return {"id": "R-1", "name": "Copy of Dossier", "ancestors": list(ancestors)}


def test_personal_folder_object_by_id_ignores_names() -> None:
    resolution = PersonalFolderResolution(root_ids={"PROFILES-ID"}, names={"profiles"})
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
