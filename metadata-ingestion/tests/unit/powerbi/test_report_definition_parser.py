import json

from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    PageFieldReference,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.fabric_resolver import (
    DefinitionPart,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.report_definition_parser import (
    parse_rdl_dataset_binding,
    parse_report_definition,
)


def _source_ref(entity: str) -> dict:
    return {"Expression": {"SourceRef": {"Entity": entity}}}


def test_pbir_visuals_group_fields_by_page() -> None:
    page_json = json.dumps({"name": "p1", "displayName": "Loss Overview"})
    card = json.dumps(
        {
            "name": "v1",
            "visual": {
                "visualType": "card",
                "query": {
                    "queryState": {
                        "Values": {
                            "projections": [
                                {
                                    "field": {
                                        "Measure": {
                                            **_source_ref("Insurance KPIs"),
                                            "Property": "Loss Ratio",
                                        }
                                    }
                                }
                            ]
                        }
                    }
                },
            },
        }
    )
    bar = json.dumps(
        {
            "name": "v2",
            "visual": {
                "visualType": "clusteredBarChart",
                "query": {
                    "queryState": {
                        "Category": {
                            "projections": [
                                {
                                    "field": {
                                        "Column": {
                                            **_source_ref("Policies"),
                                            "Property": "State",
                                        }
                                    }
                                }
                            ]
                        }
                    }
                },
            },
            "filterConfig": {
                "filters": [
                    {
                        "field": {
                            "Column": {**_source_ref("Claims"), "Property": "Status"}
                        }
                    }
                ]
            },
        }
    )
    parsed = parse_report_definition(
        [
            DefinitionPart("definition/report.json", "{}"),
            DefinitionPart("definition/pages/p1/page.json", page_json),
            DefinitionPart("definition/pages/p1/visuals/v1/visual.json", card),
            DefinitionPart("definition/pages/p1/visuals/v2/visual.json", bar),
        ]
    )

    assert set(parsed.pages) == {"p1"}
    page = parsed.pages["p1"]
    assert page.display_name == "Loss Overview"
    assert page.visual_types == ["card", "clusteredBarChart"]
    assert page.field_refs == [
        PageFieldReference("Insurance KPIs", "Loss Ratio", "measure"),
        PageFieldReference("Policies", "State", "column"),
        PageFieldReference("Claims", "Status", "column"),
    ]
    assert parsed.page_by_display_name("Loss Overview") is page


def test_legacy_report_resolves_from_aliases_and_date_hierarchies() -> None:
    config = json.dumps(
        {
            "name": "visual1",
            "singleVisual": {
                "visualType": "lineChart",
                "prototypeQuery": {
                    "From": [
                        {"Name": "o", "Entity": "Orders", "Type": 0},
                        {"Name": "t", "Entity": "Time Measures", "Type": 0},
                    ],
                    "Select": [
                        {
                            "HierarchyLevel": {
                                "Expression": {
                                    "Hierarchy": {
                                        "Expression": {
                                            "PropertyVariationSource": {
                                                "Expression": {
                                                    "SourceRef": {"Source": "o"}
                                                },
                                                "Name": "Variation",
                                                "Property": "order_date",
                                            }
                                        },
                                        "Hierarchy": "Date Hierarchy",
                                    }
                                },
                                "Level": "Year",
                            }
                        },
                        {
                            "Measure": {
                                "Expression": {"SourceRef": {"Source": "t"}},
                                "Property": "Current Year Revenue",
                            }
                        },
                    ],
                },
            },
        }
    )
    filters = json.dumps(
        [
            {
                "expression": {
                    "Column": {
                        "Expression": {"SourceRef": {"Entity": "Orders"}},
                        "Property": "country",
                    }
                }
            }
        ]
    )
    report_json = json.dumps(
        {
            "sections": [
                {
                    "name": "ReportSection1",
                    "displayName": "Executive Summary",
                    "visualContainers": [{"config": config, "filters": filters}],
                }
            ]
        }
    )

    parsed = parse_report_definition([DefinitionPart("report.json", report_json)])

    page = parsed.pages["ReportSection1"]
    assert page.display_name == "Executive Summary"
    assert [v.visual_type for v in page.visuals] == ["lineChart"]
    assert page.field_refs == [
        PageFieldReference("Orders", "order_date", "column"),
        PageFieldReference("Time Measures", "Current Year Revenue", "measure"),
        PageFieldReference("Orders", "country", "column"),
    ]


def test_legacy_visual_with_unparseable_config_is_skipped_safely() -> None:
    report_json = json.dumps(
        {
            "sections": [
                {
                    "name": "s",
                    "displayName": "Broken",
                    "visualContainers": [{"config": "not json"}],
                }
            ]
        }
    )
    parsed = parse_report_definition([DefinitionPart("report.json", report_json)])
    assert parsed.pages["s"].visuals[0].fields == []
    assert parsed.pages["s"].field_refs == []
    assert parsed.failed_parts == ["report.json#s/0"]


def test_json_of_the_wrong_shape_is_a_failed_part_not_a_crash() -> None:
    # Valid JSON that is not an object (a legacy config of "null", a PBIR
    # visual part holding a list) must be reported, not raise AttributeError
    # and take the whole workspace down.
    report_json = json.dumps(
        {"sections": [{"name": "s", "visualContainers": [{"config": "null"}]}]}
    )
    parsed = parse_report_definition([DefinitionPart("report.json", report_json)])
    assert parsed.failed_parts == ["report.json#s/0"]

    parsed = parse_report_definition(
        [DefinitionPart("definition/pages/p1/visuals/v1/visual.json", "[1, 2]")]
    )
    assert parsed.failed_parts == ["definition/pages/p1/visuals/v1/visual.json"]
    assert parsed.pages["p1"].field_refs == []


def test_rdl_dataset_binding() -> None:
    rdl = (
        '<Report><DataSources><DataSource Name="pbi"><ConnectionProperties>'
        "<ConnectString>Data Source=pbiazure://api.powerbi.com;"
        "Initial Catalog=sobe_wowvirtualserver-05169CD2-E713-41E6-9600-1D8066D95445;"
        "</ConnectString></ConnectionProperties></DataSource></DataSources></Report>"
    )
    parts = [
        DefinitionPart("definition.pbir", "{}"),
        DefinitionPart("report.rdl", rdl),
    ]
    assert parse_rdl_dataset_binding(parts) == "05169CD2-E713-41E6-9600-1D8066D95445"
    assert (
        parse_rdl_dataset_binding([DefinitionPart("report.rdl", "<Report/>")]) is None
    )
