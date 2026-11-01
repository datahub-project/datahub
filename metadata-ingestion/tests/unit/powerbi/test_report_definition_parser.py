import json
from typing import Any, Dict, List, Optional

import pytest

from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    PageFieldReference,
    ReportFilter,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.fabric_resolver import (
    DefinitionPart,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.report_definition_parser import (
    parse_filters,
    parse_rdl_dataset_binding,
    parse_report_definition,
)


def _source_ref(entity: str) -> dict:
    return {"Expression": {"SourceRef": {"Entity": entity}}}


def _column(entity: str, prop: str) -> Dict[str, Any]:
    return {"Column": {**_source_ref(entity), "Property": prop}}


def _aliased_column(alias: str, prop: str) -> Dict[str, Any]:
    return {
        "Column": {"Expression": {"SourceRef": {"Source": alias}}, "Property": prop}
    }


def _literal(value: str) -> Dict[str, Any]:
    return {"Literal": {"Value": value}}


def _comparison(kind: int, value: str) -> Dict[str, Any]:
    return {
        "Condition": {
            "Comparison": {"ComparisonKind": kind, "Left": {}, "Right": _literal(value)}
        }
    }


def _date_add_where(amount: int, time_unit: int) -> List[Dict[str, Any]]:
    return [
        {
            "Condition": {
                "Comparison": {
                    "ComparisonKind": 2,
                    "Left": {},
                    "Right": {
                        "DateAdd": {
                            "Expression": {"Now": {}},
                            "Amount": amount,
                            "TimeUnit": time_unit,
                        }
                    },
                }
            }
        }
    ]


def _categorical_filter(
    name: str = "f_status", target_key: str = "field"
) -> Dict[str, Any]:
    return {
        "name": name,
        target_key: _column("Claims", "Status"),
        "type": "Categorical",
        "filter": {
            "Version": 2,
            "From": [{"Name": "c", "Entity": "Claims", "Type": 0}],
            "Where": [
                {
                    "Condition": {
                        "In": {
                            "Expressions": [_aliased_column("c", "Status")],
                            "Values": [[_literal("'Open'")], [_literal("'Closed'")]],
                        }
                    }
                }
            ],
        },
    }


def _advanced_filter() -> Dict[str, Any]:
    return {
        "name": "f_premium",
        "field": _column("Policies", "WrittenPremium"),
        "type": "Advanced",
        "filter": {
            "Version": 2,
            "From": [{"Name": "p", "Entity": "Policies", "Type": 0}],
            "Where": [
                {
                    "Condition": {
                        "Comparison": {
                            "ComparisonKind": 2,
                            "Left": _aliased_column("p", "WrittenPremium"),
                            "Right": _literal("1000L"),
                        }
                    }
                },
                {
                    "Condition": {
                        "Not": {
                            "Expression": {
                                "Comparison": {
                                    "ComparisonKind": 0,
                                    "Left": _aliased_column("p", "WrittenPremium"),
                                    "Right": _literal("null"),
                                }
                            }
                        }
                    }
                },
            ],
        },
    }


def _relative_date_filter() -> Dict[str, Any]:
    date_span = {
        "DateSpan": {"Expression": _aliased_column("p", "EffectiveDate"), "TimeUnit": 0}
    }
    return {
        "name": "f_effective",
        "field": _column("Policies", "EffectiveDate"),
        "type": "RelativeDate",
        "filter": {
            "Version": 2,
            "From": [{"Name": "p", "Entity": "Policies", "Type": 0}],
            "Where": [
                {
                    "Condition": {
                        "Comparison": {
                            "ComparisonKind": 2,
                            "Left": date_span,
                            "Right": {
                                "DateAdd": {
                                    "Expression": {"Now": {}},
                                    "Amount": -30,
                                    "TimeUnit": 0,
                                }
                            },
                        }
                    }
                },
                {
                    "Condition": {
                        "Comparison": {
                            "ComparisonKind": 3,
                            "Left": date_span,
                            "Right": {"Now": {}},
                        }
                    }
                },
            ],
        },
    }


def _filter_summary(filters: List[ReportFilter]) -> List[Dict[str, Optional[str]]]:
    return [
        {
            "scope": f.scope,
            "name": f.name,
            "type": f.filter_type,
            "target": f.target.label if f.target else None,
            "condition": f.condition,
        }
        for f in filters
    ]


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


def test_pbir_report_and_page_filters_are_parsed_and_targets_join_page_fields() -> None:
    report_json = json.dumps(
        {"filterConfig": {"filters": [_categorical_filter(), _relative_date_filter()]}}
    )
    page_json = json.dumps(
        {
            "name": "p1",
            "displayName": "Loss Overview",
            "filterConfig": {"filters": [_advanced_filter()]},
        }
    )
    card = json.dumps(
        {
            "name": "v1",
            "visual": {
                "visualType": "card",
                "query": {
                    "queryState": {
                        "Values": {
                            "projections": [{"field": _column("Claims", "Status")}]
                        }
                    }
                },
            },
        }
    )
    parsed = parse_report_definition(
        [
            DefinitionPart("definition/report.json", report_json),
            DefinitionPart("definition/pages/p1/page.json", page_json),
            DefinitionPart("definition/pages/p1/visuals/v1/visual.json", card),
        ]
    )

    assert parsed.failed_filters == []
    assert _filter_summary(parsed.filters) == [
        {
            "scope": "report",
            "name": "f_status",
            "type": "Categorical",
            "target": "Claims[Status]",
            "condition": 'In ["Open", "Closed"]',
        },
        {
            "scope": "report",
            "name": "f_effective",
            "type": "RelativeDate",
            "target": "Policies[EffectiveDate]",
            "condition": "Last 30 days",
        },
    ]
    page = parsed.pages["p1"]
    assert _filter_summary(page.filters) == [
        {
            "scope": "page",
            "name": "f_premium",
            "type": "Advanced",
            "target": "Policies[WrittenPremium]",
            "condition": ">= 1000 And Not = null",
        }
    ]
    # Visual fields first, then page-filter targets; report filters are
    # applied per page by the caller.
    assert page.field_refs == [
        PageFieldReference("Claims", "Status", "column"),
        PageFieldReference("Policies", "WrittenPremium", "column"),
    ]


def test_legacy_report_and_section_filters_are_embedded_json_strings() -> None:
    top_n = {
        "name": "f_top",
        "expression": _column("Policies", "State"),
        "type": "TopN",
        "filter": {
            "From": [
                {"Name": "p", "Entity": "Policies", "Type": 0},
                {"Name": "k", "Entity": "Insurance KPIs", "Type": 0},
            ],
            "Where": [
                {
                    "Condition": {
                        "TopN": {
                            "Expression": _aliased_column("p", "State"),
                            "Count": 5,
                            "OrderBy": [
                                {
                                    "Direction": 2,
                                    "Expression": {
                                        "Measure": {
                                            "Expression": {
                                                "SourceRef": {"Source": "k"}
                                            },
                                            "Property": "Total Premium",
                                        }
                                    },
                                }
                            ],
                        }
                    }
                }
            ],
        },
    }
    report_json = json.dumps(
        {
            "filters": json.dumps([_categorical_filter(target_key="expression")]),
            "sections": [
                {
                    "name": "ReportSection1",
                    "displayName": "Summary",
                    "filters": json.dumps([top_n]),
                    "visualContainers": [],
                }
            ],
        }
    )

    parsed = parse_report_definition([DefinitionPart("report.json", report_json)])

    assert parsed.failed_filters == []
    assert _filter_summary(parsed.filters) == [
        {
            "scope": "report",
            "name": "f_status",
            "type": "Categorical",
            "target": "Claims[Status]",
            "condition": 'In ["Open", "Closed"]',
        }
    ]
    page = parsed.pages["ReportSection1"]
    assert _filter_summary(page.filters) == [
        {
            "scope": "page",
            "name": "f_top",
            "type": "TopN",
            "target": "Policies[State]",
            "condition": "Top 5 by [Total Premium]",
        }
    ]
    # The ordering measure is read by the filter too, so it is lineage.
    assert page.field_refs == [
        PageFieldReference("Policies", "State", "column"),
        PageFieldReference("Insurance KPIs", "Total Premium", "measure"),
    ]


def test_filter_fields_include_condition_reads_and_empty_filter_is_a_failure() -> None:
    # No declared `field`: the target stays unknown, but the column the
    # condition reads through a From alias is still lineage.
    implicit = {
        "name": "f_implicit",
        "type": "Categorical",
        "filter": {
            "From": [{"Name": "c", "Entity": "Claims", "Type": 0}],
            "Where": [
                {
                    "Condition": {
                        "In": {
                            "Expressions": [_aliased_column("c", "Status")],
                            "Values": [[_literal("'Open'")]],
                        }
                    }
                }
            ],
        },
    }
    # Nothing resolvable and no condition: not a filter we can describe.
    unresolved = {
        "name": "f_unresolved",
        "type": "Categorical",
        "field": {"Column": {"Expression": {"SourceRef": {"Source": "zz"}}}},
    }
    # A condition shape the renderer does not know falls back to raw JSON.
    exotic = {
        "name": "f_exotic",
        "field": _column("Claims", "Status"),
        "type": "Advanced",
        "filter": {"Where": [{"Condition": {"Exists": {"Expression": {}}}}]},
    }
    failed: List[str] = []
    filters = parse_filters([implicit, unresolved, exotic], "visual", "v1", failed)

    assert failed == [
        "v1#1 (ValueError: neither a target field nor a condition was found)"
    ]
    assert _filter_summary(filters) == [
        {
            "scope": "visual",
            "name": "f_implicit",
            "type": "Categorical",
            "target": None,
            "condition": 'In ["Open"]',
        },
        {
            "scope": "visual",
            "name": "f_exotic",
            "type": "Advanced",
            "target": "Claims[Status]",
            "condition": '[{"Condition":{"Exists":{"Expression":{}}}}]',
        },
    ]
    assert filters[0].fields == [PageFieldReference("Claims", "Status", "column")]
    assert filters[0].to_dict() == {
        "scope": "visual",
        "name": "f_implicit",
        "type": "Categorical",
        "condition": 'In ["Open"]',
    }


@pytest.mark.parametrize(
    "filter_type,where,expected",
    [
        pytest.param(
            None,
            [
                {
                    "Condition": {
                        "Or": {
                            "Left": {
                                "And": {
                                    "Left": _comparison(1, "10L")["Condition"],
                                    "Right": _comparison(4, "20L")["Condition"],
                                }
                            },
                            "Right": _comparison(0, "'x'")["Condition"],
                        }
                    }
                }
            ],
            '((> 10) And (<= 20)) Or (= "x")',
            id="and_or",
        ),
        pytest.param(
            None,
            [
                {"Condition": {"Contains": {"Left": {}, "Right": _literal("'ab'")}}},
                {
                    "Condition": {
                        "StartsWith": {"Left": {}, "Right": _literal("'c''d'")}
                    }
                },
            ],
            'Contains "ab" And StartsWith "c\'d"',
            id="contains_starts_with_doubled_quote",
        ),
        pytest.param(
            None,
            [
                {
                    "Condition": {
                        "TopN": {
                            "Expression": {},
                            "Count": 3,
                            "OrderBy": [
                                {
                                    "Direction": 1,
                                    "Expression": _column("Policies", "State"),
                                }
                            ],
                        }
                    }
                }
            ],
            "Bottom 3 by Policies[State]",
            id="top_n_ascending",
        ),
        pytest.param(
            None,
            [{"Condition": {"TopN": {"Expression": {}, "Count": 7}}}],
            "Top 7",
            id="top_n_without_order_by",
        ),
        pytest.param("RelativeDate", _date_add_where(0, 2), "This month", id="this"),
        pytest.param("RelativeDate", _date_add_where(1, 0), "Next 1 day", id="next"),
        pytest.param("RelativeDate", _date_add_where(-2, 3), "Last 2 years", id="last"),
        pytest.param(
            "RelativeDate",
            _date_add_where(-2, 99),
            '[{"Condition":{"Comparison":{"ComparisonKind":2,"Left":{},"Right":{"DateAdd":{"Expression":{"Now":{}},"Amount":-2,"TimeUnit":99}}}}}]',
            id="unknown_time_unit_falls_back_to_raw",
        ),
        pytest.param(
            None,
            [{"Condition": {"Unknown": {"Payload": "y" * 300}}}],
            # 200 chars in all: 199 of the raw JSON then the marker.
            '[{"Condition":{"Unknown":{"Payload":"' + "y" * 162 + "…",
            id="raw_fallback_is_truncated",
        ),
        pytest.param(
            None,
            [_comparison(2, "datetime'2024-01-01T00:00:00'")],
            '>= "2024-01-01T00:00:00"',
            id="typed_string_literal",
        ),
        pytest.param(
            None,
            [
                {
                    "Condition": {
                        "In": {
                            "Expressions": [{}, {}],
                            "Values": [
                                [_literal("'CA'"), _literal("1L")],
                                [_literal("'NY'"), _literal("2L")],
                            ],
                        }
                    }
                }
            ],
            'In [("CA", 1), ("NY", 2)]',
            id="multi_column_in",
        ),
        pytest.param(
            None,
            [
                {
                    "Condition": {
                        "In": {
                            "Expressions": [{}],
                            "Values": [[_literal(f"{i}L")] for i in range(23)],
                        }
                    }
                }
            ],
            "In [" + ", ".join(str(i) for i in range(20)) + ", … (+3 more)]",
            id="in_values_capped",
        ),
    ],
)
def test_condition_rendering(
    filter_type: Optional[str], where: List[Dict[str, Any]], expected: str
) -> None:
    failed: List[str] = []
    entry: Dict[str, Any] = {
        "name": "f",
        "field": _column("Claims", "Status"),
        "filter": {"Where": where},
    }
    if filter_type:
        entry["type"] = filter_type
    filters = parse_filters([entry], "page", "p", failed)
    assert failed == []
    assert [f.condition for f in filters] == [expected]


def test_malformed_filter_blocks_are_reported_but_never_drop_the_page() -> None:
    page_json = json.dumps(
        {
            "name": "p1",
            "displayName": "Overview",
            "filterConfig": {"filters": [_categorical_filter(), "not an object"]},
        }
    )
    parsed = parse_report_definition(
        [
            # Report-level block of the wrong shape.
            DefinitionPart(
                "definition/report.json", '{"filterConfig": {"filters": 7}}'
            ),
            DefinitionPart("definition/pages/p1/page.json", page_json),
            DefinitionPart("definition/pages/p2/page.json", '{"name": "p2"'),
        ]
    )
    assert parsed.filters == []
    assert [f.name for f in parsed.pages["p1"].filters] == ["f_status"]
    assert parsed.pages["p1"].display_name == "Overview"
    assert sorted(parsed.failed_filters) == [
        "definition/pages/p1/page.json#1",
        "definition/report.json",
    ]
    # A page part that is not JSON is still a failed part, not a failed filter.
    assert parsed.failed_parts == ["definition/pages/p2/page.json"]

    legacy = json.dumps(
        {
            "filters": "not json",
            "sections": [{"name": "s", "filters": "[1]", "visualContainers": []}],
        }
    )
    parsed = parse_report_definition([DefinitionPart("report.json", legacy)])
    assert parsed.filters == []
    assert parsed.pages["s"].filters == []
    assert parsed.failed_filters == ["report.json", "report.json#s#0"]
    assert parsed.failed_parts == []

    # Legacy `filters` must be a JSON string; an already-decoded list is
    # recorded, not raised (a TypeError here used to abort the workspace).
    legacy = json.dumps(
        {
            "filters": [{"name": "x"}],
            "sections": [
                {
                    "name": "s",
                    "displayName": "Still here",
                    "filters": [{"name": "y"}],
                    "visualContainers": [],
                }
            ],
        }
    )
    parsed = parse_report_definition([DefinitionPart("report.json", legacy)])
    assert parsed.filters == []
    assert parsed.pages["s"].display_name == "Still here"
    assert parsed.pages["s"].filters == []
    assert parsed.failed_filters == ["report.json", "report.json#s"]
    assert parsed.failed_parts == []


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


def test_empty_json_text_is_a_failed_part() -> None:
    # An empty part or embedded string is not an empty visual: the visual it
    # should describe is missing and must be reported.
    parsed = parse_report_definition(
        [DefinitionPart("definition/pages/p1/visuals/v1/visual.json", "")]
    )
    assert parsed.failed_parts == ["definition/pages/p1/visuals/v1/visual.json"]

    report_json = json.dumps(
        {"sections": [{"name": "s", "visualContainers": [{"config": ""}]}]}
    )
    parsed = parse_report_definition([DefinitionPart("report.json", report_json)])
    assert parsed.failed_parts == ["report.json#s/0"]
