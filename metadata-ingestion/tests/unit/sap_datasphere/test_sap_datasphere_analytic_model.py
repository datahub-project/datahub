from datahub.ingestion.source.sap_datasphere.analytic_model import (
    extract_projection_source_columns,
    parse_business_layer,
)


def test_extracts_fact_and_dimension_source_keys():
    bld = {
        "M": {
            "sourceModel": {
                "factSources": {
                    "F": {"dataEntity": {"key": "FINANCE_DATA.SALES_ALL_GE"}}
                },
                "dimensionSources": {
                    "_BP": {
                        "dataEntity": {"key": "FINANCE_DATA.BusinessPartner_DIM_GE"}
                    },
                    "_BPL": {
                        "dataEntity": {"key": "FINANCE_DATA.Purchasing_Power_DIM_GE"}
                    },
                },
            }
        }
    }
    r = parse_business_layer(bld, "M")
    assert r.fact_source_keys == ["FINANCE_DATA.SALES_ALL_GE"]
    assert set(r.dimension_source_keys) == {
        "FINANCE_DATA.BusinessPartner_DIM_GE",
        "FINANCE_DATA.Purchasing_Power_DIM_GE",
    }
    # upstream_keys = fact + dims, deduped, order-preserving (fact first)
    assert r.upstream_keys[0] == "FINANCE_DATA.SALES_ALL_GE"
    assert set(r.upstream_keys) == {
        "FINANCE_DATA.SALES_ALL_GE",
        "FINANCE_DATA.BusinessPartner_DIM_GE",
        "FINANCE_DATA.Purchasing_Power_DIM_GE",
    }


def test_dedups_when_fact_also_listed_as_dimension():
    bld = {
        "M": {
            "sourceModel": {
                "factSources": {"F": {"dataEntity": {"key": "S.A"}}},
                "dimensionSources": {"D": {"dataEntity": {"key": "S.A"}}},
            }
        }
    }
    assert parse_business_layer(bld, "M").upstream_keys == ["S.A"]


def test_extracts_measure_names():
    bld = {
        "M": {
            "measures": {
                "BW_ORDERVALUE": {"text": "BW_ORDERVALUE", "isAuxiliary": False},
                "AUX": {"isAuxiliary": True},
            }
        }
    }
    r = parse_business_layer(bld, "M")
    assert "BW_ORDERVALUE" in r.measure_names
    assert (
        "AUX" in r.measure_names
    )  # auxiliary measures are still measures, included by design


def test_extracts_attribute_and_variable_names():
    bld = {
        "M": {
            "attributes": {"BusinessPartner": {}, "BW_ORDERID": {}},
            "variables": {"P_DATE": {"text": "Ref Date"}},
        }
    }
    r = parse_business_layer(bld, "M")
    assert set(r.attribute_names) >= {"BusinessPartner", "BW_ORDERID"}
    assert r.variable_names == ["P_DATE"]


def test_missing_blocks_yield_empty():
    assert parse_business_layer({}, "M").upstream_keys == []
    r = parse_business_layer({"M": {}}, "M")
    assert (
        r.measure_names == []
        and r.attribute_names == []
        and r.variable_names == []
        and r.upstream_keys == []
    )


def test_malformed_entries_skipped():
    bld = {
        "M": {
            "sourceModel": {"factSources": {"F": {"dataEntity": {}}, "G": "notadict"}}
        }
    }
    assert (
        parse_business_layer(bld, "M").fact_source_keys == []
    )  # no key -> skipped; non-dict -> skipped


def test_projection_source_columns_maps_alias_to_source():
    csn_def = {
        "query": {
            "SELECT": {
                "from": {"ref": ["SPACE.v_source"]},
                "columns": [
                    # aliased projection -> keyed by alias, points at source col
                    {"ref": ["SPACE.v_source", "col_a"], "as": "renamed_a"},
                    # no alias -> keyed by the source column name
                    {"ref": ["SPACE.v_source", "col_b"]},
                ],
            }
        }
    }
    result = extract_projection_source_columns(csn_def)
    assert result["renamed_a"].source_object == "SPACE.v_source"
    assert result["renamed_a"].column == "col_a"
    assert result["col_b"].column == "col_b"


def test_projection_source_columns_skips_derived_columns():
    csn_def = {
        "query": {
            "SELECT": {
                "columns": [
                    {"ref": ["SPACE.v_source", "col_a"], "as": "col_a"},
                    # a $projection self-reference (calculated) has no external source
                    {"ref": ["$projection", "col_a"], "as": "cc_derived"},
                    # a pure expression column carries no ref at all
                    {"xpr": [{"val": 1}], "as": "cc_const"},
                ]
            }
        }
    }
    result = extract_projection_source_columns(csn_def)
    assert set(result) == {"col_a"}


def test_projection_source_columns_no_query_is_empty():
    assert extract_projection_source_columns({}) == {}
    assert extract_projection_source_columns({"query": {"SELECT": {}}}) == {}
