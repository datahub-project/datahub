from datahub.ingestion.source.sap_datasphere.analytic_model import parse_business_layer


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
    )  # auxiliary measures are still measures (included); document the choice


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
