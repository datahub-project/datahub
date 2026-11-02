from typing import Dict, List, Optional

import pytest

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


def _select(from_: object, columns: List[dict], mixin: Optional[dict] = None) -> dict:
    select: Dict[str, object] = {"from": from_, "columns": columns}
    if mixin is not None:
        select["mixin"] = mixin
    return {"query": {"SELECT": select}}


def _join(*args: dict) -> dict:
    return {"join": "inner", "args": list(args)}


def _sources(csn_def: dict) -> Dict[str, str]:
    return {
        element: f"{ref.source_object}/{ref.column}"
        for element, ref in extract_projection_source_columns(csn_def).items()
    }


def test_projection_source_columns_maps_unqualified_refs_and_output_aliases():
    csn_def = _select(
        {"ref": ["SAP_I_COMPANYCODE"]},
        [
            {"ref": ["CompanyCode"], "as": "Company Code", "key": True},
            {"ref": ["Currency"]},
        ],
    )

    assert _sources(csn_def) == {
        "Company Code": "SAP_I_COMPANYCODE/CompanyCode",
        "Currency": "SAP_I_COMPANYCODE/Currency",
    }


@pytest.mark.parametrize(
    "from_",
    [
        {"ref": ["SPACE.v_source"], "as": "source"},
        {"ref": ["SPACE.v_source"]},
        {"ref": [{"id": "SPACE.v_source", "args": {"p": {"val": 1}}}], "as": "source"},
    ],
    ids=["explicit_alias", "implicit_alias", "parameterized"],
)
def test_projection_source_columns_resolves_from_aliases(from_):
    qualifier = from_.get("as", "v_source")
    csn_def = _select(
        from_, [{"ref": [qualifier, "col_a"], "as": "renamed_a"}, {"ref": ["col_b"]}]
    )

    assert _sources(csn_def) == {
        "renamed_a": "SPACE.v_source/col_a",
        "col_b": "SPACE.v_source/col_b",
    }


def test_projection_source_columns_aliased_from_is_addressed_by_alias_only():
    # Once aliased, the source name no longer qualifies refs; a mixin of that
    # name does.
    csn_def = _select(
        {"ref": ["fact"], "as": "f"},
        [{"ref": ["f", "amount"]}, {"ref": ["fact", "name"]}],
        mixin={"fact": {"type": "cds.Association", "target": "SPACE.dim"}},
    )

    assert _sources(csn_def) == {
        "amount": "fact/amount",
        "name": "SPACE.dim/name",
    }


def test_projection_source_columns_ambiguous_join_key_keeps_both_sources():
    # `Y` names two objects, so it is dropped, but the join still has two
    # sources and unqualified refs must not resolve to the one left.
    csn_def = _select(
        _join({"ref": ["X"], "as": "Y"}, {"ref": ["Z.Y"]}),
        [{"ref": ["Y", "a"]}, {"ref": ["Z.Y", "c"]}, {"ref": ["b"]}],
    )

    assert _sources(csn_def) == {"c": "Z.Y/c"}


def test_projection_source_columns_resolves_join_aliases():
    csn_def = _select(
        _join(
            _join(
                {"ref": ["SPACE.fact"], "as": "f"}, {"ref": ["SPACE.dim"], "as": "d"}
            ),
            {"ref": ["SPACE.region"]},
        ),
        [
            {"ref": ["f", "amount"]},
            {"ref": ["d", "name"], "as": "dimension_name"},
            {"ref": ["region", "code"]},
            {"ref": ["unknown", "col_x"]},
            {"ref": ["col_y"]},
        ],
    )

    assert _sources(csn_def) == {
        "amount": "SPACE.fact/amount",
        "dimension_name": "SPACE.dim/name",
        "code": "SPACE.region/code",
    }


def test_projection_source_columns_drops_colliding_implicit_join_aliases():
    csn_def = _select(
        _join({"ref": ["SPACE_A.orders"]}, {"ref": ["SPACE_B.orders"]}),
        [{"ref": ["orders", "id"]}, {"ref": ["SPACE_B.orders", "total"]}],
    )

    assert _sources(csn_def) == {"total": "SPACE_B.orders/total"}


def test_projection_source_columns_resolves_mixin_association_targets():
    csn_def = _select(
        {"ref": ["SPACE.fact"], "as": "f"},
        [
            {"ref": ["_dim", "name"], "as": "dim_name"},
            {"ref": ["_broken", "col_x"]},
            {"ref": ["_dim"]},
            {"ref": ["amount"]},
        ],
        mixin={
            "_dim": {"type": "cds.Association", "target": "SPACE.dim"},
            "_broken": {"type": "cds.Association"},
        },
    )

    # The mixin target is not a second FROM source, so unqualified refs still
    # resolve to the single FROM object.
    assert _sources(csn_def) == {
        "dim_name": "SPACE.dim/name",
        "amount": "SPACE.fact/amount",
    }


@pytest.mark.parametrize(
    "from_",
    [
        {"SELECT": {"from": {"ref": ["SPACE.fact"]}}, "as": "sub"},
        {"ref": ["SPACE.fact", "_toDim"], "as": "sub"},
        {"ref": [{"args": {"p": {"val": 1}}}], "as": "sub"},
        None,
    ],
    ids=["subquery", "path_expression", "parameterized_without_id", "no_from"],
)
def test_projection_source_columns_skips_refs_of_unresolved_from(from_):
    csn_def = _select(
        from_,
        [{"ref": ["sub", "amount"]}, {"ref": ["region"]}, {"ref": ["_dim", "name"]}],
        mixin={"_dim": {"type": "cds.Association", "target": "SPACE.dim"}},
    )

    assert _sources(csn_def) == {"name": "SPACE.dim/name"}


def test_projection_source_columns_join_with_subquery_has_no_single_source():
    csn_def = _select(
        _join(
            {"ref": ["SPACE.fact"], "as": "f"},
            {"SELECT": {"from": {"ref": ["SPACE.dim"]}}, "as": "sub"},
        ),
        [{"ref": ["f", "amount"]}, {"ref": ["region"]}],
    )

    assert _sources(csn_def) == {"amount": "SPACE.fact/amount"}


def test_projection_source_columns_skips_non_string_segments():
    csn_def = _select(
        {"ref": ["SPACE.fact"], "as": "f"},
        [
            {"ref": [{"id": "_dim", "where": [{"ref": ["x"]}, "=", {"val": 1}]}]},
            {"ref": ["f", {"id": "col", "args": {}}]},
            {"ref": ["amount"]},
        ],
        mixin={"_dim": {"type": "cds.Association", "target": "SPACE.dim"}},
    )

    assert _sources(csn_def) == {"amount": "SPACE.fact/amount"}


def test_projection_source_columns_skips_multi_segment_navigation():
    csn_def = _select(
        {"ref": ["SPACE.fact"], "as": "fact"},
        [{"ref": ["fact", "dimension", "name"], "as": "dimension_name"}],
    )

    assert extract_projection_source_columns(csn_def) == {}


def test_projection_source_columns_skips_derived_columns():
    csn_def = _select(
        {"ref": ["SPACE.v_source"]},
        [
            {"ref": ["SPACE.v_source", "col_a"], "as": "col_a"},
            # a $projection self-reference (calculated) has no external source
            {"ref": ["$projection", "col_a"], "as": "cc_derived"},
            # a pure expression column carries no ref at all
            {"xpr": [{"val": 1}], "as": "cc_const"},
        ],
    )
    assert set(extract_projection_source_columns(csn_def)) == {"col_a"}


def test_projection_source_columns_no_query_is_empty():
    assert extract_projection_source_columns({}) == {}
    assert extract_projection_source_columns({"query": {"SELECT": {}}}) == {}
