"""Column lineage paths folded into each dataset's `lineageColumns`."""

from typing import List

from datahub_core_mcp_tools.tools.lineage import _extract_lineage_columns_from_paths


def _path(*field_paths: str) -> dict:
    return {"path": [{"type": "SCHEMA_FIELD", "fieldPath": f} for f in field_paths]}


def _result(name: str, *paths: dict, **extra: object) -> dict:
    return {
        "entity": {"urn": f"urn:li:dataset:{name}", "type": "DATASET"},
        "paths": list(paths),
        "degree": 1,
        **extra,
    }


def test_last_column_of_each_path_in_order_without_duplicates() -> None:
    results = [
        _result(
            "a",
            _path("user_id", "uid", "customer_key"),
            _path("user_id", "customer_id"),
            _path("account_id", "customer_id"),
        ),
        _result("b", _path("user_id", "col_b")),
    ]

    out = _extract_lineage_columns_from_paths(results)

    assert [r["lineageColumns"] for r in out] == [
        ["customer_key", "customer_id"],
        ["col_b"],
    ]
    assert all("paths" not in r for r in out)
    assert [r["entity"]["urn"] for r in out] == ["urn:li:dataset:a", "urn:li:dataset:b"]


def test_dataset_level_results_are_unchanged() -> None:
    results: List[dict] = [
        {"entity": {"urn": "urn:li:dataset:a", "type": "DATASET"}, "degree": 1}
    ]

    assert _extract_lineage_columns_from_paths(results) == results
    assert _extract_lineage_columns_from_paths([]) == []


def test_traversal_flags_are_kept() -> None:
    (out,) = _extract_lineage_columns_from_paths(
        [_result("a", _path("c1", "c2"), explored=True, ignoredAsHop=False)]
    )

    assert out["explored"] is True
    assert out["ignoredAsHop"] is False
    assert out["lineageColumns"] == ["c2"]


def test_malformed_paths_yield_no_columns() -> None:
    (out,) = _extract_lineage_columns_from_paths(
        [
            _result(
                "a",
                {"path": []},
                {"path": [{"type": "DATASET", "urn": "something"}]},
                {"path": [{"type": "SCHEMA_FIELD"}]},
            )
        ]
    )

    assert "lineageColumns" not in out
    assert out["degree"] == 1
