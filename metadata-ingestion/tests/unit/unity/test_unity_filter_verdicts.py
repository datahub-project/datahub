from typing import Dict, List

import pytest

from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)

# The rules test_unity_probe_parity cannot reach through one ingestion run: a
# kind switch's precedence, the containers a judged name is given, and a
# pinned `catalogs` list.

UNITY_CONFIG: Dict[str, object] = {
    "token": "t",
    "workspace_url": "https://example.cloud.databricks.com",
}


@pytest.mark.parametrize(
    "extra, parent_path, pattern, target",
    [
        ({}, ["my cat"], r"^my_cat\.analytics$", "my_cat.analytics"),
        (
            {"include_metastore": True},
            ["meta", "my cat"],
            r"^meta\.my_cat\.analytics$",
            "meta.my_cat.analytics",
        ),
    ],
)
def test_unity_schemas_are_judged_on_the_id_ingestion_filters(
    extra: Dict[str, object], parent_path: List[str], pattern: str, target: str
) -> None:
    """process_schemas matches schema_pattern against Schema.id -- the escaped
    `[metastore.]catalog.schema` -- so a correctly written recipe must not read
    as excluded."""
    result = check_filters(
        source_type="unity-catalog",
        config_dict={**UNITY_CONFIG, **extra, "schema_pattern": {"allow": [pattern]}},
        kind=str(DatasetContainerSubTypes.SCHEMA),
        parent_path=parent_path,
        names=["analytics"],
    )
    assert [(r.target, r.included) for r in result.results] == [(target, True)]
    assert result.warnings == []


def test_unity_catalogs_are_judged_on_their_escaped_id() -> None:
    result = check_filters(
        source_type="unity-catalog",
        config_dict={**UNITY_CONFIG, "catalog_pattern": {"deny": ["^my_cat$"]}},
        kind=str(DatasetContainerSubTypes.CATALOG),
        parent_path=[],
        names=["my cat"],
    )
    assert (result.results[0].target, result.results[0].included) == ("my_cat", False)


def test_a_unity_view_switched_off_by_include_views_stays_excluded_by_it() -> None:
    result = check_filters(
        source_type="unity-catalog",
        config_dict={
            **UNITY_CONFIG,
            "include_views": False,
            "table_pattern": {"deny": [".*"]},
        },
        kind=str(DatasetSubTypes.VIEW),
        parent_path=["main", "analytics"],
        names=["v_orders"],
    )
    assert result.results[0].excluded_by == "include_views"


def test_unity_notebooks_are_excluded_while_include_notebooks_is_off() -> None:
    result = check_filters(
        source_type="unity-catalog",
        config_dict=UNITY_CONFIG,
        kind=str(DatasetSubTypes.NOTEBOOK),
        parent_path=[],
        names=["/Shared/etl"],
    )
    assert (result.results[0].included, result.results[0].excluded_by) == (
        False,
        "include_notebooks",
    )
    # Top-level: no "pass the containing schema" advice.
    assert result.warnings == []


def test_a_unity_catalog_outside_a_pinned_list_is_excluded_by_it() -> None:
    result = check_filters(
        source_type="unity-catalog",
        # Pinned names are looked up case-insensitively by the API, which
        # returns the stored lowercase name, so "Main" pins "main".
        config_dict={**UNITY_CONFIG, "catalogs": ["Main"]},
        kind=str(DatasetContainerSubTypes.CATALOG),
        parent_path=[],
        names=["main", "other"],
    )
    assert [(r.name, r.included, r.excluded_by) for r in result.results] == [
        ("main", True, None),
        ("other", False, "catalogs"),
    ]


def test_a_unity_table_under_a_catalog_outside_the_pinned_list_is_excluded() -> None:
    result = check_filters(
        source_type="unity-catalog",
        config_dict={**UNITY_CONFIG, "catalogs": ["main"]},
        kind=str(DatasetSubTypes.TABLE),
        parent_path=["other", "analytics"],
        names=["orders"],
    )
    assert (result.results[0].included, result.results[0].excluded_by) == (
        False,
        "catalogs",
    )
