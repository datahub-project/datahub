"""Integration / golden-file tests for the ODCS source (D1 fan-out)."""

import json
import pathlib
from typing import Any, Dict, List, Optional

import pytest
import time_machine

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers

FROZEN_TIME = "2024-01-15 12:00:00"


def _run_pipeline(
    tmp_path: pathlib.Path,
    test_resources_dir: pathlib.Path,
    fixture: str,
    output_name: str,
    server_mappings: list,
    extra_config: Optional[Dict[str, Any]] = None,
) -> pathlib.Path:
    output_path = tmp_path / output_name
    config: Dict[str, Any] = {
        "path": str(test_resources_dir / fixture),
        "servers_to_platform": server_mappings,
        "strict_validation": False,
    }
    if extra_config:
        config.update(extra_config)
    pipeline = Pipeline.create(
        {
            "run_id": f"test-odcs-{output_name}",
            "source": {"type": "odcs", "config": config},
            "sink": {
                "type": "file",
                "config": {"filename": str(output_path)},
            },
        }
    )
    pipeline.run()
    pipeline.raise_from_status()
    return output_path


@time_machine.travel(FROZEN_TIME, tick=False)
def test_odcs_minimal(pytestconfig: pytest.Config, tmp_path: pathlib.Path) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/integration/odcs"
    output_path = _run_pipeline(
        tmp_path=tmp_path,
        test_resources_dir=test_resources_dir,
        fixture="odcs_minimal.odcs.yaml",
        output_name="odcs_minimal_mces.json",
        server_mappings=[
            {"server": "prod-snowflake", "platform": "snowflake", "env": "PROD"}
        ],
    )
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir / "odcs_minimal_mces_golden.json",
    )


@time_machine.travel(FROZEN_TIME, tick=False)
def test_odcs_full_v31(pytestconfig: pytest.Config, tmp_path: pathlib.Path) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/integration/odcs"
    output_path = _run_pipeline(
        tmp_path=tmp_path,
        test_resources_dir=test_resources_dir,
        fixture="odcs_full_v31.odcs.yaml",
        output_name="odcs_full_v31_mces.json",
        server_mappings=[
            {"server": "prod-postgres", "platform": "postgres", "env": "PROD"},
            {"server": "prod-snowflake", "platform": "snowflake", "env": "PROD"},
        ],
    )
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir / "odcs_full_v31_mces_golden.json",
    )


def _read_mces(path: pathlib.Path) -> List[Dict[str, Any]]:
    return json.loads(path.read_text())


def _editable_field_paths_by_dataset(
    mces: List[Dict[str, Any]],
) -> Dict[str, List[str]]:
    """For each dataset URN, return the field paths in EditableSchemaMetadata."""
    out: Dict[str, List[str]] = {}
    for mce in mces:
        if mce.get("entityType") != "dataset":
            continue
        if mce.get("aspectName") != "editableSchemaMetadata":
            continue
        urn = mce["entityUrn"]
        info = mce["aspect"]["json"].get("editableSchemaFieldInfo", [])
        out.setdefault(urn, []).extend(f["fieldPath"] for f in info)
    return out


@time_machine.travel(FROZEN_TIME, tick=False)
def test_odcs_synthetic_ecommerce(
    pytestconfig: pytest.Config, tmp_path: pathlib.Path
) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/integration/odcs"
    output_path = _run_pipeline(
        tmp_path=tmp_path,
        test_resources_dir=test_resources_dir,
        fixture="odcs_synthetic_ecommerce.odcs.yaml",
        output_name="odcs_synthetic_ecommerce_mces.json",
        server_mappings=[
            {"server": "prod-postgres", "platform": "postgres", "env": "PROD"}
        ],
        extra_config={"replicate_contract_metadata": True},
    )

    # ---- C1 regression guard: per-table fields attach to the right URN. ----
    mces = _read_mces(output_path)
    paths_by_urn = _editable_field_paths_by_dataset(mces)

    # Each of the 4 tables must produce a distinct dataset URN.
    dataset_urns = {
        mce["entityUrn"] for mce in mces if mce.get("entityType") == "dataset"
    }
    expected_tables = {"customers", "orders", "products", "inventory"}
    matched_tables = {
        table
        for table in expected_tables
        if any(f",{table}," in u for u in dataset_urns)
    }
    assert matched_tables == expected_tables

    # Build per-table URN lookup.
    def urn_for(table: str) -> str:
        return next(u for u in dataset_urns if f",{table}," in u)

    customers_urn = urn_for("customers")
    inventory_urn = urn_for("inventory")
    orders_urn = urn_for("orders")
    products_urn = urn_for("products")

    customers_paths = paths_by_urn.get(customers_urn, [])
    inventory_paths = paths_by_urn.get(inventory_urn, [])
    orders_paths = paths_by_urn.get(orders_urn, [])
    products_paths = paths_by_urn.get(products_urn, [])

    # email is on customers, NOT on inventory / products / orders.
    assert "email" in customers_paths
    assert "email" not in inventory_paths
    assert "email" not in orders_paths
    assert "email" not in products_paths

    # sku appears in both products and inventory schemas — but the property
    # carrying a description ("Vendor SKU; expected unique...") lives on products.
    # The inventory.sku field has no description / tags so it does not produce
    # an editable schema entry.
    assert "sku" in products_paths
    # inventory.sku has no description/tags -> no editable schema field; this
    # also confirms there's no cross-table bleed of products.sku description.
    assert "sku" not in inventory_paths

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir / "odcs_synthetic_ecommerce_mces_golden.json",
    )
