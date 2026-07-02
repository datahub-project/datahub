"""Integration / golden-file tests for the ODCS source (logical-model architecture)."""

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


def _read_mces(path: pathlib.Path) -> List[Dict[str, Any]]:
    return json.loads(path.read_text())


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
def test_odcs_minimal_no_binding(
    pytestconfig: pytest.Config, tmp_path: pathlib.Path
) -> None:
    """No server mapping: only the logical `odcs` dataset is emitted — no
    physical logicalParent link and no assertions (strict gating)."""
    test_resources_dir = pytestconfig.rootpath / "tests/integration/odcs"
    output_path = _run_pipeline(
        tmp_path=tmp_path,
        test_resources_dir=test_resources_dir,
        fixture="odcs_minimal.odcs.yaml",
        output_name="odcs_minimal_no_binding_mces.json",
        server_mappings=[],
    )
    mces = _read_mces(output_path)
    assert not any(m.get("aspectName") == "logicalParent" for m in mces)
    assert not any(m.get("entityType") == "assertion" for m in mces)
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir / "odcs_minimal_no_binding_mces_golden.json",
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


def _schema_field_paths_by_logical_dataset(
    mces: List[Dict[str, Any]],
) -> Dict[str, List[str]]:
    """For each logical `odcs` dataset URN, return its schemaMetadata field paths."""
    out: Dict[str, List[str]] = {}
    for mce in mces:
        if mce.get("aspectName") != "schemaMetadata":
            continue
        urn = mce["entityUrn"]
        if "urn:li:dataPlatform:odcs" not in urn:
            continue
        fields = mce["aspect"]["json"].get("fields", [])
        out.setdefault(urn, []).extend(f["fieldPath"] for f in fields)
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

    # ---- Field-bleed regression guard: per-table fields attach to the right
    # logical dataset, not smeared across all four. ----
    mces = _read_mces(output_path)
    paths_by_urn = _schema_field_paths_by_logical_dataset(mces)

    logical_urns = {
        mce["entityUrn"]
        for mce in mces
        if mce.get("entityType") == "dataset"
        and "urn:li:dataPlatform:odcs" in mce["entityUrn"]
    }
    expected_tables = {"customers", "orders", "products", "inventory"}
    matched = {t for t in expected_tables if any(f".{t}," in u for u in logical_urns)}
    assert matched == expected_tables

    def logical_urn_for(table: str) -> str:
        return next(u for u in logical_urns if f".{table}," in u)

    customers_paths = paths_by_urn.get(logical_urn_for("customers"), [])
    orders_paths = paths_by_urn.get(logical_urn_for("orders"), [])
    products_paths = paths_by_urn.get(logical_urn_for("products"), [])
    inventory_paths = paths_by_urn.get(logical_urn_for("inventory"), [])

    # email is only on customers — no cross-table bleed.
    assert "email" in customers_paths
    assert "email" not in orders_paths
    assert "email" not in products_paths
    assert "email" not in inventory_paths

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir / "odcs_synthetic_ecommerce_mces_golden.json",
    )
