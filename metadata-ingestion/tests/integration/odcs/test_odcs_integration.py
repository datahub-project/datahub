"""Integration / golden-file tests for the ODCS source (logical-model architecture).

Pipelines here use the file sink, so no DataHub graph is available and
`verify_physical_urns_exist` degrades to emitting `logicalParent` links without
verification (fail-open) — physical binding is exercised end-to-end.
"""

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
    extra_config: Optional[Dict[str, Any]] = None,
) -> pathlib.Path:
    output_path = tmp_path / output_name
    config: Dict[str, Any] = {
        "path": str(test_resources_dir / fixture),
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
    """Platform is derived from the typed server; the override only refines
    env / platform_instance for the physical URN."""
    test_resources_dir = pytestconfig.rootpath / "tests/integration/odcs"
    output_path = _run_pipeline(
        tmp_path=tmp_path,
        test_resources_dir=test_resources_dir,
        fixture="odcs_minimal.odcs.yaml",
        output_name="odcs_minimal_mces.json",
        extra_config={
            "server_overrides": [
                {
                    "server": "prod-snowflake",
                    "env": "PROD",
                    "platform_instance": "prod",
                }
            ]
        },
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
    """An entry explicitly unbound via physical_urn_overrides gets no
    logicalParent link — but its assertions (which target the logical dataset)
    are still emitted."""
    test_resources_dir = pytestconfig.rootpath / "tests/integration/odcs"
    output_path = _run_pipeline(
        tmp_path=tmp_path,
        test_resources_dir=test_resources_dir,
        fixture="odcs_minimal.odcs.yaml",
        output_name="odcs_minimal_no_binding_mces.json",
        extra_config={
            "physical_urn_overrides": {
                "00000000-0000-0000-0000-000000000001": {"my_table": ""}
            }
        },
    )
    mces = _read_mces(output_path)
    assert not any(m.get("aspectName") == "logicalParent" for m in mces)
    # The schema-compliance assertion targets the logical dataset and is
    # emitted regardless of binding.
    assertion_infos = [m for m in mces if m.get("aspectName") == "assertionInfo"]
    assert assertion_infos
    for info in assertion_infos:
        entity = info["aspect"]["json"]["schemaAssertion"]["entity"]
        assert "urn:li:dataPlatform:odcs" in entity
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
    )
    mces = _read_mces(output_path)
    # Every assertion targets the LOGICAL dataset, never a physical URN.
    for mce in mces:
        if mce.get("aspectName") != "assertionInfo":
            continue
        aspect = mce["aspect"]["json"]
        for key in (
            "fieldAssertion",
            "volumeAssertion",
            "sqlAssertion",
            "customAssertion",
            "schemaAssertion",
        ):
            sub = aspect.get(key)
            if sub:
                assert "urn:li:dataPlatform:odcs" in sub["entity"]
    # logicalParent links point from the composed physical URNs (postgres,
    # fully qualified) to the logical datasets.
    logical_parents = [m for m in mces if m.get("aspectName") == "logicalParent"]
    assert logical_parents
    for lp in logical_parents:
        assert "urn:li:dataPlatform:postgres,customers_db.public." in lp["entityUrn"]
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir / "odcs_full_v31_mces_golden.json",
    )


@time_machine.travel(FROZEN_TIME, tick=False)
def test_odcs_full_v31_strict(
    pytestconfig: pytest.Config, tmp_path: pathlib.Path
) -> None:
    """The full fixture is spec-conformant: strict JSON-Schema validation must
    accept it and produce the same output as the lenient run."""
    test_resources_dir = pytestconfig.rootpath / "tests/integration/odcs"
    output_path = _run_pipeline(
        tmp_path=tmp_path,
        test_resources_dir=test_resources_dir,
        fixture="odcs_full_v31.odcs.yaml",
        output_name="odcs_full_v31_strict_mces.json",
        extra_config={"strict_validation": True},
    )
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir / "odcs_full_v31_mces_golden.json",
    )


@time_machine.travel(FROZEN_TIME, tick=False)
def test_odcs_v302(pytestconfig: pytest.Config, tmp_path: pathlib.Path) -> None:
    """v3.0.2 vocabulary: `rule` key with duplicateCount / validValues /
    rowCount, and mysql two-part physical naming."""
    test_resources_dir = pytestconfig.rootpath / "tests/integration/odcs"
    output_path = _run_pipeline(
        tmp_path=tmp_path,
        test_resources_dir=test_resources_dir,
        fixture="odcs_v302.odcs.yaml",
        output_name="odcs_v302_mces.json",
        extra_config={"strict_validation": True},
    )
    mces = _read_mces(output_path)
    logical_parents = [m for m in mces if m.get("aspectName") == "logicalParent"]
    assert logical_parents
    assert (
        "urn:li:dataPlatform:mysql,logistics.shipments,"
        in (logical_parents[0]["entityUrn"])
    )
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir / "odcs_v302_mces_golden.json",
    )


@time_machine.travel(FROZEN_TIME, tick=False)
def test_odcs_bitol_official_full_example(
    pytestconfig: pytest.Config, tmp_path: pathlib.Path
) -> None:
    """The upstream project's own v3.1 full example (vendored verbatim) must
    ingest cleanly under strict validation. Unlike the hand-authored fixtures,
    this file was NOT written against this source's implementation, so it
    guards against vocabulary/shape drift between the source and real-world
    ODCS documents (e.g. the v3.1 team object form)."""
    test_resources_dir = pytestconfig.rootpath / "tests/integration/odcs"
    output_path = _run_pipeline(
        tmp_path=tmp_path,
        test_resources_dir=test_resources_dir,
        fixture="odcs_bitol_full_example.odcs.yaml",
        output_name="odcs_bitol_full_example_mces.json",
        extra_config={"strict_validation": True},
    )
    mces = _read_mces(output_path)
    # The example's postgres server carries database + schema: bindings must be
    # fully qualified.
    logical_parents = [m for m in mces if m.get("aspectName") == "logicalParent"]
    assert logical_parents
    for lp in logical_parents:
        assert (
            "urn:li:dataPlatform:postgres,pypl-edw.pp_access_views."
            in (lp["entityUrn"])
        )
    # The v3.1 team OBJECT form must yield owners (dateOut members excluded).
    ownerships = [m for m in mces if m.get("aspectName") == "ownership"]
    assert ownerships
    owner_urns = {o["owner"] for o in ownerships[0]["aspect"]["json"]["owners"]}
    assert "urn:li:corpuser:daustin" in owner_urns
    assert "urn:li:corpuser:ceastwood" not in owner_urns  # dateOut in the past
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir / "odcs_bitol_full_example_mces_golden.json",
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
