"""Two-run stateful soft-delete behaviour for the ODCS source.

Guards the exact invariant the `is_primary_source=False` design protects:
when a contract drops a schema entry, the logical `odcs` dataset (and its
assertions) for the dropped entry are soft-deleted, while the physical dataset
and its `logicalParent` link are NEVER checkpointed and therefore can never be
soft-deleted. A regression that flipped those flags to primary would soft-delete
a customer's physical dataset — the flag-level wiring tests would still pass, so
this behavioural test is the real guard.
"""

import json
import pathlib
from typing import Any, Dict, List
from unittest.mock import patch

import time_machine

from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from tests.test_helpers.state_helpers import (
    get_current_checkpoint_from_pipeline,
    mock_datahub_graph,  # noqa: F401  (pytest fixture)
    run_and_get_pipeline,
    validate_all_providers_have_committed_successfully,
)

FROZEN_TIME = "2024-01-15 12:00:00"
GMS_SERVER = "http://localhost:8080"

_ODCS_PLATFORM_FRAGMENT = "urn:li:dataPlatform:odcs"
_PHYSICAL_PLATFORM_FRAGMENT = "urn:li:dataPlatform:postgres"


def _contract_body(schema_entries: str) -> str:
    return f"""\
apiVersion: v3.1.0
kind: DataContract
id: stateful-contract
version: 1.0.0
status: active
servers:
  - server: prod-postgres
    type: postgres
    host: postgres.example.internal
    port: 5432
    database: appdb
    schema: public
schema:
{schema_entries}
"""


_SCHEMA_ORDERS = """\
  - name: orders
    physicalName: orders
    properties:
      - name: id
        logicalType: number
        quality:
          - id: orders_id_no_nulls
            metric: nullValues
            mustBe: 0
"""

_SCHEMA_SHIPMENTS = """\
  - name: shipments
    physicalName: shipments
    properties:
      - name: id
        logicalType: number
        quality:
          - id: shipments_id_no_nulls
            metric: nullValues
            mustBe: 0
"""


def _pipeline_config(
    contract_dir: pathlib.Path, output_path: pathlib.Path
) -> Dict[str, Any]:
    return {
        "run_id": "odcs-stateful-run",
        "pipeline_name": "odcs-stateful-pipeline",
        "source": {
            "type": "odcs",
            "config": {
                "path": str(contract_dir),
                "stateful_ingestion": {
                    "enabled": True,
                    "remove_stale_metadata": True,
                    "fail_safe_threshold": 100.0,
                    "state_provider": {
                        "type": "datahub",
                        "config": {"datahub_api": {"server": GMS_SERVER}},
                    },
                },
            },
        },
        "sink": {"type": "file", "config": {"filename": str(output_path)}},
    }


def _status_removed_urns(output_path: pathlib.Path) -> List[str]:
    removed: List[str] = []
    for record in json.loads(output_path.read_text()):
        proposal = record.get("proposedSnapshot") or record
        aspect_name = proposal.get("aspectName")
        if aspect_name != "status":
            continue
        aspect = proposal.get("aspect", {})
        payload = aspect.get("json") or aspect.get("value")
        if isinstance(payload, str):
            payload = json.loads(payload)
        if isinstance(payload, dict) and payload.get("removed") is True:
            removed.append(proposal["entityUrn"])
    return removed


@time_machine.travel(FROZEN_TIME, tick=False)
def test_stateful_two_run_soft_deletes_dropped_logical_dataset_only(
    tmp_path: pathlib.Path,
    mock_datahub_graph,  # noqa: F811
) -> None:
    contract_dir = tmp_path / "contracts"
    contract_dir.mkdir()
    contract_file = contract_dir / "c.odcs.yaml"

    out1 = tmp_path / "run1.json"
    out2 = tmp_path / "run2.json"

    with patch(
        "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
        mock_datahub_graph,
    ) as mock_checkpoint:
        mock_checkpoint.return_value = mock_datahub_graph

        # Run 1: both schema entries present.
        contract_file.write_text(
            _contract_body(_SCHEMA_ORDERS + _SCHEMA_SHIPMENTS), encoding="utf-8"
        )
        pipeline1 = run_and_get_pipeline(_pipeline_config(contract_dir, out1))
        checkpoint1 = get_current_checkpoint_from_pipeline(pipeline1)

        # Run 2: `shipments` dropped.
        contract_file.write_text(_contract_body(_SCHEMA_ORDERS), encoding="utf-8")
        pipeline2 = run_and_get_pipeline(_pipeline_config(contract_dir, out2))
        checkpoint2 = get_current_checkpoint_from_pipeline(pipeline2)

    assert checkpoint1 and checkpoint1.state
    assert checkpoint2 and checkpoint2.state
    validate_all_providers_have_committed_successfully(pipeline1, expected_providers=1)
    validate_all_providers_have_committed_successfully(pipeline2, expected_providers=1)

    state1 = checkpoint1.state
    state2 = checkpoint2.state
    assert isinstance(state1, GenericCheckpointState)
    assert isinstance(state2, GenericCheckpointState)

    # The physical dataset is platform-of-record for another source, so it must
    # never enter ODCS's checkpoint — otherwise stale removal could delete it.
    assert not any(_PHYSICAL_PLATFORM_FRAGMENT in urn for urn in state1.urns)
    assert not any(_PHYSICAL_PLATFORM_FRAGMENT in urn for urn in state2.urns)

    # Exactly the dropped logical dataset (and its assertions) fall out of the
    # second checkpoint. No physical dataset is ever in the checkpoint, so none
    # can fall out; the retained `orders` entry stays.
    difference = set(state1.get_urns_not_in(type="*", other_checkpoint_state=state2))
    assert difference, "expected the dropped entry's URNs to leave the checkpoint"
    assert not any(_PHYSICAL_PLATFORM_FRAGMENT in urn for urn in difference)
    assert any(
        _ODCS_PLATFORM_FRAGMENT in urn and "shipments" in urn for urn in difference
    )
    assert not any("orders" in urn for urn in difference)

    # Run 2 emits Status(removed=true) for the dropped logical dataset, and never
    # for the physical dataset.
    removed = _status_removed_urns(out2)
    assert any(_ODCS_PLATFORM_FRAGMENT in urn and "shipments" in urn for urn in removed)
    assert not any(_PHYSICAL_PLATFORM_FRAGMENT in urn for urn in removed)
