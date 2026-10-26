"""Ingest ODCS v3.2 attributes as `odcs.*` structured properties into a live GMS.

GMS validates structured-property patches against their definitions, so only a
live GMS shows that the definitions are created before the values, that the
values are accepted on datasets and schema fields, and that a re-run removes a
value dropped from the contract without touching other structured properties.
"""

import logging
import pathlib
from typing import Any, Dict, List

import pytest

from datahub.emitter.mce_builder import make_dataset_urn, make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.odcs.odcs_structured_properties import SPECS
from datahub.metadata.schema_classes import (
    StructuredPropertiesClass,
    StructuredPropertyDefinitionClass,
    StructuredPropertyValueAssignmentClass,
)
from datahub.utilities.urns.structured_properties_urn import (
    make_structured_property_urn,
)
from tests.e2e.utils import delete_urn, unique_suffix
from utilities.consistency_utils import wait_for_writes_to_sync
from utilities.domains import Domain

pytestmark = pytest.mark.domain(Domain.INGESTION)

logger = logging.getLogger(__name__)

_CONTRACT = """\
apiVersion: v3.2.0
kind: DataContract
id: {contract_id}
version: 1.0.0
status: active
context: Use for revenue analysis.
schema:
  - name: orders
    synonyms:
      - synonym: Sales
    properties:
      - name: status
        logicalType: string
        enum:
          - value: open
          - value: shipped
      - name: total
        logicalType: number
{total_attributes}
"""

_MEASURE = """\
        semanticType: measure
        transformLogic: SUM(amount)
"""


def _run_odcs(auth_session: Any, contract_file: pathlib.Path) -> None:
    pipeline = Pipeline.create(
        {
            "source": {
                "type": "odcs",
                "config": {
                    "path": str(contract_file),
                    "emit_structured_properties": True,
                    # Only the structured properties are under test here.
                    "emit_assertions": False,
                    "emit_schema_assertion": False,
                    "emit_data_contract": False,
                },
            },
            "sink": {
                "type": "datahub-rest",
                "config": {
                    "server": auth_session.gms_url(),
                    "token": auth_session.gms_token(),
                },
            },
        }
    )
    pipeline.run()
    pipeline.raise_from_status()
    wait_for_writes_to_sync()


def _values(graph_client: DataHubGraph, urn: str) -> Dict[str, List[Any]]:
    aspect = graph_client.get_aspect(urn, StructuredPropertiesClass)
    return {
        p.propertyUrn: list(p.values) for p in (aspect.properties if aspect else [])
    }


def test_odcs_structured_properties_end_to_end(
    auth_session: Any, graph_client: DataHubGraph, tmp_path: pathlib.Path
) -> None:
    suffix = unique_suffix()
    contract_id = f"smoke-odcs-sp-{suffix}"
    logical_urn = make_dataset_urn("odcs", f"{contract_id}.orders")
    total_field = make_schema_field_urn(logical_urn, "total")
    other_property = make_structured_property_urn(f"smoke.odcsOther{suffix}")
    contract_file = tmp_path / "orders.odcs.yaml"
    cleanup = [
        logical_urn,
        total_field,
        make_schema_field_urn(logical_urn, "status"),
        other_property,
    ]
    try:
        # A property this source does not own, set on a field before ingestion.
        graph_client.emit(
            MetadataChangeProposalWrapper(
                entityUrn=other_property,
                aspect=StructuredPropertyDefinitionClass(
                    qualifiedName=f"smoke.odcsOther{suffix}",
                    valueType="urn:li:dataType:datahub.string",
                    entityTypes=["urn:li:entityType:datahub.schemaField"],
                ),
            )
        )
        wait_for_writes_to_sync()
        graph_client.emit(
            MetadataChangeProposalWrapper(
                entityUrn=total_field,
                aspect=StructuredPropertiesClass(
                    properties=[
                        StructuredPropertyValueAssignmentClass(
                            propertyUrn=other_property, values=["kept"]
                        )
                    ]
                ),
            )
        )
        wait_for_writes_to_sync()

        contract_file.write_text(
            _CONTRACT.format(contract_id=contract_id, total_attributes=_MEASURE),
            encoding="utf-8",
        )
        _run_odcs(auth_session, contract_file)

        for spec in SPECS:
            assert graph_client.exists(
                make_structured_property_urn(spec.qualified_name)
            )
        dataset_values = _values(graph_client, logical_urn)
        assert dataset_values[make_structured_property_urn("odcs.synonyms")] == [
            "Sales"
        ]
        assert make_structured_property_urn("odcs.context") in dataset_values
        status_values = _values(
            graph_client, make_schema_field_urn(logical_urn, "status")
        )
        assert status_values[make_structured_property_urn("odcs.enumValues")] == [
            "open",
            "shipped",
        ]
        total_values = _values(graph_client, total_field)
        assert total_values[make_structured_property_urn("odcs.semanticType")] == [
            "measure"
        ]
        assert total_values[other_property] == ["kept"]

        # The contract drops the measure attributes: the re-run removes them and
        # leaves the property this source does not own.
        contract_file.write_text(
            _CONTRACT.format(contract_id=contract_id, total_attributes=""),
            encoding="utf-8",
        )
        _run_odcs(auth_session, contract_file)

        rerun = _values(graph_client, total_field)
        assert make_structured_property_urn("odcs.semanticType") not in rerun
        assert make_structured_property_urn("odcs.transformLogic") not in rerun
        assert rerun[other_property] == ["kept"]
    finally:
        # The `odcs.*` definitions are instance-wide and shared; leave them.
        for urn in cleanup:
            try:
                delete_urn(graph_client, urn)
            except Exception:
                logger.warning("cleanup failed for %s", urn, exc_info=True)
