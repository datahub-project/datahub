"""Ingest an ODCS v3.2 contract into a live GMS and read the result back.

The connector's integration tests use a file sink; this checks what only a
live GMS can: that the aspects are accepted, that the physical table is linked
to its logical model, and that a re-run merges into AI context a user edited.
"""

import logging
import pathlib
from typing import Any, Dict, List

import pytest

from datahub.emitter.mce_builder import make_dataset_urn, make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.run.pipeline import Pipeline
from datahub.metadata.schema_classes import (
    AiContextClass,
    DatasetPropertiesClass,
    DeprecationClass,
    LogicalParentClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
)
from tests.e2e.utils import delete_urn, execute_graphql, unique_suffix, with_test_retry
from utilities.consistency_utils import wait_for_writes_to_sync
from utilities.domains import Domain

pytestmark = pytest.mark.domain(Domain.INGESTION)

logger = logging.getLogger(__name__)

_CONTRACT = """\
apiVersion: v3.2.0
kind: DataContract
id: {contract_id}
name: Smoke orders
version: 1.0.0
status: active
context: Use for order status reporting.
servers:
  - server: prod
    type: postgres
    host: pg.example.internal
    port: 5432
    database: {database}
    schema: public
schema:
  - name: orders
    physicalName: orders
    description: One row per order.
    properties:
      - name: order_id
        logicalType: string
        primaryKey: true
        required: true
        quality:
          - type: library
            metric: nullValues
            mustBe: 0
      - name: status
        logicalType: string
        enum:
          - value: open
          - value: shipped
        synonyms: {status_synonyms}
      - name: legacy_channel
        logicalType: string
        deprecated: true
      - name: avg_amount
        logicalType: number
        semanticType: measure
        transformLogic: AVG(amount)
"""

_ASSETS_QUERY = """
query odcsAssets($urn: String!) {
  dataset(urn: $urn) {
    contract { urn }
    assertions(start: 0, count: 20) { total assertions { urn } }
  }
}
"""


def _run_odcs(auth_session: Any, contract_file: pathlib.Path) -> None:
    pipeline = Pipeline.create(
        {
            "source": {"type": "odcs", "config": {"path": str(contract_file)}},
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


def _seed_physical_table(graph_client: DataHubGraph, urn: str) -> None:
    fields = [
        SchemaFieldClass(
            fieldPath=name,
            type=SchemaFieldDataTypeClass(type=StringTypeClass()),
            nativeDataType="varchar",
        )
        for name in ("order_id", "status", "legacy_channel")
    ]
    for aspect in (
        DatasetPropertiesClass(name="orders"),
        SchemaMetadataClass(
            schemaName="orders",
            platform="urn:li:dataPlatform:postgres",
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=fields,
        ),
    ):
        graph_client.emit(MetadataChangeProposalWrapper(entityUrn=urn, aspect=aspect))
    wait_for_writes_to_sync()


@with_test_retry()
def _assets(auth_session: Any, logical_urn: str) -> Dict[str, Any]:
    dataset = execute_graphql(auth_session, _ASSETS_QUERY, {"urn": logical_urn})[
        "data"
    ]["dataset"]
    # nullValues rule + the schema-compliance assertion.
    assert dataset["assertions"]["total"] >= 2
    assert dataset["contract"] is not None
    return dataset


def test_odcs_v32_contract_end_to_end(
    auth_session: Any, graph_client: DataHubGraph, tmp_path: pathlib.Path
) -> None:
    suffix = unique_suffix()
    contract_id = f"smoke-odcs-{suffix}"
    database = f"smoke_db_{suffix}"
    physical_urn = make_dataset_urn("postgres", f"{database}.public.orders")
    logical_urn = make_dataset_urn("odcs", f"{contract_id}.orders")
    status_field = make_schema_field_urn(logical_urn, "status")
    contract_file = tmp_path / "orders.odcs.yaml"
    cleanup: List[str] = [physical_urn, logical_urn]
    try:
        _seed_physical_table(graph_client, physical_urn)
        contract_file.write_text(
            _CONTRACT.format(
                contract_id=contract_id,
                database=database,
                status_synonyms="[order state, lifecycle]",
            ),
            encoding="utf-8",
        )
        _run_odcs(auth_session, contract_file)

        properties = graph_client.get_aspect(logical_urn, DatasetPropertiesClass)
        assert properties is not None
        assert properties.customProperties["odcs.apiVersion"] == "v3.2.0"
        assert "Guidance for AI and analysts" in (properties.description or "")
        schema = graph_client.get_aspect(logical_urn, SchemaMetadataClass)
        assert schema is not None
        # The measure stays in the logical schema, its derivation in the description.
        avg_amount = next(f for f in schema.fields if f.fieldPath == "avg_amount")
        assert "AVG(amount)" in (avg_amount.description or "")
        status = next(f for f in schema.fields if f.fieldPath == "status")
        assert "`shipped`" in (status.description or "")

        deprecation = graph_client.get_aspect(
            make_schema_field_urn(logical_urn, "legacy_channel"), DeprecationClass
        )
        assert deprecation is not None and deprecation.deprecated
        ai_context = graph_client.get_aspect(status_field, AiContextClass)
        assert ai_context is not None
        assert ai_context.synonyms == ["order state", "lifecycle"]

        parent = graph_client.get_aspect(physical_urn, LogicalParentClass)
        assert parent is not None and parent.parent is not None
        assert parent.parent.destinationUrn == logical_urn

        assets = _assets(auth_session, logical_urn)
        cleanup.append(assets["contract"]["urn"])
        cleanup.extend(a["urn"] for a in assets["assertions"]["assertions"])

        # A user adds instructions in DataHub; the contract then drops every
        # synonym. The re-run clears the synonyms and keeps the instructions.
        ai_context.instructions = "Use for order status reporting."
        graph_client.emit(
            MetadataChangeProposalWrapper(entityUrn=status_field, aspect=ai_context)
        )
        wait_for_writes_to_sync()
        contract_file.write_text(
            _CONTRACT.format(
                contract_id=contract_id, database=database, status_synonyms="[]"
            ),
            encoding="utf-8",
        )
        _run_odcs(auth_session, contract_file)

        rerun = graph_client.get_aspect(status_field, AiContextClass)
        assert rerun is not None
        assert rerun.synonyms == []
        assert rerun.instructions == "Use for order status reporting."
    finally:
        for urn in cleanup:
            try:
                delete_urn(graph_client, urn)
            except Exception:
                logger.warning("cleanup failed for %s", urn, exc_info=True)
