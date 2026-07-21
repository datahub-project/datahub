"""Demonstrate AI-governance patterns for agent entities.

Shows three governance primitives:

  1. **Glossary terms** — create a "Highly Confidential" term so it can be
     applied to datasets and propagated downstream.
  2. **Schema emission** — attach a real column schema to a dataset so its
     Columns tab is populated (required for column-level governance).
  3. **Incident creation** — raise a CRITICAL incident on an agent entity so
     it renders as unhealthy in the catalog.

The ``SEED_CLASSIFY_VIA_API=1`` env flag also classifies the dataset with the
glossary term and polls until propagation reaches the agent — useful for
automated / headless runs.

Usage::

    export DATAHUB_GMS_URL=http://localhost:8080   # GMS, not the frontend
    # export DATAHUB_GMS_TOKEN=<token>             # if auth is enabled
    python register_agent_governance.py
"""

from __future__ import annotations

import logging
import os
import time

from datahub.emitter.mce_builder import (
    get_sys_time,
    make_dataset_urn,
    make_user_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph, get_default_graph
from datahub.metadata.schema_classes import (
    AuditStampClass,
    GlossaryTermAssociationClass,
    GlossaryTermInfoClass,
    GlossaryTermsClass,
    IncidentInfoClass,
    IncidentSourceClass,
    IncidentSourceTypeClass,
    IncidentStageClass,
    IncidentStateClass,
    IncidentStatusClass,
    IncidentTypeClass,
    NumberTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    TimeTypeClass,
)
from datahub.metadata.urns import IncidentUrn

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

AGENT_URN = "urn:li:aiAgent:fx-risk-scoring-agent"
CONSUMED_DATASET_URN = make_dataset_urn("bigquery", "fx.positions", "PROD")
TERM_URN = "urn:li:glossaryTerm:Classification.HighlyConfidential"


def _stamp() -> AuditStampClass:
    return AuditStampClass(time=get_sys_time(), actor=make_user_urn("datahub"))


def register_classification_term(graph: DataHubGraph) -> None:
    graph.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=TERM_URN,
            aspect=GlossaryTermInfoClass(
                name="Highly Confidential",
                definition=(
                    "Data whose exposure poses the highest risk; AI systems that "
                    "touch it require an approved AI risk review."
                ),
                termSource="INTERNAL",
            ),
        )
    )
    logger.info("Registered glossary term %s", TERM_URN)


def register_dataset_schema(graph: DataHubGraph) -> None:
    """Give the consumed dataset a real schema so its Columns tab isn't empty."""

    def _field(path: str, native: str, type_cls: type, desc: str) -> SchemaFieldClass:
        return SchemaFieldClass(
            fieldPath=path,
            type=SchemaFieldDataTypeClass(type=type_cls()),
            nativeDataType=native,
            description=desc,
        )

    fields = [
        _field(
            "position_id", "STRING", StringTypeClass, "Unique id of the FX position."
        ),
        _field(
            "currency_pair",
            "STRING",
            StringTypeClass,
            "Traded currency pair, e.g. EUR/USD.",
        ),
        _field(
            "notional_amount",
            "NUMERIC",
            NumberTypeClass,
            "Notional amount of the position.",
        ),
        _field(
            "book_value",
            "NUMERIC",
            NumberTypeClass,
            "Current book value in base currency.",
        ),
        _field(
            "trader_id",
            "STRING",
            StringTypeClass,
            "Owning trader (employee identifier).",
        ),
        _field(
            "as_of_date", "TIMESTAMP", TimeTypeClass, "Snapshot date of the position."
        ),
    ]
    graph.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=CONSUMED_DATASET_URN,
            aspect=SchemaMetadataClass(
                schemaName="fx.positions",
                platform="urn:li:dataPlatform:bigquery",
                version=0,
                hash="",
                platformSchema=OtherSchemaClass(rawSchema=""),
                fields=fields,
            ),
        )
    )
    logger.info(
        "Registered schema (%d columns) on %s", len(fields), CONSUMED_DATASET_URN
    )


def classify_consumed_dataset(graph: DataHubGraph) -> None:
    existing = graph.get_aspect(CONSUMED_DATASET_URN, GlossaryTermsClass)
    terms = existing.terms if existing else []
    if TERM_URN not in {t.urn for t in terms}:
        terms.append(GlossaryTermAssociationClass(urn=TERM_URN))
    graph.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=CONSUMED_DATASET_URN,
            aspect=GlossaryTermsClass(terms=terms, auditStamp=_stamp()),
        )
    )
    logger.info("Classified %s as Highly Confidential", CONSUMED_DATASET_URN)


def wait_for_propagation(graph: DataHubGraph, timeout_s: int = 90) -> None:
    """Block until the live automation propagates the term onto the agent.

    The seed does NOT write the term onto the agent — the Glossary Term
    Propagation automation does, asynchronously, once the dataset above is
    classified. We poll so the demo capture (which runs right after this seed)
    never races the propagation, and so a missing/disabled automation fails
    loudly instead of silently producing an ungoverned agent.
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        terms = graph.get_aspect(AGENT_URN, GlossaryTermsClass)
        if terms and any(t.urn == TERM_URN for t in terms.terms):
            logger.info("Automation propagated %s onto agent %s", TERM_URN, AGENT_URN)
            return
        time.sleep(3)
    raise RuntimeError(
        f"Timed out after {timeout_s}s waiting for {TERM_URN} to propagate onto "
        f"{AGENT_URN}. Is the 'Glossary Term Propagation' automation enabled and "
        f"running the agent-aware TermPropagationAction?"
    )


def raise_agent_incident(graph: DataHubGraph) -> None:
    incident_urn = str(IncidentUrn("fx-agent-highrisk-data"))
    stamp = _stamp()
    graph.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=incident_urn,
            aspect=IncidentInfoClass(
                type=IncidentTypeClass.OPERATIONAL,
                title="Agent connected to data classified Highly Confidential",
                description=(
                    "FX Risk Scoring Agent consumes a dataset classified Highly "
                    "Confidential without an approved AI risk review."
                ),
                entities=[AGENT_URN],
                status=IncidentStatusClass(
                    state=IncidentStateClass.ACTIVE,
                    stage=IncidentStageClass.TRIAGE,
                    message="Auto-raised: high-risk data classification propagated to agent.",
                    lastUpdated=stamp,
                ),
                priority=0,  # CRITICAL
                source=IncidentSourceClass(type=IncidentSourceTypeClass.MANUAL),
                created=stamp,
            ),
        )
    )
    logger.info("Raised incident %s on %s", incident_urn, AGENT_URN)


def main() -> None:
    graph = get_default_graph()
    register_classification_term(graph)
    register_dataset_schema(graph)

    # The agent starts HEALTHY. The classification of fx.positions is performed
    # live in the UI during the demo capture (a human clicking "Add Term"), the
    # automation propagates it onto the agent, and only THEN is the incident
    # raised — so the agent visibly turns unhealthy after propagation, not from
    # the start. Set SEED_CLASSIFY_VIA_API=1 to run that whole sequence here
    # instead (e.g. for a headless run with no capture).
    if os.environ.get("SEED_CLASSIFY_VIA_API") == "1":
        classify_consumed_dataset(graph)
        wait_for_propagation(graph)
        raise_agent_incident(graph)


if __name__ == "__main__":
    main()
