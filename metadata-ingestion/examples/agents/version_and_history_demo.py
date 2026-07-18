"""Seed agent versioning + change history for the demo.

Two things:
1. Groups three versions of the FX Risk Scoring Agent (v1.0, v1.1, v2.0) into a
   single VersionSet so the entity profile shows a version picker. v2.0 is the
   existing ``fx-risk-scoring-agent`` (kept as latest); v1.0/v1.1 are lightweight
   sibling entities representing the agent's history.
2. Applies a sequence of edits to the latest agent so the change-history timeline
   has real diffs to show (tags changed, owner added, eval score bumped, doc added).

Usage::

    export DATAHUB_GMS_URL=http://localhost:8080   # GMS, not the frontend
    # export DATAHUB_GMS_TOKEN=<token>             # if auth is enabled
    python version_and_history_demo.py
"""

from __future__ import annotations

import logging
import time
from typing import List, Tuple

from datahub.emitter.mce_builder import make_group_urn, make_tag_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph, get_default_graph
from datahub.metadata.schema_classes import (
    AuditStampClass,
    GlobalTagsClass,
    InstitutionalMemoryClass,
    InstitutionalMemoryMetadataClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    StructuredPropertiesClass,
    StructuredPropertyValueAssignmentClass,
    TagAssociationClass,
    TagPropertiesClass,
    VersioningSchemeClass,
    VersionPropertiesClass,
    VersionSetPropertiesClass,
    VersionTagClass,
)

logger = logging.getLogger(__name__)

BASE_AGENT = "urn:li:aiAgent:fx-risk-scoring-agent"  # the latest (v2.0)
V10_AGENT = "urn:li:aiAgent:fx-risk-scoring-agent-v1.0"
V11_AGENT = "urn:li:aiAgent:fx-risk-scoring-agent-v1.1"
VERSION_SET = "urn:li:versionSet:(fx-risk-scoring-agent,aiAgent)"

OWNER_GROUP = make_group_urn("quant-risk-team")
EVAL_SCORE_PROP = "urn:li:structuredProperty:io.acryl.agent.evalScore"
FRAMEWORK_PROP = "urn:li:structuredProperty:io.acryl.agent.framework"
VERSION_PROP = "urn:li:structuredProperty:io.acryl.agent.version"
WEEKLY_PROP = "urn:li:structuredProperty:io.acryl.agent.weeklyInvocations"
P95_PROP = "urn:li:structuredProperty:io.acryl.agent.p95LatencyMs"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _audit() -> AuditStampClass:
    return AuditStampClass(time=_now_ms(), actor="urn:li:corpuser:datahub")


def _emit(graph: DataHubGraph, urn: str, aspect: object) -> None:
    graph.emit(MetadataChangeProposalWrapper(entityUrn=urn, aspect=aspect))


def seed_prior_versions(graph: DataHubGraph) -> None:
    """Create the v1.0 / v1.1 sibling agents (lightweight) with agent info."""
    from datahub.api.entities.agent.agent import Agent

    Agent(
        id="fx-risk-scoring-agent-v1.0",
        name="FX Risk Scoring Agent",
        description=(
            "v1.0 — initial release. Scored FX risk from market data only, "
            "single-currency, no per-order exposure."
        ),
    ).emit(graph)
    Agent(
        id="fx-risk-scoring-agent-v1.1",
        name="FX Risk Scoring Agent",
        description=(
            "v1.1 — added open-position context and currency-pair risk. "
            "Superseded by v2.0 (per-order exposure + forecasting)."
        ),
    ).emit(graph)
    logger.info("Created prior version entities v1.0 / v1.1")


def link_versions(graph: DataHubGraph) -> None:
    """Link the three agents into one VersionSet (v2.0 is latest).

    Sets ``isLatest`` on the highest version and writes the VersionSet's
    ``latest`` pointer so the profile version switcher marks v2.0 as current
    (the native ``versionProperties`` path — no structured property involved).
    """
    versions: List[Tuple[str, str, str, str]] = [
        (V10_AGENT, "v1.0", "00000001", "Initial release."),
        (V11_AGENT, "v1.1", "00000002", "Added open positions + currency-pair risk."),
        (
            BASE_AGENT,
            "v2.0",
            "00000003",
            "Per-order exposure + forecasting; current production.",
        ),
    ]
    # `isLatest` is a computed field: the VersionProperties side-effect only
    # promotes a version (and demotes the previous latest) when a *higher*
    # sortId is linked while the version set already points at the prior latest.
    # We therefore link versions oldest-first and advance the version set's
    # `latest` pointer after each one, with a short pause so the side-effect
    # reads committed state rather than a stale in-batch snapshot. Emitting all
    # versions first and setting `latest` once at the end races the side-effect
    # and leaves every version marked latest.
    for urn, tag, sort_id, comment in versions:
        _emit(
            graph,
            urn,
            VersionPropertiesClass(
                versionSet=VERSION_SET,
                version=VersionTagClass(versionTag=tag),
                sortId=sort_id,
                versioningScheme=VersioningSchemeClass.LEXICOGRAPHIC_STRING,
                comment=comment,
                metadataCreatedTimestamp=_audit(),
            ),
        )
        _emit(
            graph,
            VERSION_SET,
            VersionSetPropertiesClass(
                latest=urn,
                versioningScheme=VersioningSchemeClass.LEXICOGRAPHIC_STRING,
            ),
        )
        logger.info("Linked %s as %s (latest) into %s", urn, tag, VERSION_SET)
        time.sleep(2)


# Display name + color for each tag the demo applies. Without a tagProperties
# aspect the tag entity doesn't exist, so the UI falls back to rendering the raw
# URN — materialize them so they show as named, colored chips.
TAG_DEFS = {
    "production": ("Production", "#54A954"),
    "experimental": ("Experimental", "#EAB308"),
    "reliability-high": ("Reliability: High", "#3B82F6"),
    "ga": ("GA", "#8B5CF6"),
}


def materialize_tags(graph: DataHubGraph) -> None:
    """Create the tag entities (name + color) so they render as chips, not URNs."""
    for tag_id, (name, color) in TAG_DEFS.items():
        graph.emit(
            MetadataChangeProposalWrapper(
                entityUrn=make_tag_urn(tag_id),
                aspect=TagPropertiesClass(name=name, colorHex=color),
            )
        )
    logger.info("Materialized %d tags", len(TAG_DEFS))


def _tags(*names: str) -> GlobalTagsClass:
    return GlobalTagsClass(
        tags=[TagAssociationClass(tag=make_tag_urn(n)) for n in names]
    )


def _structured_props(
    *, eval_score: float, weekly: int, p95: int
) -> StructuredPropertiesClass:
    return StructuredPropertiesClass(
        properties=[
            StructuredPropertyValueAssignmentClass(
                propertyUrn=FRAMEWORK_PROP, values=["LangChain"]
            ),
            StructuredPropertyValueAssignmentClass(
                propertyUrn=VERSION_PROP, values=["v0.2"]
            ),
            StructuredPropertyValueAssignmentClass(
                propertyUrn=EVAL_SCORE_PROP, values=[eval_score]
            ),
            StructuredPropertyValueAssignmentClass(
                propertyUrn=WEEKLY_PROP, values=[float(weekly)]
            ),
            StructuredPropertyValueAssignmentClass(
                propertyUrn=P95_PROP, values=[float(p95)]
            ),
        ]
    )


def seed_change_history(graph: DataHubGraph) -> None:
    """Apply a sequence of edits to the latest agent so the timeline shows diffs.

    Each emit creates a new aspect version; the timeline service diffs consecutive
    versions. A short sleep keeps the transactions visually distinct.
    """
    steps = [
        ("tag: + experimental", BASE_AGENT, _tags("production", "experimental")),
        (
            "owner: + data-platform-team",
            BASE_AGENT,
            OwnershipClass(
                owners=[
                    OwnerClass(
                        owner=OWNER_GROUP, type=OwnershipTypeClass.TECHNICAL_OWNER
                    ),
                    OwnerClass(
                        owner=make_group_urn("data-platform-team"),
                        type=OwnershipTypeClass.TECHNICAL_OWNER,
                    ),
                ]
            ),
        ),
        (
            "eval score 0.94 -> 0.96, p95 320 -> 280",
            BASE_AGENT,
            _structured_props(eval_score=0.96, weekly=18000, p95=280),
        ),
        (
            "doc: + runbook link",
            BASE_AGENT,
            InstitutionalMemoryClass(
                elements=[
                    InstitutionalMemoryMetadataClass(
                        url="https://example.com/runbooks/fx-risk-scoring-agent",
                        description="FX Risk Scoring Agent runbook",
                        createStamp=_audit(),
                    )
                ]
            ),
        ),
        (
            "tag: experimental -> ga",
            BASE_AGENT,
            _tags("production", "reliability-high", "ga"),
        ),
    ]
    for label, urn, aspect in steps:
        _emit(graph, urn, aspect)
        logger.info("history step: %s", label)
        time.sleep(2)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    graph = get_default_graph()
    materialize_tags(graph)
    seed_prior_versions(graph)
    link_versions(graph)
    seed_change_history(graph)
    print("\nDone.")
    print(f"  VersionSet: {VERSION_SET} (v1.0, v1.1, v2.0; latest = v2.0)")
    print(f"  Change history applied to {BASE_AGENT}")


if __name__ == "__main__":
    main()
