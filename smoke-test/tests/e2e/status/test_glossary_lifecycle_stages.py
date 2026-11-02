"""
Smoke tests for the full lifecycle governance workflow on GlossaryTerm entities.

Exercises the bootstrapped glossary stages (DRAFT, IN_REVIEW, REJECTED, DEPRECATED, ARCHIVED)
defined in bootstrap_mcps/lifecycle-stages.yaml.

Governance flow:
    DRAFT → IN_REVIEW → active (clear stage) → DEPRECATED → ARCHIVED
    DRAFT → IN_REVIEW → REJECTED → IN_REVIEW → active (rejection/resubmit path)

The active/approved state is represented by lifecycleStage=null (clearing the stage).
Requires a DataHub instance started after the lifecycle-stages bootstrap (v3) has been applied.
"""

from __future__ import annotations

import logging
import uuid
from typing import List, Optional

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import GlossaryTermInfoClass, StatusClass
from tests.e2e.utils import with_test_retry
from utilities.consistency_utils import wait_for_writes_to_sync
from utilities.domains import Domain

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.domain(Domain.CATALOG)

# Lifecycle stage URNs (bootstrapped via lifecycle-stages.yaml v2)
DRAFT_URN = "urn:li:lifecycleStageType:DRAFT"
IN_REVIEW_URN = "urn:li:lifecycleStageType:IN_REVIEW"
REJECTED_URN = "urn:li:lifecycleStageType:REJECTED"
DEPRECATED_URN = "urn:li:lifecycleStageType:DEPRECATED"
ARCHIVED_URN = "urn:li:lifecycleStageType:ARCHIVED"


# ── Helpers ───────────────────────────────────────────────────────────────────


def _unique_term() -> tuple[str, str]:
    """Return (name, urn) for a collision-free glossary term."""
    name = f"SmokeTestLifecycle{uuid.uuid4().hex[:8]}"
    return name, f"urn:li:glossaryTerm:{name}"


def _create_term(graph_client: DataHubGraph, urn: str, name: str) -> None:
    graph_client.emit(
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=GlossaryTermInfoClass(
                definition=f"Smoke test term for lifecycle governance — {name}",
                name=name,
                termSource="INTERNAL",
            ),
        )
    )


def _set_stage(graph_client: DataHubGraph, urn: str, stage_urn: Optional[str]) -> None:
    """Emit a Status aspect to set (or clear) the lifecycleStage."""
    graph_client.emit(
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=StatusClass(removed=False, lifecycleStage=stage_urn),
        )
    )


def _get_stage(graph_client: DataHubGraph, urn: str) -> Optional[str]:
    """Return the current lifecycleStage URN, or None when the term is active."""
    status = graph_client.get_aspect(urn, StatusClass)
    return status.lifecycleStage if status is not None else None


def _search_default(graph_client: DataHubGraph, name: str) -> List[str]:
    """Default GLOSSARY_TERM search — hidden stages (DRAFT, IN_REVIEW, REJECTED, ARCHIVED) excluded."""
    result = graph_client.execute_graphql(
        """
        query SearchTerms($input: SearchInput!) {
            search(input: $input) {
                total
                searchResults { entity { urn } }
            }
        }
        """,
        variables={
            "input": {
                "type": "GLOSSARY_TERM",
                "query": name,
                "count": 50,
                "searchFlags": {"skipCache": True},
            }
        },
    )
    return [r["entity"]["urn"] for r in result["search"]["searchResults"]]


def _search_by_stage(
    graph_client: DataHubGraph, name: str, stage_urn: str
) -> List[str]:
    """Explicit stage filter — bypasses the default hidden-stage exclusion."""
    result = graph_client.execute_graphql(
        """
        query SearchTermsByStage($input: SearchInput!) {
            search(input: $input) {
                total
                searchResults { entity { urn } }
            }
        }
        """,
        variables={
            "input": {
                "type": "GLOSSARY_TERM",
                "query": name,
                "count": 50,
                "orFilters": [
                    {"and": [{"field": "lifecycleStage", "values": [stage_urn]}]}
                ],
                "searchFlags": {"skipCache": True},
            }
        },
    )
    return [r["entity"]["urn"] for r in result["search"]["searchResults"]]


# ── Tests ─────────────────────────────────────────────────────────────────────


class TestGlossaryTermLifecycleStages:
    """End-to-end governance workflow tests for GlossaryTerm lifecycle stages."""

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self, graph_client: DataHubGraph):
        self.graph_client = graph_client
        self.created_urns: List[str] = []
        yield
        for urn in self.created_urns:
            try:
                graph_client.hard_delete_entity(urn)
            except Exception as exc:
                logger.warning("Cleanup failed for %s: %s", urn, exc)

    def _new_term(self) -> tuple[str, str]:
        """Create a fresh glossary term and register it for cleanup."""
        name, urn = _unique_term()
        _create_term(self.graph_client, urn, name)
        self.created_urns.append(urn)
        wait_for_writes_to_sync()
        return name, urn

    # ── Happy-path governance flow ────────────────────────────────────────────

    @pytest.mark.p0
    def test_full_governance_flow(self):
        """
        DRAFT → IN_REVIEW → active → DEPRECATED → ARCHIVED → active (re-activate).

        Search visibility at each stage:
        - DRAFT:      hidden (hideInSearch=true)
        - IN_REVIEW:  hidden (hideInSearch=true)
        - active:     visible
        - DEPRECATED: visible with deprecated label (hideInSearch=false)
        - ARCHIVED:   hidden (hideInSearch=true)
        - re-active:  visible again (clearing stage is always allowed)
        """
        name, urn = self._new_term()

        # 1. DRAFT — hidden from default search
        _set_stage(self.graph_client, urn, DRAFT_URN)
        wait_for_writes_to_sync()

        @with_test_retry(max_attempts=12)
        def _draft_hidden():
            assert urn not in _search_default(self.graph_client, name), (
                "DRAFT term should be hidden from default search"
            )

        _draft_hidden()
        assert _get_stage(self.graph_client, urn) == DRAFT_URN
        logger.info("DRAFT: term hidden from default search")

        # 2. IN_REVIEW — still hidden
        _set_stage(self.graph_client, urn, IN_REVIEW_URN)
        wait_for_writes_to_sync()

        @with_test_retry(max_attempts=12)
        def _in_review_hidden():
            assert urn not in _search_default(self.graph_client, name), (
                "IN_REVIEW term should be hidden from default search"
            )

        _in_review_hidden()
        assert _get_stage(self.graph_client, urn) == IN_REVIEW_URN
        logger.info("IN_REVIEW: term still hidden from default search")

        # 3. Approve — clear stage → active, visible
        _set_stage(self.graph_client, urn, None)
        wait_for_writes_to_sync()

        @with_test_retry(max_attempts=12)
        def _active_visible():
            assert urn in _search_default(self.graph_client, name), (
                "Active/approved term should appear in default search"
            )

        _active_visible()
        assert _get_stage(self.graph_client, urn) is None
        logger.info("Active (approved): term visible in default search")

        # 4. DEPRECATED — still visible in default search
        _set_stage(self.graph_client, urn, DEPRECATED_URN)
        wait_for_writes_to_sync()

        @with_test_retry(max_attempts=12)
        def _deprecated_visible():
            assert urn in _search_default(self.graph_client, name), (
                "DEPRECATED term should remain visible in default search"
            )

        _deprecated_visible()
        assert _get_stage(self.graph_client, urn) == DEPRECATED_URN
        logger.info(
            "DEPRECATED: term visible in default search (with deprecated badge)"
        )

        # 5. ARCHIVED — hidden from default search
        _set_stage(self.graph_client, urn, ARCHIVED_URN)
        wait_for_writes_to_sync()

        @with_test_retry(max_attempts=12)
        def _archived_hidden():
            assert urn not in _search_default(self.graph_client, name), (
                "ARCHIVED term should be hidden from default search"
            )

        _archived_hidden()
        assert _get_stage(self.graph_client, urn) == ARCHIVED_URN
        logger.info("ARCHIVED: term hidden from default search")

        # 6. Re-activate — clearing stage is always allowed
        _set_stage(self.graph_client, urn, None)
        wait_for_writes_to_sync()

        @with_test_retry(max_attempts=12)
        def _reactivated_visible():
            assert urn in _search_default(self.graph_client, name), (
                "Re-activated term should appear in default search"
            )

        _reactivated_visible()
        assert _get_stage(self.graph_client, urn) is None
        logger.info("Re-activated: term visible in default search")

    # ── Rejection / resubmit branch ───────────────────────────────────────────

    def test_rejection_and_resubmit(self):
        """
        DRAFT → IN_REVIEW → REJECTED → IN_REVIEW (resubmit) → active.

        REJECTED is hidden from default search.
        Authors can resubmit directly from REJECTED to IN_REVIEW without returning to DRAFT.
        """
        name, urn = self._new_term()

        _set_stage(self.graph_client, urn, DRAFT_URN)
        wait_for_writes_to_sync()
        _set_stage(self.graph_client, urn, IN_REVIEW_URN)
        wait_for_writes_to_sync()
        _set_stage(self.graph_client, urn, REJECTED_URN)
        wait_for_writes_to_sync()

        @with_test_retry(max_attempts=12)
        def _rejected_hidden():
            assert urn not in _search_default(self.graph_client, name), (
                "REJECTED term should be hidden from default search"
            )

        _rejected_hidden()
        assert _get_stage(self.graph_client, urn) == REJECTED_URN
        logger.info("REJECTED: term hidden from default search")

        # Resubmit: REJECTED → IN_REVIEW
        _set_stage(self.graph_client, urn, IN_REVIEW_URN)
        wait_for_writes_to_sync()
        assert _get_stage(self.graph_client, urn) == IN_REVIEW_URN
        logger.info("Resubmitted: REJECTED → IN_REVIEW")

        # Approve: clear stage → active
        _set_stage(self.graph_client, urn, None)
        wait_for_writes_to_sync()

        @with_test_retry(max_attempts=12)
        def _approved_visible():
            assert urn in _search_default(self.graph_client, name), (
                "Term approved after rejection/resubmit should appear in default search"
            )

        _approved_visible()
        assert _get_stage(self.graph_client, urn) is None
        logger.info("Approved after rejection/resubmit: term visible in default search")

    # ── Explicit filter bypasses hidden exclusion ─────────────────────────────

    def test_explicit_stage_filter_bypasses_hidden_exclusion(self):
        """
        Review UIs can discover hidden terms by filtering explicitly on lifecycleStage.

        The ES search layer skips the default mustNot exclusion when the caller
        already supplies a lifecycleStage filter (ESPredicateUtils logic).
        """
        name, urn = self._new_term()
        _set_stage(self.graph_client, urn, DRAFT_URN)
        wait_for_writes_to_sync()

        # Default search: hidden
        @with_test_retry(max_attempts=12)
        def _hidden_in_default():
            assert urn not in _search_default(self.graph_client, name), (
                "DRAFT term should be hidden from default search"
            )

        _hidden_in_default()

        # Explicit filter: discoverable
        @with_test_retry(max_attempts=12)
        def _visible_with_filter():
            assert urn in _search_by_stage(self.graph_client, name, DRAFT_URN), (
                "DRAFT term should be discoverable via explicit lifecycleStage filter"
            )

        _visible_with_filter()
        logger.info("DRAFT term discoverable via explicit lifecycleStage filter")
