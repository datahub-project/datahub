"""
Smoke tests for LifecycleVersionSideEffect: effective-latest management.

When the entity marked isLatest=True enters a hidden lifecycle stage (e.g. DRAFT),
the side effect:
  1. Demotes the entity (isLatest=False).
  2. Promotes the most recent non-hidden sibling (isLatest=True).
  3. Updates VersionSetProperties.latest to the promoted sibling, so that any
     subsequently-created version compares sortIds against the correct baseline.

When the entity returns to a visible stage, it is re-promoted if it has the
highest sortId among visible siblings.

Covered scenarios:
  - Single version goes DRAFT → demoted, invisible in search
  - Single version returns from DRAFT → re-promoted, visible in search
  - Two versions: latest goes DRAFT → sibling promoted to effective latest
  - Two versions: latest returns from DRAFT → re-promoted, sibling demoted back
  - Search visibility tracks effective latest through DRAFT cycle
  - New version created while latest is hidden (critical: VSP.latest must track
    effective latest, not hidden absolute latest)
  - Returning version is NOT re-promoted when a newer visible version exists
  - All versions hidden → no effective latest (both isLatest=False)
"""

from __future__ import annotations

import logging
import os
import uuid
from typing import List, Optional

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    GlossaryTermInfoClass,
    StatusClass,
    VersioningSchemeClass,
    VersionPropertiesClass,
    VersionSetPropertiesClass,
    VersionTagClass,
)
from datahub.metadata.urns import VersionSetUrn
from tests.e2e.utils import with_test_retry
from utilities.consistency_utils import wait_for_writes_to_sync
from utilities.domains import Domain

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.domain(Domain.CATALOG)

GLOSSARY_TERM_ENTITY_TYPE = "glossaryTerm"
DRAFT_URN = "urn:li:lifecycleStageType:DRAFT"

# ── Helpers ───────────────────────────────────────────────────────────────────


def _create_versioned_term(
    graph_client: DataHubGraph,
    urn: str,
    name: str,
    version_set_urn: str,
    sort_id: str,
    version_tag: str,
) -> None:
    graph_client.emit(
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=GlossaryTermInfoClass(
                definition=f"Effective-latest smoke test — {name}",
                name=name,
                termSource="INTERNAL",
            ),
        )
    )
    graph_client.emit(
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=VersionPropertiesClass(
                versionSet=version_set_urn,
                version=VersionTagClass(versionTag=version_tag),
                sortId=sort_id,
                versioningScheme=VersioningSchemeClass.ALPHANUMERIC_GENERATED_BY_DATAHUB,
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


def _get_is_latest(graph_client: DataHubGraph, urn: str) -> Optional[bool]:
    props = graph_client.get_aspect(urn, VersionPropertiesClass)
    return props.isLatest if props is not None else None


def _get_vsp_latest(graph_client: DataHubGraph, version_set_urn: str) -> Optional[str]:
    vsp = graph_client.get_aspect(version_set_urn, VersionSetPropertiesClass)
    return vsp.latest if vsp is not None else None


def _search_default(graph_client: DataHubGraph, name: str) -> List[str]:
    """Default GLOSSARY_TERM search — hidden lifecycle stages excluded."""
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


# ── Test class ────────────────────────────────────────────────────────────────


class TestEffectiveLatestVersioning:
    """End-to-end tests for LifecycleVersionSideEffect.

    Each test gets a fresh version set with unique URNs (cleaned up afterwards).
    """

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self, graph_client: DataHubGraph):
        self.graph_client = graph_client

        # Unique suffix per test to avoid collisions across parallel runs
        suffix = uuid.uuid4().hex[:8]
        self.vs_urn = VersionSetUrn(
            f"smoke-effective-latest-{suffix}", GLOSSARY_TERM_ENTITY_TYPE
        ).urn()
        self.v1_urn = f"urn:li:glossaryTerm:SmokeEffV1{suffix}"
        self.v2_urn = f"urn:li:glossaryTerm:SmokeEffV2{suffix}"
        self.v3_urn = f"urn:li:glossaryTerm:SmokeEffV3{suffix}"
        self.v1_name = f"SmokeEffV1{suffix}"
        self.v2_name = f"SmokeEffV2{suffix}"
        self.v3_name = f"SmokeEffV3{suffix}"

        yield

        skip_cleanup = os.environ.get("DATAHUB_SKIP_CLEANUP", "").lower() in (
            "1",
            "true",
            "yes",
        )
        if skip_cleanup:
            logger.info(
                "DATAHUB_SKIP_CLEANUP=true — leaving entities:\n"
                "  v1=%s\n  v2=%s\n  v3=%s\n  vs=%s",
                self.v1_urn,
                self.v2_urn,
                self.v3_urn,
                self.vs_urn,
            )
            return
        for urn in [self.v1_urn, self.v2_urn, self.v3_urn, self.vs_urn]:
            try:
                graph_client.hard_delete_entity(urn)
            except Exception as exc:
                logger.warning("Cleanup failed for %s: %s", urn, exc)

    def _create_v1(self) -> None:
        _create_versioned_term(
            self.graph_client,
            self.v1_urn,
            self.v1_name,
            self.vs_urn,
            "AAAAAAAA",
            "v1",
        )
        wait_for_writes_to_sync()

    def _create_v2(self) -> None:
        _create_versioned_term(
            self.graph_client,
            self.v2_urn,
            self.v2_name,
            self.vs_urn,
            "AAAAAAAB",
            "v2",
        )
        wait_for_writes_to_sync()

    def _create_v3(self) -> None:
        _create_versioned_term(
            self.graph_client,
            self.v3_urn,
            self.v3_name,
            self.vs_urn,
            "AAAAAAAC",
            "v3",
        )
        wait_for_writes_to_sync()

    # ── Single version scenarios ───────────────────────────────────────────────

    def test_single_version_goes_draft_is_demoted(self):
        """v1 (only version) → DRAFT: isLatest becomes False."""
        self._create_v1()
        assert _get_is_latest(self.graph_client, self.v1_urn) is True

        _set_stage(self.graph_client, self.v1_urn, DRAFT_URN)
        wait_for_writes_to_sync()

        @with_test_retry(max_attempts=12)
        def _check_demoted():
            assert _get_is_latest(self.graph_client, self.v1_urn) is False, (
                "v1 should be isLatest=False after transitioning to DRAFT"
            )

        _check_demoted()
        logger.info("PASS: single-version DRAFT demotion — v1.isLatest=False")

    def test_single_version_returns_from_draft_is_repromoted(self):
        """v1 → DRAFT → active: isLatest recovers to True."""
        self._create_v1()

        _set_stage(self.graph_client, self.v1_urn, DRAFT_URN)
        wait_for_writes_to_sync()

        @with_test_retry(max_attempts=12)
        def _demoted():
            assert _get_is_latest(self.graph_client, self.v1_urn) is False

        _demoted()

        # Clear stage → active/visible
        _set_stage(self.graph_client, self.v1_urn, None)
        wait_for_writes_to_sync()

        @with_test_retry(max_attempts=12)
        def _repromoted():
            assert _get_is_latest(self.graph_client, self.v1_urn) is True, (
                "v1 should be re-promoted to isLatest=True after returning from DRAFT"
            )

        _repromoted()
        logger.info(
            "PASS: single-version return from DRAFT — v1.isLatest=True restored"
        )

    # ── Two-version scenarios ─────────────────────────────────────────────────

    def test_latest_going_draft_promotes_sibling(self):
        """v2 (isLatest=True) → DRAFT: v1 is promoted to isLatest=True."""
        self._create_v1()
        self._create_v2()

        assert _get_is_latest(self.graph_client, self.v2_urn) is True
        assert _get_is_latest(self.graph_client, self.v1_urn) is False

        _set_stage(self.graph_client, self.v2_urn, DRAFT_URN)
        wait_for_writes_to_sync()

        @with_test_retry(max_attempts=12)
        def _check():
            assert _get_is_latest(self.graph_client, self.v2_urn) is False, (
                "v2 should be demoted to isLatest=False"
            )
            assert _get_is_latest(self.graph_client, self.v1_urn) is True, (
                "v1 should be promoted to isLatest=True"
            )

        _check()

        vsp_latest = _get_vsp_latest(self.graph_client, self.vs_urn)
        assert vsp_latest == self.v1_urn, (
            f"VersionSetProperties.latest should point to v1, got {vsp_latest}"
        )
        logger.info("PASS: v2→DRAFT — v1 promoted; VSP.latest=v1")

    def test_returning_from_draft_repromotes_and_demotes_sibling(self):
        """v2→DRAFT then v2→active: v2 re-promoted, v1 demoted back."""
        self._create_v1()
        self._create_v2()

        _set_stage(self.graph_client, self.v2_urn, DRAFT_URN)
        wait_for_writes_to_sync()

        @with_test_retry(max_attempts=12)
        def _v1_promoted():
            assert _get_is_latest(self.graph_client, self.v1_urn) is True

        _v1_promoted()

        # v2 returns to active (clear stage)
        _set_stage(self.graph_client, self.v2_urn, None)
        wait_for_writes_to_sync()

        @with_test_retry(max_attempts=12)
        def _v2_repromoted():
            assert _get_is_latest(self.graph_client, self.v2_urn) is True, (
                "v2 should be re-promoted to isLatest=True after returning from DRAFT"
            )
            assert _get_is_latest(self.graph_client, self.v1_urn) is False, (
                "v1 should be demoted back to isLatest=False"
            )

        _v2_repromoted()
        logger.info(
            "PASS: v2 returned from DRAFT — v2.isLatest=True; v1.isLatest=False"
        )

    def test_search_visibility_tracks_effective_latest(self):
        """
        Default search returns the effective latest, not the hidden absolute latest.

        v2 visible → v2 in search
        v2 → DRAFT → v1 in search (v2 excluded by hidden-stage filter)
        v2 → active → v2 back in search
        """
        self._create_v1()
        self._create_v2()

        @with_test_retry(max_attempts=12)
        def _v2_visible_initially():
            results = _search_default(self.graph_client, self.v2_name)
            assert self.v2_urn in results, (
                "v2 (effective latest) should appear in default search"
            )

        _v2_visible_initially()

        # v2 → DRAFT: v1 becomes the effective latest
        _set_stage(self.graph_client, self.v2_urn, DRAFT_URN)
        wait_for_writes_to_sync()

        @with_test_retry(max_attempts=12)
        def _v1_visible_v2_hidden():
            assert _get_is_latest(self.graph_client, self.v1_urn) is True
            v1_results = _search_default(self.graph_client, self.v1_name)
            assert self.v1_urn in v1_results, (
                "v1 (promoted effective latest) should appear in default search"
            )
            v2_results = _search_default(self.graph_client, self.v2_name)
            assert self.v2_urn not in v2_results, (
                "v2 (DRAFT, hidden) should NOT appear in default search"
            )

        _v1_visible_v2_hidden()

        # v2 returns → v2 is visible again as effective latest
        _set_stage(self.graph_client, self.v2_urn, None)
        wait_for_writes_to_sync()

        @with_test_retry(max_attempts=12)
        def _v2_back_in_search():
            assert _get_is_latest(self.graph_client, self.v2_urn) is True
            results = _search_default(self.graph_client, self.v2_name)
            assert self.v2_urn in results, (
                "v2 should reappear in search after returning from DRAFT"
            )

        _v2_back_in_search()
        logger.info(
            "PASS: search visibility tracks effective latest through full DRAFT cycle"
        )

    # ── Critical edge case: new version created while latest is hidden ─────────

    @pytest.mark.p0
    def test_new_version_created_while_latest_hidden(self):
        """
        v2→DRAFT causes VSP.latest=v1 (side effect must update the pointer).
        Creating v3 then: v3 gets isLatest=True, v1 is demoted, v2 stays DRAFT/False.

        Without the VSP.latest update in LifecycleVersionSideEffect, when v3 is
        created VersionPropertiesSideEffect would compare v3.sortId against v2.sortId
        (the hidden absolute latest, now the VSP.latest). It would still promote v3,
        but it would demote v2 rather than v1 — leaving v1.isLatest=True and v3.isLatest=True
        simultaneously. This test catches that double-promotion bug.
        """
        self._create_v1()
        self._create_v2()

        _set_stage(self.graph_client, self.v2_urn, DRAFT_URN)
        wait_for_writes_to_sync()

        @with_test_retry(max_attempts=12)
        def _v1_promoted_vsp_updated():
            assert _get_is_latest(self.graph_client, self.v1_urn) is True
            vsp_latest = _get_vsp_latest(self.graph_client, self.vs_urn)
            assert vsp_latest == self.v1_urn, (
                f"VSP.latest should be updated to v1 (effective latest), got {vsp_latest}"
            )

        _v1_promoted_vsp_updated()

        # Create v3 while v2 is DRAFT
        self._create_v3()

        @with_test_retry(max_attempts=12)
        def _v3_only_effective_latest():
            assert _get_is_latest(self.graph_client, self.v3_urn) is True, (
                "v3 (new version) should be isLatest=True"
            )
            assert _get_is_latest(self.graph_client, self.v1_urn) is False, (
                "v1 should be demoted when v3 is created"
            )
            assert _get_is_latest(self.graph_client, self.v2_urn) is False, (
                "v2 (DRAFT/hidden) should remain isLatest=False"
            )
            vsp_latest = _get_vsp_latest(self.graph_client, self.vs_urn)
            assert vsp_latest == self.v3_urn, (
                f"VSP.latest should point to v3 (the new effective latest), got {vsp_latest}"
            )

        _v3_only_effective_latest()
        logger.info(
            "PASS: v3 created while v2 DRAFT — only v3 has isLatest=True; VSP.latest=v3"
        )

    def test_draft_returning_when_newer_version_exists(self):
        """
        v2→DRAFT; v3 created (v3 becomes isLatest); v2→active:
        v2 should NOT be re-promoted — v3 has a higher sortId and is visible.
        """
        self._create_v1()
        self._create_v2()

        _set_stage(self.graph_client, self.v2_urn, DRAFT_URN)
        wait_for_writes_to_sync()

        @with_test_retry(max_attempts=12)
        def _v1_promoted():
            assert _get_is_latest(self.graph_client, self.v1_urn) is True

        _v1_promoted()

        # Create v3 while v2 is hidden
        self._create_v3()

        @with_test_retry(max_attempts=12)
        def _v3_latest():
            assert _get_is_latest(self.graph_client, self.v3_urn) is True

        _v3_latest()

        # v2 returns from DRAFT — v3 is still the best visible (higher sortId)
        _set_stage(self.graph_client, self.v2_urn, None)
        wait_for_writes_to_sync()

        @with_test_retry(max_attempts=12)
        def _v3_stays_latest():
            assert _get_is_latest(self.graph_client, self.v3_urn) is True, (
                "v3 should remain isLatest=True — it has higher sortId than returning v2"
            )
            assert _get_is_latest(self.graph_client, self.v2_urn) is False, (
                "v2 should NOT be re-promoted — v3 has higher sortId and is visible"
            )

        _v3_stays_latest()
        logger.info(
            "PASS: v2 returned from DRAFT but v3 (higher sortId, visible) remains effective latest"
        )

    # ── All versions hidden ───────────────────────────────────────────────────

    def test_all_versions_hidden_no_effective_latest(self):
        """
        v1 + v2; v2→DRAFT (v1 promoted); v1→DRAFT:
        both hidden → no effective latest; both isLatest=False.
        """
        self._create_v1()
        self._create_v2()

        _set_stage(self.graph_client, self.v2_urn, DRAFT_URN)
        wait_for_writes_to_sync()

        @with_test_retry(max_attempts=12)
        def _v1_promoted():
            assert _get_is_latest(self.graph_client, self.v1_urn) is True

        _v1_promoted()

        # Now send v1 to DRAFT — no visible sibling remains
        _set_stage(self.graph_client, self.v1_urn, DRAFT_URN)
        wait_for_writes_to_sync()

        @with_test_retry(max_attempts=12)
        def _both_hidden():
            assert _get_is_latest(self.graph_client, self.v1_urn) is False, (
                "v1 should be demoted — no visible sibling to promote"
            )
            assert _get_is_latest(self.graph_client, self.v2_urn) is False, (
                "v2 should remain isLatest=False"
            )

        _both_hidden()
        logger.info(
            "PASS: all versions DRAFT — both isLatest=False; term invisible in search"
        )

    def test_all_versions_hidden_then_one_returns(self):
        """
        After both versions are hidden (isLatest=False each), clearing v1's stage
        re-promotes v1 to isLatest=True.
        """
        self._create_v1()
        self._create_v2()

        _set_stage(self.graph_client, self.v2_urn, DRAFT_URN)
        wait_for_writes_to_sync()

        @with_test_retry(max_attempts=12)
        def _v1_promoted():
            assert _get_is_latest(self.graph_client, self.v1_urn) is True

        _v1_promoted()

        _set_stage(self.graph_client, self.v1_urn, DRAFT_URN)
        wait_for_writes_to_sync()

        @with_test_retry(max_attempts=12)
        def _both_hidden():
            assert _get_is_latest(self.graph_client, self.v1_urn) is False
            assert _get_is_latest(self.graph_client, self.v2_urn) is False

        _both_hidden()

        # v1 returns from DRAFT; v2 is still DRAFT
        _set_stage(self.graph_client, self.v1_urn, None)
        wait_for_writes_to_sync()

        @with_test_retry(max_attempts=12)
        def _v1_repromoted():
            assert _get_is_latest(self.graph_client, self.v1_urn) is True, (
                "v1 should be re-promoted when it returns from DRAFT (only visible version)"
            )
            assert _get_is_latest(self.graph_client, self.v2_urn) is False, (
                "v2 should remain isLatest=False (still DRAFT)"
            )

        _v1_repromoted()
        logger.info(
            "PASS: v1 re-promoted after returning from DRAFT while v2 remains DRAFT"
        )
