"""
Smoke tests for the lifecycle stage GraphQL APIs.

Tests create their own lifecycle stage types on the fly (no dependency on
bootstrap MCPs) and exercise the core lifecycle stage GraphQL operations:
  - setLifecycleStage mutation
  - listLifecycleStages query
  - Status.lifecycleStage resolved entity

Uses a dedicated URN namespace (urn:li:lifecycleStageType:smoketest_*) to
avoid collisions with any real bootstrapped stages.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, List, Optional

import pytest

from tests.e2e.utils import with_test_retry
from utilities.consistency_utils import wait_for_writes_to_sync
from utilities.domains import Domain

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.domain(Domain.CATALOG)

SMOKE_TEST_STAGE_PREFIX = "smoketest"

# Well-known stages are bootstrapped (see bootstrap_mcps/lifecycle-stages.yaml) and other
# tests rely on them. Tests may add entity types to them but must never narrow or
# hard-delete them.
WELL_KNOWN_LIFECYCLE_STAGE_URNS = {
    "urn:li:lifecycleStageType:PUBLISHED",
    "urn:li:lifecycleStageType:DRAFT",
}


def _unique_id(prefix: str = SMOKE_TEST_STAGE_PREFIX) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def execute_graphql(
    auth_session, query: str, variables: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    payload = {"query": query, "variables": variables or {}}
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/graphql", json=payload
    )
    response.raise_for_status()
    return response.json()


def _hard_delete(auth_session, urn: str) -> None:
    from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig

    cfg = DataHubGraphConfig(
        server=auth_session.gms_url(), token=auth_session.gms_token() or ""
    )
    with DataHubGraph(cfg) as graph:
        graph.hard_delete_entity(urn)


def _assert_ok(result: Dict[str, Any]) -> Dict[str, Any]:
    assert "errors" not in result, result.get("errors")
    return result["data"]


# ── Stage ingestion helper ────────────────────────────────────────────────────


def _ingest_lifecycle_stage(
    auth_session,
    stage_id: str,
    name: str,
    *,
    hide_in_search: bool = True,
    description: str = "Smoke test stage",
    entity_types: Optional[List[str]] = None,
    allowed_previous_stages: Optional[List[str]] = None,
) -> str:
    """Create a lifecycleStageType entity via the REST emitter. Returns the URN."""
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.emitter.rest_emitter import DatahubRestEmitter
    from datahub.metadata.schema_classes import (
        AuditStampClass,
        LifecycleStageSettingsClass,
        LifecycleStageTransitionPolicyClass,
        LifecycleStageTypeInfoClass,
    )

    stage_urn = f"urn:li:lifecycleStageType:{stage_id}"

    now_millis = 0
    audit = AuditStampClass(time=now_millis, actor="urn:li:corpuser:datahub")

    aspect = LifecycleStageTypeInfoClass(
        name=name,
        description=description,
        entityTypes=entity_types or ["dataset"],
        settings=LifecycleStageSettingsClass(hideInSearch=hide_in_search),
        transitionPolicy=(
            LifecycleStageTransitionPolicyClass(
                allowedPreviousStages=allowed_previous_stages
            )
            if allowed_previous_stages is not None
            else None
        ),
        created=audit,
        lastModified=audit,
    )

    mcp = MetadataChangeProposalWrapper(entityUrn=stage_urn, aspect=aspect)
    emitter = DatahubRestEmitter(
        gms_server=auth_session.gms_url(), token=auth_session.gms_token()
    )
    try:
        emitter.emit(mcp)
    finally:
        emitter.close()

    return stage_urn


def _ensure_well_known_lifecycle_stage(
    auth_session,
    stage_id: str,
    name: str,
    *,
    hide_in_search: bool,
    entity_types: List[str],
) -> str:
    """Ensure a shared, bootstrapped stage exists and applies to ``entity_types``.

    Well-known stages are global and other tests running concurrently rely on
    their bootstrapped entityTypes (e.g. glossaryTerm for DRAFT). Never narrow
    them: merge our required types into whatever is already there, and skip the
    write entirely when nothing is missing.
    """
    from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
    from datahub.metadata.schema_classes import LifecycleStageTypeInfoClass

    stage_urn = f"urn:li:lifecycleStageType:{stage_id}"
    cfg = DataHubGraphConfig(
        server=auth_session.gms_url(), token=auth_session.gms_token() or ""
    )
    with DataHubGraph(cfg) as graph:
        existing = graph.get_aspect(stage_urn, LifecycleStageTypeInfoClass)

    existing_types = list(existing.entityTypes or []) if existing else []
    missing = [t for t in entity_types if t not in existing_types]
    if existing is not None and not missing:
        return stage_urn

    return _ingest_lifecycle_stage(
        auth_session,
        stage_id,
        name,
        hide_in_search=hide_in_search,
        entity_types=existing_types + missing,
    )


# ── Document helpers ──────────────────────────────────────────────────────────

CREATE_DOCUMENT_MUTATION = """
    mutation CreateDoc($input: CreateDocumentInput!) {
        createDocument(input: $input)
    }
"""

DELETE_DOCUMENT_MUTATION = """
    mutation DeleteDoc($urn: String!) {
        deleteDocument(urn: $urn)
    }
"""

SEARCH_DOCUMENTS_QUERY = """
    query SearchDocs($input: SearchDocumentsInput!) {
        searchDocuments(input: $input) {
            total
            documents { urn }
        }
    }
"""

SEARCH_ACROSS_ENTITIES_QUERY = """
    query SearchAcross($input: SearchAcrossEntitiesInput!) {
        searchAcrossEntities(input: $input) {
            count
            searchResults { entity { urn } }
        }
    }
"""

SET_LIFECYCLE_STAGE_MUTATION = """
    mutation SetStage($urn: String!, $lifecycleStageUrn: String) {
        setLifecycleStage(urn: $urn, lifecycleStageUrn: $lifecycleStageUrn)
    }
"""

CREATE_GLOSSARY_TERM_MUTATION = """
    mutation CreateTerm($input: CreateGlossaryEntityInput!) {
        createGlossaryTerm(input: $input)
    }
"""

DELETE_GLOSSARY_ENTITY_MUTATION = """
    mutation DeleteTerm($urn: String!) {
        deleteGlossaryEntity(urn: $urn)
    }
"""

LIFECYCLE_TIMELINE_QUERY = """
    query Timeline($urn: String!) {
        getTimeline(input: { urn: $urn, changeCategories: [LIFECYCLE] }) {
            changeTransactions { changes { category description } }
        }
    }
"""

LIST_LIFECYCLE_STAGES_QUERY = """
    query ListStages {
        listLifecycleStages {
            urn
            name
            description
            entityTypes
            hideInSearch
            allowedPreviousStages
        }
    }
"""


def _create_document(auth_session, doc_id: str, title: str) -> str:
    """Create a PUBLISHED document. Returns URN."""
    result = execute_graphql(
        auth_session,
        CREATE_DOCUMENT_MUTATION,
        {
            "input": {
                "id": doc_id,
                "title": title,
                "contents": {"text": f"Content for {title}"},
                "state": "PUBLISHED",
            }
        },
    )
    assert "errors" not in result, f"createDocument errors: {result.get('errors')}"
    return result["data"]["createDocument"]


def _set_lifecycle_stage_via_rest(
    auth_session, urn: str, lifecycle_stage_urn: Optional[str]
) -> None:
    """Set lifecycleStage on an entity by emitting a Status aspect MCP via GMS REST."""
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.emitter.rest_emitter import DatahubRestEmitter
    from datahub.metadata.schema_classes import StatusClass

    status = StatusClass(
        removed=False,
        lifecycleStage=lifecycle_stage_urn,
    )
    mcp = MetadataChangeProposalWrapper(entityUrn=urn, aspect=status)
    emitter = DatahubRestEmitter(
        gms_server=auth_session.gms_url(), token=auth_session.gms_token()
    )
    try:
        emitter.emit(mcp)
    finally:
        emitter.close()


def _search_documents(auth_session, query: str) -> List[str]:
    """Search via the document-specific search (docs home page). Ownership-aware."""
    result = execute_graphql(
        auth_session,
        SEARCH_DOCUMENTS_QUERY,
        {"input": {"query": query, "count": 50}},
    )
    assert "errors" not in result, f"searchDocuments errors: {result.get('errors')}"
    docs = result["data"]["searchDocuments"]["documents"]
    return [d["urn"] for d in docs]


def _search_across_entities(auth_session, query: str) -> List[str]:
    """Search via cross-entity search (global search bar). No ownership context."""
    result = execute_graphql(
        auth_session,
        SEARCH_ACROSS_ENTITIES_QUERY,
        {
            "input": {
                "query": query,
                "types": ["DOCUMENT"],
                "count": 50,
            }
        },
    )
    assert "errors" not in result, (
        f"searchAcrossEntities errors: {result.get('errors')}"
    )
    return [
        r["entity"]["urn"]
        for r in result["data"]["searchAcrossEntities"]["searchResults"]
    ]


def _get_status(auth_session, urn: str) -> Dict[str, Any]:
    from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
    from datahub.metadata.schema_classes import StatusClass

    cfg = DataHubGraphConfig(
        server=auth_session.gms_url(), token=auth_session.gms_token() or ""
    )
    with DataHubGraph(cfg) as graph:
        aspect = graph.get_aspect(urn, StatusClass)
    if aspect is None:
        return {}
    result: Dict[str, Any] = {"removed": aspect.removed}
    if aspect.lifecycleStage is not None:
        result["lifecycleStage"] = aspect.lifecycleStage
    return result


# ── Tests ─────────────────────────────────────────────────────────────────────


class TestLifecycleStageAPIs:
    """Tests for lifecycle stage GraphQL APIs using self-ingested stage types."""

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self, auth_session):
        self.auth_session = auth_session
        self.created_urns: List[str] = []
        self.created_stage_urns: List[str] = []
        yield
        for urn in self.created_urns:
            try:
                execute_graphql(auth_session, DELETE_DOCUMENT_MUTATION, {"urn": urn})
            except Exception as exc:
                logger.warning(f"Cleanup failed for {urn}: {exc}")
        for stage_urn in self.created_stage_urns:
            if stage_urn in WELL_KNOWN_LIFECYCLE_STAGE_URNS:
                continue
            try:
                _hard_delete(auth_session, stage_urn)
            except Exception as exc:
                logger.warning(f"Cleanup failed for {stage_urn}: {exc}")

    def _ingest_stage(self, name: str, *, hide_in_search: bool = True) -> str:
        stage_id = _unique_id()
        stage_urn = _ingest_lifecycle_stage(
            self.auth_session,
            stage_id,
            name,
            hide_in_search=hide_in_search,
            entity_types=["dataset", "document"],
        )
        self.created_stage_urns.append(stage_urn)
        return stage_urn

    @pytest.mark.p0
    def test_list_lifecycle_stages_includes_ingested(self, auth_session):
        """
        Ingest a custom lifecycle stage and verify it appears in listLifecycleStages.
        """
        stage_name = f"TestStage {_unique_id()}"
        stage_urn = self._ingest_stage(stage_name, hide_in_search=True)
        wait_for_writes_to_sync(mcp_only=True)

        @with_test_retry(max_attempts=12)
        def _assert_listed():
            result = execute_graphql(auth_session, LIST_LIFECYCLE_STAGES_QUERY)
            assert "errors" not in result, (
                f"listLifecycleStages errors: {result.get('errors')}"
            )
            stages = result["data"]["listLifecycleStages"]
            stage_urns = [s["urn"] for s in stages]
            assert stage_urn in stage_urns, (
                f"Expected {stage_urn} in listLifecycleStages, got: {stage_urns}"
            )
            stage_obj = next(s for s in stages if s["urn"] == stage_urn)
            assert stage_obj["name"] == stage_name
            assert stage_obj["hideInSearch"] is True

        _assert_listed()
        logger.info("listLifecycleStages returns ingested stage")

    @pytest.mark.p0
    def test_set_lifecycle_stage_mutation(self, auth_session):
        """
        setLifecycleStage mutation sets the stage on an entity and the
        Status aspect reflects it.
        """
        stage_urn = self._ingest_stage("MutationTest", hide_in_search=True)
        wait_for_writes_to_sync(mcp_only=True)

        title = f"Lifecycle Mutation Test {_unique_id()}"
        doc_id = _unique_id("lc-mut")
        urn = _create_document(auth_session, doc_id, title)
        self.created_urns.append(urn)
        wait_for_writes_to_sync(mae_only=True)

        result = execute_graphql(
            auth_session,
            SET_LIFECYCLE_STAGE_MUTATION,
            {"urn": urn, "lifecycleStageUrn": stage_urn},
        )
        assert "errors" not in result, (
            f"setLifecycleStage errors: {result.get('errors')}"
        )
        assert result["data"]["setLifecycleStage"] is True
        wait_for_writes_to_sync(mae_only=True)

        status = _get_status(auth_session, urn)
        assert status.get("lifecycleStage") == stage_urn, (
            f"Expected stage {stage_urn}, got: {status}"
        )

        # Clear stage
        result = execute_graphql(
            auth_session,
            SET_LIFECYCLE_STAGE_MUTATION,
            {"urn": urn, "lifecycleStageUrn": None},
        )
        assert "errors" not in result
        assert result["data"]["setLifecycleStage"] is True
        wait_for_writes_to_sync(mae_only=True)

        status = _get_status(auth_session, urn)
        assert status.get("lifecycleStage") is None
        logger.info("setLifecycleStage mutation set and cleared stage")

    @pytest.mark.p0
    def test_hidden_stage_excludes_from_cross_entity_search(self, auth_session):
        """
        Entities in a hideInSearch=true stage are excluded from cross-entity
        search (global search bar). Clearing the stage restores visibility.
        """
        stage_urn = self._ingest_stage("HiddenStage", hide_in_search=True)
        wait_for_writes_to_sync(mcp_only=True)

        title = f"Lifecycle Search Test {_unique_id()}"
        doc_id = _unique_id("lc-search")
        urn = _create_document(auth_session, doc_id, title)
        self.created_urns.append(urn)
        wait_for_writes_to_sync(mae_only=True)

        @with_test_retry(max_attempts=12)
        def _assert_visible():
            urns = _search_across_entities(auth_session, title)
            assert urn in urns, f"Expected {urn} in cross-entity search, got: {urns}"

        _assert_visible()

        _set_lifecycle_stage_via_rest(auth_session, urn, stage_urn)
        wait_for_writes_to_sync(mae_only=True)

        @with_test_retry(max_attempts=12)
        def _assert_hidden():
            urns = _search_across_entities(auth_session, title)
            assert urn not in urns, (
                "Entity in hidden stage should not appear in cross-entity search"
            )

        _assert_hidden()
        logger.info("Hidden stage excludes entity from cross-entity search")

        _set_lifecycle_stage_via_rest(auth_session, urn, None)
        wait_for_writes_to_sync(mae_only=True)

        @with_test_retry(max_attempts=12)
        def _assert_visible_again():
            urns = _search_across_entities(auth_session, title)
            assert urn in urns, f"Cleared entity should be visible: {urns}"

        _assert_visible_again()
        logger.info("Cleared stage restores cross-entity search visibility")

    def test_hidden_stage_visible_to_owner_in_doc_search(self, auth_session):
        """
        Owners can still see their documents in the docs home page search
        (searchDocuments) even when the doc is in a hideInSearch=true stage.
        """
        stage_urn = self._ingest_stage("HiddenOwnerStage", hide_in_search=True)
        wait_for_writes_to_sync(mcp_only=True)

        title = f"Lifecycle Owner Test {_unique_id()}"
        doc_id = _unique_id("lc-owner")
        urn = _create_document(auth_session, doc_id, title)
        self.created_urns.append(urn)
        wait_for_writes_to_sync(mae_only=True)

        @with_test_retry(max_attempts=12)
        def _assert_visible_before():
            urns = _search_documents(auth_session, title)
            assert urn in urns, f"Expected {urn} in doc search, got: {urns}"

        _assert_visible_before()

        _set_lifecycle_stage_via_rest(auth_session, urn, stage_urn)
        wait_for_writes_to_sync(mae_only=True)

        @with_test_retry(max_attempts=12)
        def _assert_still_visible_to_owner():
            urns = _search_documents(auth_session, title)
            assert urn in urns, (
                "Owner should still see doc in hidden stage via doc search"
            )

        _assert_still_visible_to_owner()
        logger.info("Owner can see hidden-stage doc in document search")

    def test_visible_stage_remains_in_search(self, auth_session):
        """
        Entities in the well-known PUBLISHED lifecycle stage remain visible in
        default document search.

        Note: searchDocuments uses document-specific filter logic that matches
        on well-known lifecycle stage URNs (PUBLISHED, UNPUBLISHED, DRAFT),
        not the generic hideInSearch flag. We use the PUBLISHED stage to verify
        that a visible stage keeps the document searchable.
        """
        # Ensure the well-known PUBLISHED stage type entity exists (it may not
        # be bootstrapped in all environments).
        stage_urn = _ensure_well_known_lifecycle_stage(
            auth_session,
            "PUBLISHED",
            "Published",
            hide_in_search=False,
            entity_types=["document"],
        )
        self.created_stage_urns.append(stage_urn)
        wait_for_writes_to_sync(mcp_only=True)

        title = f"Lifecycle Visible Test {_unique_id()}"
        doc_id = _unique_id("lc-vis")
        urn = _create_document(auth_session, doc_id, title)
        self.created_urns.append(urn)
        wait_for_writes_to_sync(mae_only=True)

        @with_test_retry(max_attempts=12)
        def _assert_visible_before():
            urns = _search_documents(auth_session, title)
            assert urn in urns, f"Expected {urn} in search, got: {urns}"

        _assert_visible_before()

        _set_lifecycle_stage_via_rest(auth_session, urn, stage_urn)
        wait_for_writes_to_sync(mae_only=True)

        @with_test_retry(max_attempts=12)
        def _assert_still_visible():
            urns = _search_documents(auth_session, title)
            assert urn in urns, (
                f"Entity in visible stage should remain in search: {urns}"
            )

        _assert_still_visible()

        status = _get_status(auth_session, urn)
        assert status.get("lifecycleStage") == stage_urn
        logger.info("Visible stage keeps entity in search results")

    def test_draft_stage_hidden_from_owner_in_doc_search(self, auth_session):
        """
        Documents in the well-known DRAFT lifecycle stage must not appear in
        document search (searchDocuments) even for the owning user.

        Unlike generic hidden stages (which are still visible to owners),
        DRAFT is treated as a staging state that is never surfaced in the UI
        regardless of ownership. DocumentSearchFilterUtils excludes DRAFT
        from Clause 2 (the owner-visible non-published clause) so that DRAFT
        documents match no clause and are filtered out entirely.
        """
        # Ensure well-known stages exist. createDocument writes PUBLISHED onto
        # status.lifecycleStage, so that stage type must be present first.
        _ensure_well_known_lifecycle_stage(
            auth_session,
            "PUBLISHED",
            "Published",
            hide_in_search=False,
            entity_types=["document"],
        )
        draft_stage_urn = _ensure_well_known_lifecycle_stage(
            auth_session,
            "DRAFT",
            "Draft",
            hide_in_search=True,
            entity_types=["document"],
        )
        self.created_stage_urns.append(draft_stage_urn)
        wait_for_writes_to_sync(mcp_only=True)

        title = f"Lifecycle Draft Test {_unique_id()}"
        doc_id = _unique_id("lc-draft")
        urn = _create_document(auth_session, doc_id, title)
        self.created_urns.append(urn)
        wait_for_writes_to_sync(mae_only=True)

        @with_test_retry(max_attempts=12)
        def _assert_visible_before():
            urns = _search_documents(auth_session, title)
            assert urn in urns, f"Expected {urn} in doc search, got: {urns}"

        _assert_visible_before()

        _set_lifecycle_stage_via_rest(auth_session, urn, draft_stage_urn)
        wait_for_writes_to_sync(mae_only=True)

        @with_test_retry(max_attempts=12)
        def _assert_hidden_from_owner():
            urns = _search_documents(auth_session, title)
            assert urn not in urns, (
                "Owner should NOT see a DRAFT document in doc search "
                f"(found in results): {urns}"
            )

        _assert_hidden_from_owner()
        logger.info("DRAFT stage hides document from owner in document search")

    @pytest.mark.p0
    def test_glossary_term_stage_change_respects_transition_rules(self, auth_session):
        """
        A stage change that breaks the destination's entry rule is refused with BAD_REQUEST;
        a valid one is applied and appears in the term's lifecycle history.
        """
        first_id, second_id = _unique_id(), _unique_id()
        first_stage = _ingest_lifecycle_stage(
            auth_session, first_id, "First", entity_types=["glossaryTerm"]
        )
        second_stage = _ingest_lifecycle_stage(
            auth_session,
            second_id,
            "Second",
            entity_types=["glossaryTerm"],
            allowed_previous_stages=[first_stage],
        )
        self.created_stage_urns.extend([first_stage, second_stage])
        wait_for_writes_to_sync(mcp_only=True)

        term_urn = _assert_ok(
            execute_graphql(
                auth_session,
                CREATE_GLOSSARY_TERM_MUTATION,
                {
                    "input": {
                        "id": _unique_id("smoketest_term"),
                        "name": "Stage change term",
                    }
                },
            )
        )["createGlossaryTerm"]
        try:
            refused = execute_graphql(
                auth_session,
                SET_LIFECYCLE_STAGE_MUTATION,
                {"urn": term_urn, "lifecycleStageUrn": second_stage},
            )
            assert refused.get("errors"), refused
            assert refused["errors"][0]["extensions"]["type"] == "BAD_REQUEST", refused

            for stage in (first_stage, second_stage):
                assert _assert_ok(
                    execute_graphql(
                        auth_session,
                        SET_LIFECYCLE_STAGE_MUTATION,
                        {"urn": term_urn, "lifecycleStageUrn": stage},
                    )
                )["setLifecycleStage"]

            @with_test_retry(max_attempts=12)
            def _assert_applied_and_recorded():
                assert (
                    _get_status(auth_session, term_urn).get("lifecycleStage")
                    == second_stage
                )
                timeline = _assert_ok(
                    execute_graphql(
                        auth_session, LIFECYCLE_TIMELINE_QUERY, {"urn": term_urn}
                    )
                )
                descriptions = [
                    change["description"]
                    for tx in timeline["getTimeline"]["changeTransactions"]
                    for change in tx["changes"] or []
                ]
                assert any(second_id.lower() in (d or "") for d in descriptions), (
                    descriptions
                )

            _assert_applied_and_recorded()
        finally:
            try:
                execute_graphql(
                    auth_session, DELETE_GLOSSARY_ENTITY_MUTATION, {"urn": term_urn}
                )
            except Exception as exc:
                logger.warning(f"Cleanup failed for {term_urn}: {exc}")
