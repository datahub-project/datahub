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

from tests.consistency_utils import wait_for_writes_to_sync
from tests.utils import with_test_retry

logger = logging.getLogger(__name__)

SMOKE_TEST_STAGE_PREFIX = "smoketest"


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


# ── Stage ingestion helper ────────────────────────────────────────────────────


def _ingest_lifecycle_stage(
    auth_session,
    stage_id: str,
    name: str,
    *,
    hide_in_search: bool = True,
    description: str = "Smoke test stage",
    entity_types: Optional[List[str]] = None,
) -> str:
    """Create a lifecycleStageType entity via the REST emitter. Returns the URN."""
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.emitter.rest_emitter import DatahubRestEmitter
    from datahub.metadata.schema_classes import (
        AuditStampClass,
        LifecycleStageSettingsClass,
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

SET_LIFECYCLE_STAGE_MUTATION = """
    mutation SetStage($urn: String!, $lifecycleStageUrn: String) {
        setLifecycleStage(urn: $urn, lifecycleStageUrn: $lifecycleStageUrn)
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
    result = execute_graphql(
        auth_session,
        SEARCH_DOCUMENTS_QUERY,
        {"input": {"query": query, "count": 50}},
    )
    assert "errors" not in result, f"searchDocuments errors: {result.get('errors')}"
    docs = result["data"]["searchDocuments"]["documents"]
    return [d["urn"] for d in docs]


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
            try:
                from datahub.ingestion.graph.client import (
                    DataHubGraph,
                    DataHubGraphConfig,
                )

                cfg = DataHubGraphConfig(
                    server=auth_session.gms_url(),
                    token=auth_session.gms_token() or "",
                )
                with DataHubGraph(cfg) as graph:
                    graph.hard_delete_entity(stage_urn)
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

    def test_list_lifecycle_stages_includes_ingested(self, auth_session):
        """
        Ingest a custom lifecycle stage and verify it appears in listLifecycleStages.
        """
        stage_name = f"TestStage {_unique_id()}"
        stage_urn = self._ingest_stage(stage_name, hide_in_search=True)
        wait_for_writes_to_sync()

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

    def test_set_lifecycle_stage_mutation(self, auth_session):
        """
        setLifecycleStage mutation sets the stage on an entity and the
        Status aspect reflects it.
        """
        stage_urn = self._ingest_stage("MutationTest", hide_in_search=True)
        wait_for_writes_to_sync()

        title = f"Lifecycle Mutation Test {_unique_id()}"
        doc_id = _unique_id("lc-mut")
        urn = _create_document(auth_session, doc_id, title)
        self.created_urns.append(urn)
        wait_for_writes_to_sync()

        result = execute_graphql(
            auth_session,
            SET_LIFECYCLE_STAGE_MUTATION,
            {"urn": urn, "lifecycleStageUrn": stage_urn},
        )
        assert "errors" not in result, (
            f"setLifecycleStage errors: {result.get('errors')}"
        )
        assert result["data"]["setLifecycleStage"] is True
        wait_for_writes_to_sync()

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
        wait_for_writes_to_sync()

        status = _get_status(auth_session, urn)
        assert status.get("lifecycleStage") is None
        logger.info("setLifecycleStage mutation set and cleared stage")

    def test_hidden_stage_excludes_from_search(self, auth_session):
        """
        Entities in a hideInSearch=true stage are excluded from default search.
        Clearing the stage restores visibility.
        """
        stage_urn = self._ingest_stage("HiddenStage", hide_in_search=True)
        wait_for_writes_to_sync()

        title = f"Lifecycle Search Test {_unique_id()}"
        doc_id = _unique_id("lc-search")
        urn = _create_document(auth_session, doc_id, title)
        self.created_urns.append(urn)
        wait_for_writes_to_sync()

        @with_test_retry(max_attempts=12)
        def _assert_visible():
            urns = _search_documents(auth_session, title)
            assert urn in urns, f"Expected {urn} in search, got: {urns}"

        _assert_visible()

        _set_lifecycle_stage_via_rest(auth_session, urn, stage_urn)
        wait_for_writes_to_sync()

        @with_test_retry(max_attempts=12)
        def _assert_hidden():
            urns = _search_documents(auth_session, title)
            assert urn not in urns, "Entity in hidden stage should not appear in search"

        _assert_hidden()
        logger.info("Hidden stage excludes entity from search")

        _set_lifecycle_stage_via_rest(auth_session, urn, None)
        wait_for_writes_to_sync()

        @with_test_retry(max_attempts=12)
        def _assert_visible_again():
            urns = _search_documents(auth_session, title)
            assert urn in urns, f"Cleared entity should be visible: {urns}"

        _assert_visible_again()
        logger.info("Cleared stage restores search visibility")

    def test_visible_stage_remains_in_search(self, auth_session):
        """
        Entities in a hideInSearch=false stage remain visible in default search.
        """
        stage_urn = self._ingest_stage("VisibleStage", hide_in_search=False)
        wait_for_writes_to_sync()

        title = f"Lifecycle Visible Test {_unique_id()}"
        doc_id = _unique_id("lc-vis")
        urn = _create_document(auth_session, doc_id, title)
        self.created_urns.append(urn)
        wait_for_writes_to_sync()

        @with_test_retry(max_attempts=12)
        def _assert_visible_before():
            urns = _search_documents(auth_session, title)
            assert urn in urns, f"Expected {urn} in search, got: {urns}"

        _assert_visible_before()

        _set_lifecycle_stage_via_rest(auth_session, urn, stage_urn)
        wait_for_writes_to_sync()

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
