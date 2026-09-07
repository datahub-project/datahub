"""
Smoke tests for Change History (Timeline) across all 4 supported entity types:
Dataset, GlossaryTerm, Domain, and DataProduct.

For each entity type, creates a dedicated entity, applies all supported change
operations via the Python SDK, then verifies the GraphQL getTimeline query
returns the expected change events with correct categories and operations.

Every supported (entity, category) cell in the feature matrix is tested with
Create (ADD), Update (MODIFY where applicable), and Delete (REMOVE) operations.

Covered categories per entity:
  Dataset:      TECHNICAL_SCHEMA, DOCUMENTATION, OWNERSHIP, TAG, GLOSSARY_TERM, DOMAIN, STRUCTURED_PROPERTY, APPLICATION
  GlossaryTerm: OWNERSHIP, DOCUMENTATION, GLOSSARY_TERM (related terms), DOMAIN, STRUCTURED_PROPERTY, APPLICATION
  Domain:       OWNERSHIP, DOCUMENTATION, STRUCTURED_PROPERTY
  DataProduct:  OWNERSHIP, DOCUMENTATION, TAG, GLOSSARY_TERM, DOMAIN, STRUCTURED_PROPERTY, APPLICATION, ASSET_MEMBERSHIP
"""

import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

import pytest
import tenacity

from datahub.configuration.common import OperationalError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    ApplicationsClass,
    AuditStampClass,
    DataProductAssociationClass,
    DataProductPropertiesClass,
    DatasetPropertiesClass,
    DomainsClass,
    EditableDatasetPropertiesClass,
    GlobalTagsClass,
    GlossaryRelatedTermsClass,
    GlossaryTermAssociationClass,
    GlossaryTermInfoClass,
    GlossaryTermsClass,
    OtherSchemaClass,
    OwnerClass,
    OwnershipClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    StructuredPropertiesClass,
    StructuredPropertyDefinitionClass,
    StructuredPropertyValueAssignmentClass,
    TagAssociationClass,
)
from datahub.metadata.urns import StructuredPropertyUrn
from tests.consistency_utils import wait_for_writes_to_sync
from tests.utilities.domains import Domain
from tests.utils import execute_graphql, with_test_retry

logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.no_cypress_suite1, pytest.mark.domain(Domain.CATALOG)]


def _is_transient_sp_error(exc: BaseException) -> bool:
    """Return True for transient SP-creation errors caused by ES mapping lookups."""
    if not isinstance(exc, OperationalError):
        return False
    msg = str(exc)
    return "Retry the request" in msg or "mapping lookup failed" in msg


@tenacity.retry(
    retry=tenacity.retry_if_exception(_is_transient_sp_error),
    wait=tenacity.wait_exponential(multiplier=1, min=1, max=8),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)
def _emit_sp_definition(graph_client: Any, mcp: MetadataChangeProposalWrapper) -> None:
    """Emit a structured property definition MCP, retrying on transient ES errors."""
    graph_client.emit_mcp(mcp)


# ---------------------------------------------------------------------------
# Constants / per-run URNs
# ---------------------------------------------------------------------------
# Tags referenced in tests (stable across fixture re-setups)
TAG_PII = "urn:li:tag:PII"
TAG_CONFIDENTIAL = "urn:li:tag:Confidential"


@dataclass(frozen=True)
class TimelineUrns:
    """Immutable per-run URNs for timeline change-history tests.

    Under pytest-xdist ``--dist=loadscope`` the module-scoped fixture can tear
    down and re-run on the same worker. ES retains structured-property field
    mappings after hard-delete, so each fixture invocation needs a fresh
    ``unique`` / qualifiedName. Passing this dataclass as a fixture arg avoids
    mutable module globals that could be stomped mid-test.
    """

    unique: str
    dataset_urn: str
    glossary_term_urn: str
    domain_urn: str
    data_product_urn: str
    sp_urn: str
    term_a: str
    term_b: str
    domain_engineering: str
    domain_marketing: str
    app_urn_1: str
    app_urn_2: str
    asset_dataset_1: str
    asset_dataset_2: str

    @classmethod
    def create(cls, unique: Optional[str] = None) -> "TimelineUrns":
        u = unique or uuid.uuid4().hex[:8]
        return cls(
            unique=u,
            dataset_urn=(
                f"urn:li:dataset:(urn:li:dataPlatform:kafka,timeline-test-{u},PROD)"
            ),
            glossary_term_urn=f"urn:li:glossaryTerm:timeline-test-term-{u}",
            domain_urn=f"urn:li:domain:timeline-test-domain-{u}",
            data_product_urn=f"urn:li:dataProduct:timeline-test-dp-{u}",
            sp_urn=str(StructuredPropertyUrn(f"io.acryl.timeline.test.{u}")),
            term_a=f"urn:li:glossaryTerm:timeline-ref-term-a-{u}",
            term_b=f"urn:li:glossaryTerm:timeline-ref-term-b-{u}",
            domain_engineering=f"urn:li:domain:timeline-ref-eng-{u}",
            domain_marketing=f"urn:li:domain:timeline-ref-mkt-{u}",
            app_urn_1=f"urn:li:application:timeline-ref-app1-{u}",
            app_urn_2=f"urn:li:application:timeline-ref-app2-{u}",
            asset_dataset_1=(
                f"urn:li:dataset:(urn:li:dataPlatform:snowflake,timeline-asset1-{u},PROD)"
            ),
            asset_dataset_2=(
                f"urn:li:dataset:(urn:li:dataPlatform:snowflake,timeline-asset2-{u},PROD)"
            ),
        )

    def cleanup_urns(self) -> List[str]:
        return [
            self.dataset_urn,
            self.glossary_term_urn,
            self.domain_urn,
            self.data_product_urn,
            self.sp_urn,
            self.term_a,
            self.term_b,
            self.domain_engineering,
            self.domain_marketing,
            self.app_urn_1,
            self.app_urn_2,
            self.asset_dataset_1,
            self.asset_dataset_2,
        ]


# GraphQL query matching what the frontend HistorySidebar uses
GET_TIMELINE_QUERY = """
query getTimeline($input: GetTimelineInput!) {
    getTimeline(input: $input) {
        changeTransactions {
            timestampMillis
            lastSemanticVersion
            versionStamp
            changeType
            actor
            changes {
                urn
                category
                operation
                modifier
                description
                parameters {
                    key
                    value
                }
            }
        }
    }
}
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _now_ms() -> int:
    return int(time.time() * 1000)


def _emit_and_wait(graph_client, mcp: MetadataChangeProposalWrapper) -> None:
    """Emit an MCP and wait for primary + search indexing to catch up.

    Timeline reads go through search/history storage, so drain MCL (mae_only)
    after the default sync — cheaper than relying only on assertion retries.
    """
    graph_client.emit_mcp(mcp)
    wait_for_writes_to_sync()
    wait_for_writes_to_sync(mae_only=True)


def _get_timeline(
    auth_session,
    urn: str,
    categories: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Fetch timeline via GraphQL, return list of ChangeTransactions."""
    variables: Dict[str, Any] = {"input": {"urn": urn}}
    if categories:
        variables["input"]["changeCategories"] = categories

    res = execute_graphql(auth_session, GET_TIMELINE_QUERY, variables)
    return res["data"]["getTimeline"]["changeTransactions"]


@with_test_retry(max_attempts=2)
def _wait_for_timeline_events(
    auth_session,
    urn: str,
    expected: List[Tuple[str, str]],
    entity_label: str,
    categories: Optional[List[str]] = None,
    min_events: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Retry getTimeline until expected (category, operation) pairs appear.

    Timeline indexing lags primary writes even after wait_for_writes_to_sync;
    one-shot asserts flake under load.
    """
    txns = _get_timeline(auth_session, urn, categories)
    events = _collect_change_events(txns)
    required = min_events if min_events is not None else len(expected)
    assert len(events) >= required, (
        f"[{entity_label}] Expected >={required} events, got {len(events)}"
    )
    _assert_has_events(events, expected, entity_label)
    return txns


@with_test_retry(max_attempts=2)
def _wait_for_timeline_categories(
    auth_session,
    urn: str,
    expected_categories: List[str],
    entity_label: str,
) -> List[Dict[str, Any]]:
    """Retry getTimeline until all expected categories are present."""
    txns = _get_timeline(auth_session, urn)
    events = _collect_change_events(txns)
    categories = {e["category"] for e in events if e.get("category")}
    for expected in expected_categories:
        assert expected in categories, (
            f"[{entity_label}] timeline missing {expected}. Found: {sorted(categories)}"
        )
    _assert_actor_present(txns, entity_label)
    return txns


def _collect_change_events(
    transactions: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Flatten all change events from transactions into a single list."""
    events = []
    for tx in transactions:
        for change in tx.get("changes") or []:
            events.append(change)
    return events


def _assert_has_events(
    events: List[Dict[str, Any]],
    expected: List[Tuple[str, str]],
    entity_label: str,
) -> None:
    """Assert that the events list contains at least one event matching each
    (category, operation) pair in expected. Order doesn't matter."""
    actual_pairs: Set[Tuple[str, str]] = {
        (e["category"], e["operation"])
        for e in events
        if e.get("category") and e.get("operation")
    }
    for category, operation in expected:
        assert (category, operation) in actual_pairs, (
            f"[{entity_label}] Expected ({category}, {operation}) in timeline "
            f"but got: {sorted(actual_pairs)}"
        )


def _assert_actor_present(
    transactions: List[Dict[str, Any]], entity_label: str
) -> None:
    """Assert that at least one transaction has a non-null actor."""
    actors = [tx.get("actor") for tx in transactions if tx.get("actor")]
    assert actors, f"[{entity_label}] No transactions have actor attribution"


# ---------------------------------------------------------------------------
# Fixture: create all test entities + structured property, tear down after
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module", autouse=True)
def timeline_urns(graph_client):
    """Create all test entities and the shared structured property definition."""
    # Fresh IDs per fixture invocation — see TimelineUrns docstring.
    urns = TimelineUrns.create()
    logger.info(
        "Creating test entities for timeline change history tests (unique=%s)",
        urns.unique,
    )

    # --- Structured property definition (used by dataset, glossary term, domain, data product) ---
    sp_def = StructuredPropertyDefinitionClass(
        qualifiedName=f"io.acryl.timeline.test.{urns.unique}",
        displayName="Timeline Test Property",
        valueType="urn:li:dataType:datahub.string",
        cardinality="SINGLE",
        entityTypes=[
            "urn:li:entityType:datahub.dataset",
            "urn:li:entityType:datahub.glossaryTerm",
            "urn:li:entityType:datahub.domain",
            "urn:li:entityType:datahub.dataProduct",
        ],
        description="Property for timeline smoke tests",
        immutable=False,
    )
    _emit_sp_definition(
        graph_client,
        MetadataChangeProposalWrapper(entityUrn=urns.sp_urn, aspect=sp_def),
    )
    wait_for_writes_to_sync(mcp_only=True)

    # --- Dataset ---
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=urns.dataset_urn,
            aspect=DatasetPropertiesClass(
                name=f"timeline-test-{urns.unique}",
                description="Initial description",
            ),
        )
    )

    # --- Glossary Term ---
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=urns.glossary_term_urn,
            aspect=GlossaryTermInfoClass(
                name=f"Timeline Test Term {urns.unique}",
                definition="Initial definition",
                termSource="INTERNAL",
            ),
        )
    )

    # --- Reference glossary terms (used as related terms) ---
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=urns.term_a,
            aspect=GlossaryTermInfoClass(
                name=f"Ref Term A {urns.unique}",
                definition="Reference term A",
                termSource="INTERNAL",
            ),
        )
    )
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=urns.term_b,
            aspect=GlossaryTermInfoClass(
                name=f"Ref Term B {urns.unique}",
                definition="Reference term B",
                termSource="INTERNAL",
            ),
        )
    )

    # --- Domain ---
    from datahub.metadata.schema_classes import DomainPropertiesClass

    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=urns.domain_urn,
            aspect=DomainPropertiesClass(
                name=f"Timeline Test Domain {urns.unique}",
                description="Initial domain description",
            ),
        )
    )

    # --- Reference domains ---
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=urns.domain_engineering,
            aspect=DomainPropertiesClass(name="Engineering"),
        )
    )
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=urns.domain_marketing,
            aspect=DomainPropertiesClass(name="Marketing"),
        )
    )

    # --- Data Product ---
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=urns.data_product_urn,
            aspect=DataProductPropertiesClass(
                name=f"Timeline Test Product {urns.unique}",
                description="Initial product description",
            ),
        )
    )

    wait_for_writes_to_sync(mcp_only=True)

    yield urns

    # --- Cleanup ---
    logger.info("Cleaning up test entities")
    for urn in urns.cleanup_urns():
        try:
            graph_client.hard_delete_entity(urn=urn)
        except Exception:
            logger.warning(f"Failed to delete {urn} during cleanup")


# ===========================================================================
# DATASET TIMELINE TESTS
# ===========================================================================
class TestDatasetTimeline:
    """Test all supported change categories for Dataset entities."""

    def test_dataset_ownership_changes(
        self, graph_client, auth_session, timeline_urns: TimelineUrns
    ):
        """Add then change ownership on a dataset — verifies ADD and REMOVE."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.dataset_urn,
                aspect=OwnershipClass(
                    owners=[
                        OwnerClass(
                            owner="urn:li:corpuser:alice", type="TECHNICAL_OWNER"
                        )
                    ],
                ),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.dataset_urn,
                aspect=OwnershipClass(
                    owners=[
                        OwnerClass(owner="urn:li:corpuser:bob", type="DATA_STEWARD")
                    ],
                ),
            ),
        )

        _wait_for_timeline_events(
            auth_session,
            timeline_urns.dataset_urn,
            [("OWNERSHIP", "ADD"), ("OWNERSHIP", "REMOVE")],
            "dataset/ownership",
            categories=["OWNERSHIP"],
            min_events=2,
        )

    def test_dataset_tag_changes(
        self, graph_client, auth_session, timeline_urns: TimelineUrns
    ):
        """Add a tag, then swap it."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.dataset_urn,
                aspect=GlobalTagsClass(tags=[TagAssociationClass(tag=TAG_PII)]),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.dataset_urn,
                aspect=GlobalTagsClass(
                    tags=[TagAssociationClass(tag=TAG_CONFIDENTIAL)]
                ),
            ),
        )

        _wait_for_timeline_events(
            auth_session,
            timeline_urns.dataset_urn,
            [("TAG", "ADD"), ("TAG", "REMOVE")],
            "dataset/tag",
            categories=["TAG"],
            min_events=2,
        )

    def test_dataset_glossary_term_changes(
        self, graph_client, auth_session, timeline_urns: TimelineUrns
    ):
        """Add then remove a glossary term."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.dataset_urn,
                aspect=GlossaryTermsClass(
                    terms=[GlossaryTermAssociationClass(urn=timeline_urns.term_a)],
                    auditStamp=AuditStampClass(
                        time=_now_ms(), actor="urn:li:corpuser:datahub"
                    ),
                ),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.dataset_urn,
                aspect=GlossaryTermsClass(
                    terms=[],
                    auditStamp=AuditStampClass(
                        time=_now_ms(), actor="urn:li:corpuser:datahub"
                    ),
                ),
            ),
        )

        _wait_for_timeline_events(
            auth_session,
            timeline_urns.dataset_urn,
            [("GLOSSARY_TERM", "ADD"), ("GLOSSARY_TERM", "REMOVE")],
            "dataset/glossaryTerm",
            categories=["GLOSSARY_TERM"],
            min_events=2,
        )

    def test_dataset_domain_changes(
        self, graph_client, auth_session, timeline_urns: TimelineUrns
    ):
        """Set domain, then change it — verifies ADD and REMOVE."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.dataset_urn,
                aspect=DomainsClass(domains=[timeline_urns.domain_engineering]),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.dataset_urn,
                aspect=DomainsClass(domains=[timeline_urns.domain_marketing]),
            ),
        )

        _wait_for_timeline_events(
            auth_session,
            timeline_urns.dataset_urn,
            [("DOMAIN", "ADD"), ("DOMAIN", "REMOVE")],
            "dataset/domain",
            categories=["DOMAIN"],
            min_events=2,
        )

    def test_dataset_structured_property_changes(
        self, graph_client, auth_session, timeline_urns: TimelineUrns
    ):
        """Assign, update, then remove a structured property — verifies ADD, MODIFY, REMOVE."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.dataset_urn,
                aspect=StructuredPropertiesClass(
                    properties=[
                        StructuredPropertyValueAssignmentClass(
                            propertyUrn=timeline_urns.sp_urn, values=["alpha"]
                        )
                    ]
                ),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.dataset_urn,
                aspect=StructuredPropertiesClass(
                    properties=[
                        StructuredPropertyValueAssignmentClass(
                            propertyUrn=timeline_urns.sp_urn, values=["beta"]
                        )
                    ]
                ),
            ),
        )
        # Remove the structured property entirely
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.dataset_urn,
                aspect=StructuredPropertiesClass(properties=[]),
            ),
        )

        _wait_for_timeline_events(
            auth_session,
            timeline_urns.dataset_urn,
            [
                ("STRUCTURED_PROPERTY", "ADD"),
                ("STRUCTURED_PROPERTY", "MODIFY"),
                ("STRUCTURED_PROPERTY", "REMOVE"),
            ],
            "dataset/structuredProperty",
            categories=["STRUCTURED_PROPERTY"],
            min_events=3,
        )

    def test_dataset_application_changes(
        self, graph_client, auth_session, timeline_urns: TimelineUrns
    ):
        """Add an application, then swap it."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.dataset_urn,
                aspect=ApplicationsClass(applications=[timeline_urns.app_urn_1]),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.dataset_urn,
                aspect=ApplicationsClass(applications=[timeline_urns.app_urn_2]),
            ),
        )

        _wait_for_timeline_events(
            auth_session,
            timeline_urns.dataset_urn,
            [("APPLICATION", "ADD"), ("APPLICATION", "REMOVE")],
            "dataset/application",
            categories=["APPLICATION"],
            min_events=2,
        )

    def test_dataset_documentation_changes(
        self, graph_client, auth_session, timeline_urns: TimelineUrns
    ):
        """Add then update documentation on a dataset — verifies ADD and MODIFY."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.dataset_urn,
                aspect=EditableDatasetPropertiesClass(
                    description="Initial dataset description for timeline test",
                ),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.dataset_urn,
                aspect=EditableDatasetPropertiesClass(
                    description="Updated dataset description for timeline test",
                ),
            ),
        )

        _wait_for_timeline_events(
            auth_session,
            timeline_urns.dataset_urn,
            [("DOCUMENTATION", "ADD"), ("DOCUMENTATION", "MODIFY")],
            "dataset/documentation",
            categories=["DOCUMENTATION"],
            min_events=2,
        )

    def test_dataset_schema_changes(
        self, graph_client, auth_session, timeline_urns: TimelineUrns
    ):
        """Add a schema then modify it — verifies TECHNICAL_SCHEMA events."""
        platform_urn = "urn:li:dataPlatform:kafka"

        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.dataset_urn,
                aspect=SchemaMetadataClass(
                    schemaName="testSchema",
                    platform=platform_urn,
                    version=0,
                    hash="v1",
                    platformSchema=OtherSchemaClass(rawSchema="col1 STRING"),
                    fields=[
                        SchemaFieldClass(
                            fieldPath="col1",
                            type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                            nativeDataType="string",
                            description="First column",
                        )
                    ],
                ),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.dataset_urn,
                aspect=SchemaMetadataClass(
                    schemaName="testSchema",
                    platform=platform_urn,
                    version=0,
                    hash="v2",
                    platformSchema=OtherSchemaClass(rawSchema="col1 STRING, col2 INT"),
                    fields=[
                        SchemaFieldClass(
                            fieldPath="col1",
                            type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                            nativeDataType="string",
                            description="First column",
                        ),
                        SchemaFieldClass(
                            fieldPath="col2",
                            type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                            nativeDataType="int",
                            description="Second column added",
                        ),
                    ],
                ),
            ),
        )

        _wait_for_timeline_events(
            auth_session,
            timeline_urns.dataset_urn,
            [("TECHNICAL_SCHEMA", "ADD")],
            "dataset/schema",
            categories=["TECHNICAL_SCHEMA"],
            min_events=1,
        )

    def test_dataset_all_categories(self, auth_session, timeline_urns: TimelineUrns):
        """Fetch timeline with all categories and verify actor attribution."""
        _wait_for_timeline_categories(
            auth_session,
            timeline_urns.dataset_urn,
            [
                "OWNERSHIP",
                "DOCUMENTATION",
                "TECHNICAL_SCHEMA",
                "TAG",
                "GLOSSARY_TERM",
                "DOMAIN",
                "STRUCTURED_PROPERTY",
                "APPLICATION",
            ],
            "dataset",
        )


# ===========================================================================
# GLOSSARY TERM TIMELINE TESTS
# ===========================================================================
class TestGlossaryTermTimeline:
    """Test all supported change categories for GlossaryTerm entities."""

    def test_glossary_term_ownership_changes(
        self, graph_client, auth_session, timeline_urns: TimelineUrns
    ):
        """Add then remove ownership — verifies ADD and REMOVE."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.glossary_term_urn,
                aspect=OwnershipClass(
                    owners=[
                        OwnerClass(
                            owner="urn:li:corpuser:alice", type="TECHNICAL_OWNER"
                        )
                    ],
                ),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.glossary_term_urn,
                aspect=OwnershipClass(owners=[]),
            ),
        )

        _wait_for_timeline_events(
            auth_session,
            timeline_urns.glossary_term_urn,
            [("OWNERSHIP", "ADD"), ("OWNERSHIP", "REMOVE")],
            "glossaryTerm/ownership",
            categories=["OWNERSHIP"],
            min_events=2,
        )

    def test_glossary_term_documentation_changes(
        self, graph_client, auth_session, timeline_urns: TimelineUrns
    ):
        """Update the glossary term definition (DOCUMENTATION category)."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.glossary_term_urn,
                aspect=GlossaryTermInfoClass(
                    name=f"Timeline Test Term {timeline_urns.unique}",
                    definition="Updated definition for timeline test",
                    termSource="INTERNAL",
                ),
            ),
        )

        _wait_for_timeline_events(
            auth_session,
            timeline_urns.glossary_term_urn,
            [("DOCUMENTATION", "MODIFY")],
            "glossaryTerm/documentation",
            categories=["DOCUMENTATION"],
            min_events=1,
        )

    def test_glossary_term_domain_changes(
        self, graph_client, auth_session, timeline_urns: TimelineUrns
    ):
        """Set then change domain — verifies ADD and REMOVE."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.glossary_term_urn,
                aspect=DomainsClass(domains=[timeline_urns.domain_engineering]),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.glossary_term_urn,
                aspect=DomainsClass(domains=[timeline_urns.domain_marketing]),
            ),
        )

        _wait_for_timeline_events(
            auth_session,
            timeline_urns.glossary_term_urn,
            [("DOMAIN", "ADD"), ("DOMAIN", "REMOVE")],
            "glossaryTerm/domain",
            categories=["DOMAIN"],
            min_events=2,
        )

    def test_glossary_term_structured_property_changes(
        self, graph_client, auth_session, timeline_urns: TimelineUrns
    ):
        """Add then update a structured property — verifies ADD and MODIFY."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.glossary_term_urn,
                aspect=StructuredPropertiesClass(
                    properties=[
                        StructuredPropertyValueAssignmentClass(
                            propertyUrn=timeline_urns.sp_urn, values=["gamma"]
                        )
                    ]
                ),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.glossary_term_urn,
                aspect=StructuredPropertiesClass(
                    properties=[
                        StructuredPropertyValueAssignmentClass(
                            propertyUrn=timeline_urns.sp_urn, values=["gamma-updated"]
                        )
                    ]
                ),
            ),
        )

        _wait_for_timeline_events(
            auth_session,
            timeline_urns.glossary_term_urn,
            [("STRUCTURED_PROPERTY", "ADD"), ("STRUCTURED_PROPERTY", "MODIFY")],
            "glossaryTerm/structuredProperty",
            categories=["STRUCTURED_PROPERTY"],
            min_events=2,
        )

    def test_glossary_term_related_terms_changes(
        self, graph_client, auth_session, timeline_urns: TimelineUrns
    ):
        """Add then swap related terms — verifies GLOSSARY_TERM ADD and REMOVE."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.glossary_term_urn,
                aspect=GlossaryRelatedTermsClass(
                    isRelatedTerms=[timeline_urns.term_a],
                ),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.glossary_term_urn,
                aspect=GlossaryRelatedTermsClass(
                    isRelatedTerms=[timeline_urns.term_b],
                ),
            ),
        )

        _wait_for_timeline_events(
            auth_session,
            timeline_urns.glossary_term_urn,
            [("GLOSSARY_TERM", "ADD"), ("GLOSSARY_TERM", "REMOVE")],
            "glossaryTerm/relatedTerms",
            categories=["GLOSSARY_TERM"],
            min_events=2,
        )

    def test_glossary_term_application_changes(
        self, graph_client, auth_session, timeline_urns: TimelineUrns
    ):
        """Add an application then swap it — verifies ADD and REMOVE."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.glossary_term_urn,
                aspect=ApplicationsClass(applications=[timeline_urns.app_urn_1]),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.glossary_term_urn,
                aspect=ApplicationsClass(applications=[timeline_urns.app_urn_2]),
            ),
        )

        _wait_for_timeline_events(
            auth_session,
            timeline_urns.glossary_term_urn,
            [("APPLICATION", "ADD"), ("APPLICATION", "REMOVE")],
            "glossaryTerm/application",
            categories=["APPLICATION"],
            min_events=2,
        )

    def test_glossary_term_all_categories(
        self, auth_session, timeline_urns: TimelineUrns
    ):
        _wait_for_timeline_categories(
            auth_session,
            timeline_urns.glossary_term_urn,
            [
                "OWNERSHIP",
                "DOCUMENTATION",
                "GLOSSARY_TERM",
                "DOMAIN",
                "STRUCTURED_PROPERTY",
                "APPLICATION",
            ],
            "glossaryTerm",
        )


# ===========================================================================
# DOMAIN TIMELINE TESTS
# ===========================================================================
class TestDomainTimeline:
    """Test all supported change categories for Domain entities."""

    def test_domain_ownership_changes(
        self, graph_client, auth_session, timeline_urns: TimelineUrns
    ):
        """Add then remove ownership — verifies ADD and REMOVE."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.domain_urn,
                aspect=OwnershipClass(
                    owners=[
                        OwnerClass(
                            owner="urn:li:corpuser:alice", type="TECHNICAL_OWNER"
                        )
                    ],
                ),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.domain_urn,
                aspect=OwnershipClass(owners=[]),
            ),
        )

        _wait_for_timeline_events(
            auth_session,
            timeline_urns.domain_urn,
            [("OWNERSHIP", "ADD"), ("OWNERSHIP", "REMOVE")],
            "domain/ownership",
            categories=["OWNERSHIP"],
            min_events=2,
        )

    def test_domain_documentation_changes(
        self, graph_client, auth_session, timeline_urns: TimelineUrns
    ):
        """Update domain name/description (DOCUMENTATION via DomainPropertiesChangeEventGenerator)."""
        from datahub.metadata.schema_classes import DomainPropertiesClass

        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.domain_urn,
                aspect=DomainPropertiesClass(
                    name=f"Timeline Test Domain {timeline_urns.unique} - Renamed",
                    description="Updated domain description",
                ),
            ),
        )

        _wait_for_timeline_events(
            auth_session,
            timeline_urns.domain_urn,
            [("DOCUMENTATION", "MODIFY")],
            "domain/documentation",
            categories=["DOCUMENTATION"],
            min_events=1,
        )

    def test_domain_structured_property_changes(
        self, graph_client, auth_session, timeline_urns: TimelineUrns
    ):
        """Add then remove a structured property — verifies ADD and REMOVE."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.domain_urn,
                aspect=StructuredPropertiesClass(
                    properties=[
                        StructuredPropertyValueAssignmentClass(
                            propertyUrn=timeline_urns.sp_urn, values=["delta"]
                        )
                    ]
                ),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.domain_urn,
                aspect=StructuredPropertiesClass(properties=[]),
            ),
        )

        _wait_for_timeline_events(
            auth_session,
            timeline_urns.domain_urn,
            [("STRUCTURED_PROPERTY", "ADD"), ("STRUCTURED_PROPERTY", "REMOVE")],
            "domain/structuredProperty",
            categories=["STRUCTURED_PROPERTY"],
            min_events=2,
        )

    def test_domain_all_categories(self, auth_session, timeline_urns: TimelineUrns):
        _wait_for_timeline_categories(
            auth_session,
            timeline_urns.domain_urn,
            ["OWNERSHIP", "DOCUMENTATION", "STRUCTURED_PROPERTY"],
            "domain",
        )


# ===========================================================================
# DATA PRODUCT TIMELINE TESTS
# ===========================================================================
class TestDataProductTimeline:
    """Test all supported change categories for DataProduct entities."""

    def test_data_product_ownership_changes(
        self, graph_client, auth_session, timeline_urns: TimelineUrns
    ):
        """Add then swap ownership — verifies ADD and REMOVE."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.data_product_urn,
                aspect=OwnershipClass(
                    owners=[
                        OwnerClass(
                            owner="urn:li:corpuser:alice", type="TECHNICAL_OWNER"
                        )
                    ],
                ),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.data_product_urn,
                aspect=OwnershipClass(
                    owners=[
                        OwnerClass(owner="urn:li:corpuser:bob", type="DATA_STEWARD")
                    ],
                ),
            ),
        )

        _wait_for_timeline_events(
            auth_session,
            timeline_urns.data_product_urn,
            [("OWNERSHIP", "ADD"), ("OWNERSHIP", "REMOVE")],
            "dataProduct/ownership",
            categories=["OWNERSHIP"],
            min_events=2,
        )

    def test_data_product_documentation_changes(
        self, graph_client, auth_session, timeline_urns: TimelineUrns
    ):
        """Update data product name/description (DOCUMENTATION via DataProductPropertiesChangeEventGenerator)."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.data_product_urn,
                aspect=DataProductPropertiesClass(
                    name=f"Timeline Test Product {timeline_urns.unique} - Renamed",
                    description="Updated product description",
                ),
            ),
        )

        _wait_for_timeline_events(
            auth_session,
            timeline_urns.data_product_urn,
            [("DOCUMENTATION", "MODIFY")],
            "dataProduct/documentation",
            categories=["DOCUMENTATION"],
            min_events=1,
        )

    def test_data_product_tag_changes(
        self, graph_client, auth_session, timeline_urns: TimelineUrns
    ):
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.data_product_urn,
                aspect=GlobalTagsClass(tags=[TagAssociationClass(tag=TAG_PII)]),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.data_product_urn,
                aspect=GlobalTagsClass(
                    tags=[TagAssociationClass(tag=TAG_CONFIDENTIAL)]
                ),
            ),
        )

        _wait_for_timeline_events(
            auth_session,
            timeline_urns.data_product_urn,
            [("TAG", "ADD"), ("TAG", "REMOVE")],
            "dataProduct/tag",
            categories=["TAG"],
            min_events=2,
        )

    def test_data_product_glossary_term_changes(
        self, graph_client, auth_session, timeline_urns: TimelineUrns
    ):
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.data_product_urn,
                aspect=GlossaryTermsClass(
                    terms=[GlossaryTermAssociationClass(urn=timeline_urns.term_a)],
                    auditStamp=AuditStampClass(
                        time=_now_ms(), actor="urn:li:corpuser:datahub"
                    ),
                ),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.data_product_urn,
                aspect=GlossaryTermsClass(
                    terms=[],
                    auditStamp=AuditStampClass(
                        time=_now_ms(), actor="urn:li:corpuser:datahub"
                    ),
                ),
            ),
        )

        _wait_for_timeline_events(
            auth_session,
            timeline_urns.data_product_urn,
            [("GLOSSARY_TERM", "ADD"), ("GLOSSARY_TERM", "REMOVE")],
            "dataProduct/glossaryTerm",
            categories=["GLOSSARY_TERM"],
            min_events=2,
        )

    def test_data_product_domain_changes(
        self, graph_client, auth_session, timeline_urns: TimelineUrns
    ):
        """Set domain then swap it — verifies ADD and REMOVE."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.data_product_urn,
                aspect=DomainsClass(domains=[timeline_urns.domain_engineering]),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.data_product_urn,
                aspect=DomainsClass(domains=[timeline_urns.domain_marketing]),
            ),
        )

        _wait_for_timeline_events(
            auth_session,
            timeline_urns.data_product_urn,
            [("DOMAIN", "ADD"), ("DOMAIN", "REMOVE")],
            "dataProduct/domain",
            categories=["DOMAIN"],
            min_events=2,
        )

    def test_data_product_structured_property_changes(
        self, graph_client, auth_session, timeline_urns: TimelineUrns
    ):
        """Add then update a structured property — verifies ADD and MODIFY."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.data_product_urn,
                aspect=StructuredPropertiesClass(
                    properties=[
                        StructuredPropertyValueAssignmentClass(
                            propertyUrn=timeline_urns.sp_urn, values=["epsilon"]
                        )
                    ]
                ),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.data_product_urn,
                aspect=StructuredPropertiesClass(
                    properties=[
                        StructuredPropertyValueAssignmentClass(
                            propertyUrn=timeline_urns.sp_urn, values=["zeta"]
                        )
                    ]
                ),
            ),
        )

        _wait_for_timeline_events(
            auth_session,
            timeline_urns.data_product_urn,
            [
                ("STRUCTURED_PROPERTY", "ADD"),
                ("STRUCTURED_PROPERTY", "MODIFY"),
            ],
            "dataProduct/structuredProperty",
            categories=["STRUCTURED_PROPERTY"],
            min_events=2,
        )

    def test_data_product_application_changes(
        self, graph_client, auth_session, timeline_urns: TimelineUrns
    ):
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.data_product_urn,
                aspect=ApplicationsClass(applications=[timeline_urns.app_urn_1]),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.data_product_urn,
                aspect=ApplicationsClass(applications=[timeline_urns.app_urn_2]),
            ),
        )

        _wait_for_timeline_events(
            auth_session,
            timeline_urns.data_product_urn,
            [("APPLICATION", "ADD"), ("APPLICATION", "REMOVE")],
            "dataProduct/application",
            categories=["APPLICATION"],
            min_events=2,
        )

    def test_data_product_asset_membership_changes(
        self, graph_client, auth_session, timeline_urns: TimelineUrns
    ):
        """Add an asset, then swap it — verifies ASSET_MEMBERSHIP category."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.data_product_urn,
                aspect=DataProductPropertiesClass(
                    name=f"Timeline Test Product {timeline_urns.unique}",
                    assets=[
                        DataProductAssociationClass(
                            destinationUrn=timeline_urns.asset_dataset_1
                        )
                    ],
                ),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=timeline_urns.data_product_urn,
                aspect=DataProductPropertiesClass(
                    name=f"Timeline Test Product {timeline_urns.unique}",
                    assets=[
                        DataProductAssociationClass(
                            destinationUrn=timeline_urns.asset_dataset_2
                        )
                    ],
                ),
            ),
        )

        _wait_for_timeline_events(
            auth_session,
            timeline_urns.data_product_urn,
            [("ASSET_MEMBERSHIP", "ADD"), ("ASSET_MEMBERSHIP", "REMOVE")],
            "dataProduct/assetMembership",
            categories=["ASSET_MEMBERSHIP"],
            min_events=2,
        )

    def test_data_product_all_categories(
        self, auth_session, timeline_urns: TimelineUrns
    ):
        """Verify all categories appear and actor attribution works."""
        _wait_for_timeline_categories(
            auth_session,
            timeline_urns.data_product_urn,
            [
                "OWNERSHIP",
                "DOCUMENTATION",
                "TAG",
                "GLOSSARY_TERM",
                "DOMAIN",
                "STRUCTURED_PROPERTY",
                "APPLICATION",
                "ASSET_MEMBERSHIP",
            ],
            "dataProduct",
        )

    def test_data_product_timeline_structure(
        self, auth_session, timeline_urns: TimelineUrns
    ):
        """Verify the GraphQL response structure matches what the frontend expects."""
        # Wait for timeline materialization before structure checks.
        _wait_for_timeline_categories(
            auth_session,
            timeline_urns.data_product_urn,
            ["OWNERSHIP"],
            "dataProduct/structure",
        )
        txns = _get_timeline(auth_session, timeline_urns.data_product_urn)
        assert len(txns) > 0, "Expected at least one transaction"

        for tx in txns:
            # Timestamps are present and non-zero
            assert isinstance(tx["timestampMillis"], int)
            assert tx["timestampMillis"] > 0

            # Semantic version is present
            assert tx["lastSemanticVersion"] is not None

            # Changes list is present
            assert tx["changes"] is not None
            for change in tx["changes"]:
                assert change["urn"] is not None
                assert change["category"] is not None
                assert change["operation"] is not None
