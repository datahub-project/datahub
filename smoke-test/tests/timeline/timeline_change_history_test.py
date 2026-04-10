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
from typing import Any, Dict, List, Optional, Set, Tuple

import pytest

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
from tests.utils import execute_graphql

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.no_cypress_suite1

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
UNIQUE = uuid.uuid4().hex[:8]

DATASET_URN = f"urn:li:dataset:(urn:li:dataPlatform:kafka,timeline-test-{UNIQUE},PROD)"
GLOSSARY_TERM_URN = f"urn:li:glossaryTerm:timeline-test-term-{UNIQUE}"
DOMAIN_URN = f"urn:li:domain:timeline-test-domain-{UNIQUE}"
DATA_PRODUCT_URN = f"urn:li:dataProduct:timeline-test-dp-{UNIQUE}"

# Structured property used across entity types
SP_URN = str(StructuredPropertyUrn(f"io.acryl.timeline.test.{UNIQUE}"))

# Tags and terms referenced in tests
TAG_PII = "urn:li:tag:PII"
TAG_CONFIDENTIAL = "urn:li:tag:Confidential"
TERM_A = f"urn:li:glossaryTerm:timeline-ref-term-a-{UNIQUE}"
TERM_B = f"urn:li:glossaryTerm:timeline-ref-term-b-{UNIQUE}"
DOMAIN_ENGINEERING = f"urn:li:domain:timeline-ref-eng-{UNIQUE}"
DOMAIN_MARKETING = f"urn:li:domain:timeline-ref-mkt-{UNIQUE}"
APP_URN_1 = f"urn:li:application:timeline-ref-app1-{UNIQUE}"
APP_URN_2 = f"urn:li:application:timeline-ref-app2-{UNIQUE}"
ASSET_DATASET_1 = (
    f"urn:li:dataset:(urn:li:dataPlatform:snowflake,timeline-asset1-{UNIQUE},PROD)"
)
ASSET_DATASET_2 = (
    f"urn:li:dataset:(urn:li:dataPlatform:snowflake,timeline-asset2-{UNIQUE},PROD)"
)

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
    """Emit an MCP and wait for writes to propagate."""
    graph_client.emit_mcp(mcp)
    wait_for_writes_to_sync()


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
def setup_entities(graph_client):
    """Create all test entities and the shared structured property definition."""
    logger.info("Creating test entities for timeline change history tests")

    # --- Structured property definition (used by dataset, glossary term, domain, data product) ---
    sp_def = StructuredPropertyDefinitionClass(
        qualifiedName=f"io.acryl.timeline.test.{UNIQUE}",
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
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(entityUrn=SP_URN, aspect=sp_def)
    )

    # --- Dataset ---
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=DATASET_URN,
            aspect=DatasetPropertiesClass(
                name=f"timeline-test-{UNIQUE}",
                description="Initial description",
            ),
        )
    )

    # --- Glossary Term ---
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=GLOSSARY_TERM_URN,
            aspect=GlossaryTermInfoClass(
                name=f"Timeline Test Term {UNIQUE}",
                definition="Initial definition",
                termSource="INTERNAL",
            ),
        )
    )

    # --- Reference glossary terms (used as related terms) ---
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=TERM_A,
            aspect=GlossaryTermInfoClass(
                name=f"Ref Term A {UNIQUE}",
                definition="Reference term A",
                termSource="INTERNAL",
            ),
        )
    )
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=TERM_B,
            aspect=GlossaryTermInfoClass(
                name=f"Ref Term B {UNIQUE}",
                definition="Reference term B",
                termSource="INTERNAL",
            ),
        )
    )

    # --- Domain ---
    from datahub.metadata.schema_classes import DomainPropertiesClass

    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=DOMAIN_URN,
            aspect=DomainPropertiesClass(
                name=f"Timeline Test Domain {UNIQUE}",
                description="Initial domain description",
            ),
        )
    )

    # --- Reference domains ---
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=DOMAIN_ENGINEERING,
            aspect=DomainPropertiesClass(name="Engineering"),
        )
    )
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=DOMAIN_MARKETING,
            aspect=DomainPropertiesClass(name="Marketing"),
        )
    )

    # --- Data Product ---
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=DATA_PRODUCT_URN,
            aspect=DataProductPropertiesClass(
                name=f"Timeline Test Product {UNIQUE}",
                description="Initial product description",
            ),
        )
    )

    wait_for_writes_to_sync()

    yield

    # --- Cleanup ---
    logger.info("Cleaning up test entities")
    for urn in [
        DATASET_URN,
        GLOSSARY_TERM_URN,
        DOMAIN_URN,
        DATA_PRODUCT_URN,
        SP_URN,
        TERM_A,
        TERM_B,
        DOMAIN_ENGINEERING,
        DOMAIN_MARKETING,
        APP_URN_1,
        APP_URN_2,
        ASSET_DATASET_1,
        ASSET_DATASET_2,
    ]:
        try:
            graph_client.hard_delete_entity(urn=urn)
        except Exception:
            logger.warning(f"Failed to delete {urn} during cleanup")


# ===========================================================================
# DATASET TIMELINE TESTS
# ===========================================================================
class TestDatasetTimeline:
    """Test all supported change categories for Dataset entities."""

    def test_dataset_ownership_changes(self, graph_client, auth_session):
        """Add then change ownership on a dataset — verifies ADD and REMOVE."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_URN,
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
                entityUrn=DATASET_URN,
                aspect=OwnershipClass(
                    owners=[
                        OwnerClass(owner="urn:li:corpuser:bob", type="DATA_STEWARD")
                    ],
                ),
            ),
        )

        txns = _get_timeline(auth_session, DATASET_URN, ["OWNERSHIP"])
        events = _collect_change_events(txns)
        assert len(events) >= 2, f"Expected >=2 ownership events, got {len(events)}"
        _assert_has_events(
            events,
            [("OWNERSHIP", "ADD"), ("OWNERSHIP", "REMOVE")],
            "dataset/ownership",
        )

    def test_dataset_tag_changes(self, graph_client, auth_session):
        """Add a tag, then swap it."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_URN,
                aspect=GlobalTagsClass(tags=[TagAssociationClass(tag=TAG_PII)]),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_URN,
                aspect=GlobalTagsClass(
                    tags=[TagAssociationClass(tag=TAG_CONFIDENTIAL)]
                ),
            ),
        )

        txns = _get_timeline(auth_session, DATASET_URN, ["TAG"])
        events = _collect_change_events(txns)
        assert len(events) >= 2, f"Expected >=2 tag events, got {len(events)}"
        _assert_has_events(events, [("TAG", "ADD"), ("TAG", "REMOVE")], "dataset/tag")

    def test_dataset_glossary_term_changes(self, graph_client, auth_session):
        """Add then remove a glossary term."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_URN,
                aspect=GlossaryTermsClass(
                    terms=[GlossaryTermAssociationClass(urn=TERM_A)],
                    auditStamp=AuditStampClass(
                        time=_now_ms(), actor="urn:li:corpuser:datahub"
                    ),
                ),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_URN,
                aspect=GlossaryTermsClass(
                    terms=[],
                    auditStamp=AuditStampClass(
                        time=_now_ms(), actor="urn:li:corpuser:datahub"
                    ),
                ),
            ),
        )

        txns = _get_timeline(auth_session, DATASET_URN, ["GLOSSARY_TERM"])
        events = _collect_change_events(txns)
        assert len(events) >= 2, f"Expected >=2 term events, got {len(events)}"
        _assert_has_events(
            events,
            [("GLOSSARY_TERM", "ADD"), ("GLOSSARY_TERM", "REMOVE")],
            "dataset/glossaryTerm",
        )

    def test_dataset_domain_changes(self, graph_client, auth_session):
        """Set domain, then change it — verifies ADD and REMOVE."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_URN,
                aspect=DomainsClass(domains=[DOMAIN_ENGINEERING]),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_URN,
                aspect=DomainsClass(domains=[DOMAIN_MARKETING]),
            ),
        )

        txns = _get_timeline(auth_session, DATASET_URN, ["DOMAIN"])
        events = _collect_change_events(txns)
        assert len(events) >= 2, f"Expected >=2 domain events, got {len(events)}"
        _assert_has_events(
            events,
            [("DOMAIN", "ADD"), ("DOMAIN", "REMOVE")],
            "dataset/domain",
        )

    def test_dataset_structured_property_changes(self, graph_client, auth_session):
        """Assign, update, then remove a structured property — verifies ADD, MODIFY, REMOVE."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_URN,
                aspect=StructuredPropertiesClass(
                    properties=[
                        StructuredPropertyValueAssignmentClass(
                            propertyUrn=SP_URN, values=["alpha"]
                        )
                    ]
                ),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_URN,
                aspect=StructuredPropertiesClass(
                    properties=[
                        StructuredPropertyValueAssignmentClass(
                            propertyUrn=SP_URN, values=["beta"]
                        )
                    ]
                ),
            ),
        )
        # Remove the structured property entirely
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_URN,
                aspect=StructuredPropertiesClass(properties=[]),
            ),
        )

        txns = _get_timeline(auth_session, DATASET_URN, ["STRUCTURED_PROPERTY"])
        events = _collect_change_events(txns)
        assert len(events) >= 3, f"Expected >=3 SP events, got {len(events)}"
        _assert_has_events(
            events,
            [
                ("STRUCTURED_PROPERTY", "ADD"),
                ("STRUCTURED_PROPERTY", "MODIFY"),
                ("STRUCTURED_PROPERTY", "REMOVE"),
            ],
            "dataset/structuredProperty",
        )

    def test_dataset_application_changes(self, graph_client, auth_session):
        """Add an application, then swap it."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_URN,
                aspect=ApplicationsClass(applications=[APP_URN_1]),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_URN,
                aspect=ApplicationsClass(applications=[APP_URN_2]),
            ),
        )

        txns = _get_timeline(auth_session, DATASET_URN, ["APPLICATION"])
        events = _collect_change_events(txns)
        assert len(events) >= 2, f"Expected >=2 application events, got {len(events)}"
        _assert_has_events(
            events,
            [("APPLICATION", "ADD"), ("APPLICATION", "REMOVE")],
            "dataset/application",
        )

    def test_dataset_documentation_changes(self, graph_client, auth_session):
        """Add then update documentation on a dataset — verifies ADD and MODIFY."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_URN,
                aspect=EditableDatasetPropertiesClass(
                    description="Initial dataset description for timeline test",
                ),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_URN,
                aspect=EditableDatasetPropertiesClass(
                    description="Updated dataset description for timeline test",
                ),
            ),
        )

        txns = _get_timeline(auth_session, DATASET_URN, ["DOCUMENTATION"])
        events = _collect_change_events(txns)
        assert len(events) >= 2, f"Expected >=2 documentation events, got {len(events)}"
        _assert_has_events(
            events,
            [("DOCUMENTATION", "ADD"), ("DOCUMENTATION", "MODIFY")],
            "dataset/documentation",
        )

    def test_dataset_schema_changes(self, graph_client, auth_session):
        """Add a schema then modify it — verifies TECHNICAL_SCHEMA events."""
        platform_urn = "urn:li:dataPlatform:kafka"

        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_URN,
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
                entityUrn=DATASET_URN,
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

        txns = _get_timeline(auth_session, DATASET_URN, ["TECHNICAL_SCHEMA"])
        events = _collect_change_events(txns)
        assert len(events) >= 1, f"Expected >=1 schema events, got {len(events)}"
        _assert_has_events(
            events,
            [("TECHNICAL_SCHEMA", "ADD")],
            "dataset/schema",
        )

    def test_dataset_all_categories(self, auth_session):
        """Fetch timeline with all categories and verify actor attribution."""
        txns = _get_timeline(auth_session, DATASET_URN)
        events = _collect_change_events(txns)
        categories = {e["category"] for e in events if e.get("category")}

        for expected in [
            "OWNERSHIP",
            "DOCUMENTATION",
            "TECHNICAL_SCHEMA",
            "TAG",
            "GLOSSARY_TERM",
            "DOMAIN",
            "STRUCTURED_PROPERTY",
            "APPLICATION",
        ]:
            assert expected in categories, (
                f"Dataset timeline missing category {expected}. "
                f"Found: {sorted(categories)}"
            )

        _assert_actor_present(txns, "dataset")


# ===========================================================================
# GLOSSARY TERM TIMELINE TESTS
# ===========================================================================
class TestGlossaryTermTimeline:
    """Test all supported change categories for GlossaryTerm entities."""

    def test_glossary_term_ownership_changes(self, graph_client, auth_session):
        """Add then remove ownership — verifies ADD and REMOVE."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=GLOSSARY_TERM_URN,
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
                entityUrn=GLOSSARY_TERM_URN,
                aspect=OwnershipClass(owners=[]),
            ),
        )

        txns = _get_timeline(auth_session, GLOSSARY_TERM_URN, ["OWNERSHIP"])
        events = _collect_change_events(txns)
        assert len(events) >= 2, f"Expected >=2 ownership events, got {len(events)}"
        _assert_has_events(
            events,
            [("OWNERSHIP", "ADD"), ("OWNERSHIP", "REMOVE")],
            "glossaryTerm/ownership",
        )

    def test_glossary_term_documentation_changes(self, graph_client, auth_session):
        """Update the glossary term definition (DOCUMENTATION category)."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=GLOSSARY_TERM_URN,
                aspect=GlossaryTermInfoClass(
                    name=f"Timeline Test Term {UNIQUE}",
                    definition="Updated definition for timeline test",
                    termSource="INTERNAL",
                ),
            ),
        )

        txns = _get_timeline(auth_session, GLOSSARY_TERM_URN, ["DOCUMENTATION"])
        events = _collect_change_events(txns)
        assert len(events) >= 1, f"Expected >=1 documentation events, got {len(events)}"

    def test_glossary_term_domain_changes(self, graph_client, auth_session):
        """Set then change domain — verifies ADD and REMOVE."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=GLOSSARY_TERM_URN,
                aspect=DomainsClass(domains=[DOMAIN_ENGINEERING]),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=GLOSSARY_TERM_URN,
                aspect=DomainsClass(domains=[DOMAIN_MARKETING]),
            ),
        )

        txns = _get_timeline(auth_session, GLOSSARY_TERM_URN, ["DOMAIN"])
        events = _collect_change_events(txns)
        assert len(events) >= 2, f"Expected >=2 domain events, got {len(events)}"
        _assert_has_events(
            events,
            [("DOMAIN", "ADD"), ("DOMAIN", "REMOVE")],
            "glossaryTerm/domain",
        )

    def test_glossary_term_structured_property_changes(
        self, graph_client, auth_session
    ):
        """Add then update a structured property — verifies ADD and MODIFY."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=GLOSSARY_TERM_URN,
                aspect=StructuredPropertiesClass(
                    properties=[
                        StructuredPropertyValueAssignmentClass(
                            propertyUrn=SP_URN, values=["gamma"]
                        )
                    ]
                ),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=GLOSSARY_TERM_URN,
                aspect=StructuredPropertiesClass(
                    properties=[
                        StructuredPropertyValueAssignmentClass(
                            propertyUrn=SP_URN, values=["gamma-updated"]
                        )
                    ]
                ),
            ),
        )

        txns = _get_timeline(auth_session, GLOSSARY_TERM_URN, ["STRUCTURED_PROPERTY"])
        events = _collect_change_events(txns)
        assert len(events) >= 2, f"Expected >=2 SP events, got {len(events)}"
        _assert_has_events(
            events,
            [("STRUCTURED_PROPERTY", "ADD"), ("STRUCTURED_PROPERTY", "MODIFY")],
            "glossaryTerm/structuredProperty",
        )

    def test_glossary_term_related_terms_changes(self, graph_client, auth_session):
        """Add then swap related terms — verifies GLOSSARY_TERM ADD and REMOVE."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=GLOSSARY_TERM_URN,
                aspect=GlossaryRelatedTermsClass(
                    isRelatedTerms=[TERM_A],
                ),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=GLOSSARY_TERM_URN,
                aspect=GlossaryRelatedTermsClass(
                    isRelatedTerms=[TERM_B],
                ),
            ),
        )

        txns = _get_timeline(auth_session, GLOSSARY_TERM_URN, ["GLOSSARY_TERM"])
        events = _collect_change_events(txns)
        assert len(events) >= 2, f"Expected >=2 related term events, got {len(events)}"
        _assert_has_events(
            events,
            [("GLOSSARY_TERM", "ADD"), ("GLOSSARY_TERM", "REMOVE")],
            "glossaryTerm/relatedTerms",
        )

    def test_glossary_term_application_changes(self, graph_client, auth_session):
        """Add an application then swap it — verifies ADD and REMOVE."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=GLOSSARY_TERM_URN,
                aspect=ApplicationsClass(applications=[APP_URN_1]),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=GLOSSARY_TERM_URN,
                aspect=ApplicationsClass(applications=[APP_URN_2]),
            ),
        )

        txns = _get_timeline(auth_session, GLOSSARY_TERM_URN, ["APPLICATION"])
        events = _collect_change_events(txns)
        assert len(events) >= 2, f"Expected >=2 application events, got {len(events)}"
        _assert_has_events(
            events,
            [("APPLICATION", "ADD"), ("APPLICATION", "REMOVE")],
            "glossaryTerm/application",
        )

    def test_glossary_term_all_categories(self, auth_session):
        txns = _get_timeline(auth_session, GLOSSARY_TERM_URN)
        events = _collect_change_events(txns)
        categories = {e["category"] for e in events if e.get("category")}

        for expected in [
            "OWNERSHIP",
            "DOCUMENTATION",
            "GLOSSARY_TERM",
            "DOMAIN",
            "STRUCTURED_PROPERTY",
            "APPLICATION",
        ]:
            assert expected in categories, (
                f"GlossaryTerm timeline missing {expected}. Found: {sorted(categories)}"
            )
        _assert_actor_present(txns, "glossaryTerm")


# ===========================================================================
# DOMAIN TIMELINE TESTS
# ===========================================================================
class TestDomainTimeline:
    """Test all supported change categories for Domain entities."""

    def test_domain_ownership_changes(self, graph_client, auth_session):
        """Add then remove ownership — verifies ADD and REMOVE."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=DOMAIN_URN,
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
                entityUrn=DOMAIN_URN,
                aspect=OwnershipClass(owners=[]),
            ),
        )

        txns = _get_timeline(auth_session, DOMAIN_URN, ["OWNERSHIP"])
        events = _collect_change_events(txns)
        assert len(events) >= 2, f"Expected >=2 ownership events, got {len(events)}"
        _assert_has_events(
            events,
            [("OWNERSHIP", "ADD"), ("OWNERSHIP", "REMOVE")],
            "domain/ownership",
        )

    def test_domain_documentation_changes(self, graph_client, auth_session):
        """Update domain name/description (DOCUMENTATION via DomainPropertiesChangeEventGenerator)."""
        from datahub.metadata.schema_classes import DomainPropertiesClass

        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=DOMAIN_URN,
                aspect=DomainPropertiesClass(
                    name=f"Timeline Test Domain {UNIQUE} - Renamed",
                    description="Updated domain description",
                ),
            ),
        )

        txns = _get_timeline(auth_session, DOMAIN_URN, ["DOCUMENTATION"])
        events = _collect_change_events(txns)
        assert len(events) >= 1, f"Expected >=1 documentation events, got {len(events)}"
        _assert_has_events(
            events, [("DOCUMENTATION", "MODIFY")], "domain/documentation"
        )

    def test_domain_structured_property_changes(self, graph_client, auth_session):
        """Add then remove a structured property — verifies ADD and REMOVE."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=DOMAIN_URN,
                aspect=StructuredPropertiesClass(
                    properties=[
                        StructuredPropertyValueAssignmentClass(
                            propertyUrn=SP_URN, values=["delta"]
                        )
                    ]
                ),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=DOMAIN_URN,
                aspect=StructuredPropertiesClass(properties=[]),
            ),
        )

        txns = _get_timeline(auth_session, DOMAIN_URN, ["STRUCTURED_PROPERTY"])
        events = _collect_change_events(txns)
        assert len(events) >= 2, f"Expected >=2 SP events, got {len(events)}"
        _assert_has_events(
            events,
            [("STRUCTURED_PROPERTY", "ADD"), ("STRUCTURED_PROPERTY", "REMOVE")],
            "domain/structuredProperty",
        )

    def test_domain_all_categories(self, auth_session):
        txns = _get_timeline(auth_session, DOMAIN_URN)
        events = _collect_change_events(txns)
        categories = {e["category"] for e in events if e.get("category")}

        for expected in ["OWNERSHIP", "DOCUMENTATION", "STRUCTURED_PROPERTY"]:
            assert expected in categories, (
                f"Domain timeline missing {expected}. Found: {sorted(categories)}"
            )
        _assert_actor_present(txns, "domain")


# ===========================================================================
# DATA PRODUCT TIMELINE TESTS
# ===========================================================================
class TestDataProductTimeline:
    """Test all supported change categories for DataProduct entities."""

    def test_data_product_ownership_changes(self, graph_client, auth_session):
        """Add then swap ownership — verifies ADD and REMOVE."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=DATA_PRODUCT_URN,
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
                entityUrn=DATA_PRODUCT_URN,
                aspect=OwnershipClass(
                    owners=[
                        OwnerClass(owner="urn:li:corpuser:bob", type="DATA_STEWARD")
                    ],
                ),
            ),
        )

        txns = _get_timeline(auth_session, DATA_PRODUCT_URN, ["OWNERSHIP"])
        events = _collect_change_events(txns)
        assert len(events) >= 2, f"Expected >=2 ownership events, got {len(events)}"
        _assert_has_events(
            events,
            [("OWNERSHIP", "ADD"), ("OWNERSHIP", "REMOVE")],
            "dataProduct/ownership",
        )

    def test_data_product_documentation_changes(self, graph_client, auth_session):
        """Update data product name/description (DOCUMENTATION via DataProductPropertiesChangeEventGenerator)."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=DATA_PRODUCT_URN,
                aspect=DataProductPropertiesClass(
                    name=f"Timeline Test Product {UNIQUE} - Renamed",
                    description="Updated product description",
                ),
            ),
        )

        txns = _get_timeline(auth_session, DATA_PRODUCT_URN, ["DOCUMENTATION"])
        events = _collect_change_events(txns)
        assert len(events) >= 1, f"Expected >=1 documentation events, got {len(events)}"
        _assert_has_events(
            events, [("DOCUMENTATION", "MODIFY")], "dataProduct/documentation"
        )

    def test_data_product_tag_changes(self, graph_client, auth_session):
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=DATA_PRODUCT_URN,
                aspect=GlobalTagsClass(tags=[TagAssociationClass(tag=TAG_PII)]),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=DATA_PRODUCT_URN,
                aspect=GlobalTagsClass(
                    tags=[TagAssociationClass(tag=TAG_CONFIDENTIAL)]
                ),
            ),
        )

        txns = _get_timeline(auth_session, DATA_PRODUCT_URN, ["TAG"])
        events = _collect_change_events(txns)
        assert len(events) >= 2, f"Expected >=2 tag events, got {len(events)}"
        _assert_has_events(
            events, [("TAG", "ADD"), ("TAG", "REMOVE")], "dataProduct/tag"
        )

    def test_data_product_glossary_term_changes(self, graph_client, auth_session):
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=DATA_PRODUCT_URN,
                aspect=GlossaryTermsClass(
                    terms=[GlossaryTermAssociationClass(urn=TERM_A)],
                    auditStamp=AuditStampClass(
                        time=_now_ms(), actor="urn:li:corpuser:datahub"
                    ),
                ),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=DATA_PRODUCT_URN,
                aspect=GlossaryTermsClass(
                    terms=[],
                    auditStamp=AuditStampClass(
                        time=_now_ms(), actor="urn:li:corpuser:datahub"
                    ),
                ),
            ),
        )

        txns = _get_timeline(auth_session, DATA_PRODUCT_URN, ["GLOSSARY_TERM"])
        events = _collect_change_events(txns)
        assert len(events) >= 2, f"Expected >=2 term events, got {len(events)}"
        _assert_has_events(
            events,
            [("GLOSSARY_TERM", "ADD"), ("GLOSSARY_TERM", "REMOVE")],
            "dataProduct/glossaryTerm",
        )

    def test_data_product_domain_changes(self, graph_client, auth_session):
        """Set domain then swap it — verifies ADD and REMOVE."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=DATA_PRODUCT_URN,
                aspect=DomainsClass(domains=[DOMAIN_ENGINEERING]),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=DATA_PRODUCT_URN,
                aspect=DomainsClass(domains=[DOMAIN_MARKETING]),
            ),
        )

        txns = _get_timeline(auth_session, DATA_PRODUCT_URN, ["DOMAIN"])
        events = _collect_change_events(txns)
        assert len(events) >= 2, f"Expected >=2 domain events, got {len(events)}"
        _assert_has_events(
            events,
            [("DOMAIN", "ADD"), ("DOMAIN", "REMOVE")],
            "dataProduct/domain",
        )

    def test_data_product_structured_property_changes(self, graph_client, auth_session):
        """Add then update a structured property — verifies ADD and MODIFY."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=DATA_PRODUCT_URN,
                aspect=StructuredPropertiesClass(
                    properties=[
                        StructuredPropertyValueAssignmentClass(
                            propertyUrn=SP_URN, values=["epsilon"]
                        )
                    ]
                ),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=DATA_PRODUCT_URN,
                aspect=StructuredPropertiesClass(
                    properties=[
                        StructuredPropertyValueAssignmentClass(
                            propertyUrn=SP_URN, values=["zeta"]
                        )
                    ]
                ),
            ),
        )

        txns = _get_timeline(auth_session, DATA_PRODUCT_URN, ["STRUCTURED_PROPERTY"])
        events = _collect_change_events(txns)
        assert len(events) >= 2, f"Expected >=2 SP events, got {len(events)}"
        _assert_has_events(
            events,
            [
                ("STRUCTURED_PROPERTY", "ADD"),
                ("STRUCTURED_PROPERTY", "MODIFY"),
            ],
            "dataProduct/structuredProperty",
        )

    def test_data_product_application_changes(self, graph_client, auth_session):
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=DATA_PRODUCT_URN,
                aspect=ApplicationsClass(applications=[APP_URN_1]),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=DATA_PRODUCT_URN,
                aspect=ApplicationsClass(applications=[APP_URN_2]),
            ),
        )

        txns = _get_timeline(auth_session, DATA_PRODUCT_URN, ["APPLICATION"])
        events = _collect_change_events(txns)
        assert len(events) >= 2, f"Expected >=2 application events, got {len(events)}"
        _assert_has_events(
            events,
            [("APPLICATION", "ADD"), ("APPLICATION", "REMOVE")],
            "dataProduct/application",
        )

    def test_data_product_asset_membership_changes(self, graph_client, auth_session):
        """Add an asset, then swap it — verifies ASSET_MEMBERSHIP category."""
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=DATA_PRODUCT_URN,
                aspect=DataProductPropertiesClass(
                    name=f"Timeline Test Product {UNIQUE}",
                    assets=[
                        DataProductAssociationClass(destinationUrn=ASSET_DATASET_1)
                    ],
                ),
            ),
        )
        _emit_and_wait(
            graph_client,
            MetadataChangeProposalWrapper(
                entityUrn=DATA_PRODUCT_URN,
                aspect=DataProductPropertiesClass(
                    name=f"Timeline Test Product {UNIQUE}",
                    assets=[
                        DataProductAssociationClass(destinationUrn=ASSET_DATASET_2)
                    ],
                ),
            ),
        )

        txns = _get_timeline(auth_session, DATA_PRODUCT_URN, ["ASSET_MEMBERSHIP"])
        events = _collect_change_events(txns)
        assert len(events) >= 2, f"Expected >=2 asset events, got {len(events)}"
        _assert_has_events(
            events,
            [("ASSET_MEMBERSHIP", "ADD"), ("ASSET_MEMBERSHIP", "REMOVE")],
            "dataProduct/assetMembership",
        )

    def test_data_product_all_categories(self, auth_session):
        """Verify all categories appear and actor attribution works."""
        txns = _get_timeline(auth_session, DATA_PRODUCT_URN)
        events = _collect_change_events(txns)
        categories = {e["category"] for e in events if e.get("category")}

        for expected in [
            "OWNERSHIP",
            "DOCUMENTATION",
            "TAG",
            "GLOSSARY_TERM",
            "DOMAIN",
            "STRUCTURED_PROPERTY",
            "APPLICATION",
            "ASSET_MEMBERSHIP",
        ]:
            assert expected in categories, (
                f"DataProduct timeline missing {expected}. Found: {sorted(categories)}"
            )

        _assert_actor_present(txns, "dataProduct")

    def test_data_product_timeline_structure(self, auth_session):
        """Verify the GraphQL response structure matches what the frontend expects."""
        txns = _get_timeline(auth_session, DATA_PRODUCT_URN)
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
