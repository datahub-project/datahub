# Inlined from metadata-ingestion/examples/library/create_document.py
"""Example: Creating documents using the DataHub SDK.

This example demonstrates how to create both native documents (stored in DataHub)
and external documents (references to content in other platforms).
"""

from datahub.metadata.urns import CorpUserUrn
from datahub.sdk import DataHubClient, Document

# Initialize the client
client = DataHubClient.from_env()

# ============================================================================
# Example 1: Create a simple native document
# ============================================================================
# Native documents are stored directly in DataHub's knowledge base.
doc = Document.create_document(
    id="getting-started-tutorial",
    title="Getting Started with DataHub",
    text="# Getting Started with DataHub\n\nThis tutorial will help you get started...",
    subtype="Tutorial",
)

# Emit the document to DataHub
client.entities.upsert(doc)
print(f"Created native document: {doc.urn}")

# ============================================================================
# Example 2: Create a document with full metadata
# ============================================================================
doc = Document.create_document(
    id="faq-data-quality",
    title="Data Quality FAQ",
    text="# Data Quality FAQ\n\n## Q: How do we measure data quality?\n\nA: We use...",
    subtype="FAQ",
    related_assets=["urn:li:dataset:(urn:li:dataPlatform:snowflake,my_table,PROD)"],
    owners=[CorpUserUrn("john")],
    domain="urn:li:domain:engineering",
    tags=["urn:li:tag:important"],
    custom_properties={"team": "data-platform", "version": "1.0"},
)

client.entities.upsert(doc)
print(f"Created document with metadata: {doc.urn}")

# ============================================================================
# Example 3: Create an external document (from Notion)
# ============================================================================
# External documents reference content stored in other platforms.
# The external_url links to the original content.
doc = Document.create_external_document(
    id="notion-engineering-handbook",
    title="Engineering Handbook",
    platform="urn:li:dataPlatform:notion",
    external_url="https://notion.so/team/engineering-handbook",
    external_id="notion-page-abc123",
    text="Summary of the handbook for search indexing...",  # Optional
    owners=[CorpUserUrn("engineering-lead")],
)

client.entities.upsert(doc)
print(f"Created external document: {doc.urn}")

# ============================================================================
# Example 4: Create an external document (from Confluence)
# ============================================================================
doc = Document.create_external_document(
    id="confluence-runbook",
    title="Incident Runbook",
    platform="confluence",  # Shorthand - will become urn:li:dataPlatform:confluence
    external_url="https://company.atlassian.net/wiki/spaces/ENG/pages/123",
    subtype="Runbook",
    tags=["urn:li:tag:oncall"],
)

client.entities.upsert(doc)
print(f"Created Confluence document: {doc.urn}")

# ============================================================================
# Example 5: Create a document hidden from global context (AI-only)
# ============================================================================
# This document won't appear in global search or sidebar navigation.
# It's only accessible via the related asset - useful for AI agent context.
doc = Document.create_document(
    id="orders-dataset-context",
    title="Orders Dataset Context",
    text="# Context for AI Agents\n\nThe orders dataset contains daily summaries...",
    show_in_global_context=False,  # Hidden from global search/sidebar
    related_assets=["urn:li:dataset:(urn:li:dataPlatform:snowflake,orders,PROD)"],
)

client.entities.upsert(doc)
print(f"Created AI-only context document: {doc.urn}")

# ============================================================================
# Example 6: Create a hierarchical document (with parent)
# ============================================================================
# Documents can be organized hierarchically with parent-child relationships.
parent_doc = Document.create_document(
    id="parent-guide",
    title="User Guide",
    text="# User Guide\n\nThis is the main guide...",
)

child_doc = Document.create_document(
    id="child-section",
    title="Getting Started Section",
    text="# Getting Started\n\nThis is a subsection of the User Guide...",
    parent_document=str(parent_doc.urn),
)

client.entities.upsert(parent_doc)
client.entities.upsert(child_doc)
print(f"Created child document: {child_doc.urn}")
