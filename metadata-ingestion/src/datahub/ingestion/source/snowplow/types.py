"""
Domain types for Snowplow connector.

This module defines NewType aliases for domain concepts to improve type safety
and make the code more self-documenting. These types help catch bugs at type
check time by distinguishing between different string types.

Example usage:
    def process_organization(org_id: OrganizationId) -> DataHubURN:
        ...

    # Type checker will warn if a plain string is passed instead of OrganizationId
"""

from typing import NewType

# Organization and entity identifiers
OrganizationId = NewType("OrganizationId", str)
"""UUID identifying a Snowplow organization in BDP."""

EventSpecId = NewType("EventSpecId", str)
"""UUID identifying an event specification in Snowplow BDP."""

ScenarioId = NewType("ScenarioId", str)
"""UUID identifying a tracking scenario in Snowplow BDP."""

PipelineId = NewType("PipelineId", str)
"""UUID identifying a pipeline in Snowplow BDP."""

EnrichmentId = NewType("EnrichmentId", str)
"""UUID identifying an enrichment configuration in Snowplow BDP."""

DataProductId = NewType("DataProductId", str)
"""UUID identifying a data product in Snowplow BDP."""

# URN types
# Note: Each URN type is based on str directly (not on a parent URN type)
# because NewType inheritance doesn't work well with type checkers.
DataHubURN = NewType("DataHubURN", str)
"""DataHub URN string (e.g., 'urn:li:dataset:...')."""

DatasetURN = NewType("DatasetURN", str)
"""DataHub dataset URN string (e.g., 'urn:li:dataset:(platform,name,ENV)')."""

ContainerURN = NewType("ContainerURN", str)
"""DataHub container URN string (e.g., 'urn:li:container:...')."""

SchemaFieldURN = NewType("SchemaFieldURN", str)
"""DataHub schema field URN string (e.g., 'urn:li:schemaField:(urn,fieldPath)')."""

DataFlowURN = NewType("DataFlowURN", str)
"""DataHub data flow URN string (e.g., 'urn:li:dataFlow:(orchestrator,flowId,cluster)')."""

DataJobURN = NewType("DataJobURN", str)
"""DataHub data job URN string (e.g., 'urn:li:dataJob:(urn:li:dataFlow:...,jobId)')."""

# Schema-related types
IgluURI = NewType("IgluURI", str)
"""Iglu schema URI (e.g., 'iglu:com.vendor/name/jsonschema/1-0-0')."""

SchemaVersion = NewType("SchemaVersion", str)
"""Schema version string in format 'Major-Minor-Patch' (e.g., '1-0-2')."""

SchemaVendor = NewType("SchemaVendor", str)
"""Schema vendor name (e.g., 'com.snowplowanalytics.snowplow')."""

SchemaName = NewType("SchemaName", str)
"""Schema name (e.g., 'web_page')."""

# Field path types
FieldPath = NewType("FieldPath", str)
"""Field path in a schema (e.g., 'contexts.product.id')."""
