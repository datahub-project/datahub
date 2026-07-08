# DataHub APIs for drift root-cause

The exact reads and writes this skill uses. All examples assume a self-hosted DataHub Core with `DATAHUB_GMS_URL` set (and a token in `DATAHUB_GMS_TOKEN` if auth is on).

## Reads

### Walk lineage (Agent Context Kit)

```python
from datahub.sdk import DataHubClient
from datahub_agent_context.langchain_tools import build_langchain_tools

client = DataHubClient(server=GMS_URL, token=GMS_TOKEN)
tools = {t.name: t for t in build_langchain_tools(client, include_mutations=False)}

# upstream lineage from the model
tools["get_lineage"].invoke({"urn": MODEL_URN, "upstream": True, "max_hops": 2})
# model metadata and ownership
tools["get_entities"].invoke({"urns": [MODEL_URN]})
```

### Deterministic aspect reads (SDK)

Immediately after ingestion the async graph index can lag, so for reliable traversal read aspects directly:

```python
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig

graph = DataHubGraph(DataHubGraphConfig(server=GMS_URL, token=GMS_TOKEN))
props = graph.get_aspect(MODEL_URN, aspect_type=MLModelPropertiesClass)  # upstreamFeatures, etc.
owners = graph.get_aspect(DATASET_URN, aspect_type=OwnershipClass)
```

## Writes

### Structured property on the model

Define the property once (entity type `mlModel`), then set it:

```python
# via MCP tool add_structured_properties, or the SDK:
from datahub.emitter.mcp import MetadataChangeProposalWrapper
# emit a StructuredProperties aspect with your propertyUrn and value(s)
```

The MCP tool `add_structured_properties` is available on self-hosted DataHub Core when `TOOLS_IS_MUTATION_ENABLED=true` on `mcp-server-datahub` >= v0.5.0.

### Document on the model

```python
# via MCP save_document, or the Document SDK:
from datahub.sdk import DataHubClient
# Document.create_document(...) attaching the RCA text and linking the model as a related asset
```

Do not use `update_description` in the open-source path; it is Cloud-only in recent MCP versions.

### Incident on the upstream dataset (GraphQL)

Incidents have no typed Python SDK yet. POST the mutation to `${GMS_URL}/api/graphql`:

```graphql
mutation raiseIncident($input: RaiseIncidentInput!) {
  raiseIncident(input: $input)
}
```

```json
{
  "input": {
    "resourceUrn": "<UPSTREAM_DATASET_URN>",
    "type": "FRESHNESS",
    "title": "Upstream change degraded downstream model",
    "description": "<one line naming the model, the feature, and the estimated impact>"
  }
}
```

## The metamodel constraint

Incidents attach only to `dataset`, `chart`, `dashboard`, `dataFlow`, `dataJob`, and `schemaField`. Not `mlModel`. This is why the model gets a structured property and a document, and the upstream dataset gets the incident.
