# Agent Context Kit

**DataHub Agent Context** provides a collection of tools and utilities for building AI agents that interact with DataHub metadata. This package contains MCP (Model Context Protocol) tools that enable AI agents to search, retrieve, and manipulate metadata in DataHub. These can be used directly to create an agent, or be included in an MCP server such as Datahub's open source MCP server.

## Installation

```
pip install datahub-agent-context
```

## Use

### Basic Example

These tools are designed to be used with an AI agent and have the responses passed directly to an LLM, so the return schema is a simple dict, but they can be used independently if desired.

```python
from datahub.ingestion.graph.client import DataHubGraph
from datahub_agent_context.mcp_tools.search import search
from datahub_agent_context.mcp_tools.entities import get_entities

# Initialize DataHub graph client
client = DataHubClient.from_env()

# Search for datasets
with client.graph as graph:
    results = search(
        query="user_data",
        filters={"entity_type": ["dataset"]},
        num_results=10
    )

# Get detailed entity information
with client.graph as graph:
    entities = get_entities(
        urns=[result["entity"]["urn"] for result in results["searchResults"]]
    )
```

## Agent Platforms

| Platform   | Status      |
| ---------- | ----------- |
| Custom     | Launched    |
| Langchain  | Launched    |
| Google ADK | Coming Soon |
| Crew AI    | Coming Soon |
| OpenAI     | Coming Soon |

### Available Tools

#### Search Tools

- `search()` - Search across all entity types with filters and sorting
- `search_documents()` - Search specifically for Document entities
- `grep_documents()` - Grep for patterns in document content

#### Entity Tools

- `get_entities()` - Get detailed information about entities by URN
- `list_schema_fields()` - List and filter schema fields for datasets

#### Lineage Tools

- `get_lineage()` - Get upstream or downstream lineage
- `get_lineage_paths_between()` - Get detailed paths between two entities

#### Query Tools

- `get_dataset_queries()` - Get SQL queries for datasets or columns

#### Mutation Tools

- `add_tags()`, `remove_tags()` - Manage tags
- `update_description()` - Update entity descriptions
- `set_domains()`, `remove_domains()` - Manage domains
- `add_owners()`, `remove_owners()` - Manage owners
- `add_glossary_terms()`, `remove_glossary_terms()` - Manage glossary terms
- `add_structured_properties()`, `remove_structured_properties()` - Manage structured properties

#### User Tools

- `get_me()` - Get information about the authenticated user

## MCP Server

It is also possible to connect your agent or tool directly to the **Datahub MCP Server**: [DataHub MCP Server](../../features/feature-guides/mcp.md) with your chosen framework.
