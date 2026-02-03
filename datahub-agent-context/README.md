# DataHub Agent Context

**DataHub Agent Context** provides a collection of tools and utilities for building AI agents that interact with DataHub metadata. This package contains MCP (Model Context Protocol) tools that enable AI agents to search, retrieve, and manipulate metadata in DataHub. These can be used directly to create an agent, or be included in an MCP server such as Datahub's open source MCP server.

## Features

## Installation

### Base Installation

```shell
python3 -m pip install --upgrade pip wheel setuptools
python3 -m pip install --upgrade datahub-agent-context
```

### With LangChain Support

For building LangChain agents with pre-built tools:

```shell
python3 -m pip install --upgrade "datahub-agent-context[langchain]"
```

## Prerequisites

This package requires:

- Python 3.9 or higher
- `acryl-datahub` package

## Quick Start

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

### LangChain Integration

Build AI agents with pre-built LangChain tools:

```python
from datahub.sdk.main_client import DataHubClient
from datahub_agent_context.langchain_tools import build_langchain_tools
from langchain.agents import create_agent

# Initialize DataHub client
client = DataHubClient.from_env()

# Build all tools (read-only by default)
tools = build_langchain_tools(client, include_mutations=False)

# Or include mutation tools for tagging, descriptions, etc.
tools = build_langchain_tools(client, include_mutations=True)

# Create agent
agent = create_agent(model, tools=tools, system_prompt="...")
```

**See [examples/langchain/](examples/langchain/)** for complete LangChain agent examples including:

- [simple_search.py](examples/langchain/simple_search.py) - Minimal example with AWS Bedrock

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

## Architecture

The package is organized into the following modules:

- `mcp_tools/` - Core MCP tool implementations
  - `base.py` - Base GraphQL execution and response cleaning
  - `search.py` - Search functionality
  - `documents.py` - Document search and grep
  - `entities.py` - Entity retrieval
  - `lineage.py` - Lineage querying
  - `queries.py` - Query retrieval
  - `tags.py`, `descriptions.py`, `domains.py`, etc. - Mutation tools
  - `helpers.py` - Shared utility functions
  - `gql/` - GraphQL query definitions

## Development

### Setup

```shell
# Clone the repository
git clone https://github.com/datahub-project/datahub.git
cd datahub/datahub-agent-context

# Set up development environment
./gradlew :datahub-agent-context:installDev

# Run tests
./gradlew :datahub-agent-context:testQuick

# Run linting
./gradlew :datahub-agent-context:lintFix
```

### Testing

The package includes comprehensive unit tests for all tools:

```shell
# Run quick tests
./gradlew :datahub-agent-context:testQuick

# Run full test suite
./gradlew :datahub-agent-context:testFull
```

## Support

- [Documentation](https://datahubproject.io/docs/)
- [Slack Community](https://datahub.com/slack)
- [GitHub Issues](https://github.com/datahub-project/datahub/issues)
