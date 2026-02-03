# Agent Context Kit

> **📚 Navigation**: [LangChain Integration →](./langchain.md) | [Snowflake Integration →](./snowflake.md)

## What Problem Does This Solve?

When building AI agents that answer questions about data, agents often face these challenges:

- **Hallucinate metadata**: Generate table or column names that don't exist
- **Lack context**: Can't discover related datasets, lineage, or business definitions
- **Missing ownership info**: Don't know who owns what data or how to contact them
- **No quality signals**: Can't distinguish certified datasets from deprecated ones

**Agent Context Kit solves this** by giving AI agents real-time access to your DataHub metadata catalog, enabling them to provide accurate, contextual answers about your data ecosystem.

### Example Use Cases

- **Data Discovery**: "Show me all datasets owned by the analytics team"
- **Schema Exploration**: "What tables have a customer_id column?"
- **Lineage Tracing**: "Trace lineage from raw data to this dashboard"
- **Documentation Search**: "Find the business definition of 'churn rate'"
- **Compliance Queries**: "List all PII fields and their owners"

## Overview

**DataHub Agent Context** provides a collection of tools and utilities for building AI agents that interact with DataHub metadata. This package contains MCP (Model Context Protocol) tools that enable AI agents to search, retrieve, and manipulate metadata in DataHub. These can be used directly to create an agent, or be included in an MCP server such as DataHub's open source MCP server.

### Quick Start Guide

1. **New to Agent Context?** Start here with the basic example below
2. **Using LangChain?** See the [LangChain integration guide](./langchain.md)
3. **Using Snowflake Intelligence?** See the [Snowflake integration guide](./snowflake.md)

## Installation

```bash
pip install datahub-agent-context
```

### Prerequisites

- DataHub instance (Cloud or self-hosted)
- Python 3.10 or higher
- DataHub personal access token (for authentication)

## Basic Usage

### Simple Example

These tools are designed to be used with an AI agent and have the responses passed directly to an LLM, so the return schema is a simple dict, but they can be used independently if desired.

```python
from datahub.sdk.main_client import DataHubClient
from datahub_agent_context.context import DataHubContext
from datahub_agent_context.mcp_tools.search import search
from datahub_agent_context.mcp_tools.entities import get_entities

# Initialize DataHub client from environment (or specify server/token)
client = DataHubClient.from_env()
# Or: client = DataHubClient(server="http://localhost:8080", token="YOUR_TOKEN")

# Use DataHubContext to set up the graph for tool calls
with DataHubContext(client.graph):
    # Search for datasets
    results = search(
        query="user_data",
        filters={"entity_type": ["dataset"]},
        num_results=10
    )

    print(f"Found {len(results['searchResults'])} datasets")
    for result in results["searchResults"]:
        print(f"- {result['entity']['name']} ({result['entity']['urn']})")

    # Get detailed entity information
    entity_urns = [result["entity"]["urn"] for result in results["searchResults"]]
    entities = get_entities(urns=entity_urns)

    print(f"\nDetailed info for {len(entities['entities'])} entities:")
    for entity in entities["entities"]:
        print(f"- {entity['urn']}: {entity.get('properties', {}).get('description', 'No description')}")
```

## Key Concepts (Glossary)

Before using Agent Context Kit, familiarize yourself with these DataHub concepts:

- **Entity**: A metadata object in DataHub (e.g., Dataset, Dashboard, Chart, User). Think of these as the "nouns" of your data ecosystem.
- **URN (Uniform Resource Name)**: A unique identifier for an entity. Format: `urn:li:dataset:(urn:li:dataPlatform:mysql,mydb.users,PROD)`. This is like a primary key for metadata.
- **Aspect**: A facet of metadata attached to an entity (e.g., Schema, Ownership, Documentation). Entities are composed of multiple aspects.
- **MCP (Model Context Protocol)**: A standard protocol for connecting AI agents to external data sources. These tools implement MCP for DataHub.
- **Graph**: The DataHubGraph client object used to interact with the DataHub API.

## Agent Platforms

| Platform   | Status      | Guide                             |
| ---------- | ----------- | --------------------------------- |
| Custom     | Launched    | See below                         |
| Langchain  | Launched    | [LangChain Guide](./langchain.md) |
| Snowflake  | Launched    | [Snowflake Guide](./snowflake.md) |
| Google ADK | Coming Soon | -                                 |
| Crew AI    | Coming Soon | -                                 |
| OpenAI     | Coming Soon | -                                 |

## Available Tools

#### Search Tools

- **`search(graph, query, filters, num_results)`**

  - **Use when**: Finding entities by keyword across DataHub
  - **Returns**: List of matching entities with URNs, names, and descriptions
  - **Example**: `search(graph, "customer", {"entity_type": ["dataset"]}, 10)` to find datasets about customers
  - **Filters**: Can filter by entity_type, platform, domain, tags, and more

- **`search_documents(graph, query, num_results)`**

  - **Use when**: Searching for documentation, business glossaries, or knowledge base articles
  - **Returns**: Document entities with titles and content
  - **Example**: `search_documents(graph, "data retention policy", 5)` to find policy documents

- **`grep_documents(graph, pattern, num_results)`**
  - **Use when**: Searching for specific patterns or exact phrases in documentation
  - **Returns**: Documents containing the pattern with matched excerpts
  - **Example**: `grep_documents(graph, "PII.*encrypted", 10)` to find docs mentioning PII encryption

#### Entity Tools

- **`get_entities(graph, urns)`**

  - **Use when**: Retrieving detailed metadata for specific entities you already know the URNs for
  - **Returns**: Full entity metadata including all aspects (schema, ownership, properties, etc.)
  - **Example**: After search, use this to get complete details about the found entities

- **`list_schema_fields(graph, urn, filters)`**
  - **Use when**: Exploring columns/fields in a dataset
  - **Returns**: List of fields with names, types, descriptions, and tags
  - **Example**: `list_schema_fields(graph, dataset_urn, {"field_path": "customer_"})` to find customer-related columns
  - **Filters**: Can filter by field name patterns, data types, or tags

#### Lineage Tools

- **`get_lineage(graph, urn, direction, max_depth)`**

  - **Use when**: Understanding data flow and dependencies
  - **Returns**: Upstream (sources) or downstream (consumers) entities
  - **Example**: `get_lineage(graph, dashboard_urn, "UPSTREAM", 3)` to trace data sources for a dashboard
  - **Direction**: Use "UPSTREAM" for sources, "DOWNSTREAM" for consumers

- **`get_lineage_paths_between(graph, source_urn, destination_urn)`**
  - **Use when**: Finding how data flows between two specific entities
  - **Returns**: All paths connecting the entities with intermediate steps
  - **Example**: Find how raw data flows to a specific dashboard

#### Query Tools

- **`get_dataset_queries(graph, urn, column_name)`**
  - **Use when**: Finding SQL queries that use a dataset or specific column
  - **Returns**: List of queries with SQL text and metadata
  - **Example**: `get_dataset_queries(graph, dataset_urn, "email")` to see how the email column is used
  - **Use cases**: Understanding data usage patterns, finding query examples

#### Mutation Tools

**Note**: These tools modify metadata. Use with caution in production environments.

- **`add_tags(graph, urn, tags)`** / **`remove_tags(graph, urn, tags)`**

  - **Use when**: Categorizing or labeling entities
  - **Example**: `add_tags(graph, dataset_urn, ["PII", "Finance"])` to mark sensitive data

- **`update_description(graph, urn, description)`**

  - **Use when**: Adding or updating documentation for entities
  - **Example**: Agents can auto-generate and update descriptions

- **`set_domains(graph, urn, domain_urns)`** / **`remove_domains(graph, urn, domain_urns)`**

  - **Use when**: Organizing entities into business domains
  - **Example**: Assign datasets to "Marketing" or "Finance" domains

- **`add_owners(graph, urn, owners)`** / **`remove_owners(graph, urn, owners)`**

  - **Use when**: Assigning data ownership and accountability
  - **Example**: `add_owners(graph, dataset_urn, [{"owner": user_urn, "type": "TECHNICAL_OWNER"}])`

- **`add_glossary_terms(graph, urn, term_urns)`** / **`remove_glossary_terms(graph, urn, term_urns)`**

  - **Use when**: Linking entities to business glossary definitions
  - **Example**: Link a revenue column to the "Revenue" glossary term

- **`add_structured_properties(graph, urn, properties)`** / **`remove_structured_properties(graph, urn, properties)`**
  - **Use when**: Adding custom metadata fields to entities
  - **Example**: Add "data_retention_days" or "compliance_tier" properties

#### User Tools

- **`get_me(graph)`**
  - **Use when**: Getting information about the authenticated user
  - **Returns**: User details including name, email, and roles
  - **Use cases**: Personalization, permission checks, audit logging

## MCP Server

It is also possible to connect your agent or tool directly to the **DataHub MCP Server**: [DataHub MCP Server](../../features/feature-guides/mcp.md) with your chosen framework.

## Troubleshooting

### Authentication Errors

**Problem**: `Unauthorized` or `401` errors when calling tools

**Solutions**:

- Verify your DataHub token is valid: `datahub check metadata-service`
- Ensure the token has the required permissions (read access for search tools, write access for mutation tools)
- Check that the token hasn't expired

### Connection Errors

**Problem**: `Connection refused` or timeout errors

**Solutions**:

- Verify DataHub server URL is correct and accessible
- Check network connectivity: `curl -I https://your-datahub-instance.com/api/gms/health`
- Ensure firewall rules allow outbound connections to DataHub
- For self-hosted DataHub, verify the service is running

### Empty or Unexpected Results

**Problem**: Search returns no results or missing expected entities

**Solutions**:

- Verify entities exist in DataHub UI first
- Check that your search query isn't too restrictive
- Try removing filters to broaden the search
- Ensure entity types are spelled correctly (case-sensitive): `dataset`, not `Dataset`
- For schema fields, verify the dataset URN is correct

### Import Errors

**Problem**: `ModuleNotFoundError: No module named 'datahub_agent_context'`

**Solutions**:

- Ensure package is installed: `pip install datahub-agent-context`
- If using LangChain: `pip install datahub-agent-context[langchain]`
- If using Snowflake: `pip install datahub-agent-context[snowflake]`
- Verify you're using the correct Python environment

### Rate Limiting

**Problem**: `429 Too Many Requests` errors

**Solutions**:

- Implement exponential backoff and retry logic
- Reduce the frequency of API calls
- For batch operations, use pagination instead of large single requests
- Contact DataHub admin to adjust rate limits if needed

### Debugging Tips

Enable debug logging to see detailed API calls:

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Your agent code here
```

Check the DataHub server logs for more details on server-side errors.

### Getting Help

- **Documentation**: [DataHub Docs](https://docs.datahub.com/)
- **Community Slack**: [Join DataHub Slack](https://datahub.com/slack/)
- **GitHub Issues**: [Report bugs](https://github.com/datahub-project/datahub/issues)
- **Email Support**: For DataHub Cloud customers, contact support@acryl.io
