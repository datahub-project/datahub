# DataHub Agent Context

**MCP tools for AI agents to search and query your DataHub metadata catalog** — works with Claude, Cursor, Copilot, and any MCP-compatible AI assistant.

## What you can do

* **Search** datasets, dashboards, pipelines, and other data assets by name or description
* **Retrieve entity details** — schema, lineage, ownership, tags, glossary terms, and more
* **Trace lineage** upstream and downstream across your data assets
* **Mutate metadata** — update descriptions, tags, owners, domains, and glossary terms
* **Build LangChain or Google ADK agents** with pre-built tool bindings
* **Set up Snowflake AI agents** with one CLI command

## Installation

```bash
pip install datahub-agent-context

# With LangChain support
pip install "datahub-agent-context[langchain]"
```

## Quickstart

### LangChain agent

```python
from datahub.sdk.main_client import DataHubClient
from datahub_agent_context.langchain_tools import build_langchain_tools

client = DataHubClient.from_env()

# Read-only tools (search, lineage, entity details)
tools = build_langchain_tools(client, include_mutations=False)

# Include write tools (tags, descriptions, owners, etc.)
tools = build_langchain_tools(client, include_mutations=True)

# DataHub Cloud: add Ask DataHub AI assistant
from datahub_agent_context.langchain_tools import build_langchain_cloud_tools

tools += build_langchain_cloud_tools(client, ask_datahub=True)
```

### Snowflake AI agent setup

```bash
datahub agent create snowflake \
  --datahub-url https://your-datahub-instance \
  --datahub-token your-token
```

## Available tools

**Search** — `search()`, `search_documents()`, `grep_documents()`

**Entities** — `get_entities()`, `list_schema_fields()`

**Lineage** — `get_lineage()`, `get_lineage_paths_between()`

**Queries** — `get_dataset_queries()`

**Mutations** — `add_tags()`, `remove_tags()`, `update_description()`, `set_domains()`, `add_owners()`, `add_glossary_terms()`, `add_structured_properties()`, `save_document()`

**Cloud-only** — `ask_datahub_chat()` (DataHub Cloud AI assistant)

## Mutation tools

Mutation tools can be included when building an agent with
`include_mutations=True`.

| Tool                           | Required arguments                            | Optional arguments                        |
| ------------------------------ | --------------------------------------------- | ----------------------------------------- |
| `add_tags`                     | `tag_urns`, `entity_urns`                     | `column_paths`                            |
| `remove_tags`                  | `tag_urns`, `entity_urns`                     | `column_paths`                            |
| `update_description`           | `entity_urn`                                  | `operation`, `description`, `column_path` |
| `add_owners`                   | `owner_urns`, `entity_urns`, `ownership_type` | —                                         |
| `remove_owners`                | `owner_urns`, `entity_urns`                   | —                                         |
| `set_domains`                  | `domain_urn`, `entity_urns`                   | —                                         |
| `remove_domains`               | `entity_urns`                                 | —                                         |
| `add_structured_properties`    | `property_values`, `entity_urns`              | —                                         |
| `remove_structured_properties` | `property_urns`, `entity_urns`                | —                                         |
| `add_glossary_terms`           | `term_urns`, `entity_urns`                    | `column_paths`                            |
| `remove_glossary_terms`        | `term_urns`, `entity_urns`                    | `column_paths`                            |

### Examples

Add tags:

```python
add_tags(
    tag_urns=["urn:li:tag:PII"],
    entity_urns=["urn:li:dataset:(...)"],
)
```

Add structured properties:

```python
add_structured_properties(
    property_values={
        "urn:li:structuredProperty:example": ["HIGH"]
    },
    entity_urns=["urn:li:dataset:(...)"],
)
```

For structured properties, `property_values` maps property URNs to lists of values.

## Links

- [Documentation](https://docs.datahub.com/)
- [GitHub](https://github.com/datahub-project/datahub)
- [Slack community](https://datahub.com/slack)
