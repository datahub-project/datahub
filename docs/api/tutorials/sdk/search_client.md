# Search

DataHub's Python SDK makes it easy to search and discover metadata across your data ecosystem. Whether you're exploring unknown datasets, filtering by environment, or building advanced search tools, this guide walks you through how to do it all programmatically.

**With search SDK, you can:**

- Search for data assets by keyword or using structured filters
- Filter by environment, platform, type, custom properties or other metadata fields
- Use AND / OR / NOT logic for advanced queries

## Getting Started

To use DataHub SDK, you'll need to install [`acryl-datahub`](https://pypi.org/project/acryl-datahub/) and set up a connection to your DataHub instance. Follow the [installation guide](https://docs.datahub.com/docs/metadata-ingestion/cli-ingestion#installing-datahub-cli) to get started.

Connect to your DataHub instance:

```python
from datahub.sdk import DataHubClient

client = DataHubClient(server="<your_server>", token="<your_token>")
```

- **server**: The URL of your DataHub GMS server
  - local: `http://localhost:8080`
  - hosted: `https://<your_datahub_url>/gms`
- **token**: You'll need to [generate a Personal Access Token](https://docs.datahub.com/docs/authentication/personal-access-tokens) from your DataHub instance.

## Core Search Concepts

DataHub offers two primary search approaches:

### Query-Based Search

Query-based search allows you to search using simple keywords. This matches across common fields like name, description, and column names. This is useful for exploration when you're unsure of the exact asset you're looking for.

For example, the script below searches for any assets that have sales in their metadata.

```python
{{ inline /metadata-ingestion/examples/library/search_with_query.py show_path_as_comment }}
```

Example output:

```python
[
  DatasetUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_revenue_2023,PROD)"),
  DatasetUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_forecast,PROD)")
]
```

### Filter-Based Search

Filter-based search allows you to scope results by platform, environment, entity type, and other structured fields.
This is useful when you want to narrow down results to specific asset types or metadata fields.

For example, the script below searches for entities on the Snowflake platform.

```python
{{ inline /metadata-ingestion/examples/library/search_with_filter.py show_path_as_comment }}
```

## Common Search Patterns

### Combined Search

You can combine query and filters to refine search results further.
For example, search for anything containing "forecast" that is either a chart or a Snowflake dataset.

```python
{{ inline /metadata-ingestion/examples/library/search_with_query_and_filter.py show_path_as_comment }}
```

### Logical Operations

You can compose filters using logical operations like `AND`, `OR`, and `NOT`.

#### AND filter

Return entities matching all specified conditions.

```python
{{ inline /metadata-ingestion/examples/library/search_filter_and.py show_path_as_comment }}
```

#### OR filter

Return entities matching at least one of the conditions.

```python
{{ inline /metadata-ingestion/examples/library/search_filter_or.py show_path_as_comment }}
```

#### NOT filter

Exclude entities that match a given condition.

```python
{{ inline /metadata-ingestion/examples/library/search_filter_not.py show_path_as_comment }}
```

**Combining Multiple Operations**

You can combine multiple logical operations in a single search expression.

```python
{{ inline /metadata-ingestion/examples/library/search_filter_combined_operation.py show_path_as_comment }}
```

### Custom Field Filtering

Use field-level filters to target specific fields such as `urn`, `name`, or `description`.

```python
{{ inline /metadata-ingestion/examples/library/search_filter_custom.py show_path_as_comment }}
```

## Search Filter Examples

#### Filter by Entity Type

```python
{{ inline /metadata-ingestion/examples/library/search_filter_by_entity_type.py show_path_as_comment }}
```

#### Filter by Platform

```python
{{ inline /metadata-ingestion/examples/library/search_filter_by_platform.py show_path_as_comment }}
```

#### Filter by Environment

```python
{{ inline /metadata-ingestion/examples/library/search_filter_by_env.py show_path_as_comment }}
```

#### Filter by Domain

```python
{{ inline /metadata-ingestion/examples/library/search_filter_by_domain.py show_path_as_comment }}
```

#### Filter by Subtype

```python
{{ inline /metadata-ingestion/examples/library/search_filter_by_entity_subtype.py show_path_as_comment }}
```

#### Filter by Custom Property

```python
{{ inline /metadata-ingestion/examples/library/search_filter_by_custom_property.py show_path_as_comment }}
```

#### Custom Field Filtering with Conditions

```python
{{ inline /metadata-ingestion/examples/library/search_filter_custom.py show_path_as_comment }}
```

**Supported Conditions for Custom Fields**

- `EQUAL` – Exact match
- `CONTAIN` – Contains substring
- `START_WITH` – Begins with
- `END_WITH` – Ends with
- `GREATER_THAN` – For numeric or timestamp fields
- `LESS_THAN` – For numeric or timestamp fields

## FAQ

**How do I handle authentication?**
Generate a Personal Access Token from your DataHub instance settings and pass it into the `DataHubClient`.
[See the guide](https://docs.datahub.com/docs/authentication/personal-access-tokens)

**Can I combine query and filters?**
Yes. Use `query` along with `filter` for more precise searches.
