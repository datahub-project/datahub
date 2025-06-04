# Search

DataHub’s Python SDK makes it easy to search and discover metadata across your data ecosystem. Whether you're exploring unknown datasets, filtering by environment, or building advanced search tools, this guide walks you through how to do it all programmatically.

## What You Can Do

- Search metadata by keyword or structured filters
- Filter by environment, platform, type, or custom properties
- Use AND / OR / NOT logic for advanced queries
- Perform targeted searches using custom fields like URN

## Getting Started

Install the SDK:

```bash
pip install acryl-datahub
```

Then, connect to your DataHub instance:

```python
from datahub.sdk import DatahubClient

client = DatahubClient(gms_server="<your_server>", token="<your_token>")
```

If authentication is enabled, you’ll need to generate a Personal Access Token from your DataHub instance. [See the guide](https://docs.datahub.com/docs/authentication/personal-access-tokens).

## Search Options

DataHub supports two main search styles:

- **Query-based search**: flexible keyword search
- **Filter-based search**: structured, field-specific search

You can combine both for powerful and precise results.

## Query-Based Search

Query-based search is like using a search bar. It matches against names, descriptions, column names, and more.

```python
{{ inline /metadata-ingestion/examples/library/search_with_query.py show_path_as_comment }}
```

The result will be a list of entity URNs.

Example Output:

```python
[
  DatasetUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_revenue_2023,PROD)"),
  DatasetUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_forecast,PROD)")
]
```

You can also search everything using `query="*"`:

## Filter-Based Search

Filters help you search by platform, environment, entity type, and more.

```python
{{ inline /metadata-ingestion/examples/library/search_with_filter.py show_path_as_comment }}
```

## Combining Query and Filter

You can refine your results by combining a keyword search with structured filters.

Example: Search for anything containing "forecast" that is either a chart or a Snowflake dataset.

```python
{{ inline /metadata-ingestion/examples/library/search_with_query_and_filter.py show_path_as_comment }}
```

## Common Filtering Examples

Filters allow you to precisely scope your search results, especially when you know which kind of assets you’re looking for. Below are common use cases:

```python
{{ inline /metadata-ingestion/examples/library/search_filter_options.py show_path_as_comment }}
```

Available filters:

| Filter Type     | Method                  | Example                         |
| --------------- | ----------------------- | ------------------------------- |
| Platform        | `platform()`            | "snowflake"                     |
| Environment     | `env()`                 | "PROD", "DEV"                   |
| Entity Type     | `entity_type()`         | "dataset", "dashboard"          |
| Domain          | `domain()`              | ["urn:li:domain:xyz"]           |
| Subtype         | `entity_subtype()`      | "ML Experiment"                 |
| Deletion Status | `soft_deleted()`        | `NOT_SOFT_DELETED`              |
| Custom Property | `has_custom_property()` | key="department", value="sales" |

## Filtering Operations

You can build more complex searches using logical operations like AND, OR, and NOT.

### AND Filters

Entities must match all conditions.

```python
{{ inline /metadata-ingestion/examples/library/search_filter_and.py show_path_as_comment }}
```

### OR Filters

Entities must match at least one condition.

```python
{{ inline /metadata-ingestion/examples/library/search_filter_or.py show_path_as_comment }}
```

### NOT Filters

Entities must not match the condition.

```python
{{ inline /metadata-ingestion/examples/library/search_filter_not.py show_path_as_comment }}
```

You can also combine these operations.

```python
{{ inline /metadata-ingestion/examples/library/search_filter_combined_operation.py show_path_as_comment }}
```

### Custom Field Filtering

Use custom field filtering when you want to search specific fields like `urn`.

```python
{{ inline /metadata-ingestion/examples/library/search_filter_custom.py show_path_as_comment }}
```

**Supported Conditions:**

- `EQUAL`: Exact match
- `CONTAIN`: Partial match
- `START_WITH`: Begins with
- `END_WITH`: Ends with
- `GREATER_THAN`: Numeric/date comparison
- `LESS_THAN`: Numeric/date comparison

## FAQ

**Q: How do I handle authentication?**

Generate a Personal Access Token from your DataHub instance settings and pass it into the `DatahubClient`. [See the guide](https://docs.datahub.com/docs/authentication/personal-access-tokens).

**Q: Can I combine query and filters?**

Yes — use `query` along with `filter` for more precise search.
