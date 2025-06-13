# Search

DataHub's Python SDK makes it easy to search and discover metadata across your data ecosystem. Whether you're exploring unknown datasets, filtering by environment, or building advanced search tools, this guide walks you through how to do it all programmatically.

**With the Search SDK, you can:**

- Search for data assets by keyword or using structured filters
- Filter by environment, platform, type, custom properties, or other metadata fields
- Use `AND` / `OR` / `NOT` logic for advanced queries

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
- **token**: You'll need to [generate a Personal Access Token](../../../authentication/personal-access-tokens.md) from your DataHub instance.

## Search Types

DataHub offers two primary search approaches:

- **Query-based search** : search using simple keywords across common fields like name, description, and column names.
- **Filter-based search** : search using structured filters to scope results by platform, environment, entity type, and other metadata fields.

:::note Combining Query and Filters

Query and filters can be used together for more precise searches. Check out [this example](#find-all-snowflake-datasets-related-to-forecast) for more details.

:::

### Query-Based Search

Query-based search allows you to search using simple keywords. This matches across common fields like name, description, and column names. This is useful for exploration when you're unsure of the exact asset you're looking for.

#### Find All Entities Related to Sales

For example, the script below searches for any assets that have `sales` in their metadata.

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

#### Find All Snowflake Entities

For example, the script below searches for entities on the Snowflake platform.

```python
{{ inline /metadata-ingestion/examples/library/search_with_filter.py show_path_as_comment }}
```

#### Find All Snowflake Datasets Related to Forecast

You can combine query and filters to refine search results further.
For example, search for anything containing "forecast" that is either a chart or a Snowflake dataset.

```python
{{ inline /metadata-ingestion/examples/library/search_with_query_and_filter.py show_path_as_comment }}
```

For more details on available filters, see the [filter options](#filter-options).

## Common Search Patterns

Here are some common examples of advanced queries using filters and logical operations:

#### Find All Dashboards

```python
{{ inline /metadata-ingestion/examples/library/search_filter_by_entity_type.py show_path_as_comment }}
```

#### Find All Snowflake Entities

```python
{{ inline /metadata-ingestion/examples/library/search_filter_by_platform.py show_path_as_comment }}
```

#### Find All Entities in the Production Environment

```python
{{ inline /metadata-ingestion/examples/library/search_filter_by_env.py show_path_as_comment }}
```

#### Find All Entities in a Specific Domain

```python
{{ inline /metadata-ingestion/examples/library/search_filter_by_domain.py show_path_as_comment }}
```

#### Find All Entities With a Specific Subtype

```python
{{ inline /metadata-ingestion/examples/library/search_filter_by_entity_subtype.py show_path_as_comment }}
```

#### Find All Entities With Specific Custom Properties

```python
{{ inline /metadata-ingestion/examples/library/search_filter_by_custom_property.py show_path_as_comment }}
```

#### Find All Charts and Snowflake Datasets

You can combine filters using logical operations like `and_`, `or_`, and `not_` to build advanced queries. Check the [Logical Operator Options](#logical-operator-options) for more details.

```python
{{ inline /metadata-ingestion/examples/library/search_filter_combined_operation.py show_path_as_comment }}
```

#### Find All Charts That Are Not in the Production Environment

```python
{{ inline /metadata-ingestion/examples/library/search_filter_not.py show_path_as_comment }}
```

#### Advanced: Find entities by other searchable fields

Use `F.custom_filter()` to target specific fields such as urn, name, or description. Check the [Supported Conditions for Custom Filter](#supported-conditions-for-custom-filter) for the full list of allowed `condition` values.

```python
{{ inline /metadata-ingestion/examples/library/search_filter_custom.py show_path_as_comment }}
```

:::note Searchable Fields
With `F.custom_filter()`, the fields annotated with `@Searchable` in the PDL file can be used for filtering. For example, you can filter datajob entities by fields like `name`, `description`, or `env` since they are annotated with `@Searchable` in the [DataJobInfo.pdl](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/datajob/DataJobInfo.pdl#L21).
:::

## Search SDK Reference

For a full reference, see the [search SDK reference](../../../python-sdk/sdk-v2/search-client.mdx).

### Filter Options

The following filter options are available in the SDK:

| Filter Type     | Example Code                                   |
| --------------- | ---------------------------------------------- |
| Platform        | `F.platform("snowflake")`                      |
| Environment     | `F.env("PROD")`                                |
| Entity Type     | `F.entity_type("dataset")`                     |
| Domain          | `F.domain("urn:li:domain:xyz")`                |
| Subtype         | `F.entity_subtype("ML Experiment")`            |
| Deletion Status | `F.soft_deleted("NOT_SOFT_DELETED")`           |
| Custom Property | `F.has_custom_property("department", "sales")` |

### Logical Operator Options

The following logical operators can be used to combine filters:

| Operator | Example Code  | Description                                        |
| -------- | ------------- | -------------------------------------------------- |
| AND      | `F.and_(...)` | Return entities matching all specified conditions. |
| OR       | `F.or_(...)`  | Return entities matching at least one condition.   |
| NOT      | `F.not_(...)` | Exclude entities that match a given condition.     |

### Supported Conditions for Custom Filter

Use `F.custom_filter()` to apply conditions on specific fields such as urn, name, or description.

| Condition      | Description                                                                               |
| -------------- | ----------------------------------------------------------------------------------------- |
| `EQUAL`        | Exact match for string fields.                                                            |
| `CONTAIN`      | Contains substring in string fields.                                                      |
| `START_WITH`   | Begins with a specific substring.                                                         |
| `END_WITH`     | Ends with a specific substring.                                                           |
| `GREATER_THAN` | For numeric or timestamp fields, checks if the value is greater than the specified value. |
| `LESS_THAN`    | For numeric or timestamp fields, checks if the value is less than the specified value.    |

## FAQ

**How do I handle authentication?**
Generate a Personal Access Token from your DataHub instance settings and pass it into the `DataHubClient`. Check out the [Personal Access Token Guide](../../../authentication/personal-access-tokens.md).

**Can I combine query and filters?**
Yes. Use `query` along with `filter` for more precise searches.
