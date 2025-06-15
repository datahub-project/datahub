# Lineage

DataHub’s Python SDK allows you to programmatically define and retrieve lineage between metadata entities. With the DataHub Lineage SDK, you can:

- Add **table-level and column-level lineage** across datasets, data jobs, dashboards, and charts
- Automatically **infer lineage from SQL queries**
- **Read lineage** (upstream or downstream) for a given entity or column
- **Filter lineage results** using structured filters

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

## Add Lineage

The `add_lineage()` method allows you to define lineage between two entities.

### Add Entity Lineage

You can create lineage between two datasets, data jobs, dashboards, or charts. The `upstream` and `downstream` parameters should be the URNs of the entities you want to link.

#### Add Entity Lineage Between Datasets

```python
{{ inline /metadata-ingestion/examples/library/add_lineage_dataset_to_dataset.py show_path_as_comment }}
```

#### Add Entity Lineage Between Datajobs

```python
{{ inline /metadata-ingestion/examples/library/lineage_datajob_to_datajob.py show_path_as_comment }}
```

:::note Lineage Combinations
For supported lineage combinations, see [Supported Lineage Combinations](#supported-lineage-combinations).
:::

### Add Column Lineage

You can add column-level lineage by using `column_lineage` parameter when linking datasets.

#### Add Column Lineage with Fuzzy Matching

```python
{{ inline /metadata-ingestion/examples/library/lineage_dataset_column.py show_path_as_comment }}
```

When `column_lineage` is set to **True**, DataHub will automatically map columns based on their names, allowing for fuzzy matching. This is useful when upstream and downstream datasets have similar but not identical column names. (e.g. `customer_id` in upstream and `CustomerId` in downstream).

#### Add Column Lineage with Strict Matching

```python
{{ inline /metadata-ingestion/examples/library/lineage_dataset_column_auto_strict.py show_path_as_comment }}
```

This will create column-level lineage with strict matching, meaning the column names must match exactly between upstream and downstream datasets.

#### Add Column Lineage with Custom Mapping

For custom mapping, you can use a dictionary where keys are downstream column names and values represent lists of upstream column names. This allows you to specify complex relationships.

```python
{{ inline /metadata-ingestion/examples/library/lineage_dataset_column_custom_mapping.py show_path_as_comment }}
```

### Infer Lineage from SQL

You can infer lineage directly from a SQL query using `infer_lineage_from_sql()`. This will parse the query, determine upstream and downstream datasets, and automatically add lineage (including column-level lineage when possible).

```python
{{ inline /metadata-ingestion/examples/library/lineage_dataset_from_sql.py show_path_as_comment }}
```

:::note DataHub SQL Parser

Check out more information on how we handle SQL parsing below.

- [The DataHub SQL Parser Documentation](../../lineage/sql_parsing.md)
- [Blog Post : Extracting Column-Level Lineage from SQL](https://medium.com/datahub-project/extracting-column-level-lineage-from-sql-779b8ce17567)

:::

### Add Query Node with Lineage

If you provide a `transformation_text` to `add_lineage`, DataHub will create a query node that represents the transformation logic. This is useful for tracking how data is transformed between datasets.

```python
{{ inline /metadata-ingestion/examples/library/add_lineage_dataset_to_dataset_with_query_node.py show_path_as_comment }}
```

Transformation text can be any transformation logic, Python scripts, Airflow DAG code, or any other code that describes how the upstream dataset is transformed into the downstream dataset.

<p align="center">
  <img width="80%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/lineage/query-node.png"/>
</p>

:::note
Providing `transformation_text` will NOT create column lineage. You need to specify `column_lineage` parameter to enable column-level lineage.

If you have a SQL query that describes the transformation, you can use [infer_lineage_from_sql](#infer-lineage-from-sql) to automatically parse the query and add column level lineage.
:::

## Get Lineage

The `get_lineage()` method allows you to retrieve lineage for a given entity.

### Get Entity Lineage

#### Get Upstream Lineage for a Dataset

This will return the direct upstream entity that the dataset depends on. By default, it retrieves only the immediate upstream entities (1 hop).

```python
{{ inline /metadata-ingestion/examples/library/get_lineage_basic.py show_path_as_comment }}
```

#### Get Downstream Lineage for a Dataset Across Multiple Hops

To get upstream/downstream entities that are more than one hop away, you can use the `max_hops` parameter. This allows you to traverse the lineage graph up to a specified number of hops.

```python
{{ inline /metadata-ingestion/examples/library/get_lineage_with_hops.py show_path_as_comment }}

```

:::note USING MAX_HOPS
if you provide `max_hops` greater than 2, it will traverse the full lineage graph and limit the results by `count`.
:::

#### Return Type

`get_lineage()` returns a list of `LineageResult` objects.

```python
results = [
  LineageResult(
    urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_2,PROD)",
    type="DATASET",
    hops=1,
    direction="downstream",
    platform="snowflake",
    name="table_2", # name of the entity
    paths=[] # Only populated for column-level lineage
  )
]
```

### Get Column-Level Lineage

#### Get Downstream Lineage for a Dataset Column

You can retrieve column-level lineage by specifying the `source_column` parameter. This will return lineage paths that include the specified column.

```python
{{ inline /metadata-ingestion/examples/library/get_column_lineage.py show_path_as_comment }}
```

You can also pass `SchemaFieldUrn` as the `source_urn` to get column-level lineage.

```python
{{ inline /metadata-ingestion/examples/library/get_column_lineage_from_schemafield.py show_path_as_comment }}

```

#### Return type

The return type is the same as for entity lineage, but with additional `paths` field that contains column lineage paths.

```python
results = [
  LineageResult(
    urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_2,PROD)",
    type="DATASET",
    hops=1,
    direction="downstream",
    platform="snowflake",
    name="table_2", # name of the entity
    paths=[
      LineagePath(
        urn="urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,table_1,PROD),col1)",
        column_name="col1", # name of the column
        entity_name="table_1", # name of the entity that contains the column
      ),
      LineagePath(
        urn="urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,table_2,PROD),col4)",
        column_name="col4", # name of the column
        entity_name="table_2", # name of the entity that contains the column
      )
    ] # Only populated for column-level lineage
  )
]
```

For more details on how to interpret the results, see [Interpreting Lineage Results](#interpreting-lineage-results).

### Filter Lineage Results

You can filter by platform, type, domain, environment, and more.

```python
{{ inline /metadata-ingestion/examples/library/get_lineage_with_filter.py show_path_as_comment }}
```

You can check more details about the available filters in the [Search SDK documentation](./sdk/search_client.md#filter-based-search).

## Lineage SDK Reference

### Supported Lineage Combinations

The Lineage APIs support the following entity combinations:

| Upstream Entity | Downstream Entity |
| --------------- | ----------------- |
| Dataset         | Dataset           |
| Dataset         | DataJob           |
| DataJob         | DataJob           |
| Dataset         | Dashboard         |
| Chart           | Dashboard         |
| Dashboard       | Dashboard         |
| Dataset         | Chart             |

> ℹ️ Column-level lineage and creating query node with transformation text are **only supported** for `Dataset → Dataset` lineage.

### Column Lineage Options

For dataset-to-dataset lineage, you can specify `column_lineage` parameter in `add_lineage()` in several ways:

| Value           | Description                                                                       |
| --------------- | --------------------------------------------------------------------------------- |
| `False`         | Disable column-level lineage (default)                                            |
| `True`          | Enable column-level lineage with automatic mapping (same as "auto_fuzzy")         |
| `"auto_fuzzy"`  | Enable column-level lineage with fuzzy matching (useful for similar column names) |
| `"auto_strict"` | Enable column-level lineage with strict matching (exact column names required)    |
| Column Mapping  | A dictionary mapping downstream column names to lists of upstream column names    |

:::note Auto_Fuzzy vs Auto_Strict

- **Auto_Fuzzy**: Automatically matches columns based on similar names, allowing for some flexibility in naming conventions. For example, these two columns would be considered a match:
  - user_id → userId
  - customer_id → CustomerId
- **Auto_Strict**: Requires exact column name matches between upstream and downstream datasets. For example, `customer_id` in the upstream dataset must match `customer_id` in the downstream dataset exactly.

:::

### Interpreting Column Lineage Results

When retrieving column-level lineage, the results include `paths` that show how columns are related across datasets. Each path is a list of column URNs that represent the lineage from the source column to the target column.

For example, let's say we have the following lineage across three tables:

<p align="center">
  <img width="80%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/lineage/column-lineage.png"/>
</p>

#### Example with `max_hops=1`

```python
>>> client.lineage.get_lineage(
        source_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_1,PROD)",
        source_column="col1",
        direction="downstream",
        max_hops=1
    )
```

**Returns:**

```python
[
    {
        "urn": "...table_2...",
        "hops": 1,
        "paths": [
            ["...table_1.col1", "...table_2.col4"],
            ["...table_1.col1", "...table_2.col5"]
        ]
    }
]
```

#### Example with `max_hops=2`

```python
>>> client.lineage.get_lineage(
        source_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_1,PROD)",
        source_column="col1",
        direction="downstream",
        max_hops=2
    )
```

**Returns:**

```python
[
    {
        "urn": "...table_2...",
        "hops": 1,
        "paths": [
            ["...table_1.col1", "...table_2.col4"],
            ["...table_1.col1", "...table_2.col5"]
        ]
    },
    {
        "urn": "...table_3...",
        "hops": 2,
        "paths": [
            ["...table_1.col1", "...table_2.col4", "...table_3.col7"]
        ]
    }
]
```

### Lineage GraphQL Examples

You can also use the GraphQL API to add and retrieve lineage.

#### Add Lineage Between Datasets with GraphQL

```graphql
mutation updateLineage {
  updateLineage(
    input: {
      edgesToAdd: [
        {
          downstreamUrn: "urn:li:dataset:(urn:li:dataPlatform:hive,logging_events,PROD)"
          upstreamUrn: "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)"
        }
      ]
      edgesToRemove: []
    }
  )
}
```

#### Get Downstream Lineage with GraphQL

```graphql
query scrollAcrossLineage {
  scrollAcrossLineage(
    input: {
      query: "*"
      urn: "urn:li:dataset:(urn:li:dataPlatform:hive,logging_events,PROD)"
      count: 10
      direction: DOWNSTREAM
      orFilters: [
        {
          and: [
            {
              condition: EQUAL
              negated: false
              field: "degree"
              values: ["1", "2", "3+"]
            }
          ]
        }
      ]
    }
  ) {
    searchResults {
      degree
      entity {
        urn
        type
      }
    }
  }
}
```

## FAQ

**Can I get lineage at the column level?**
Yes — for dataset-to-dataset lineage, both `add_lineage()` and `get_lineage()` support column-level lineage.

**Can I pass a SQL query and get lineage automatically?**
Yes — use `infer_lineage_from_sql()` to parse a query and extract table and column lineage.

**Can I use filters when retrieving lineage?**
Yes — `get_lineage()` accepts structured filters via `FilterDsl`, just like in the Search SDK.
