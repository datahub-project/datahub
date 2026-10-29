### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### dbt meta automated mappings

dbt allows authors to define meta properties for datasets. Checkout this link to know more - [dbt meta](https://docs.getdbt.com/reference/resource-configs/meta). Our dbt source allows users to define
actions such as add a tag, term or owner. For example if a dbt model has a meta config `"has_pii": True`, we can define an action
that evaluates if the property is set to true and add, lets say, a `pii` tag.
To leverage this feature we require users to define mappings as part of the recipe. The following section describes how you can build these mappings. Listed below is a `meta_mapping` and `column_meta_mapping` section that among other things, looks for keys like `business_owner` and adds owners that are listed there.

```yaml
meta_mapping:
  business_owner:
    match: ".*"
    operation: "add_owner"
    config:
      owner_type: user
      owner_category: BUSINESS_OWNER
  has_pii:
    match: True
    operation: "add_tag"
    config:
      tag: "has_pii_test"
  int_property:
    match: 1
    operation: "add_tag"
    config:
      tag: "int_meta_property"
  double_property:
    match: 2.5
    operation: "add_term"
    config:
      term: "double_meta_property"
  data_governance.team_owner:
    match: "Finance"
    operation: "add_term"
    config:
      term: "Finance_test"
  terms_list:
    match: ".*"
    operation: "add_terms"
    config:
      separator: ","
  documentation_link:
    match: "(?:https?)?\:\/\/\w*[^#]*"
    operation: "add_doc_link"
    config:
      link: {{ $match }}
      description: "Documentation Link"
  business_domain:
    match: ".*"
    operation: "add_domain"
    config:
      domain: "{{ $match }}"
  data_load_frequency:
    match: ".*"
    operation: "add_structured_property"
    config:
      structured_property_urn: "urn:li:structuredProperty:io.acme.data_load_frequency"
column_meta_mapping:
  terms_list:
    match: ".*"
    operation: "add_terms"
    config:
      separator: ","
  is_sensitive:
    match: True
    operation: "add_tag"
    config:
      tag: "sensitive"
  gdpr.pii:
    match: true
    operation: "add_tag"
    config:
      tag: "pii"
  classification:
    match: ".*"
    operation: "add_structured_property"
    config:
      structured_property_urn: "urn:li:structuredProperty:io.acme.classification"
```

We support the following operations:

1. add_tag - Requires `tag` property in config.
2. add_term - Requires `term` property in config.
3. add_terms - Accepts an optional `separator` property in config.
4. add_owner - Requires `owner_type` property in config which can be either `user` or `group`. Optionally accepts the `owner_category` config property which can be set to either a [custom ownership type](../../../../docs/ownership/ownership-types.md) urn like `urn:li:ownershipType:architect` or one of `['TECHNICAL_OWNER', 'BUSINESS_OWNER', 'DATA_STEWARD', 'DATAOWNER'` (defaults to `DATAOWNER`).

   - The `owner_type` property will be ignored if the owner is a fully qualified urn.
   - You can use commas to specify multiple owners - e.g. `business_owner: "jane,john,urn:li:corpGroup:data-team"`.

5. add_doc_link - Requires `link` and `description` properties in config. Upon ingestion run, this will overwrite current links in the institutional knowledge section with this new link. The anchor text is defined here in the meta_mappings as `description`.
6. add_domain - Adds the dataset to a DataHub Domain. The `domain` config value can be a short ID (e.g. `Marketing`) or a fully-qualified URN (`urn:li:domain:Marketing`). Supports `{{ $match }}` substitution.
7. add_structured_property - Assigns a value to a [DataHub Structured Property](../../../../docs/features/feature-guides/properties/overview.md). Required config: `structured_property_urn` (full URN or qualified name). Optional config: `value` (literal or `{{ $match }}` template; defaults to the raw meta value, preserving numeric types) and `value_type` (`string` or `number`). Multiple rules targeting the same property URN have their values aggregated into a single assignment. The structured property itself must be defined in DataHub before ingestion runs — this operation only assigns values, it does not create the property definition.

Note:

1. The dbt `meta_mapping` config works at the model level, while the `column_meta_mapping` config works at the column level. The `add_owner` operation is not supported at the column level. The `add_structured_property` operation is supported at both levels — at the column level it produces a `structuredProperties` aspect attached to each matching `schemaField` URN.
2. For string meta properties we support regex matching.
3. **List support**: YAML lists are now supported in meta properties. Each item in the list that matches the regex pattern will be processed.

With regex matching, you can also use the matched value to customize how you populate the tag, term or owner fields. Here are a few advanced examples:

##### Data Tier - Bronze, Silver, Gold

If your meta section looks like this:

```yaml
meta:
  data_tier: Bronze # chosen from [Bronze,Gold,Silver]
```

and you wanted to attach a glossary term like `urn:li:glossaryTerm:Bronze` for all the models that have this value in the meta section attached to them, the following meta_mapping section would achieve that outcome:

```yaml
meta_mapping:
  data_tier:
    match: "Bronze|Silver|Gold"
    operation: "add_term"
    config:
      term: "{{ $match }}"
```

to match any data_tier of Bronze, Silver or Gold and maps it to a glossary term with the same name.

##### Case Numbers - create tags

If your meta section looks like this:

```yaml
meta:
  case: PLT-4678 # internal Case Number
```

and you want to generate tags that look like `case_4678` from this, you can use the following meta_mapping section:

```yaml
meta_mapping:
  case:
    match: "PLT-(.*)"
    operation: "add_tag"
    config:
      tag: "case_{{ $match }}"
```

##### Nested meta properties

If your meta section has nested properties and looks like this:

```yaml
meta:
  data_governance:
    team_owner: "Finance"
```

and you want attach term Finance_test in case of data_governance.team_owner is set to Finance, you can use the following meta_mapping section:

```yaml
meta_mapping:
  data_governance.team_owner:
    match: "Finance"
    operation: "add_term"
    config:
      term: "Finance_test"
```

Note: nested meta properties mapping is supported also for column_meta_mapping

##### Stripping out leading @ sign

You can also match specific groups within the value to extract subsets of the matched value. e.g. if you have a meta section that looks like this:

```yaml
meta:
  owner: "@finance-team"
  business_owner: "@janet"
```

and you want to mark the finance-team as a group that owns the dataset (skipping the leading @ sign), while marking janet as an individual user (again, skipping the leading @ sign) that owns the dataset, you can use the following meta-mapping section.

```yaml
meta_mapping:
  owner:
    match: "^@(.*)"
    operation: "add_owner"
    config:
      owner_type: group
  business_owner:
    match: "^@(?P<owner>(.*))"
    operation: "add_owner"
    config:
      owner_type: user
      owner_category: BUSINESS_OWNER
```

In the examples above, we show two ways of writing the matching regexes. In the first one, `^@(.*)` the first matching group (a.k.a. match.group(1)) is automatically inferred. In the second example, `^@(?P<owner>(.*))`, we use a named matching group (called owner, since we are matching an owner) to capture the string we want to provide to the ownership urn.

##### Working with Lists

YAML lists are fully supported in dbt meta properties. Each item in the list is evaluated against the match pattern, and only matching items are processed.

```yaml
meta:
  owners:
    - alice@company.com
    - bob@company.com
    - contractor@external.com
```

```yaml
meta_mapping:
  owners:
    match: ".*@company.com"
    operation: "add_owner"
    config:
      owner_type: user
```

This will add `alice@company.com` and `bob@company.com` as owners (matching `.*@company.com`) but skip `contractor@external.com` (doesn't match the pattern).

#### dbt query_tag automated mappings

This works similarly as the dbt meta mapping but for the query tags

We support the below actions -

1. add_tag - Requires `tag` property in config.

The below example set as global tag the query tag `tag` key's value.

```json
"query_tag_mapping":
{
   "tag":
      "match": ".*"
      "operation": "add_tag"
      "config":
        "tag": "{{ $match }}"
}
```

#### Integrating with dbt test

To integrate with dbt tests, the `dbt` source needs access to the `run_results.json` file generated after a `dbt test` or `dbt build` execution. Typically, this is written to the `target` directory. A common pattern you can follow is:

1. Run `dbt build`
2. Copy the `target/run_results.json` file to a separate location. This is important, because otherwise subsequent `dbt` commands will overwrite the run results.
3. Run `dbt docs generate` to generate the `manifest.json` and `catalog.json` files
4. The dbt source makes use of the manifest, catalog, and run results file, and hence will need to be moved to a location accessible to the `dbt` source (e.g. s3 or local file system). In the ingestion recipe, the `run_results_paths` config must be set to the location of the `run_results.json` file from the `dbt build` or `dbt test` run.

The connector will produce the following things:

- Assertion definitions that are attached to the dataset (or datasets)
- Results from running the tests attached to the timeline of the dataset

A test's custom `name:` is used as the assertion's title; tests with dbt's generated names keep the default title. A test's `description:` (dbt 1.9+) is written as the assertion note, which is shown in DataHub Cloud. Notes need a connection to a DataHub server that supports them (DataHub 1.7.0.1+, or DataHub Cloud 1.0.0+); otherwise they are skipped. The source reads existing notes first and never overwrites a note edited in DataHub. It only updates a note it wrote earlier, or clears it once the description is removed, when `pipeline_name` is set (with the default `set_system_metadata_pipeline_name: true`) and unchanged between runs.

##### How dbt test statuses map to DataHub assertion results

Each dbt test result is translated into a DataHub `AssertionResult` with a `type` (SUCCESS, FAILURE, or ERROR) and, on failures, an optional `severity` (LOW, MEDIUM, HIGH).

DataHub distinguishes **the test produced a verdict** (SUCCESS / FAILURE) from **the test could not run** (ERROR). This lets you separate data-quality regressions from infrastructure or SQL-compile problems downstream.

The mapping for regular dbt tests:

| dbt status      | Meaning                                                                             | `AssertionResult.type` | `AssertionResult.severity` |
| --------------- | ----------------------------------------------------------------------------------- | ---------------------- | -------------------------- |
| `pass`          | Test ran, no failing rows                                                           | `SUCCESS`              | —                          |
| `warn`          | Test ran, found failing rows, configured with `severity: warn`                      | see below              | `LOW` (if emitted)         |
| `fail`          | Test ran, found failing rows, configured with `severity: error` (the default)       | `FAILURE`              | `HIGH`                     |
| `error`         | Test invocation failed to compile/start (SQL error, missing ref, permissions, etc.) | `ERROR`                | —                          |
| `runtime error` | Test started running and then the warehouse raised an exception                     | `ERROR`                | —                          |

The `warn` row depends on the `test_warnings_are_errors` config:

- `test_warnings_are_errors: false` (default): `warn` → `SUCCESS`. Honors dbt's author-declared "soft concern" semantics.
- `test_warnings_are_errors: true`: `warn` → `FAILURE` with `severity: LOW`. Stricter — every failing test row counts as a failure, but soft ones are tagged for downstream filtering.

:::note Default will change in a future release

`test_warnings_are_errors` is planned to default to `true` once assertion result consumers (UI, saved searches, alerting) can filter by severity. At that point, `warn` will always emit as `FAILURE` with `severity: LOW`, and the flag itself will be deprecated. Set `test_warnings_are_errors: true` today to adopt the forthcoming behavior.

:::

For dbt source freshness checks, the semantics of `error` differ: `error` means the `error_after` threshold was exceeded, i.e. the check ran and the source is considered stale. The freshness mapping:

| dbt freshness status | Meaning                              | `AssertionResult.type`               | `AssertionResult.severity` |
| -------------------- | ------------------------------------ | ------------------------------------ | -------------------------- |
| `pass`               | Source is fresh                      | `SUCCESS`                            | —                          |
| `warn`               | `warn_after` threshold exceeded      | see `test_warnings_are_errors` above | `LOW` (if emitted)         |
| `error`              | `error_after` threshold exceeded     | `FAILURE`                            | `HIGH`                     |
| `runtime error`      | Freshness check itself failed to run | `ERROR`                              | —                          |

:::note Missing test results?

The most common reason for missing test results is that the `run_results.json` with the test result information is getting overwritten by a subsequent `dbt` command. We recommend copying the `run_results.json` file before running other `dbt` commands.

```sh
dbt source snapshot-freshness
dbt build
cp target/run_results.json target/run_results_backup.json
dbt docs generate
cp target/run_results_backup.json target/run_results.json
```

:::

#### Glob patterns for run_results_paths

If your dbt setup produces many `run_results.json` files (e.g. one per Airflow DAG task or retry), you can use glob patterns instead of listing every file explicitly. This works for both S3 URIs and local paths.

```yaml
source:
  type: dbt
  config:
    manifest_path: "s3://my-bucket/dbt/target/manifest.json"
    catalog_path: "s3://my-bucket/dbt/target/catalog.json"
    target_platform: postgres
    run_results_paths:
      - "s3://my-bucket/dbt/run_results/*/*/*.json"
    aws_connection: {}
```

```yaml
# Local paths also support glob patterns
source:
  type: dbt
  config:
    manifest_path: /dbt/target/manifest.json
    catalog_path: /dbt/target/catalog.json
    target_platform: postgres
    run_results_paths:
      - "/dbt/run_results/*/run_results.json"
```

Supported wildcard characters: `*` (any characters within a path segment), `?` (single character), `[...]` (character set). If a glob pattern matches zero files, a warning is emitted in the ingestion report. The expanded file list is also recorded in the report for debugging.

:::warning Avoid globbing over historical run result files
Glob patterns that match timestamped or versioned files (e.g. `run_results/2024-01-01/run_results.json`) will cause DataHub to re-process old test failures on every ingestion cycle, triggering repeated alerts. Instead, write results to a **stable path that is overwritten on each run** and glob across jobs, not history:

```yaml
# ✅ Glob across tasks, stable path overwritten each run
- "s3://my-bucket/run_results_latest/my_dag/*/run_results.json"

# ❌ Glob across timestamps — re-processes historical failures every cycle
- "s3://my-bucket/run_results/*/2024-*/run_results.json"
```

:::

:::note S3 IAM permissions for glob patterns
When using glob patterns with S3 paths, the IAM role or user must have **`s3:ListBucket`** permission on the bucket in addition to `s3:GetObject`. Without `s3:ListBucket`, the glob expansion will fail with an `AccessDenied` error. Explicit (non-glob) S3 paths only require `s3:GetObject`.
:::

##### View of dbt tests for a dataset

![test view](https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/dbt-tests-view.png)

##### Viewing the SQL for a dbt test

![test logic view](https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/dbt-test-logic-view.png)

##### Viewing timeline for a failed dbt test

![test view](https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/dbt-tests-failure-view.png)

##### Separating test result emission from other metadata emission

You can segregate emission of test results from the emission of other dbt metadata using the `entities_enabled` config flag.
The following recipe shows you how to emit only test results.

```yaml
source:
  type: dbt
  config:
    manifest_path: _path_to_manifest_json
    catalog_path: _path_to_catalog_json
    run_results_paths:
      - _path_to_run_results_json
    target_platform: postgres
    entities_enabled:
      test_results: Only
```

Similarly, the following recipe shows you how to emit everything (i.e. models, sources, seeds, test definitions) but not test results:

```yaml
source:
  type: dbt
  config:
    manifest_path: _path_to_manifest_json
    catalog_path: _path_to_catalog_json
    run_results_paths:
      - _path_to_run_results_json
    target_platform: postgres
    entities_enabled:
      test_results: No
```

:::note Tests not showing up in the Assertion UI?

Double check you are using the same `job_id` for your `test_results: Only` and `test_results: No` recipes. Otherwise you might end up reporting `tests_results` for tests that haven't had their `test_definitions` ingested yet. This can lead to orphaned assertions that do not show up under your dataset. With `ingest_contracts`, the `test_results: No` recipe writes the contract and its assertions, and the `test_results: Only` recipe writes their run events.

If you choose not to share a `job_id`, you should adjust your recipe to also ingest the `test_definitions`

```yaml
entities_enabled:
  models: No
  sources: No
  seeds: No
  snapshots: No
  test_definitions: Yes
  test_results: Yes
```

:::

#### Multiple dbt projects

In more complex dbt setups, you may have multiple dbt projects, where models from one project are used as sources in another project.
DataHub supports this setup natively, and a `source()` in one project that points at another project's model is connected through the shared warehouse dataset.

There are two ways to ingest several dbt projects:

- **One recipe per project.** Choose this when a project's identity in DataHub must not change if the project is renamed, when projects need different settings (`target_platform`, `git_info`, filters, schedules), or when the artifacts come from a dbt version older than 1.6.
- **One recipe with a globbed `manifest_path`.** Choose this when there are many projects, new projects should be picked up without writing a new recipe, the artifacts share a common layout, and the projects can share one configuration. Each project's `platform_instance` is then its dbt project name. See [Multi-Project Ingestion (glob patterns)](#multi-project-ingestion-glob-patterns) for how it works and what a project rename or a duplicate project name means.

With one recipe per project, set `platform_instance` in each recipe to the dbt project name. That is the value a glob recipe would derive, so you can switch between the two approaches without changing dataset, assertion or semantic model URNs. Query entities from `meta.queries` are the exception: under a glob their URNs also include the project name, so they are re-created on the switch.

For example, if you have two dbt projects `analytics` and `data_mart`, you would have two ingestion recipes.
If you have models in the `data_mart` project that are used as sources in the `analytics` project, the lineage will be automatically captured.

```yaml
# Analytics dbt project
source:
  type: dbt
  config:
    platform_instance: analytics
    target_platform: postgres
    manifest_path: analytics/target/manifest.json
    catalog_path: analytics/target/catalog.json
    # ... other configs
```

```yaml
# Data Mart dbt project
source:
  type: dbt
  config:
    platform_instance: data_mart
    target_platform: postgres
    manifest_path: data_mart/target/manifest.json
    catalog_path: data_mart/target/catalog.json
    # ... other configs
```

The same two projects with one recipe, assuming both write their artifacts under `dbt-artifacts/<project>/`:

```yaml
source:
  type: dbt
  config:
    target_platform: postgres
    manifest_path: dbt-artifacts/*/manifest.json
    # ... other configs
```

If you have models that have tons of sources from other projects listed in the "Composed Of" section, it may also make sense to hide sources.

#### Reducing "composed of" sprawl by hiding sources

When many dbt projects use a single table as a source, the "Composed Of" relationships can become very large and difficult to navigate
and extra source nodes can clutter the lineage graph.

This is particularly useful for multi-project setups, but can be useful in single-project setups as well.

The benefit is that your entire dbt estate becomes much easier to navigate, and the borders between projects less noticeable.
The downside is that we will not pick up any documentation or meta mappings applied to dbt sources.

To enable this, set `entities_enabled.sources: No` and `skip_sources_in_lineage: true` in your dbt source config:

```yaml
source:
  type: dbt
  config:
    platform_instance: analytics
    target_platform: postgres
    manifest_path: analytics/target/manifest.json
    catalog_path: analytics/target/catalog.json
    # ... other configs
    entities_enabled:
      sources: No
    skip_sources_in_lineage: true
```

[Experimental] It's also possible to use `skip_sources_in_lineage: true` without disabling sources entirely. If you do this, sources will not participate in the lineage graph - they'll have upstreams but no downstreams. However, they will still contribute to docs, tags, etc to the warehouse entity.

#### Semantic Views

DataHub can ingest dbt models that have been materialized as `semantic_view` objects, a pattern used to define a semantic layer directly in warehouses like Snowflake.

##### What are Materialized Semantic Views?

A materialized [semantic view](https://docs.snowflake.com/en/user-guide/views-semantic/overview) is a dbt model (a `.sql` file) that uses the `materialized='semantic_view'` configuration via the [dbt_semantic_view package](https://github.com/Snowflake-Labs/dbt_semantic_view). This creates a `SEMANTIC VIEW` object in Snowflake, containing a rich set of metadata including dimensions and metrics.

When you define a dbt model as a semantic view:

```sql
-- models/sales_analytics.sql
{{ config(
    materialized='semantic_view'
) }}

TABLES (
    OrdersTable AS {{ source('coffee_shop_source', 'ORDERS') }}
)
DIMENSIONS (
    OrdersTable.CUSTOMER_ID AS CUSTOMER_ID
)
METRICS (
    OrdersTable.GROSS_REVENUE AS SUM(ORDER_TOTAL)
)
```

DataHub will:

1. Create a dataset with the subtype `Semantic View`.
2. Create sibling relationships to the underlying Snowflake `SEMANTIC VIEW` object.
3. Extract column-level lineage from the semantic view's DDL.

##### Configuration

Semantic views are dbt models with `materialized='semantic_view'`. They are emitted by default along with other models when `entities_enabled.models: Yes` (the default).

##### How Semantic Views Appear in DataHub

- **Subtype**: Datasets are tagged with the subtype `Semantic View`.
- **Lineage**: Upstream lineage to the source dbt model is created, with column-level lineage where available.

##### Column-Level Lineage for Snowflake Semantic Views

For dbt models materialized as semantic views in Snowflake, DataHub can extract column-level lineage from the compiled DDL. This requires:

1. Using Snowflake as the `target_platform`.
2. Having the `compiled_code` for the model available in the dbt manifest or dbt Cloud API.

If these conditions are not met, warnings will appear in the ingestion report:

| Condition               | Warning                                 |
| ----------------------- | --------------------------------------- |
| Non-Snowflake adapter   | `Semantic View CLL Unsupported Adapter` |
| Missing `compiled_code` | `Semantic View Missing compiled_code`   |
| Empty CLL results       | `Semantic View CLL Empty`               |
| Parsing failure         | `Semantic View CLL Parsing Failed`      |

> **Note: Limitations**
>
> Column-level lineage is currently only supported for Snowflake semantic views, as it relies on parsing the Snowflake-specific DDL.

#### Semantic Models

DataHub ingests [dbt semantic models](https://docs.getdbt.com/docs/build/semantic-models) (dbt 1.6+) - the `semantic_models:` entries in your YAML that define entities, dimensions, and measures on top of a dbt model.

A semantic model is a **logical** structure: it lives in your dbt project and is never materialized in the warehouse. DataHub models it accordingly.

##### How Semantic Models Appear in DataHub

- **Subtype**: Datasets are tagged with the subtype `Semantic Model`.
- **Name**: The dataset is named from dbt's own unique id, `semantic_model.<project>.<semantic_model_name>`, giving a urn like:

  ```
  urn:li:dataset:(urn:li:dataPlatform:dbt,semantic_model.jaffle_shop.orders,PROD)
  ```

  This is the same rule already used for [exposures](#exposures), which are named `exposure.<project>.<name>`. A semantic model has no warehouse address of its own, so it is not named `<database>.<schema>.<name>` like a model, seed, or snapshot. Naming it that way would give it the address of the model it sits on - colliding with that model whenever the two share a name, which is dbt's own documented convention:

  ```yaml
  semantic_models:
    - name: orders
      model: ref("orders")
  ```

- **No warehouse entity**: Unlike models, seeds, and snapshots, a semantic model produces no dataset on the target platform, no sibling relationship, and no lineage into one - there is no warehouse table to describe.
- **Schema**: Entities, dimensions, and measures are surfaced as columns, with native types of the form `entity:primary`, `dimension:time`, and `measure:sum`.
- **Lineage**: Upstream lineage points at the dbt model the semantic model is built on.

##### Configuration

Semantic models are emitted by default. Control them with `entities_enabled.semantic_models`:

```yaml
source:
  type: dbt
  config:
    entities_enabled:
      semantic_models: "NO"
```

Two connector settings do not apply to semantic models, because both concern physical assets: `include_database_name` (the urn carries no database) and `materialized_node_pattern` (there is no materialized location to match against). To filter semantic models by name, use `node_name_pattern`, which matches the dbt unique id.

#### Semantic Model and Metric Entities

On top of the datasets described above, DataHub can also describe your dbt semantic layer with first-class [Semantic Model](../../metamodel/entities/semanticModel.md) and [Metric](../../metamodel/entities/metric.md) entities. This is **purely additive**: it does not change the dataset urns, and it does not change any aspect the connector already writes for them.

##### What Gets Emitted

- **One `semanticModel` per dbt project**, holding the join relationships between your semantic models. Its urn is `urn:li:semanticModel:(urn:li:dataPlatform:dbt,<project>,semantic_layer)`.
- **Semantic annotations on each semantic model's dataset.** The dataset keeps the urn, name, description, schema, subtype, tags, owners and lineage described above, and gains a link to its parent `semanticModel` plus a per-column annotation recording whether the column is a dimension or a measure, its aggregation function, and whether it is a time dimension.
- **One `metric` per measure with `create_metric: true`**, and one per top-level `metrics:` entry. Its urn is `urn:li:metric:(urn:li:dataPlatform:dbt,<project>,<metric_name>)`. Metrics carry an expression, a subtype naming dbt's metric type (`Simple`, `Ratio`, `Derived`, `Cumulative`, `Conversion`), upstream lineage to the datasets of the semantic models they read, and `derivedFrom` edges to the metrics they are built on.

Relationships follow MetricFlow's own join semantics: a semantic model that declares an entity as `foreign`, `unique` or `natural` joins to every semantic model that declares an entity of the **same name** as `primary`, `unique` or `natural`. When a key is declared in more than one semantic model, an edge is emitted to each - MetricFlow joins to each of them too. A reference to a key that is not in the ingested project is counted in the ingestion report rather than warned about, since that is normal when you filter part of a project.

##### Enabling It

```yaml
source:
  type: dbt
  config:
    emit_semantic_model_entities: true
```

`emit_semantic_model_entities` is three-valued:

| Value           | Behavior                                                                                                                                                                                                                        |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| unset (default) | Follow the server. Enabled on DataHub Cloud new enough to register these entity types, unless the Metrics feature is disabled. Off on OSS, older DataHub Cloud, and runs with no server connection (for example a `file` sink). |
| `true`          | Request emission. Refused, with the reason in the ingestion report, where the server cannot accept these entities.                                                                                                              |
| `false`         | Off, whatever the server says.                                                                                                                                                                                                  |

Because nothing about the datasets changes, you can turn this on and off freely: there is no urn change and no migration.

##### Project Name

The `semanticModel` and `metric` urns are keyed by your dbt project name, read from `manifest.metadata.project_name` (dbt Core) or from the semantic models' package name (dbt Cloud). Since it is part of the entities' identity, pin it if it might change:

```yaml
source:
  type: dbt
  config:
    emit_semantic_model_entities: true
    semantic_model_project_name: jaffle_shop
```

If the project name cannot be determined, the run reports a failure and emits no semantic model or metric entities rather than inventing a placeholder name - the datasets are unaffected.

When `platform_instance` is set it is folded into the path segment of these urns (`urn:li:semanticModel:(urn:li:dataPlatform:dbt,<instance>.<project>,semantic_layer)`), because `semanticModelKey` and `metricKey` have no platform-instance field of their own. It is not folded twice when `platform_instance` and the project name are the same, which is the documented setup for multi-project dbt.

##### Limitations

- **dbt Cloud has no `metrics:` block.** The Discovery API does not expose it, so only metrics from `create_metric: true` measures are ingested there. The ingestion report says so on every dbt Cloud run that emits these entities. Use the dbt Core source if you need the `metrics:` block.
- **Metric type, filters and cumulative windows have no field of their own on `metricInfo`.** The type becomes a subtype; a filter is folded into the metric's expression as a `FILTER (WHERE ...)` clause, or stated as a trailing SQL comment where the expression cannot carry one (a ratio, or an author-written `expr`); a cumulative window becomes a trailing comment such as `/* cumulative over 7 day */`. dbt keeps filters as Jinja templates, so a folded filter is not always parseable SQL.
- **Time-dimension granularity** is recorded only as "this is a time dimension" - the annotation has no field for the grain itself.

#### Exposures

DataHub supports ingesting [dbt exposures](https://docs.getdbt.com/docs/build/exposures) - downstream consumers of your dbt models such as dashboards, notebooks, ML models, and applications.

##### What are dbt Exposures?

Exposures define how your dbt models are used downstream. They help you understand the full picture of your data ecosystem by documenting:

- **Dashboards** - BI tools like Looker, Tableau, or Metabase
- **Notebooks** - Jupyter notebooks or other analysis tools
- **ML Models** - Machine learning pipelines consuming your data
- **Applications** - Apps or services that use your data
- **Analysis** - Ad-hoc analysis or reports

##### How Exposures Map to DataHub

dbt exposures are ingested as **Dashboard** entities in DataHub, with the exposure type preserved as a subtype:

| dbt Exposure Type | DataHub SubType |
| ----------------- | --------------- |
| `dashboard`       | Dashboard       |
| `notebook`        | Notebook        |
| `ml`              | ML Model        |
| `application`     | Application     |
| `analysis`        | Analysis        |

##### Configuration

Exposures are enabled by default. You can control their emission using the `entities_enabled.exposures` config:

```yaml
source:
  type: dbt
  config:
    manifest_path: _path_to_manifest_json
    catalog_path: _path_to_catalog_json
    target_platform: postgres
    entities_enabled:
      exposures: Yes # Default - emit exposures
      # exposures: No  # Disable exposure ingestion
      # exposures: Only  # Only emit exposures, skip other entities
```

##### Lineage

Exposures automatically create lineage relationships to their upstream dbt models. The `depends_on` field in your exposure definition determines which models appear as upstreams:

```yaml
# models/exposures.yml
exposures:
  - name: weekly_metrics_dashboard
    type: dashboard
    owner:
      email: analytics@company.com
    depends_on:
      - ref('orders')
      - ref('customers')
    url: https://bi.company.com/dashboards/weekly-metrics
```

This creates lineage: `orders` → `weekly_metrics_dashboard` and `customers` → `weekly_metrics_dashboard`.

##### Owner Resolution

Owner resolution for exposures is **the same** as for other dbt assets (models, sources, etc.). The `enable_owner_extraction` config applies to all of them: when `true` (default), ownership is extracted; when `false`, no ownership aspect is emitted for any dbt asset, including exposures.

For exposures, the owner is read from the exposure definition:

- **`owner.email`** (recommended): used as the user URN (e.g., `analytics@company.com` → `urn:li:corpuser:analytics@company.com`)
- **`owner.name`** (fallback): converted to lowercase with underscores (e.g., `"John Doe"` → `urn:li:corpuser:john_doe`)

The same configs apply as elsewhere: `strip_user_ids_from_email` strips the domain from email-based owners when set.

:::note
When only `owner.name` is provided (without email), the generated URN may not match existing users in DataHub. We recommend providing `owner.email` for accurate user matching.
:::

##### Example Output

An exposure like:

```yaml
exposures:
  - name: weekly_metrics_dashboard
    type: dashboard
    description: Weekly business metrics for leadership
    owner:
      email: analytics@company.com
    maturity: high
    url: https://bi.company.com/dashboards/123
    depends_on:
      - ref('orders')
```

Will create a Dashboard entity in DataHub with:

- **Title**: `weekly_metrics_dashboard`
- **Description**: `Weekly business metrics for leadership`
- **SubType**: `Dashboard`, `Notebook`, `ML Model`, `Application`, `Analysis`
- **Owner**: `urn:li:corpuser:analytics@company.com`
- **External URL**: `https://bi.company.com/dashboards/123`
- **Upstream Lineage**: Link to the `orders` dbt model
- **Custom Properties**: `exposure_type`, `maturity`, `dbt_unique_id`

#### Ingesting dbt Contracts as DataHub Data Contracts

[dbt model contracts](https://docs.getdbt.com/docs/collaborate/govern/model-contracts) (introduced in dbt 1.5) let you declare schema guarantees on your models — column names, data types, and constraints like `not_null`, `unique`, `primary_key`, `foreign_key`, and `check`. DataHub can ingest these contracts and represent them as [Data Contracts](docs/generated/metamodel/entities/dataContract.md), so the same guarantees are visible in your catalog alongside your datasets.

**Where the contract appears.** The Data Contract is attached to the dbt dataset, never to its warehouse sibling. On a page that combines a dbt model with its warehouse table, the Contract tab is disabled. To see the contract, open the dbt asset on its own, for example from the **Composed Of** section in the page's sidebar.

##### Enabling Contract Ingestion

For **dbt Core**, set `ingest_contracts: true` in your recipe:

```yaml
source:
  type: dbt
  config:
    manifest_path: _path_to_manifest_json
    catalog_path: _path_to_catalog_json
    target_platform: postgres

    # Enable Data Contract ingestion
    ingest_contracts: true
```

For **dbt Cloud**, contract data is read from the Discovery API's environment endpoint, because the job-scoped API the connector otherwise uses does not expose it. The environment defaults to the one the ingested run belongs to; set `environment_id` only to pin it:

```yaml
source:
  type: dbt-cloud
  config:
    account_id: 123456
    project_id: 7890
    job_id: 11111
    environment_id: 22222 # optional; defaults to the run's environment
    token: ${DBT_CLOUD_TOKEN}
    target_platform: snowflake

    # Enable Data Contract ingestion
    ingest_contracts: true
```

If `environment_id` is set and the ingested run belongs to a different environment, those models get no contract and the ingestion report carries a warning.

If the Discovery API request for contract data fails, the whole ingestion run is marked as failed, so stateful ingestion does not soft-delete the contracts from earlier runs.

##### What Gets Ingested

When a dbt model has `contract.enforced: true`, DataHub creates:

1. **Schema assertion (CUSTOM)** — declared columns and types, with pass/fail from the model build (dbt's preflight). Linked on the Data Contract **data quality** section (not the schema slot, which is reserved for DataHub-native schema assertions).
2. **Constraint assertions (CUSTOM)** — one per constraint in the contract. A named constraint's `name` becomes the assertion's display name (for a `primary_key`, only on its unique assertion); otherwise DataHub builds the title from the assertion definition:
   - `not_null` → a `NOT_NULL` assertion on the column
   - `unique` → a unique-proportion assertion with parameter `1.0`
   - `primary_key` → **both** a unique assertion and a not-null assertion (PK = unique AND not null)
   - Composite primary keys produce a single multi-column unique assertion over the tuple, plus per-column not-null assertions
   - The same check declared more than once (for example `not_null` and `primary_key` on one column) produces one assertion, labelled with the strongest enforcement. Column names are compared case-insensitively, and a foreign key's target is identified by the relation it resolves to, so manifests from `dbt parse` and `dbt compile` produce the same assertions
   - `foreign_key` carries `to` (referenced model) and `to_columns` on custom properties (dbt Core only; see the dbt Cloud limitations below). dbt 1.9+ declares foreign keys with `to` / `to_columns`; the older `expression: "other_table (id)"` form is also parsed.
   - `check` and `custom` carry the expression on custom properties
3. **Matching dbt tests** — existing test assertions (already CUSTOM + run events) are attached when tagged or when the test type and column match a declared constraint.
4. **Data Contract entity** — bundles the assertions above and links them to the dbt dataset.

dbt never merges into a Data Contract it did not create. dbt finds a dataset's existing contract the same way the DataHub UI does (the contract that points at the dataset), so contracts created in the UI or from YAML are found too. A dataset without a live contract gets dbt's contract at a URN derived from the dataset; when the lookup returns soft-deleted contracts only, a soft-deleted contract at that URN is revived. An existing contract is treated as dbt's only when it is at that dataset-derived URN and still lists the dataset's dbt schema assertion; dbt then rewrites it in place. A contract at another URN (for example one created in the UI) is never treated as dbt's, even if it lists dbt's schema assertion, and a contract at the derived URN without that assertion (for example one authored in YAML, which uses the same URN) is not either. The schema assertion is keyed on the dataset, so renaming a model or moving it to another package without changing its relation keeps the same contract. dbt's contract is rewritten on every run with only the current run's assertions, so removed constraints, tests, and columns drop out. Edits made in DataHub to a dbt-owned contract (freshness, schema, state, or extra entries) are overwritten on the next run; make those changes in dbt instead.

If the dataset already has a contract that is not dbt's, dbt does not change its content by default; it only re-sends the contract's existing status each run so stateful ingestion does not soft-delete it. The dbt contract assertions are still emitted as standalone assertions, and the dataset is listed on the ingestion report (`contracts_skipped_existing`). Removing the dbt schema assertion from a contract in DataHub hands that contract over: dbt stops updating it. Set `replace_existing_contracts_with_dbt: true` to always write dbt's contract instead, replacing whatever is there; with a DataHub graph connection it is written to the existing contract, so the dataset keeps one contract. A dataset with more than one contract is never changed (only its contracts' status is re-sent, as above) and is reported as a warning.

Finding existing contracts needs a DataHub graph connection. With a sink that has none (for example the file sink, or a Kafka sink without `datahub_api`), no Data Contract is written: contract assertions are still emitted and the report carries a warning. With stateful stale removal enabled, the run instead stops at startup with a configuration error, since unwritten contracts would otherwise be soft-deleted. Configure `datahub_api`, or set `replace_existing_contracts_with_dbt: true`, to write contracts. Without a graph, `replace_existing_contracts_with_dbt` writes to the URN derived from the dataset, so a dataset that already has a contract at another URN (for example one created in the UI) ends up with two contracts. Existing contracts are looked up with one request per contracted model, one after another, which adds noticeable time on very large projects. If existing contracts cannot be read, the whole ingestion run is marked as failed (so stateful ingestion does not soft-delete earlier contracts), contract assertions are still emitted, and no Data Contract is written or removed.

With stateful ingestion, dbt's contracts are soft-deleted like other stale dbt entities when a model stops enforcing its contract or `ingest_contracts` is turned off. Contracts that are not dbt's are not soft-deleted, unless `replace_existing_contracts_with_dbt` is set: dbt then owns the contract it writes, including stale removal. A contract dbt wrote and a user later took over (by removing dbt's schema assertion) is left untouched by the next run that still ingests the model's contract, and that run drops it from the checkpoint, so later runs never soft-delete it. If the model stops enforcing its contract, or `ingest_contracts` is turned off, before such a run, the contract is still in the previous checkpoint and is soft-deleted. As for other dbt nodes, if a model's contract fails to emit (reported as a warning), stale removal may soft-delete that model's earlier contract and assertions.

Unsupported or unknown constraint types are recorded on the ingestion report (`contract_constraints_skipped_unsupported`) and skipped rather than raised, so one unusual constraint in one model doesn't fail ingestion for the rest of the project.

##### How evaluations are reported

Contract assertions are **externally reported**: DataHub does not re-run them. A schema **SUCCESS** means only that dbt's preflight check passed, and that check is the adapter's: dbt never compares size, precision or scale, and some adapters compare less than every column's name and type. `AssertionRunEvent`s come from artifacts you already ingest (`run_results.json` on Core; job `status`/`error` on Cloud):

| Signal                                                                                             | Result                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| -------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Model build `success`                                                                              | Schema **SUCCESS**. On `table` models, constraints with `enforced_by: database` also get **SUCCESS** (the table was rebuilt with the constraints and the warehouse accepted the write). An incremental build gets the constraint DDL only when its table is (re)created, and dbt applies no constraints to views, so those builds only report the schema check. Databricks adds constraints after writing the table, so a failed constraint leaves the data in place; its builds also only report the schema check. |
| Model `error` whose message contains `enforced contract that failed` / `assert_columns_equivalent` | Schema **FAILURE**. The mismatch table is stored on `nativeResults.message`. No constraint events (preflight never reached DDL).                                                                                                                                                                                                                                                                                                                                                                                    |
| Any other model `error`                                                                            | No contract run event. This includes a build that fails because the data violates a database-enforced constraint: dbt reports that as a generic database error, so the constraint assertion keeps its previous result. Add a matching dbt test (for example `not_null`) to get a failure signal.                                                                                                                                                                                                                    |
| Matching dbt test                                                                                  | The existing test `AssertionRunEvent` (the high-quality signal for `not_null` / `unique` / `relationships`).                                                                                                                                                                                                                                                                                                                                                                                                        |

On dbt Core, only `run_results.json` from `dbt run`, `dbt build`, and `dbt retry` produce contract run events; `dbt compile` reports models as successful without building them. dbt Cloud reports no command per model, so the job's model status is used, except that a successful model without execute timings (not built in that run) produces no run event.

Unenforced constraints (`enforced_by: dbt_contract` or `unsupported`) get **no synthetic success**. Their constraint assertion is never run; a matching dbt test attached to the contract carries the result instead, and the constraint does not flip the contract red by itself. The not-null half of a `primary_key` carries the primary key's enforcement, since on most warehouses declaring a primary key does not enforce it. On PostgreSQL, Snowflake and Databricks, which enforce a primary key's NOT NULL, it carries the `not_null` enforcement instead.

On dbt Cloud, contract run events are stamped with the model's `executeCompletedAt` (or `executeStartedAt`) from the job API, so ingesting the same run again does not add new run events. A failed model without timings (for example a contract preflight failure) is stamped with the run's `runGeneratedAt`, and with the ingestion time only if that is missing too.

##### Configuration Options

| Option                                    | Default      | Description                                                                                                                                                                                                                                                                                                                               |
| ----------------------------------------- | ------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ingest_contracts`                        | `false`      | Enable Data Contract creation from dbt contracts. Schema and constraint checks are CUSTOM assertions; run events come from model builds and matching tests.                                                                                                                                                                               |
| `replace_existing_contracts_with_dbt`     | `false`      | Replace an existing Data Contract that dbt did not create with dbt's contract. By default dbt does not change such contracts and only the dbt contract assertions are emitted. Also needed to write contracts without a DataHub graph connection; without a graph, a dataset that already has a contract at another URN ends up with two. |
| `contract_test_tag`                       | `"contract"` | Tag on extra dbt tests to include in the Data Contract data quality section. Tests that already match a declared constraint (`not_null`, `unique`, `relationships`) are auto-attached even without this tag. **The tag is a DataHub convention** — dbt itself does not bundle tests with contracts.                                       |
| `ingest_column_constraints_as_assertions` | `true`       | Emit CUSTOM assertions from contract constraints (`not_null`, `unique`, `primary_key`, `foreign_key`, `check`, `custom`). Whether each constraint is DDL-enforced is surfaced via `enforced_by`. Run events come from model builds (schema + DDL-enforced constraints on success) and from matching dbt tests.                            |
| `environment_id` _(dbt Cloud only)_       | `None`       | dbt Cloud environment to read contract data from. Defaults to the ingested run's environment.                                                                                                                                                                                                                                             |

##### Including dbt Tests in Contracts (DataHub Convention)

dbt itself does not bundle specific tests with model contracts — contracts validate **shape** (column names and types at build time), while dbt tests validate **content** after building. They are distinct features in dbt.

DataHub attaches a test to the generated Data Contract when either:

- the test type and column match a declared constraint (`not_null`, `unique`, `relationships` ↔ `foreign_key`) and the test has no `where:` filter or dbt_expectations `row_condition` (a filtered test checks only some rows), or
- the test is tagged with `contract_test_tag` (default: `contract`) — use this for `accepted_values` and other extra checks.

The tag is a DataHub-specific extension, not a dbt feature — your dbt project will still run the tagged tests as ordinary dbt tests. Matching tests reuse the same assertion URN the test path already emits, so run history stays on one entity.

```yaml
# In your dbt schema.yml
models:
  - name: orders
    config:
      contract:
        enforced: true
    columns:
      - name: order_id
        data_type: int
        constraints:
          - type: primary_key
        data_tests:
          - unique # matches the primary key, attached automatically
          - not_null # matches the primary key, attached automatically
      - name: status
        data_type: varchar
        data_tests:
          - accepted_values:
              values: ["placed", "shipped", "returned"]
              config:
                tags: ["contract"] # no matching constraint, so tag it
```

With the default `contract_test_tag: "contract"`, any test tagged with `contract` is added to the Data Contract's data quality assertions and also continues to run as a regular dbt test. A tagged test that reads several models (such as `relationships`) is attached only to the model it is declared on. A `relationships` test is matched to a `foreign_key` only when both targets resolve to the same relation in the ingested project (a foreign key declared with `expression: "other_table (id)"` is matched on that table and column); for a target in another project or package, tag the test instead.

On dbt Cloud, the API exposes neither test arguments nor the model a test is declared on. So `relationships` tests are never auto-attached to a `foreign_key` constraint; tag a test with `contract_test_tag` to include it. A tagged test is attached only when it reads that one model, so a tagged test that reads several models (such as `relationships`) is not attached on dbt Cloud; use the dbt Core source to include those. The API also exposes no test `where:` config, so on dbt Cloud a filtered `not_null` or `unique` test is still auto-attached to a matching constraint.

##### Warehouse Constraint Enforcement

dbt's [constraint support matrix](https://docs.getdbt.com/reference/resource-properties/constraints) varies widely by adapter, and dbt documents that the matrix will keep changing. DataHub reflects three states on each constraint assertion's `enforced_by` custom property:

| Adapter    | `not_null`   | `unique`     | `primary_key` / `foreign_key` | `check`      |
| ---------- | ------------ | ------------ | ----------------------------- | ------------ |
| PostgreSQL | database     | database     | database                      | database     |
| Snowflake  | database     | dbt_contract | dbt_contract                  | unsupported  |
| Redshift   | database     | dbt_contract | dbt_contract                  | unsupported  |
| Fabric     | database     | dbt_contract | dbt_contract                  | unsupported  |
| BigQuery   | database     | unsupported  | dbt_contract                  | unsupported  |
| Databricks | database     | unsupported  | dbt_contract                  | database     |
| Spark      | dbt_contract | dbt_contract | dbt_contract                  | dbt_contract |
| Athena     | unsupported  | unsupported  | unsupported                   | unsupported  |

- **`enforced_by: database`** means dbt emits real DDL constraints and the warehouse validates them at write time.
- **`enforced_by: dbt_contract`** means the constraint is definable as metadata but is not DDL-enforced.
- **`enforced_by: unsupported`** means this adapter cannot define that constraint type. The assertion is still emitted so the contract declaration is visible.
- If `dbt_adapter` is missing or unrecognized, DataHub falls back to `dbt_contract` rather than guessing.
- `custom` constraints are added to the DDL as written on every adapter, so they are always `dbt_contract`: whether the warehouse enforces one depends on its expression.
- dbt applies constraint DDL only to `table` and `incremental` models, so constraints on other materializations (for example views) are always `dbt_contract`.

The table is a snapshot of dbt's published matrix. Prefer `warn_unenforced` / `warn_unsupported` on the assertion custom properties (copied from the manifest when present) as the source of truth for a given project.

Contracts are always written to the dbt dataset, regardless of `dbt_is_primary_sibling`. A contract created on the warehouse sibling is not seen, so a dataset can end up with one contract on each sibling. See [Where the contract appears](#ingesting-dbt-contracts-as-datahub-data-contracts) for viewing it.

##### Limitations on dbt Cloud

On dbt Cloud, contract column types come from the applied catalog (the post-run warehouse state), not the pre-DDL manifest declaration — the Discovery API does not expose declared types via `definition.models` in a form the connector can use. This means:

- Schema **definitions** on dbt Cloud are looser than on dbt Core: a catalog-driven assertion can't detect a mismatch between the contract declaration and the actual table schema, and it shows the warehouse column types (for example `BIGINT` for a declared `int`). Schema **run events** still come from the job's model `status` / `error`.
- Constraints are read from `applied.models.constraints`, which holds model-level constraints only; the Discovery API returns no column-level constraints. So column-level `not_null`, `primary_key` and `unique` constraints produce no assertions on dbt Cloud, and their matching tests are not auto-attached. Declare them as model-level constraints with `columns: [...]` to ingest them.
- Contract data reflects the environment's latest applied state, not the specific `run_id` being ingested. If the contract changed after that run, the ingested definition is the newer one.
- Foreign keys carry no `to` / `to_columns` on dbt Cloud: the Discovery API has no such fields (it returns `name`, `type`, `expression`, and `columns`). Because the target is unknown, two unnamed foreign keys on the same columns become one assertion; name them to keep them apart.
- If you need strict declared-type matching (for example, when using `alias_types: false` to force platform-specific types in your contracts), use the dbt Core source, which reads the manifest directly.

Assertions emitted from dbt contracts are durable CUSTOM metadata on `assertion` entities. The wrapping `dataContract` entity is what the current DataHub UI uses to group them and roll up the latest run events; if you later migrate contracts to another representation, the assertions remain.

##### Notes on `alias_types`

dbt's `alias_types` config (default `true`) controls whether generic type names in a contract (`string`, `int`) are translated to platform-specific equivalents at DDL time. DataHub preserves whatever type the user wrote in the manifest on the schema assertion's `logic` field:

- With `alias_types: true` and a declared type of `string`, the assertion describes `string`.
- With `alias_types: false` and a declared type of `VARCHAR(64)`, the assertion describes `VARCHAR(64)` verbatim, which matches what dbt enforces at the DDL layer.

Pass/fail for the schema check is dbt's preflight (names and types), not a DataHub-native schema evaluator.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
