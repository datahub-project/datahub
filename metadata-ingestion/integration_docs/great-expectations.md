---
description: "Send Great Expectations assertion results to DataHub using the DataHubValidationAction and the Python REST emitter integration."
---

# Great Expectations

This guide helps to setup and configure `DataHubValidationAction` in Great Expectations to send assertions(expectations) and their results to DataHub using DataHub's Python Rest emitter.

## Capabilities

`DataHubValidationAction` pushes assertions metadata to DataHub. This includes

- **Assertion Details**: Details of assertions (i.e. expectation) set on a Dataset (Table).
- **Assertion Results**: Evaluation results for an assertion tracked over time.

This integration supports v3 api datasources using SqlAlchemyExecutionEngine and SparkDFExecutionEngine.

For SparkDFExecutionEngine, DataHubValidationAction would map the **Data Asset** of GX to dataSet's entity name when constructing datasets URN.

## Limitations

This integration does not support

- v2 Datasources such as SqlAlchemyDataset
- v3 Datasources using execution engine other than SqlAlchemyExecutionEngine,SparkDFExecutionEngine (Pandas)
- Cross-dataset expectations (those involving > 1 table)

## Compatibility

| GX version          | Action module                                         |
| ------------------- | ----------------------------------------------------- |
| `>=0.17.15, <1.0.0` | `datahub_gx_plugin.action.DataHubValidationAction`    |
| `>=1.0.0`           | `datahub_gx_plugin.action_v1.DataHubValidationAction` |

- GX 0.x SparkDFExecutionEngine has been tested with **Great Expectations >= 0.18.0, <1.0.0**.
- GX Core 1.x uses a separate, additive action class so existing 0.x checkpoint YAML continues to work unchanged.

## Setting up (GX 0.17 / 0.18)

1. Install the required dependency in your Great Expectations environment.

   ```shell
   pip install 'acryl-datahub-gx-plugin'
   ```

2. To add `DataHubValidationAction` in Great Expectations Checkpoint, add following configuration in action_list for your Great Expectations `Checkpoint`. For more details on setting action_list, see [Checkpoints and Actions](https://docs.greatexpectations.io/docs/reference/checkpoints_and_actions/)
   ```yml
   action_list:
     - name: datahub_action
       action:
         module_name: datahub_gx_plugin.action
         class_name: DataHubValidationAction
         server_url: http://localhost:8080 #datahub server url
   ```
   **Configuration options:**
   - `server_url` (required): URL of DataHub GMS endpoint
   - `env` (optional, defaults to "PROD"): Environment to use in namespace when constructing dataset URNs.
   - `exclude_dbname` (optional): Exclude dbname / catalog when constructing dataset URNs. (Highly applicable to Trino / Presto where we want to omit catalog e.g. `hive`)
   - `platform_alias` (optional): Platform alias when constructing dataset URNs. e.g. main data platform is `presto-on-hive` but using `trino` to run the test
   - `platform_instance_map` (optional): Platform instance mapping to use when constructing dataset URNs. Maps the GX 'data source' name to a platform instance on DataHub. e.g. `platform_instance_map: { "datasource_name": "warehouse" }`
   - `graceful_exceptions` (defaults to true): If set to true, most runtime errors in the lineage backend will be suppressed and will not cause the overall checkpoint to fail. Note that configuration issues will still throw exceptions.
   - `token` (optional): Bearer token used for authentication.
   - `timeout_sec` (optional): Per-HTTP request timeout.
   - `retry_status_codes` (optional): Retry HTTP request also on these status codes.
   - `retry_max_times` (optional): Maximum times to retry if HTTP request fails. The delay between retries is increased exponentially.
   - `extra_headers` (optional): Extra headers which will be added to the datahub request.
   - `parse_table_names_from_sql` (defaults to false): The integration can use an SQL parser to try to parse the datasets being asserted. This parsing is disabled by default, but can be enabled by setting `parse_table_names_from_sql: True`. The parser is based on the [`sqllineage`](https://pypi.org/project/sqllineage/) package.
   - `convert_urns_to_lowercase` (optional): Whether to convert dataset urns to lowercase.
   - `emit_mode` (defaults to `ASYNC`): Emit mode for writes to DataHub. `ASYNC` avoids blocking on a synchronous commit per write, reducing GMS load at high volume. Use `SYNC_WAIT`/`SYNC_PRIMARY` for read-after-write or raise-on-failure guarantees.

## Setting up (GX Core 1.x)

GX 1.x replaced legacy checkpoint YAML `action_list` with Fluent Checkpoints and Pydantic Actions. Use the V1 action module:

```python
import great_expectations as gx
from datahub_gx_plugin.action_v1 import DataHubValidationAction

context = gx.get_context()
# Assume validation_definitions already exist on the context.

checkpoint = context.checkpoints.add(
    gx.Checkpoint(
        name="my_checkpoint",
        validation_definitions=validation_definitions,
        actions=[
            DataHubValidationAction(
                name="datahub_action",
                server_url="http://localhost:8080",
                token="${DATAHUB_TOKEN}",  # prefer ConfigStr over a literal secret
                # Optional explicit identity when validation meta lacks batch_spec:
                # platform="postgres",
                # dataset_name="public.my_table",
                # platform_instance="warehouse",
            )
        ],
    )
)

checkpoint.run()
```

V1 supports the same core options as the 0.x action (`server_url`, `env`, `token`, `platform_alias`, `platform_instance_map`, `graceful_exceptions`, `emit_mode`, etc.), plus optional `platform` / `dataset_name` / `platform_instance` when dataset identity cannot be inferred from validation `meta.batch_spec`.

For `token`, prefer a GX config variable (e.g. `token="${DATAHUB_TOKEN}"`) rather than a literal secret. GX persists checkpoints to disk; a plain string is written cleartext into `gx/checkpoints/<name>.json`, while a `ConfigStr` placeholder is stored as-is and resolved only at run time.

## What gets emitted

Each expectation becomes a DataHub assertion (plus an `assertionRunEvent` per checkpoint run).

| GX input                                             | DataHub field                                                                                                        |
| ---------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| Expectation type + kwargs                            | Assertion URN identity (`nativeType` / `nativeParameters`) and mapped `customAssertion` structured fields when known |
| Expectation `description`                            | `AssertionInfo.description`                                                                                          |
| Expectation suite name                               | `AssertionInfo.customProperties.expectation_suite_name`                                                              |
| Expectation `id` (when present)                      | `AssertionInfo.customProperties.expectation_id` (not used in the URN — IDs can change if a suite is recreated)       |
| Checkpoint name / id                                 | `AssertionInfo.customProperties.checkpoint_name` / `checkpoint_id`                                                   |
| Validation definition name / id                      | `AssertionInfo.customProperties.validation_definition_name` / `validation_id`                                        |
| Expectation `severity` on **failure**                | `AssertionResult.severity`: `critical`→`HIGH`, `warning`→`MEDIUM`, `info`→`LOW`                                      |
| `result_url`, else Data Docs URL from a prior action | `AssertionResult.externalUrl`                                                                                        |
| Pass/fail + counts / observed values                 | `AssertionResult` (`type`, `rowCount`, `unexpectedCount`, `missingCount`, `actualAggValue`, `nativeResults`)         |

**Dataset URN resolution (V1):** explicit action `platform` + `dataset_name` → SQLAlchemy `batch_spec` table identity → GX `asset_name` / `data_asset_name` with platform hints.

## Debugging

Set environment variable `DATAHUB_DEBUG` (default `false`) to `true` to enable debug logging for `DataHubValidationAction`.

## Learn more

To see the Great Expectations in action, check out [this demo](https://www.loom.com/share/d781c9f0b270477fb5d6b0c26ef7f22d) from the Feb 2022 townhall.
