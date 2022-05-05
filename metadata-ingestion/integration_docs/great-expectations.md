# Great Expectations

This guide helps to setup and configure `DataHubValidationAction` in Great Expectations to send assertions(expectations) and their results to DataHub using DataHub's Python Rest emitter.

## Capabilities

`DataHubValidationAction` pushes assertions metadata to DataHub. This includes

- **Assertion Details**: Details of assertions (i.e. expectation) set on a Dataset (Table). 
- **Assertion Results**: Evaluation results for an assertion tracked over time. 

This integration supports v3 api datasources using SqlAlchemyExecutionEngine. 

## Limitations

This integration does not support

- v2 Datasources such as SqlAlchemyDataset
- v3 Datasources using execution engine other than SqlAlchemyExecutionEngine (Spark, Pandas)
- Cross-dataset expectations (those involving > 1 table)

## Setting up 

1. Install the required dependency in your Great Expectations environment.  
    ```shell
    pip install 'acryl-datahub[great-expectations]'
    ```


2. To add `DataHubValidationAction` in Great Expectations Checkpoint, add following configuration in action_list for your Great Expectations `Checkpoint`. For more details on setting action_list, see [Checkpoints and Actions](https://docs.greatexpectations.io/docs/reference/checkpoints_and_actions/) 
    ```yml
    action_list:
      - name: datahub_action
        action:
          module_name: datahub.integrations.great_expectations.action
          class_name: DataHubValidationAction
          server_url: http://localhost:8080 #datahub server url
    ```
    **Configuration options:**
    - `server_url` (required): URL of DataHub GMS endpoint
    - `env` (optional, defaults to "PROD"): Environment to use in namespace when constructing dataset URNs.
    - `platform_instance_map` (optional): Platform instance mapping to use when constructing dataset URNs. Maps the GE 'data source' name to a platform instance on DataHub. e.g. `platform_instance_map: { "datasource_name": "warehouse" }`
    - `graceful_exceptions` (defaults to true): If set to true, most runtime errors in the lineage backend will be suppressed and will not cause the overall checkpoint to fail. Note that configuration issues will still throw exceptions.
    - `token` (optional): Bearer token used for authentication.
    - `timeout_sec` (optional): Per-HTTP request timeout.
    - `retry_status_codes` (optional): Retry HTTP request also on these status codes.
    - `retry_max_times` (optional): Maximum times to retry if HTTP request fails. The delay between retries is increased exponentially.
    - `extra_headers` (optional): Extra headers which will be added to the datahub request.
    - `parse_table_names_from_sql` (defaults to false): The integration can use an SQL parser to try to parse the datasets being asserted. This parsing is disabled by default, but can be enabled by setting `parse_table_names_from_sql: True`.  The parser is based on the [`sqllineage`](https://pypi.org/project/sqllineage/) package.
    
## Learn more

To see the Great Expectations in action, check out [this demo](https://www.loom.com/share/d781c9f0b270477fb5d6b0c26ef7f22d) from the Feb 2022 townhall. 
