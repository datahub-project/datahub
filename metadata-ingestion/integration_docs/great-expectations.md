# Data Quality with Great Expectations

This guide helps to setup and configure `DatahubValidationAction` in Great Expectations to send assertions(expectations) and their results to Datahub using Datahub's Python Rest emitter.


## Capabilities

`DatahubValidationAction` pushes assertions metadata to DataHub. This includes

- Details of assertions set on a dataset
- Details of whether assertion succeeded against the dataset at a particular time

This integration supports v3 api datasources using SqlAlchemyExecutionEngine. 

## Known Limitations

This integration does not support
- v2 Datasources such as SqlAlchemyDataset
- v3 Datasources using execution engine other than SqlAlchemyExecutionEngine

## Setting up 

1. Install the required dependency in your Great Expectations environment.  
    ```shell
    pip install 'acryl-datahub[great-expectations]'
    ```


2. To add `DatahubValidationAction` in Great Expectations Checkpoint, add following configuration in action_list for your Great Expectations `Checkpoint`. For more details on setting action_list, see [Checkpoints and Actions](https://docs.greatexpectations.io/docs/reference/checkpoints_and_actions/) 
    ```yml
    action_list:
      - name: datahub_action
        action:
          module_name: datahub.integrations.great_expectations.action
          class_name: DatahubValidationAction
          server_url: http://localhost:8080 #datahub server url
    ```
    **Configuration options:**
    - `server_url` (required): URL of DataHub GMS endpoint
    - `env` (optional, defaults to "PROD"): Environment to use in namespace when constructing dataset URNs.
    - `graceful_exceptions` (defaults to true): If set to true, most runtime errors in the lineage backend will be suppressed and will not cause the overall checkpoint to fail. Note that configuration issues will still throw exceptions.
    - `token` (optional): Bearer token used for authentication.
    - `timeout_sec` (optional): Per-HTTP request timeout.
    - `retry_status_codes` (optional): Retry HTTP request also on these status codes.
    - `retry_max_times` (optional): Maximum times to retry if HTTP request fails. The delay between retries is increased exponentially.
    - `extra_headers` (optional): Extra headers which will be added to the datahub request.