# Stateful Ingestion
The stateful ingestion feature enables sources to be configured to save custom checkpoint states from their
runs, and query these states back from subsequent runs to make decisions about the current run based on the state saved
from the previous run(s) using a supported ingestion state provider. This is an explicit opt-in feature and is not enabled
by default.

**_NOTE_**: This feature requires the server to be `statefulIngestion` capable. This is a feature of metadata service with version >= `0.8.20`.

To check if you are running a stateful ingestion capable server:
```console
curl http://<datahub-gms-endpoint>/config

{
models: { },
statefulIngestionCapable: true, # <-- this should be present and true
retention: "true",
noCode: "true"
}
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                                                        | Required | Default                                                                                                          | Description                                                                                                                                                 |
|--------------------------------------------------------------| -------- |------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `source.config.stateful_ingestion.enabled`                   |          | False                                                                                                            | The type of the ingestion state provider registered with datahub.                                                                                           |
| `source.config.stateful_ingestion.ignore_old_state`          |          | False                                                                                                            | If set to True, ignores the previous checkpoint state.                                                                                                      |
| `source.config.stateful_ingestion.ignore_new_state`          |          | False                                                                                                            | If set to True, ignores the current checkpoint state.                                                                                                       |
| `source.config.stateful_ingestion.max_checkpoint_state_size` |          | 2^24 (16MB)                                                                                                      | The maximum size of the checkpoint state in bytes.                                                                                                          |
| `source.config.stateful_ingestion.state_provider`            |          | The default [datahub ingestion state provider](#datahub-ingestion-state-provider) configuration. | The ingestion state provider configuration.                                                                                                                 |
| `pipeline_name`                                              |    ✅    |                                                                                                                  | The name of the ingestion pipeline the checkpoint states of various source connector job runs are saved/retrieved against via the ingestion state provider. |

NOTE: If either `dry-run` or `preview` mode are set, stateful ingestion will be turned off regardless of the rest of the configuration.
## Use-cases powered by stateful ingestion.
Following is the list of current use-cases powered by stateful ingestion in datahub.
### Removal of stale tables and views.
Stateful ingestion can be used to automatically soft-delete the tables and views that are seen in a previous run
but absent in the current run (they are either deleted or no longer desired).

![Stale Metadata Deletion](./stale_metadata_deletion.png)

#### Supported sources
* All sql based sources.
#### Additional config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                                      | Required | Default | Description                                                                                                                                  |
|--------------------------------------------| -------- |---------|----------------------------------------------------------------------------------------------------------------------------------------------|
| `stateful_ingestion.remove_stale_metadata` |          | True    | Soft-deletes the tables and views that were found in the last successful run but missing in the current run with stateful_ingestion enabled. |
#### Sample configuration
```yaml
source:
  type: "snowflake"
  config:
    username: <user_name>
    password: <password>
    host_port: <host_port>
    warehouse: <ware_house>
    role: <role>
    include_tables: True
    include_views: True
    # Rest of the source specific params ...
    ## Stateful Ingestion config ##
    stateful_ingestion:
        enabled: True # False by default
        remove_stale_metadata: True # default value
        ## Default state_provider configuration ##
        # state_provider:
            # type: "datahub" # default value
            # This section is needed if the pipeline-level `datahub_api` is not configured.
            # config:  # default value
            #    datahub_api:
            #        server: "http://localhost:8080"

# The pipeline_name is mandatory for stateful ingestion and the state is tied to this.
# If this is changed after using with stateful ingestion, the previous state will not be available to the next run.
pipeline_name: "my_snowflake_pipeline_1"

# Pipeline-level datahub_api configuration.
datahub_api: # Optional. But if provided, this config will be used by the "datahub" ingestion state provider.
    server: "http://localhost:8080"

sink:
  type: "datahub-rest"
  config:
    server: 'http://localhost:8080'
```

### Prevent redundant reruns for usage source.
Typically, the usage runs are configured to fetch the usage data for the previous day(or hour) for each run. Once a usage
run has finished, subsequent runs until the following day would be fetching the same usage data. With stateful ingestion,
the redundant fetches can be avoided even if the ingestion job is scheduled to run more frequently than the granularity of
usage ingestion.
#### Supported sources
* Snowflake Usage source.
#### Additional config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                            | Required | Default | Description                                                                                                                               |
|----------------------------------| -------- |---------|-------------------------------------------------------------------------------------------------------------------------------------------|
| `stateful_ingestion.force_rerun` |          | False   | Custom-alias for `stateful_ingestion.ignore_old_state`. Prevents a rerun for the same time window if there was a previous successful run. |
#### Sample Configuration
```yaml
source:
  type: "snowflake-usage-legacy"
  config:
    username: <user_name>
    password: <password>
    role: <role>
    host_port: <host_port>
    warehouse: <ware_house>
    # Rest of the source specific params ...
    ## Stateful Ingestion config ##
    stateful_ingestion:
        enabled: True # default is false
        force_rerun: False # Specific to this source(alias for ignore_old_state), used to override default behavior if True.

# The pipeline_name is mandatory for stateful ingestion and the state is tied to this.
# If this is changed after using with stateful ingestion, the previous state will not be available to the next run.
pipeline_name: "my_snowflake_usage_ingestion_pipeline_1"
sink:
  type: "datahub-rest"
  config:
    server: 'http://localhost:8080'
```

## The Checkpointing Ingestion State Provider (Developer Guide)
The ingestion checkpointing state provider is responsible for saving and retrieving the ingestion checkpoint state associated with the ingestion runs
of various jobs inside the source connector of the ingestion pipeline. The checkpointing data model is [DatahubIngestionCheckpoint](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/datajob/datahub/DatahubIngestionCheckpoint.pdl) and it supports any custom state to be stored using the [IngestionCheckpointState](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/datajob/datahub/IngestionCheckpointState.pdl#L9). A checkpointing ingestion state provider needs to implement the
[IngestionCheckpointingProviderBase](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/api/ingestion_job_checkpointing_provider_base.py) interface and
register itself with datahub by adding an entry under `datahub.ingestion.checkpointing_provider.plugins` key of the entry_points section in [setup.py](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/setup.py) with its type and implementation class as shown below.
```python
entry_points = {
    # <snip other keys>"
    "datahub.ingestion.checkpointing_provider.plugins": [
        "datahub = datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider:DatahubIngestionCheckpointingProvider",
    ],
}
```

### Datahub Checkpointing Ingestion State Provider
This is the state provider implementation that is available out of the box. Its type is `datahub` and it is implemented on top
of the `datahub_api` client and the timeseries aspect capabilities of the datahub-backend.
#### Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                                                    | Required | Default                                                                                                                                                                                                                                 | Description                                                      |
|----------------------------------------------------------| -------- |-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------|
| `state_provider.type`   |          | `datahub`                                                                                                                                                                                                                               | The type of the ingestion state provider registered with datahub |
| `state_provider.config` |          | The `datahub_api` config if set at pipeline level. Otherwise, the default `DatahubClientConfig`. See the [defaults](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/graph/client.py#L19) here. | The configuration required for initializing the state provider.  |
