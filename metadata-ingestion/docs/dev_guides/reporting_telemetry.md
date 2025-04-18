# Datahub's Reporting Framework for Ingestion Job Telemetry
The Datahub's reporting framework allows for configuring reporting providers with the ingestion pipelines to send 
telemetry about the ingestion job runs to external systems for monitoring purposes. It is powered by the Datahub's 
stateful ingestion framework. The `datahub` reporting provider comes with the standard client installation, 
and allows for reporting ingestion job telemetry to the datahub backend as the destination.

**_NOTE_**: This feature requires the server to be `statefulIngestion` capable. 
This is a feature of metadata service with version >= `0.8.20`.

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
The ingestion reporting providers are a list of reporting provider configurations under the `reporting` config
param of the pipeline, each reporting provider configuration begin a type and config pair object. The telemetry data will
be sent to all the reporting providers in this list.

Note that a `.` is used to denote nested fields, and `[idx]` is used to denote an element of an array of objects in the YAML recipe.

| Field                   | Required | Default                                                                                                          | Description                                                                                                                                              |
|-------------------------| -------- |------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `reporting[idx].type`   |  ✅      | `datahub`                                                                                                                                                                                                                               | The type of the ingestion reporting provider registered with datahub.                                                                                    |
| `reporting[idx].config` |          | The `datahub_api` config if set at pipeline level. Otherwise, the default `DatahubClientConfig`. See the [defaults](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/graph/client.py#L19) here. | The configuration required for initializing the datahub reporting provider.                                                                              |
| `pipeline_name`         |    ✅    |                                                                                                                  | The name of the ingestion pipeline. This is used as a part of the identifying key for the telemetry data reported by each job in the ingestion pipeline. | 

#### Supported sources
* All sql based sources.
* snowflake_usage.
#### Sample configuration
```yaml
source:
  type: "snowflake"
  config:
    username: <user_name>
    password: <password>
    role: <role>
    host_port: <host_port>
    warehouse: <ware_house>
    # Rest of the source specific params ...
# This is mandatory. Changing it will cause old telemetry correlation to be lost.
pipeline_name: "my_snowflake_pipeline_1"

# Pipeline-level datahub_api configuration.
datahub_api: # Optional. But if provided, this config will be used by the "datahub" ingestion state provider.
    server: "http://localhost:8080"
    
sink:
  type: "datahub-rest"
  config:
    server: 'http://localhost:8080'

reporting:
  - type: "datahub" # Required
    config: # Optional. 
      datahub_api: # default value
        server: "http://localhost:8080"
```

## Reporting Ingestion State Provider (Developer Guide)
An ingestion reporting state provider is responsible for saving and retrieving the ingestion telemetry 
associated with the ingestion runs of various jobs inside the source connector of the ingestion pipeline. 
The data model used for capturing the telemetry is [DatahubIngestionRunSummary](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/datajob/datahub/DatahubIngestionRunSummary.pdl). 
A reporting ingestion state provider needs to implement the IngestionReportingProviderBase.
interface and register itself with datahub by adding an entry under `datahub.ingestion.reporting_provider.plugins` 
key of the entry_points section in [setup.py](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/setup.py) 
with its type and implementation class as shown below. 
```python
entry_points = {
    # <snip other keys>"
    "datahub.ingestion.reporting_provider.plugins": [
        "datahub = datahub.ingestion.reporting.datahub_ingestion_run_summary_provider:DatahubIngestionRunSummaryProvider",
        "file = datahub.ingestion.reporting.file_reporter:FileReporter",
    ],
}
```

### Datahub Reporting Ingestion State Provider
This is the reporting state provider implementation that is available out of the box in datahub. Its type is `datahub` and it is implemented on top
of the `datahub_api` client and the timeseries aspect capabilities of the datahub-backend.
#### Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                                                    | Required | Default                                                                                                                                                                                                                                 | Description                                                                 |
|----------------------------------------------------------|----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------|
| `type`   |  ✅      | `datahub`                                                                                                                                                                                                                               | The type of the ingestion reporting provider registered with datahub.       |
| `config` |          | The `datahub_api` config if set at pipeline level. Otherwise, the default `DatahubClientConfig`. See the [defaults](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/graph/client.py#L19) here. | The configuration required for initializing the datahub reporting provider. |
