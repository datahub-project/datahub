# DataHub CLI

DataHub comes with a friendly cli called `datahub` that allows you to perform a lot of common operations using just the command line. 

## Install

### Using pip

We recommend python virtual environments (venv-s) to namespace pip modules. Here's an example setup:

```shell
python3 -m venv datahub-env             # create the environment
source datahub-env/bin/activate         # activate the environment
```

**_NOTE:_** If you install `datahub` in a virtual environment, that same virtual environment must be re-activated each time a shell window or session is created.

Once inside the virtual environment, install `datahub` using the following commands
```console
# Requires Python 3.6+
python3 -m pip install --upgrade pip wheel setuptools
python3 -m pip install --upgrade acryl-datahub
datahub version
# If you see "command not found", try running this instead: python3 -m datahub version
```

If you run into an error, try checking the [_common setup issues_](../metadata-ingestion/developing.md#Common-setup-issues).

## User Guide

The `datahub` cli allows you to do many things, such as quickstarting a DataHub docker instance locally, ingesting metadata from your sources, as well as retrieving and modifying metadata. 
Like most command line tools, `--help` is your best friend. Use it to discover the capabilities of the cli and the different commands and sub-commands that are supported. 

```console
datahub --help                                           
Usage: datahub [OPTIONS] COMMAND [ARGS]...

Options:
  --debug / --no-debug
  --version             Show the version and exit.
  --help                Show this message and exit.

Commands:
  check    Helper commands for checking various aspects of DataHub.
  delete   Delete metadata from datahub using a single urn or a combination of filters
  docker   Helper commands for setting up and interacting with a local DataHub instance using Docker.
  get      Get metadata for an entity with an optional list of aspects to project
  ingest   Ingest metadata into DataHub.
  init     Configure which datahub instance to connect to
  put      Update a single aspect of an entity
  version  Print version number and exit.
```

The following top-level commands listed below are here mainly to give the reader a high-level picture of what are the kinds of things you can accomplish with the cli. 
We've ordered them roughly in the order we expect you to interact with these commands as you get deeper into the `datahub`-verse. 

### docker

The `docker` command allows you to start up a local DataHub instance using `datahub docker quickstart`. You can also check if the docker cluster is healthy using `datahub docker check`. 

### ingest

The `ingest` command allows you to ingest metadata from your sources using ingestion configuration files, which we call recipes. The main [ingestion page](../metadata-ingestion/README.md) contains detailed instructions about how you can use the ingest command and perform advanced operations like rolling-back previously ingested metadata through the `rollback` sub-command.

### check

The datahub package is composed of different plugins that allow you to connect to different metadata sources and ingest metadata from them.
The `check` command allows you to check if all plugins are loaded correctly as well as validate an individual MCE-file.

### init

The init command is used to tell `datahub` about where your DataHub instance is located. The CLI will point to localhost DataHub by default.
Running `datahub init` will allow you to customize the datahub instance you are communicating with.

**_Note_**: Provide your GMS instance's host when the prompt asks you for the DataHub host.

Alternatively, you can set the following env variables if you don't want to use a config file

```shell
DATAHUB_SKIP_CONFIG=True
DATAHUB_GMS_HOST=http://localhost:8080
DATAHUB_GMS_TOKEN= # Used for communicating with DataHub Cloud
The env variables take precedence over what is in the config.
```

### delete

The `delete` command allows you to delete metadata from DataHub. Read this [guide](./how/delete-metadata.md) to understand how you can delete metadata from DataHub.

```console
datahub delete --urn "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)" --soft
```

### get

The `get` command allows you to easily retrieve metadata from DataHub, by using the REST API. 
For example the following command gets the ownership aspect from the dataset `urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)`

```console
datahub get --urn "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)" --aspect ownership | jq                                                                       put_command
{
  "value": {
    "com.linkedin.metadata.snapshot.DatasetSnapshot": {
      "aspects": [
        {
          "com.linkedin.metadata.key.DatasetKey": {
            "name": "SampleHiveDataset",
            "origin": "PROD",
            "platform": "urn:li:dataPlatform:hive"
          }
        },
        {
          "com.linkedin.common.Ownership": {
            "lastModified": {
              "actor": "urn:li:corpuser:jdoe",
              "time": 1581407189000
            },
            "owners": [
              {
                "owner": "urn:li:corpuser:jdoe",
                "type": "DATAOWNER"
              },
              {
                "owner": "urn:li:corpuser:datahub",
                "type": "DATAOWNER"
              }
            ]
          }
        }
      ],
      "urn": "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"
    }
  }
}
```

### put 

The `put` command allows you to write metadata into DataHub. This is a flexible way for you to issue edits to metadata from the command line. 
For example, the following command instructs `datahub` to set the `ownership` aspect of the dataset `urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)` to the value in the file `ownership.json`. 
The JSON in the `ownership.json` file needs to conform to the [`Ownership`](https://github.com/linkedin/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/common/Ownership.pdl) Aspect model as shown below.
```json
{
  "owners": [
    {
      "owner": "urn:li:corpUser:jdoe",
      "type": "DEVELOPER"
    },
    {
      "owner": "urn:li:corpUser:jdub",
      "type": "DATAOWNER"
    }
  ]
}
```

```console
datahub --debug put --urn "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)" --aspect ownership -d ownership.json

[DATE_TIMESTAMP] DEBUG    {datahub.cli.cli_utils:340} - Attempting to emit to DataHub GMS; using curl equivalent to:
curl -X POST -H 'User-Agent: python-requests/2.26.0' -H 'Accept-Encoding: gzip, deflate' -H 'Accept: */*' -H 'Connection: keep-alive' -H 'X-RestLi-Protocol-Version: 2.0.0' -H 'Content-Type: application/json' --data '{"proposal": {"entityType": "dataset", "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)", "aspectName": "ownership", "changeType": "UPSERT", "aspect": {"contentType": "application/json", "value": "{\"owners\": [{\"owner\": \"urn:li:corpUser:jdoe\", \"type\": \"DEVELOPER\"}, {\"owner\": \"urn:li:corpUser:jdub\", \"type\": \"DATAOWNER\"}]}"}}}' 'http://localhost:8080/aspects/?action=ingestProposal'
Update succeeded with status 200
```





