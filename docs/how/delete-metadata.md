# Removing Metadata from DataHub

There are a two ways to delete data from DataHub.


## Configuring DataHub CLI

The CLI will point to localhost DataHub by default. Running

```
datahub init
```

will allow you to customize the datahub instance you are communicating with.

_Note: Provide your GMS instance's host when the prompt asks you for the DataHub host._

Alternatively, you can set the following env variables if you don't want to use a config file
```
DATAHUB_SKIP_CONFIG=True
DATAHUB_GMS_HOST=http://localhost:8080
DATAHUB_GMS_TOKEN=
```

The env variables take precendence over what is in the config.

## Delete By Urn

To delete all the data related to a single entity, run

```
datahub delete --urn "<my urn>"
```

You can optionally add `-f` or `--force` to skip confirmations

_Note: make sure you surround your urn with quotes! If you do not include the quotes, your terminal may misinterpret the command._

## Rollback Ingestion Batch Run

Whenever you run `datahub ingest -c ...`, all the metadata ingested with that run will have the same run id.

To view the ids of the most recent set of ingestion batches, execute

```
datahub ingest list-runs
```

That will print out a table of all the runs. Once you have an idea of which run you want to roll back, run

```
datahub ingest show --run-id <run-id>
```

to see more info of the run.

Finally, run

```
datahub ingest rollback --run-id <run-id>
```

To rollback all aspects added with this run and all entities created by this run.
