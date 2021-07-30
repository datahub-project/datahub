# Removing Metadata from DataHub

There are a two ways to delete data from DataHub.

## Delete By Urn

To delete all the data related to a single entity, run

```aidl
datahub delete --urn "<my urn>"
```

_Note: make sure you surround your urn with quotes! If you do not include the quotes, your terminal may misinterpret the command._

## Rollback Ingestion Batch Run

Whenever you run `datahub ingest -c ...`, all the metadata ingested with that run will have the same run id.

To view the ids of the most recent set of ingestion batches, execute

```aidl
datahub ingest list-runs
```

That will print out a table of all the runs. Once you have an idea of which run you want to roll back, run

```aidl
datahub ingest show --run-id <run-id>
```

to see more info of the run.

Finally, run

```aidl
datahub ingest rollback --run-id <run-id>
```

To rollback all aspects added with this run and all entities created by this run.
