# Removing Metadata from DataHub

There are a two ways to delete metadata from DataHub. 
- Delete metadata attached to entities by providing a specific urn or a filter that identifies a set of entities
- Delete metadata affected by a single ingestion run

Read on to find out how to perform these kinds of deletes.

_Note: Deleting metadata should only be done with care. Always use `--dry-run` to understand what will be deleted before proceeding. Prefer soft-deletes (`--soft`) unless you really want to nuke metadata rows. Hard deletes will actually delete rows in the primary store and recovering them will require using backups of the primary metadata store. Make sure you understand the implications of issuing soft-deletes versus hard-deletes before proceeding._ 

## The `datahub` CLI

To use the datahub CLI you follow the installation and configuration guide at [DataHub CLI](../cli.md) or you can use the `datahub-ingestion` docker image as explained in [Docker Images](../../docker/README.md). In case you are using Kubernetes you can start a pod with the `datahub-ingestion` docker image, log onto a shell on the pod and you should have the access to datahub CLI in your kubernetes cluster.


## Delete By Urn

To delete all the data related to a single entity, run

### Soft Delete (the default)

This sets the `Status` aspect of the entity to `Removed`, which hides the entity and all its aspects from being returned by the UI.
```
datahub delete --urn "<my urn>"
```
or
```
datahub delete --urn "<my urn>" --soft
```

### Hard Delete

This physically deletes all rows for all aspects of the entity. This action cannot be undone, so execute this only after you are sure you want to delete all data associated with this entity. 

```
datahub delete --urn "<my urn>" --hard
```

You can optionally add `-n` or `--dry-run` to execute a dry run before issuing the final delete command.
You can optionally add `-f` or `--force` to skip confirmations

_Note: make sure you surround your urn with quotes! If you do not include the quotes, your terminal may misinterpret the command._

## Delete using Broader Filters

_Note: All these commands below support the soft-delete option (`-s/--soft`) as well as the dry-run option (`-n/--dry-run`)._ 

### Delete all datasets in the DEV environment
```
datahub delete --env DEV --entity_type dataset
```

### Delete all bigquery datasets in the PROD environment
```
datahub delete --env PROD --entity_type dataset --platform bigquery
```

### Delete all looker dashboards and charts
```
datahub delete --entity_type dashboard --platform looker
datahub delete --entity_type chart --platform looker
```

### Delete all datasets that match a query
```
datahub delete --entity_type dataset --query "_tmp" -n
```

## Rollback Ingestion Batch Run

The second way to delete metadata is to identify entities (and the aspects affected) by using an ingestion `run-id`. Whenever you run `datahub ingest -c ...`, all the metadata ingested with that run will have the same run id.

To view the ids of the most recent set of ingestion batches, execute

```
datahub ingest list-runs
```

That will print out a table of all the runs. Once you have an idea of which run you want to roll back, run

```
datahub ingest show --run-id <run-id>
```

to see more info of the run.

Alternately, you can execute a dry-run rollback to achieve the same outcome. 
```
datahub ingest rollback --dry-run --run-id <run-id>
```

Finally, once you are sure you want to delete this data forever, run

```
datahub ingest rollback --run-id <run-id>
```

to rollback all aspects added with this run and all entities created by this run.
