# Removing Metadata from DataHub

There are a two ways to delete metadata from DataHub. 
- Delete metadata attached to entities by providing a specific urn or a filter that identifies a set of entities
- Delete metadata affected by a single ingestion run

To follow this guide you need to use [DataHub CLI](../cli.md).

Read on to find out how to perform these kinds of deletes.

_Note: Deleting metadata should only be done with care. Always use `--dry-run` to understand what will be deleted before proceeding. Prefer soft-deletes (`--soft`) unless you really want to nuke metadata rows. Hard deletes will actually delete rows in the primary store and recovering them will require using backups of the primary metadata store. Make sure you understand the implications of issuing soft-deletes versus hard-deletes before proceeding._ 

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

If you wish to hard-delete using a curl request you can use something like below. Replace the URN with the URN that you wish to delete

```
curl "http://localhost:8080/entities?action=delete" -X POST --data '{"urn": "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)"}'
```

## Delete using Broader Filters

_Note: All these commands below support the soft-delete option (`-s/--soft`) as well as the dry-run option (`-n/--dry-run`). Additionally, as of v0.8.29 there is a new option: `--include-removed` that deletes softly deleted entities that match the provided filter.


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

### Unsafe Entities and Rollback

> **_NOTE:_** Preservation of unsafe entities has been added in datahub `0.8.32`. Read on to understand what it means and how it works.

In some cases, entities that were initially ingested by a run might have had further modifications to their metadata (e.g. adding terms, tags, or documentation) through the UI or other means. During a roll back of the ingestion that initially created these entities (technically, if the key aspect for these entities are being rolled back), the ingestion process will analyse the metadata graph for aspects that will be left "dangling" and will:
1. Leave these aspects untouched in the database, and soft-delete the entity. A re-ingestion of these entities will result in this additional metadata becoming visible again in the UI, so you don't lose any of your work. 
2. The datahub cli will save information about these unsafe entities as a CSV for operators to later review and decide on next steps (keep or remove).

The rollback command will report how many entities have such aspects and save as a CSV the urns of these entities under a rollback reports directory, which defaults to `rollback_reports` under the current directory where the cli is run, and can be configured further using the `--reports-dir` command line arg.

The operator can use `datahub get --urn <>` to inspect the aspects that were left behind and either keep them (do nothing) or delete the entity (and its aspects) completely using `datahub delete --urn <urn> --hard`. If the operator wishes to remove all the metadata associated with these unsafe entities, they can re-issue the rollback command with the `--nuke` flag.
