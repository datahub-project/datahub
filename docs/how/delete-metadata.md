# Removing Metadata from DataHub

:::tip
To follow this guide, you'll need the [DataHub CLI](../cli.md).
:::

There are two ways to delete metadata from DataHub:

1. Delete metadata attached to entities by providing a specific urn or filters that identify a set of urns (delete CLI).
2. Delete metadata created by a single ingestion run (rollback).

:::caution Be careful when deleting metadata

- Always use `--dry-run` to test your delete command before executing it.
- Prefer reversible soft deletes (`--soft`) over irreversible hard deletes (`--hard`).

:::

## Delete CLI Usage

:::info

Deleting metadata using DataHub's CLI is a simple, systems-level action. If you attempt to delete an entity with children, such as a container, it will not delete those children. Instead, you will need to delete each child by URN in addition to deleting the parent.

:::

All the commands below support the following options:

- `-n/--dry-run`: Execute a dry run instead of the actual delete.
- `--force`: Skip confirmation prompts.

### Selecting entities to delete

You can either provide a single urn to delete, or use filters to select a set of entities to delete.

```shell
# Soft delete a single urn.
datahub delete --urn "<my urn>"

# Soft delete using a filter.
datahub delete --platform snowflake

# Filters can be combined, which will select entities that match all filters.
datahub delete --platform looker --entity-type chart
datahub delete --platform bigquery --env PROD

# You can also do recursive deletes for container and dataPlatformInstance entities.
datahub delete --urn "urn:li:container:f76..." --recursive
```

When performing hard deletes, you can optionally add the `--only-soft-deleted` flag to only hard delete entities that were previously soft deleted.

### Performing the delete

#### Soft delete an entity (default)

By default, the delete command will perform a soft delete.

This will set the `status` aspect's `removed` field to `true`, which will hide the entity from the UI. However, you'll still be able to view the entity's metadata in the UI with a direct link.

```shell
# The `--soft` flag is redundant since it's the default.
datahub delete --urn "<urn>" --soft
# or using a filter
datahub delete --platform snowflake --soft
```

#### Hard delete an entity

This will physically delete all rows for all aspects of the entity. This action cannot be undone, so execute this only after you are sure you want to delete all data associated with this entity.

```shell
datahub delete --urn "<my urn>" --hard
# or using a filter
datahub delete --platform snowflake --hard
```

As of datahub v0.10.2.3, hard deleting tags, glossary terms, users, and groups will also remove references to those entities across the metadata graph.

#### Hard delete a timeseries aspect

It's also possible to delete a range of timeseries aspect data for an entity without deleting the entire entity.

For these deletes, the aspect and time ranges are required. You can delete all data for a timeseries aspect by providing `--start-time min --end-time max`.

```shell
datahub delete --urn "<my urn>" --aspect <aspect name> --start-time '-30 days' --end-time '-7 days'
# or using a filter
datahub delete --platform snowflake --entity-type dataset --aspect datasetProfile --start-time '0' --end-time '2023-01-01'
```

The start and end time fields filter on the `timestampMillis` field of the timeseries aspect. Allowed start and end times formats:

- `YYYY-MM-DD`: a specific date
- `YYYY-MM-DD HH:mm:ss`: a specific timestamp, assumed to be in UTC unless otherwise specified
- `+/-<number> <unit>` (e.g. `-7 days`): a relative time, where `<number>` is an integer and `<unit>` is one of `days`, `hours`, `minutes`, `seconds`
- `ddddddddd` (e.g. `1684384045`): a unix timestamp
- `min`, `max`, `now`: special keywords

#### Undo-ing soft deletion of entities

You can restore soft-deleted entities using the `undo-by-filter` command. This reverts the effect of a soft delete. 

```shell
# Restore (un-soft-delete) a single soft-deleted entity
datahub delete undo-by-filter --urn "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)"

# Restore all soft-deleted entities from a specific platform
datahub delete undo-by-filter --platform snowflake

# You can adjust the batch size (default 3000, max 10000) for better performance
datahub delete undo-by-filter --platform snowflake --batch-size 5000
```

## Delete CLI Examples

:::note

Make sure you surround your urn with quotes! If you do not include the quotes, your terminal may misinterpret the command.

:::

_Note: All of the commands below support `--dry-run` and `--force` (skips confirmation prompts)._

#### Soft delete a single entity

```shell
datahub delete --urn "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)"
```

#### Hard delete a single entity

```shell
datahub delete --urn "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)" --hard
```

#### Delete everything from the Snowflake DEV environment

```shell
datahub delete --platform snowflake --env DEV
```

#### Delete everything within a specific Snowflake DB

```shell
# You can find your container urn by navigating to the relevant
# DB in the DataHub UI and clicking the "copy urn" button.
datahub delete --urn "urn:li:container:77644901c4f574845578ebd18b7c14fa" --recursive
```

#### Delete all BigQuery datasets in the PROD environment

```shell
# Note: this will leave BigQuery containers intact.
datahub delete --env PROD --entity-type dataset --platform bigquery
```

#### Delete everything within a MySQL platform instance

```shell
# The instance name comes from the `platform_instance` config option in the ingestion recipe.
datahub delete --urn 'urn:li:dataPlatformInstance:(urn:li:dataPlatform:mysql,my_instance_name)' --recursive
```

#### Delete all pipelines and tasks from Airflow

```shell
datahub delete --platform "airflow"
```

#### Delete all containers for a particular platform

```shell
# Note: this will leave S3 datasets intact.
datahub delete --entity-type container --platform s3
```

#### Delete everything in the DEV environment

```shell
# This is a pretty broad filter, so make sure you know what you're doing!
datahub delete --env DEV
```

#### Delete all Looker dashboards and charts

```shell
datahub delete --platform looker
```

#### Delete all Looker charts (but not dashboards)

```shell
datahub delete --platform looker --entity-type chart
```

#### Clean up old datasetProfiles

```shell
datahub delete --entity-type dataset --aspect datasetProfile --start-time 'min' --end-time '-60 days'
```

#### Delete a tag

```shell
# Soft delete.
datahub delete --urn 'urn:li:tag:Legacy' --soft

# Or, using a hard delete. This will automatically clean up all tag associations.
datahub delete --urn 'urn:li:tag:Legacy' --hard
```

#### Delete all datasets that match a query

```shell
# Note: the query is an advanced feature, but can sometimes select extra entities - use it with caution!
datahub delete --entity-type dataset --query "_tmp"
```

#### Hard delete everything in Snowflake that was previously soft deleted

```shell
datahub delete --platform snowflake --only-soft-deleted --hard
```

## Deletes using the SDK and APIs

The Python SDK's [DataHubGraph](../../python-sdk/clients.md) client supports deletes via the following methods:

- `soft_delete_entity`
- `hard_delete_entity`
- `hard_delete_timeseries_aspect`

Deletes via the REST API are also possible, although we recommend using the SDK instead.

```shell
# hard delete an entity by urn
curl "http://localhost:8080/entities?action=delete" -X POST --data '{"urn": "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)"}'
```

## Rollback Ingestion Run

The second way to delete metadata is to identify entities (and the aspects affected) by using an ingestion `run-id`. Whenever you run `datahub ingest -c ...`, all the metadata ingested with that run will have the same run id.

To view the ids of the most recent set of ingestion batches, execute

```shell
datahub ingest list-runs
```

That will print out a table of all the runs. To see run statuses or to filter runs by URN/source run

```shell
datahub ingest list-source-runs
```

Once you have an idea of which run you want to roll back, run

```shell
datahub ingest show --run-id <run-id>
```

to see more info of the run.

Alternately, you can execute a dry-run rollback to achieve the same outcome.

```shell
datahub ingest rollback --dry-run --run-id <run-id>
```

Finally, once you are sure you want to delete this data forever, run

```shell
datahub ingest rollback --run-id <run-id>
```

to rollback all aspects added with this run and all entities created by this run.
This deletes both the versioned and the timeseries aspects associated with these entities.

### Unsafe Entities and Rollback

In some cases, entities that were initially ingested by a run might have had further modifications to their metadata (e.g. adding terms, tags, or documentation) through the UI or other means. During a roll back of the ingestion that initially created these entities (technically, if the key aspect for these entities are being rolled back), the ingestion process will analyse the metadata graph for aspects that will be left "dangling" and will:

1. Leave these aspects untouched in the database, and soft delete the entity. A re-ingestion of these entities will result in this additional metadata becoming visible again in the UI, so you don't lose any of your work.
2. The datahub cli will save information about these unsafe entities as a CSV for operators to later review and decide on next steps (keep or remove).

The rollback command will report how many entities have such aspects and save as a CSV the urns of these entities under a rollback reports directory, which defaults to `rollback_reports` under the current directory where the cli is run, and can be configured further using the `--reports-dir` command line arg.

The operator can use `datahub get --urn <>` to inspect the aspects that were left behind and either keep them (do nothing) or delete the entity (and its aspects) completely using `datahub delete --urn <urn> --hard`. If the operator wishes to remove all the metadata associated with these unsafe entities, they can re-issue the rollback command with the `--nuke` flag.
