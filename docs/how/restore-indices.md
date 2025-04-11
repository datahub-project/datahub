# Restoring Search and Graph Indices from Local Database

If search infrastructure (Elasticsearch/Opensearch) or graph services (Elasticsearch/Opensearch/Neo4j) become inconsistent, 
you can restore them from the aspects stored in the local database.

When a new version of the aspect gets ingested, GMS initiates an MCL event for the aspect which is consumed to update
the search and graph indices. As such, we can fetch the latest version of each aspect in the local database and produce
MCL events corresponding to the aspects to restore the search and graph indices.

By default, restoring the indices from the local database will not remove any existing documents in
the search and graph indices that no longer exist in the local database, potentially leading to inconsistencies
between the search and graph indices and the local database.

## Configuration

The upgrade jobs take arguments as command line args to the job itself rather than environment variables for job specific 
configuration. The RestoreIndices job is specified through the `-u RestoreIndices` upgrade ID parameter and then additional 
parameters are specified like `-a batchSize=1000`.

The following configurations are available:

### Time-Based Filtering

* `lePitEpochMs`: Restore records created before this timestamp (in milliseconds)
* `gePitEpochMs`: Restore records created after this timestamp (in milliseconds)

### Pagination and Performance Options

* `urnBasedPagination`: Enable key-based pagination instead of offset-based pagination. Recommended for large datasets as it's typically more efficient.
* `startingOffset`: When using default pagination, start from this offset
* `lastUrn`: Resume from a specific URN when using URN-based pagination
* `lastAspect`: Used with lastUrn to resume from a specific aspect, preventing reprocessing
* `numThreads`: Number of concurrent threads for processing restoration, only used with default offset based paging
* `batchSize`: Configures the size of each batch as the job pages through rows
* `batchDelayMs`: Adds a delay in between each batch to avoid overloading backend systems

### Content Filtering

* `aspectNames`: Comma-separated list of aspects to restore (e.g., "ownership,status")
* `urnLike`: SQL LIKE pattern to filter URNs (e.g., "urn:li:dataset%")


### Default Aspects

* `createDefaultAspects`: Create default aspects in both SQL and ES if missing.

During the restore indices process, it will create default aspects in SQL. While this may be
desired in some situations, disabling this feature is required when using a read-only SQL replica.

### Nuclear option
* `clean`: This option wipes out the current indices by running deletes of all the documents to guarantee a consistent state with SQL. This is generally not recommended unless there is significant data corruption on the instance.

### Helm

These are available in the helm charts as configurations for Kubernetes deployments under the `datahubUpgrade.restoreIndices.args` path which will set them up as args to the pod command.

## Execution Methods

### Quickstart

If you're using the quickstart images, you can use the `datahub` cli to restore the indices.

```shell
datahub docker quickstart --restore-indices
```

:::info
Using the `datahub` CLI to restore the indices when using the quickstart images will also clear the search and graph indices before restoring.
:::

See [this section](../quickstart.md#restore-datahub) for more information. 

### Docker-compose

If you are on a custom docker-compose deployment, run the following command (you need to checkout [the source repository](https://github.com/datahub-project/datahub)) from the root of the repo to send MAE for each aspect in the local database.

```shell
./docker/datahub-upgrade/datahub-upgrade.sh -u RestoreIndices
```

:::info
By default this command will not clear the search and graph indices before restoring, thous potentially leading to inconsistencies between the local database and the indices, in case aspects were previously deleted in the local database but were not removed from the correponding index.
:::

If you need to clear the search and graph indices before restoring, add `-a clean` to the end of the command. Please take note that the search and graph services might not be fully functional during reindexing when the indices are cleared.

```shell
./docker/datahub-upgrade/datahub-upgrade.sh -u RestoreIndices -a clean
```

Refer to this [doc](../../docker/datahub-upgrade/README.md#environment-variables) on how to set environment variables
for your environment.

### Kubernetes

Run `kubectl get cronjobs` to see if the restoration job template has been deployed. If you see results like below, you
are good to go.

```
NAME                                          SCHEDULE    SUSPEND   ACTIVE   LAST SCHEDULE   AGE
datahub-datahub-cleanup-job-template          * * * * *   True      0        <none>          2d3h
datahub-datahub-restore-indices-job-template  * * * * *   True      0        <none>          2d3h
```

If not, deploy latest helm charts to use this functionality.

Once restore indices job template has been deployed, run the following command to start a job that restores indices.

```shell
kubectl create job --from=cronjob/datahub-datahub-restore-indices-job-template datahub-restore-indices-adhoc
```

Once the job completes, your indices will have been restored.

:::info
By default the restore indices job template will not clear the search and graph indices before restoring, thous potentially leading to inconsistencies between the local database and the indices, in case aspects were previously deleted in the local database but were not removed from the correponding index.
:::

If you need to clear the search and graph indices before restoring, modify the `values.yaml` for your deployment and overwrite the default arguments of the restore indices job template to include the `-a clean` argument. Please take note that the search and graph services might not be fully functional during reindexing when the indices are cleared.

```yaml
datahubUpgrade:
  restoreIndices:
    image:
      args:
        - "-u"
        - "RestoreIndices"
        - "-a"
        - "batchSize=1000" # default value of datahubUpgrade.batchSize
        - "-a"
        - "batchDelayMs=100" # default value of datahubUpgrade.batchDelayMs
        - "-a"
        - "clean"
```

### Through APIs

See also the [Best Practices](#best-practices) section below, however note that the APIs are able to handle a few thousand
aspects. In this mode one of the GMS instances will perform the required actions, however it is subject to timeout. Use one of the
approaches above for longer running restoreIndices.

#### OpenAPI

There are two primary APIs, one which exposes the common parameters for restoreIndices and another one designed
to accept a list of URNs where all aspects are to be restored.

Full configuration:

<p align="center">
    <img width="80%" src="https://github.com/datahub-project/static-assets/blob/main/imgs/how/restore-indices/openapi-restore-indices.png?raw=true"/>
</p>

All Aspects:

<p align="center">
    <img width="80%" src="https://github.com/datahub-project/static-assets/blob/main/imgs/how/restore-indices/openapi-restore-indices-urns.png?raw=true"/>
</p>

#### Rest.li

For Rest.li, see [Restore Indices API](../api/restli/restore-indices.md).

## Best Practices

In general, this process is not required to run unless there has been a disruption of storage services or infrastructure, 
such as Elasticsearch/Opensearch cluster failures, data corruption events, or significant version upgrade inconsistencies 
that have caused the search and graph indices to become out of sync with the local database.

### K8 Job vs. API

#### When to Use Kubernetes Jobs
For operations affecting 2,000 or more aspects, it's strongly recommended to use the Kubernetes job approach. This job is 
designed for long-running processes and provide several advantages:

* Won't time out like API calls
* Can be monitored through Kubernetes logging
* Won't consume resources from your primary GMS instances
* Can be scheduled during off-peak hours to minimize system impact

#### When to Use APIs
The RestoreIndices APIs (available through both Rest.li and OpenAPI) is best suited for:

* Targeted restores affecting fewer than 2,000 aspects
* Emergencies where you need to quickly restore critical metadata
* Testing or validating the restore process before running a full-scale job
* Scenarios where you don't have direct access to the Kubernetes cluster

Remember that API-based restoration runs within one of your GMS instances and is subject to timeouts, which could lead to 
incomplete restorations for larger installations.

### Targeted Restoration Strategies
Being selective about what you restore is crucial for efficiency. Combining these filtering strategies can dramatically 
reduce the restoration scope, saving resources and time.

#### Entity Type Filtering
Entity Type Filtering: Use the `urnLike` parameter to target specific entity types:

* For datasets: `urnLike=urn:li:dataset:%`
* For users: `urnLike=urn:li:corpuser:%`
* For dashboards: `urnLike=urn:li:dashboard:%`

#### Single Entity
Single Entity Restoration: When only one entity is affected, provide the specific URN to minimize processing overhead.
Aspect-Based Filtering: Use aspectNames to target only the specific aspects that need restoration:

* For ownership inconsistencies: `aspectNames=ownership`
* For tag issues: `aspectNames=globalTags`

#### Time-Based
Time-Based Recovery: If you know when the inconsistency began, use time-based filtering:

* gePitEpochMs={timestamp} to process only records created after the incident
* lePitEpochMs={timestamp} to limit processing to records before a certain time

### Parallel Processing Strategies

To optimize restoration speed while managing system load:

#### Multiple Parallel Jobs
Run several restoreIndices processes simultaneously by:

* Work on non-overlapping sets of aspects or entities
  * Dividing work by entity type (one job for datasets, another for users, etc.)
  * Splitting aspects among different jobs (one for ownership aspects, another for lineage, etc.)
  * Partitioning large entity types by prefix or time range
* Have staggered start times to prevent initial resource contention
* Monitor system metrics closely during concurrent restoration to ensure you're not overloading your infrastructure.

:::caution
Avoid Conflicts: Ensure that concurrent jobs:

Never specify the --clean argument in concurrent jobs
:::

### Temporary Workload Reduction

* Pause scheduled ingestion jobs during restoration
* Temporarily disable or reduce frequency of the datahub-gc job to prevent conflicting deletes
* Consider pausing automated workflows or integrations that generate metadata events

### Infrastructure Tuning
Implementing these expanded best practices should help ensure a smoother, more efficient restoration process while 
minimizing impact on your DataHub environment.

This operation can be I/O intensive from the read-side from SQL and on the Elasticsearch write side. If you're able to leverage
provisioned I/O. or throughput, you might want to monitor your infrastructure for a possible.

#### Elasticsearch/Opensearch Optimization
To improve write performance during restoration:

##### Refresh Interval Adjustment:

Temporarily increase the refresh_interval setting from the default (typically 1s) to something like 30s or 60s.
Run the system update job with the following environment variable `ELASTICSEARCH_INDEX_BUILDER_REFRESH_INTERVAL_SECONDS=60`

:::caution
Remember to reset this after restoration completes!
:::caution

##### Bulk Processing Improvements:

* Adjust the Elasticsearch batching parameters to optimize bulk request size (try values between 2000-5000)
  * Run your GMS or `mae-consumer` with environment variables
    * `ES_BULK_REQUESTS_LIMIT=3000`
    * `ES_BULK_FLUSH_PERIOD=60`
* Configure `batchDelayMs` on restoreIndices to add breathing room between batches if your cluster is struggling

##### Shard Management:

* Ensure your indices have an appropriate number of shards for your cluster size.
* Consider temporarily adding nodes to your search cluster during massive restorations.

#### SQL/Primary Storage

Consider using a read replica as the source of the job's data. If you configure a read-only replica
you must also provide the parameter `createDefaultAspects=false`.

#### Kafka & Consumers

##### Partition Strategy:

* Verify that the Kafka Metadata Change Log (MCL) topic have enough partitions to allow for parallel processing.
* Recommended: At least 10-20 partitions for the MCL topic in production environments.

##### Consumer Scaling:

* Temporarily increase the number of `mae-consumer` pods to process the higher event volume.
* Scale GMS instances if they're handling consumer duties.

##### Monitoring:

* Watch consumer lag metrics closely during restoration.
* Be prepared to adjust scaling or batch parameters if consumers fall behind.