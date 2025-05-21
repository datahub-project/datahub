# Search and Graph Reindexing

If your search infrastructure (Elasticsearch/OpenSearch) or graph services (Elasticsearch/OpenSearch/Neo4j) become inconsistent or out-of-sync with your primary metadata store, you can **rebuild them from the source of truth**: the `metadata_aspect_v2` table in your relational database (MySQL/Postgres).

This process works by fetching the latest version of each aspect from the database and replaying them as Metadata Change Log (MCL) events. These events will regenerate your search and graph indexes, effectively restoring a consistent view.

> ⚠️ **Note**: By default, this process does **not remove** stale documents from the index that no longer exist in the database. To ensure full consistency, we recommend reindexing into a clean instance, or using the `-a clean` option to wipe existing index contents before replay.

---

## How it Works

Reindexing is powered by the `datahub-upgrade` utility (packaged as the `datahub-upgrade` container in Docker/Kubernetes). It supports a special upgrade task called `RestoreIndices`, which replays aspects from the database back into search and graph stores.

You can run this utility in three main environments:

- Quickstart (via CLI)
- Docker Compose (via shell script)
- Kubernetes (via Helm + CronJob)

---

## Reindexing Configuration Options

When running the `RestoreIndices` job, you can pass additional arguments to customize the behavior:

### 🔄 Pagination & Performance

| Argument             | Description                                                                 |
| -------------------- | --------------------------------------------------------------------------- |
| `urnBasedPagination` | Use URN-based pagination instead of offset. Recommended for large datasets. |
| `startingOffset`     | Starting offset for offset-based pagination.                                |
| `lastUrn`            | Resume from this URN (used with URN pagination).                            |
| `lastAspect`         | Resume from this aspect name (used with `lastUrn`).                         |
| `numThreads`         | Number of concurrent threads for reindexing.                                |
| `batchSize`          | Number of records per batch.                                                |
| `batchDelayMs`       | Delay in milliseconds between each batch (throttling).                      |

### 📅 Time Filtering

| Argument       | Description                                                     |
| -------------- | --------------------------------------------------------------- |
| `gePitEpochMs` | Only restore aspects created **after** this timestamp (in ms).  |
| `lePitEpochMs` | Only restore aspects created **before** this timestamp (in ms). |

### 🔍 Content Filtering

| Argument      | Description                                                            |
| ------------- | ---------------------------------------------------------------------- |
| `aspectNames` | Comma-separated list of aspects to restore (e.g., `ownership,status`). |
| `urnLike`     | SQL LIKE pattern to filter URNs (e.g., `urn:li:dataset%`).             |

### 🧱 Other Options

| Argument               | Description                                                                                                 |
| ---------------------- | ----------------------------------------------------------------------------------------------------------- |
| `createDefaultAspects` | Whether to create default aspects in SQL & index if missing. **Disable** this if using a read-only replica. |
| `clean`                | **Deletes existing index documents before restoring.** Use with caution.                                    |

---

## Running the Restore Job

### 🧪 Quickstart CLI

If you're using DataHub's quickstart image, you can restore indices using a single CLI command:

```bash
datahub docker quickstart --restore-indices
```

:::info
This command automatically clears the search and graph indices before restoring them.
:::

More details in the [Quickstart Docs](../quickstart.md#restore-datahub).

---

### 🐳 Docker Compose

If you're using Docker Compose and have cloned the [DataHub source repo](https://github.com/datahub-project/datahub), run:

```bash
./docker/datahub-upgrade/datahub-upgrade.sh -u RestoreIndices
```

To clear existing index contents before restore (recommended if you suspect inconsistencies), add `-a clean`:

```bash
./docker/datahub-upgrade/datahub-upgrade.sh -u RestoreIndices -a clean
```

:::info
Without the `-a clean` flag, old documents may remain in your search/graph index, even if they no longer exist in your SQL database.
:::

Refer to the [Upgrade Script Docs](../../docker/datahub-upgrade/README.md#environment-variables) for more info on environment configuration.

---

### ☸️ Kubernetes (Helm)

1. **Check if the Job Template Exists**

Run:

```bash
kubectl get cronjobs
```

You should see a result like:

```bash
datahub-datahub-restore-indices-job-template
```

If not, make sure you're using the latest Helm chart version that includes the restore job.

2. **Trigger the Restore Job**

Run:

```bash
kubectl create job --from=cronjob/datahub-datahub-restore-indices-job-template datahub-restore-indices-adhoc
```

This will create and run a one-off job to restore indices from your SQL database.

3. **To Enable Clean Reindexing**

Edit your `values.yaml` to include the `-a clean` argument:

```yaml
datahubUpgrade:
  restoreIndices:
    image:
      args:
        - "-u"
        - "RestoreIndices"
        - "-a"
        - "batchSize=1000"
        - "-a"
        - "batchDelayMs=100"
        - "-a"
        - "clean"
```

:::info
The default job does **not** delete existing documents before restoring. Add `-a clean` to ensure full sync.
:::

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

Some pointers to keep in mind when running this process:

- Always test reindexing in a **staging environment** first.
- Consider taking a backup of your Elasticsearch/OpenSearch index before a `clean` restore.
- For very large deployments, use `urnBasedPagination` and limit `batchSize` to avoid overloading your backend.
- Monitor Elasticsearch/OpenSearch logs during the restore for throttling or memory issues.

### K8 Job vs. API

#### When to Use Kubernetes Jobs

For operations affecting 2,000 or more aspects, it's strongly recommended to use the Kubernetes job approach. This job is
designed for long-running processes and provide several advantages:

- Won't time out like API calls
- Can be monitored through Kubernetes logging
- Won't consume resources from your primary GMS instances
- Can be scheduled during off-peak hours to minimize system impact

#### When to Use APIs

The RestoreIndices APIs (available through both Rest.li and OpenAPI) is best suited for:

- Targeted restores affecting fewer than 2,000 aspects
- Emergencies where you need to quickly restore critical metadata
- Testing or validating the restore process before running a full-scale job
- Scenarios where you don't have direct access to the Kubernetes cluster

Remember that API-based restoration runs within one of your GMS instances and is subject to timeouts, which could lead to
incomplete restorations for larger installations.

### Targeted Restoration Strategies

Being selective about what you restore is crucial for efficiency. Combining these filtering strategies can dramatically
reduce the restoration scope, saving resources and time.

#### Entity Type Filtering

Entity Type Filtering: Use the `urnLike` parameter to target specific entity types:

- For datasets: `urnLike=urn:li:dataset:%`
- For users: `urnLike=urn:li:corpuser:%`
- For dashboards: `urnLike=urn:li:dashboard:%`

#### Single Entity

Single Entity Restoration: When only one entity is affected, provide the specific URN to minimize processing overhead.
Aspect-Based Filtering: Use aspectNames to target only the specific aspects that need restoration:

- For ownership inconsistencies: `aspectNames=ownership`
- For tag issues: `aspectNames=globalTags`

#### Time-Based

Time-Based Recovery: If you know when the inconsistency began, use time-based filtering:

- gePitEpochMs={timestamp} to process only records created after the incident
- lePitEpochMs={timestamp} to limit processing to records before a certain time

### Parallel Processing Strategies

To optimize restoration speed while managing system load:

#### Multiple Parallel Jobs

Run several restoreIndices processes simultaneously by:

- Work on non-overlapping sets of aspects or entities
  - Dividing work by entity type (one job for datasets, another for users, etc.)
  - Splitting aspects among different jobs (one for ownership aspects, another for lineage, etc.)
  - Partitioning large entity types by prefix or time range
- Have staggered start times to prevent initial resource contention
- Monitor system metrics closely during concurrent restoration to ensure you're not overloading your infrastructure.

:::caution
Avoid Conflicts: Ensure that concurrent jobs:

Never specify the --clean argument in concurrent jobs
:::

### Temporary Workload Reduction

- Pause scheduled ingestion jobs during restoration
- Temporarily disable or reduce frequency of the datahub-gc job to prevent conflicting deletes
- Consider pausing automated workflows or integrations that generate metadata events

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

- Adjust the Elasticsearch batching parameters to optimize bulk request size (try values between 2000-5000)
  - Run your GMS or `mae-consumer` with environment variables
    - `ES_BULK_REQUESTS_LIMIT=3000`
    - `ES_BULK_FLUSH_PERIOD=60`
- Configure `batchDelayMs` on restoreIndices to add breathing room between batches if your cluster is struggling

##### Shard Management:

- Ensure your indices have an appropriate number of shards for your cluster size.
- Consider temporarily adding nodes to your search cluster during massive restorations.

#### SQL/Primary Storage

Consider using a read replica as the source of the job's data. If you configure a read-only replica
you must also provide the parameter `createDefaultAspects=false`.

#### Kafka & Consumers

##### Partition Strategy:

- Verify that the Kafka Metadata Change Log (MCL) topic have enough partitions to allow for parallel processing.
- Recommended: At least 10-20 partitions for the MCL topic in production environments.

##### Consumer Scaling:

- Temporarily increase the number of `mae-consumer` pods to process the higher event volume.
- Scale GMS instances if they're handling consumer duties.

##### Monitoring:

- Watch consumer lag metrics closely during restoration.
- Be prepared to adjust scaling or batch parameters if consumers fall behind.
