# Migrate Elasticsearch to OpenSearch

There are two ways to move a DataHub deployment from Elasticsearch to OpenSearch:

- **Run both clusters and let DataHub move the data** (recommended). The graph, timeseries,
  system-metadata and usage-event indices are copied to OpenSearch while DataHub keeps serving, the
  entity indices are rebuilt from the database, and nothing is lost.
- **Switch the cluster and re-index** (fallback). Only the entity indices come back; the timeseries
  and usage data are lost unless you export and import them by hand. Use it when both clusters
  cannot be reachable from DataHub at the same time.

## Why Migrate to OpenSearch?

OpenSearch offers several advantages over Elasticsearch for DataHub deployments:

### Semantic Search Support

**OpenSearch is required for DataHub's Semantic Search feature.** Semantic search uses vector embeddings and k-NN (k-nearest neighbors) algorithms to find semantically similar assets based on meaning rather than just keyword matching. This enables:

- Natural language queries that understand intent
- Discovery of related assets even when they use different terminology
- Improved search relevance based on conceptual similarity
- Better metadata exploration across diverse data landscapes

Elasticsearch does not support the vector search capabilities needed for this functionality.

### Additional Benefits

- **Open source licensing**: OpenSearch maintains a fully open source license (Apache 2.0) without licensing concerns
- **Active development**: Regular updates and new features from the OpenSearch community
- **AWS integration**: Native support and managed services through Amazon OpenSearch Service
- **Cost efficiency**: Often more cost-effective for cloud deployments
- **Feature parity**: Supports all core DataHub search functionality while enabling advanced capabilities

## Choose a path

| Path                                           | What it keeps                                                                                                                                                  | What it needs                                                                                                                      |
| ---------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| **Run both clusters, DataHub moves the data**  | Everything. Graph, timeseries, system metadata and usage events are copied with the live writes mirrored to both clusters; entity indices are rebuilt.         | Both clusters reachable from every DataHub component for the length of the migration; the system-update job sized for a bulk copy. |
| **Switch the cluster and re-index** (fallback) | The entity indices, rebuilt from the database. Timeseries and usage data are lost unless exported with `elasticdump` first; graph and system metadata rebuild. | Only the new cluster. Expect a gap in search results until the rebuild finishes.                                                   |

## Recommended: run both clusters and let DataHub move the data

DataHub can connect to two search clusters at once and migrate the index families that only exist
in the index (timeseries, usage events) and the ones that are slow to rebuild (graph, system
metadata) from one to the other, without downtime. Every variable named below is described under
[Optional second search cluster](../deploy/environment-vars.md#optional-second-search-cluster) and
[Moving a non-entity index family to another cluster](../deploy/environment-vars.md#moving-a-non-entity-index-family-to-another-cluster).
Set each of them on every DataHub component (GMS, the MAE and MCE consumers and the system-update
job), for example through the chart's `extraEnvs` of each.

### 1. Stand up OpenSearch next to Elasticsearch

Keep Elasticsearch running and add an OpenSearch cluster. In the
[prerequisites chart](https://github.com/acryldata/datahub-helm/blob/master/charts/prerequisites/values.yaml):

```diff
elasticsearch:
  enabled: true

opensearch:
- enabled: false
+ enabled: true
```

```
helm upgrade prerequisites datahub/datahub-prerequisites --values <<path-to-values-file>>
```

Or bring your own OpenSearch domain. See the
[OpenSearch helm chart](https://docs.opensearch.org/latest/install-and-configure/install-opensearch/helm/)
for the chart's values.

### 2. Declare OpenSearch as DataHub's second cluster

Elasticsearch stays the `primary` cluster. Point the `secondary` cluster at OpenSearch:

```
ELASTICSEARCH_CLUSTERS_SECONDARY_URI=http://opensearch-cluster-master:9200
ELASTICSEARCH_CLUSTERS_SECONDARY_USERNAME=<user, if any>
ELASTICSEARCH_CLUSTERS_SECONDARY_PASSWORD=<password, if any>
```

DataHub detects the engine of each cluster on its own. Leave
`ELASTICSEARCH_CLUSTERS_SECONDARY_INDEX_PREFIX` unset: the copied indices then carry the
deployment's index prefix and keep their names when OpenSearch becomes the primary cluster in step 5.
Deploy this step on its own, before step 3. Nothing is served from OpenSearch yet, but from now on
every GMS and MAE consumer reports itself as a live writer, which is how the next deploy's copy knows
when the pods of this one have gone.

### 3. Migrate the four index families

For each of `GRAPH`, `TIMESERIES`, `SYSTEM_METADATA` and `USAGE`:

```
ELASTICSEARCH_CLUSTER_MIGRATION_<FAMILY>_TARGET_CLUSTER=secondary
```

Deploy. The blocking system-update creates the family's indices on OpenSearch from DataHub's own
mappings (usage events get their OpenSearch layout, a rollover alias, in place of the Elasticsearch
data stream); every component then mirrors each write to both clusters; and the non-blocking
system-update copies the history, replays the deletes that happened meanwhile, adopts the copied usage
generations and verifies. The copy passes every document through the system-update pod, so give that
job the memory and network of a bulk load, and keep `ELASTICSEARCH_THREAD_COUNT` at or above the copy
parallelism (default `4`).

The family is ready to flip when its row reads `VERIFIED`:

```
datahub get --urn "urn:li:dataHubUpgrade:ClusterMigration_<family>_primary_to_secondary" --aspect dataHubUpgradeResult
```

The non-blocking job logs the same as
`ClusterMigrationCopy_<family>: migration ... verified; flip componentCluster.<family> to 'secondary' on the next deploy`.
If it instead reports that it is waiting for the live writers or that its time budget ran out, run it
again (`-u SystemUpdateNonBlocking -n ClusterMigrationCopy`); it resumes where it stopped.

### 4. Flip the families to OpenSearch

For each verified family:

```
ELASTICSEARCH_COMPONENT_CLUSTER_<FAMILY>=secondary
ELASTICSEARCH_CLUSTER_MIGRATION_<FAMILY>_TARGET_CLUSTER=primary
ELASTICSEARCH_CLUSTER_MIGRATION_<FAMILY>_COPY_ENABLED=false
```

Deploy. The family is served from OpenSearch and every write is still mirrored back to Elasticsearch,
so a rollback is the three values the other way round, with no data gap. Let it bake for as long as
you want the rollback to stay a flag flip.

### 5. Make OpenSearch the primary cluster and rebuild the entity indices

In one deploy, point DataHub's primary connection at OpenSearch and return the families to it under
that name:

- The chart values of the [fallback's configuration section](#configure-datahub-to-use-opensearch)
  below: `elasticsearch.host`, `implementation: "opensearch"`, and `USE_AWS_ELASTICSEARCH` on the setup
  job.
- `ELASTICSEARCH_COMPONENT_CLUSTER_<FAMILY>=primary` for the four families.
- Remove every `ELASTICSEARCH_CLUSTER_MIGRATION_<FAMILY>_*` and `ELASTICSEARCH_CLUSTERS_SECONDARY_*`
  variable.

The four families already live on OpenSearch under their names, so system-update finds them in place.
The entity indices do not exist there yet: run a
[Restore-Indices](https://docs.datahub.com/docs/how/restore-indices) job to rebuild them from the
database. Search results are incomplete until it finishes.

```
kubectl create job --from=cronjob/datahub-datahub-restore-indices-job-template datahub-restore-indices-adhoc
```

### 6. Retire Elasticsearch

Once the restore has finished and search, lineage, profiles and usage analytics look right, disable
Elasticsearch in the prerequisites chart (`elasticsearch.enabled: false`). DataHub never deletes the
old cluster's indices itself; that stays a deliberate step of yours.

## Fallback: switch the cluster and re-index

:::caution Data Loss Warning

Switching DataHub from Elasticsearch to OpenSearch this way results in the **loss of all timeseries data**. This includes:

- Usage statistics and query counts
- Dataset profile history
- Operation metrics over time
- Any other time-series analytics data

Prefer the migration above, which keeps this data. If you must take this path and need the history, export it before switching and import it afterwards; see [Preserving Timeseries Data](#preserving-timeseries-data) below.
:::

### Enable OpenSearch and disable Elasticsearch

If you want to provide your own OpenSearch Instance, keep the setting `false` and skip to the next step.

[Default prerequisites values](https://github.com/acryldata/datahub-helm/blob/master/charts/prerequisites/values.yaml)

Disable Elasticsearch. Note, this can be done as the last step if you want to migrate data or have a backup.

```diff
elasticsearch:
  # set this to false, if you want to provide your own ES instance.
- enabled: true
+ enabled: false

```

Enable OpenSearch cluster. See the [OpenSearch helm chart](https://docs.opensearch.org/latest/install-and-configure/install-opensearch/helm/) for a full list of values and options.

```diff

opensearch:
- enabled: false
+ enabled: true
```

Run helm upgrade to apply changes

```
helm upgrade prerequisites datahub/datahub-prerequisites --values <<path-to-values-file>>
```

### Configure DataHub to use OpenSearch

[Default values](https://github.com/acryldata/datahub-helm/blob/master/charts/datahub/values.yaml)

#### Configure elasticsearchSetupJob to use OpenSearch

```diff
# If you want to use OpenSearch instead of ElasticSearch add the USE_AWS_ELASTICSEARCH environment variable below
- # extraEnvs:
- #   - name: USE_AWS_ELASTICSEARCH
- #     value: "true"
+ extraEnvs:
+   - name: USE_AWS_ELASTICSEARCH
+     value: "true"

```

#### Set your OpenSearch cluster hostname

If you are bringing your own OpenSearch Cluster, set the cluster hostname to your OpenSearch Domain.

```diff
elasticsearch:
-# host: "elasticsearch-master"
  # If you want to use OpenSearch instead of ElasticSearch use different hostname below
+ host: "opensearch-cluster-master"

...

# Elasticsearch/OpenSearch implementation configuration
- implementation: "elasticsearch"  # Sets
ELASTICSEARCH_IMPLEMENTATION - "elasticsearch" or "opensearch"
+ implementation: "opensearch"  # Sets
ELASTICSEARCH_IMPLEMENTATION - "elasticsearch" or "opensearch"
```

Run helm upgrade to apply changes

```
helm upgrade datahub datahub/datahub --values <<path-to-values-file>>

```

### Re-index

Run a [Restore-Indices](https://docs.datahub.com/docs/how/restore-indices) job to complete the migration.

```
kubectl create job --from=cronjob/datahub-datahub-restore-indices-job-template datahub-restore-indices-adhoc
```

### Preserving Timeseries Data

We recommend exporting the ElasticSearch data using a tool such as [elasticsearch-dump](https://github.com/elasticsearch-dump/elasticsearch-dump) before migrating to OpenSearch.

Setting up an OpenSearch Cluster before tearing down the ElasticSearch Cluster is the easiest way.

#### Example of how to directly export from ElasticSearch and import into OpenSearch.

```
elasticdump \
  --input=http://elasticsaerch-domain:9200/ \
  --output=http://opensearch-domain:9200/ \
  --type=data
```

The export and import can also be done independently to a file.

#### Export ElasticSearch to JSON file

```
elasticdump \
--input=http://elasticsearch-domain:9200/ \
--output=/data/datahub_data.json \
--type=data
```

#### Import JSON to OpenSearch

```
elasticdump \
--intput=/data/datahub_data.json \
--output=http://opensearch-domain:9200/ \
--type=data
```

See [elasticsearch-dump](https://github.com/elasticsearch-dump/elasticsearch-dump) for a full list of supported options.
