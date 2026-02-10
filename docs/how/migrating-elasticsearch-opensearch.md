# Migrate Elasticsearch to Opensearch

:::caution Data Loss Warning

Switching from Elasticsearch to OpenSearch will result in the **loss of all timeseries data**. This includes:

- Usage statistics and query counts
- Dataset profile history
- Operation metrics over time
- Any other time-series analytics data

If you need to preserve this historical data, consider exporting it before migration or plan for a data retention gap.
:::

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

## Enable Opensearch and disable Elasticsearch

If you want to provide your own Opensearch Instance, keep the setting `false` and skip to the next step.

[Default prerequisites values](https://github.com/acryldata/datahub-helm/blob/master/charts/prerequisites/values.yaml)

```diff
elasticsearch:
  # set this to false, if you want to provide your own ES instance.
- enabled: true
+ enabled: false

...

opensearch:
- enabled: false
+ enabled: true
```

Run helm upgrade to apply changes

```
helm upgrade prerequisites datahub/datahub-prerequisites --values <<path-to-values-file>>
```

## Configure Datahub to use Opensearch

[Default values](https://github.com/acryldata/datahub-helm/blob/master/charts/datahub/values.yaml)

### Configure elasticsearchSetupJob to use Opensearch

```diff
# If you want to use OpenSearch instead of ElasticSearch add the USE_AWS_ELASTICSEARCH environment variable below
- # extraEnvs:
- #   - name: USE_AWS_ELASTICSEARCH
- #     value: "true"
+ extraEnvs:
+   - name: USE_AWS_ELASTICSEARCH
+     value: "true"

```

### Set your Opensearch cluster hostname

If you are bringing your own Opensearch Cluster, set the cluster hostname to your Opensearch Domain.

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

## Re-index

Run a [Restore-Indices](https://docs.datahub.com/docs/how/restore-indices) job to complete the migration.

```
kubectl create job --from=cronjob/datahub-datahub-restore-indices-job-template datahub-restore-indices-adhoc
```
