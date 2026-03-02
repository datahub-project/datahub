# Migrate Elasticsearch to OpenSearch

:::caution Data Loss Warning

Switching from Elasticsearch to OpenSearch will result in the **loss of all timeseries data**. This includes:

- Usage statistics and query counts
- Dataset profile history
- Operation metrics over time
- Any other time-series analytics data

If you need to preserve this historical data, consider exporting it before migration or plan for a data retention gap. See [Preserving Timeseries Data](#preserving-timeseries-data) section below to back up and restore of this data.
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

## Enable OpenSearch and disable Elasticsearch

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

## Configure Datahub to use OpenSearch

[Default values](https://github.com/acryldata/datahub-helm/blob/master/charts/datahub/values.yaml)

### Configure elasticsearchSetupJob to use OpenSearch

```diff
# If you want to use OpenSearch instead of ElasticSearch add the USE_AWS_ELASTICSEARCH environment variable below
- # extraEnvs:
- #   - name: USE_AWS_ELASTICSEARCH
- #     value: "true"
+ extraEnvs:
+   - name: USE_AWS_ELASTICSEARCH
+     value: "true"

```

### Set your OpenSearch cluster hostname

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

## Re-index

Run a [Restore-Indices](https://docs.datahub.com/docs/how/restore-indices) job to complete the migration.

```
kubectl create job --from=cronjob/datahub-datahub-restore-indices-job-template datahub-restore-indices-adhoc
```

## Preserving Timeseries Data

We recommend exporting the ElasticSearch data using a tool such as [elasticsearch-dump](https://github.com/elasticsearch-dump/elasticsearch-dump) before migrating to OpenSearch.

Setting up an OpenSearch Cluster before tearing down the ElasticSearch Cluster is the easiest way.

### Example of how to directly export from ElasticSearch and import into OpenSearch.

```
elasticdump \
  --input=http://elasticsaerch-domain:9200/ \
  --output=http://opensearch-domain:9200/ \
  --type=data
```

The export and import can also be done independently to a file.

### Export ElasticSearch to JSON file

```
elasticdump \
--input=http://elasticsearch-domain:9200/ \
--output=/data/datahub_data.json \
--type=data
```

### Import JSON to OpenSearch

```
elasticdump \
--intput=/data/datahub_data.json \
--output=http://opensearch-domain:9200/ \
--type=data
```

See [elasticsearch-dump](https://github.com/elasticsearch-dump/elasticsearch-dump) for a full list of supported options.
