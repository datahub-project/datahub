# DataHub MetadataAuditEvent (MAE) Consumer Docker Image

[![datahub-mae-consumer docker](https://github.com/datahub-project/datahub/workflows/datahub-mae-consumer%20docker/badge.svg)](https://github.com/datahub-project/datahub/actions?query=workflow%3A%22datahub-mae-consumer+docker%22)

Refer to [DataHub MAE Consumer Job](../../metadata-jobs/mae-consumer-job) to have a quick understanding of the architecture and
responsibility of this service for the DataHub.

By-query `RequestOptions` (delete/update-by-query, etc.) use the **same** tuning as GMS: **`ELASTICSEARCH_BULK_BY_QUERY_SLOW_OPERATION_TIMEOUT_SECONDS`** → `elasticsearch.bulkProcessor.slowByQueryOperationTimeoutSeconds`. **MAE-specific** Elasticsearch settings are **`MAE_ELASTICSEARCH_SOCKET_TIMEOUT`** and **`MAE_ELASTICSEARCH_CONNECTION_REQUEST_TIMEOUT`** only (`maeConsumer.elasticsearch`), merged with global RestClient timeouts when **`MAE_CONSUMER_ENABLED=true`**. **`ELASTICSEARCH_BUILD_INDICES_SLOW_OPERATION_TIMEOUT_SECONDS`** is for system-update / build-indices jobs only, not the bulk processor. [`docker.env`](env/docker.env) sets longer RestClient values where appropriate.
