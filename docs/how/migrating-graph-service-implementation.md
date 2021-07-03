# Migrate Graph Service Implementation to Elasticsearch

We currently support either Elasticsearch or Neo4j as backend implementations for the graph service. We recommend
Elasticsearch for those looking for a lighter deployment or do not want to manage a Neo4j database.
If you started using Neo4j as your graph service backend, here is how you can migrate to Elasticsearch.

## Docker-compose

If you are running your instance through docker locally, you will want to spin up your Datahub instance with
elasticsearch as the backend. On a clean start, this happens by default. However, if you've written data to
Neo4j you need to explicitly ask DataHub to start in Elastic mode.

```aidl
datahub docker quickstart --graph-service-impl=elasticsearch
```

Next, run the following command from root to rebuild your graph index.

```
./docker/datahub-upgrade/datahub-upgrade.sh -u RestoreIndices
```

After this command completes, you should be migrated. Open up the DataHub UI and verify your relationships are
visible.

Once you confirm the migration is successful, you must remove your neo4j volume by running

```aidl
docker volume rm datahub_neo4jdata
```

This prevents your DataHub instance from coming up in neo4j mode in the future.

## Helm

First, adjust your helm variables to turn off neo4j and set your graph_service_impl to elasticsearch.

To turn off neo4j in your prerequisites file, set `neo4j-community`'s `enabled` property to `false`
in [datahub-kubernetes/prerequisites/values.yaml](https://github.com/linkedin/datahub/blob/69dc2682ec36d68d2ac2d88b86cbd9c84d766b0b/datahub-kubernetes/prerequisites/values.yaml#L54).

Then, set `graph_service_impl` to `elasticsearch` in
[datahub-kubernetes/datahub/values.yaml](https://github.com/linkedin/datahub/blob/eb60da975dce1f855883e628541cdbcef89717af/datahub-kubernetes/datahub/values.yaml#L63).


See the [deployment helm guide](../../datahub-kubernetes/README.md#components) for more details on how to
set up your helm deployment.

Finally, follow the [restore-indices helm guide](./restore-indices.md) to re-build
your graph index.

Once the job completes, your data will be migrated. 
