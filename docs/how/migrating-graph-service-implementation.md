# Migrate Graph Service Implementation to Elasticsearch

We currently support either Elasticsearch or Neo4j as backend implementations for the graph service. We recommend
Elasticsearch for those looking for a lighter deployment or do not want to manage a Neo4j database.
If you started using Neo4j as your graph service backend, here is how you can migrate to Elasticsearch.

## Docker-compose

If you are running your instance through docker locally, you will want to spin up your Datahub instance with
elasticsearch as the backend manually:

```aidl
docker-compose \
  -f quickstart/docker-compose-without-neo4j.quickstart.yml \
  pull && \
docker-compose -p datahub \
  -f quickstart/docker-compose-without-neo4j.quickstart.yml \
  up
```

Next, run the following command from root to rebuild your graph index.

```
./docker/datahub-upgrade/datahub-upgrade.sh -u RestoreIndices
```

After this command completes, you should be migrated. Open up the DataHub UI and verify your relationships are
visible.

After you confirm the migration is successful, you must remove your neo4j volume by running

```aidl
docker volume rm datahub_neo4jdata
```

This prevents your DataHub instance from coming up in neo4j mode in the future.

## Helm

First, follow the [deployment helm guide](../../datahub-kubernetes/README.md#components) to set up your helm deployment
to use elasticsearch as your backend. Then, follow the [restore-indices helm guide](./restore-indices.md) to re-build
your indexes.

Once the job completes, your data will be migrated. 
