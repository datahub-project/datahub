# DataHub Upgrade Docker Image

This container is used to automatically apply upgrades from one version of DataHub to another.

## Supported Upgrades

As of today, there are 2 supported upgrades:

1. **NoCodeDataMigration**: Performs a series of pre-flight qualification checks and then migrates metadata_aspect table data
to metadata_aspect_v2 table. Arguments:
    - *batchSize* (Optional): The number of rows to migrate at a time. Defaults to 1000.
    - *batchDelayMs* (Optional): The number of milliseconds of delay between migrated batches. Used for rate limiting. Defaults to 250. 
    - *dbType* (optional): The target DB type. Valid values are `MYSQL`, `MARIA`, `POSTGRES`. Defaults to `MYSQL`. 
   
2. **NoCodeDataMigrationCleanup**: Cleanses graph index, search index, and key-value store of legacy DataHub data (metadata_aspect table) once
the No Code Data Migration has completed successfully. No arguments. 

3. **RestoreIndices**: Restores indices by fetching the latest version of each aspect and producing MAE. Arguments:
    - *batchSize* (Optional): The number of rows to migrate at a time. Defaults to 1000.
    - *batchDelayMs* (Optional): The number of milliseconds of delay between migrated batches. Used for rate limiting. Defaults to 250. 
    - *numThreads* (Optional): The number of threads to use, defaults to 1. Note that this is not used if `urnBasedPagination` is true.
    - *aspectName* (Optional): The aspect name for producing events.
    - *urn* (Optional): The urn for producing events.
    - *urnLike* (Optional): The urn pattern for producing events, using `%` as a wild card
    - *urnBasedPagination* (Optional): Paginate the SQL results using the urn + aspect string instead of `OFFSET`. Defaults to false,
        though should improve performance for large amounts of data.
    
4. **RestoreBackup**: Restores the storage stack from a backup of the local database

## Environment Variables

To run the `datahub-upgrade` container, some environment variables must be provided in order to tell the upgrade CLI
where the running DataHub containers reside. 

Below details the required configurations. By default, these configs are provided for local docker-compose deployments of 
DataHub within `docker/datahub-upgrade/env/docker.env`. They assume that there is a Docker network called datahub_network
where the DataHub containers can be found. 

These are also the variables used when the provided `datahub-upgrade.sh` script is executed. To run the upgrade CLI for non-local deployments,
follow these steps: 

1. Define new ".env" variable to hold your environment variables.

The following variables may be provided: 

```aidl
# Required Environment Variables
EBEAN_DATASOURCE_USERNAME=datahub
EBEAN_DATASOURCE_PASSWORD=datahub
EBEAN_DATASOURCE_HOST=<your-ebean-host>:3306
EBEAN_DATASOURCE_URL=jdbc:mysql://<your-ebean-host>:3306/datahub?verifyServerCertificate=false&useSSL=true&useUnicode=yes&characterEncoding=UTF-8
EBEAN_DATASOURCE_DRIVER=com.mysql.jdbc.Driver

KAFKA_BOOTSTRAP_SERVER=<your-kafka-host>:29092
KAFKA_SCHEMAREGISTRY_URL=http://<your-kafka-host>:8081

ELASTICSEARCH_HOST=<your-elastic-host>
ELASTICSEARCH_PORT=9200

NEO4J_HOST=http://<your-neo-host>:7474
NEO4J_URI=bolt://<your-neo-host>
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=datahub

DATAHUB_GMS_HOST=<your-gms-host>>
DATAHUB_GMS_PORT=8080

# Datahub protocol (default http)
# DATAHUB_GMS_PROTOCOL=http

DATAHUB_MAE_CONSUMER_HOST=<your-mae-consumer-host>
DATAHUB_MAE_CONSUMER_PORT=9091

# Optional Arguments

# Uncomment and set these to support SSL connection to Elasticsearch
# ELASTICSEARCH_USE_SSL=
# ELASTICSEARCH_SSL_PROTOCOL=
# ELASTICSEARCH_SSL_SECURE_RANDOM_IMPL=
# ELASTICSEARCH_SSL_TRUSTSTORE_FILE=
# ELASTICSEARCH_SSL_TRUSTSTORE_TYPE=
# ELASTICSEARCH_SSL_TRUSTSTORE_PASSWORD=
# ELASTICSEARCH_SSL_KEYSTORE_FILE=
# ELASTICSEARCH_SSL_KEYSTORE_TYPE=
# ELASTICSEARCH_SSL_KEYSTORE_PASSWORD=
```
2. Pull (or build) & execute the `datahub-upgrade` container:

```aidl
docker pull acryldata/datahub-upgrade:head && docker run --env-file *path-to-custom-env-file.env* acryldata/datahub-upgrade:head -u NoCodeDataMigration
```

## Arguments

The primary argument required by the datahub-upgrade container is the name of the upgrade to perform. This argument
can be specified using the `-u` flag when running the `datahub-upgrade` container. 

For example, to run the migration named "NoCodeDataMigration", you would do execute the following:

```aidl
./datahub-upgrade.sh -u NoCodeDataMigration
```

OR

```aidl
docker pull acryldata/datahub-upgrade:head && docker run --env-file env/docker.env acryldata/datahub-upgrade:head -u NoCodeDataMigration
```

In addition to the required `-u` argument, each upgrade may require specific arguments. You can provide arguments to individual
upgrades using multiple `-a` arguments. 

For example, the NoCodeDataMigration upgrade provides 2 optional arguments detailed above: *batchSize* and *batchDelayMs*. 
To specify these, you can use a combination of `-a` arguments and of the form *argumentName=argumentValue* as follows:

```aidl
./datahub-upgrade.sh -u NoCodeDataMigration -a batchSize=500 -a batchDelayMs=1000 // Small batches with 1 second delay. 
```

OR 

```aidl
docker pull acryldata/datahub-upgrade:head && docker run --env-file env/docker.env acryldata/datahub-upgrade:head -u NoCodeDataMigration -a batchSize=500 -a batchDelayMs=1000
```