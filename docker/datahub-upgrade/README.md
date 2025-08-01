# DataHub Upgrade Docker Image

This container is used to automatically apply upgrades from one version of DataHub to another. It contains
a set of executable jobs, which can be used to perform various types of system maintenance on-demand.

It also supports regularly upgrade tasks that need to occur between versions of DataHub, and should be run
each time DataHub is deployed. More on this below. (TODO)

## Supported Upgrades

The following jobs are supported:

1. **SystemUpdate**: Performs any tasks required to update to a new version of DataHub. For example, applying new configurations to the search & graph indexes, ingesting default settings, and more. Once completed, emits a message to the DataHub Upgrade History Kafka (`DataHubUpgradeHistory_v1`) topic, which signals to other pods that DataHub is ready to start.
   Note that this _must_ be executed any time the DataHub version is incremented before starting or restarting other system containers. Dependent services will wait until the Kafka message is emitted corresponding to the code they are running.
   A unique "version id" is generated based on a combination of the a) embedded git tag corresponding to the version of DataHub running and b) an optional revision number, provided via the `DATAHUB_REVISION` environment variable. Helm uses
   the latter to ensure that the system upgrade job is executed every single time a deployment of DataHub is performed, even if the container version has not changed.
   Important: This job runs as a pre-install hook via the DataHub Helm Charts, i.e. before deploying new version tags for each container.

2. **SystemUpdateBlocking**: Performs any _blocking_ tasks required to update to a new version of DataHub, as a subset of **SystemUpdate**.

3. **SystemUpdateNonBlocking**: Performs any _nonblocking_ tasks required to update to a new version of DataHub, as a subset of **SystemUpdate**.

4. **RestoreIndices**: Restores indices by fetching the latest version of each aspect and restating MetadataChangeLog events for each latest aspect. Arguments include:

   - _batchSize_ (Optional): The number of rows to migrate at a time. Defaults to 1000.
   - _batchDelayMs_ (Optional): The number of milliseconds of delay between migrated batches. Used for rate limiting. Defaults to 250.
   - _numThreads_ (Optional): The number of threads to use, defaults to 1. Note that this is not used if `urnBasedPagination` is true.
   - _aspectName_ (Optional): The aspect name for producing events.
   - _urn_ (Optional): The urn for producing events.
   - _urnLike_ (Optional): The urn pattern for producing events, using `%` as a wild card
   - _urnBasedPagination_ (Optional): Paginate the SQL results using the urn + aspect string instead of `OFFSET`. Defaults to false,
     though should improve performance for large amounts of data.

5. **RestoreBackup**: Restores the primary storage - the SQL document DB - from an available backup of the local database. Requires that the backup reader and backup are provided. Note that this does not also restore the secondary indexes, the graph or search storage. To do so, you should run the **RestoreIndices** upgrade job.
   Arguments include:

   - _BACKUP_READER_ (Required): The backup reader to use to read and restore the db. The only backup reader currently supported is `LOCAL_PARQUET`, which requires a parquet-formatted backup file path to be specified via the `BACKUP_FILE_PATH` argument.
   - _BACKUP_FILE_PATH_ (Required): The path of the backup file. If you are running in a container, this needs to the location where the backup file has been mounted into the container.

6. **EvaluateTests**: Executes all Metadata Tests in batches. Running this job can slow down DataHub, and it in some cases requires full scans of the document db. Generally, it's recommended to configure this to run one time per day (which is the helm CronJob default).
   Arguments include:

   - _batchSize_ (Optional): The number of assets to test at a time. Defaults to 1000.
   - _batchDelayMs_ (Optional): The number of milliseconds of delay between evaluated asset batches. Used for rate limiting. Defaults to 250.

7. (Legacy) **NoCodeDataMigration**: Performs a series of pre-flight qualification checks and then migrates metadata\*aspect table data
   to metadata_aspect_v2 table. Arguments include:

   - _batchSize_ (Optional): The number of rows to migrate at a time. Defaults to 1000.
   - _batchDelayMs_ (Optional): The number of milliseconds of delay between migrated batches. Used for rate limiting. Defaults to 250.
   - _dbType_ (Optional): The target DB type. Valid values are `MYSQL`, `MARIA`, `POSTGRES`. Defaults to `MYSQL`.

   If you are using newer versions of DataHub (v1.0.0 or above), this upgrade job will not be relevant.

8. (Legacy) **NoCodeDataMigrationCleanup**: Cleanses graph index, search index, and key-value store of legacy DataHub data (metadata_aspect table) once
   the No Code Data Migration has completed successfully. No arguments.

   If you are using newer versions of DataHub (v1.0.0 or above), this upgrade job will not be relevant.

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

These variables tell the upgrade job how to connect to critical storage systems like Kafka, MySQL / Postgres, and Elasticsearch or OpenSearch.

2. Pull (or build) & execute the `datahub-upgrade` container:

```aidl
docker pull acryldata/datahub-upgrade:head && docker run --env-file *path-to-custom-env-file.env* acryldata/datahub-upgrade:head -u <Upgrade Job Name> -a <Upgrade Job Arguments>
```

## Command-Line Arguments

### Selecting the Upgrade to Run

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

### Provided Arguments for a Given Upgrade Job

In addition to the required `-u` argument, each upgrade may require specific arguments. You can provide arguments to individual
upgrades using multiple `-a` arguments.

For example, the NoCodeDataMigration upgrade provides 2 optional arguments detailed above: _batchSize_ and _batchDelayMs_.
To specify these, you can use a combination of `-a` arguments and of the form _argumentName=argumentValue_ as follows:

```aidl
./datahub-upgrade.sh -u NoCodeDataMigration -a batchSize=500 -a batchDelayMs=1000 // Small batches with 1 second delay.
```

OR

```aidl
docker pull acryldata/datahub-upgrade:head && docker run --env-file env/docker.env acryldata/datahub-upgrade:head -u NoCodeDataMigration -a batchSize=500 -a batchDelayMs=1000
```
