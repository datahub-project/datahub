# Spark

To integrate Spark with DataHub, we provide a lightweight Java agent that listens for Spark application and job events
and pushes metadata out to DataHub in real-time. The agent listens to events such as application start/end, and
SQLExecution start/end to create pipelines (i.e. DataJob) and tasks (i.e. DataFlow) in DataHub along with lineage to
datasets that are being read from and written to. Read on to learn how to configure this for different Spark scenarios.

## Quick Start Guide

Follow these steps to quickly integrate Spark with DataHub:

1. **Add the DataHub Spark agent jar** to your Spark configuration:

   ```
   spark.jars.packages io.acryl:acryl-spark-lineage:0.2.17
   spark.extraListeners datahub.spark.DatahubSparkListener
   ```

2. **Configure the DataHub connection**:

   ```
   spark.datahub.rest.server http://your-datahub-host:8080
   # If using authentication:
   spark.datahub.rest.token your-datahub-token
   ```

3. **Run your Spark job** as normal. The agent will automatically capture lineage information and send it to DataHub.

## Configuring Spark agent

The Spark agent can be configured using a config file or while creating a Spark Session. If you are using Spark on
Databricks, refer to [Configuration Instructions for Databricks](#configuration-instructions-databricks).

### Before you begin: Versions and Release Notes

Versioning of the jar artifact follows the semantic versioning of the
main [DataHub repo](https://github.com/datahub-project/datahub) and release notes are
available [here](https://github.com/datahub-project/datahub/releases).

To find the latest version:

1. Check [Maven Central](https://search.maven.org/search?q=a:acryl-spark-lineage) for the most recent release
2. Replace the version number in the examples below with the latest version

### Configuration Instructions: spark-submit

When running jobs using spark-submit, the agent needs to be configured in the config file.

```text
#Configuring DataHub spark agent jar
spark.jars.packages                          io.acryl:acryl-spark-lineage:0.2.17
spark.extraListeners                         datahub.spark.DatahubSparkListener
spark.datahub.rest.server                    http://localhost:8080
```

## spark-submit command line

```sh
spark-submit --packages io.acryl:acryl-spark-lineage:0.2.17 --conf "spark.extraListeners=datahub.spark.DatahubSparkListener" my_spark_job_to_run.py
```

### Configuration Instructions: Amazon EMR

Set the following spark-defaults configuration properties as
described [here](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-configure.html)

```text
spark.jars.packages                          io.acryl:acryl-spark-lineage:0.2.17
spark.extraListeners                         datahub.spark.DatahubSparkListener
spark.datahub.rest.server                    https://your_datahub_host/gms
#If you have authentication set up then you also need to specify the Datahub access token
spark.datahub.rest.token                     yourtoken
```

### Configuration Instructions: Notebooks

When running interactive jobs from a notebook, the listener can be configured while building the Spark Session.

```python
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("test-application") \
    .config("spark.jars.packages", "io.acryl:acryl-spark-lineage:0.2.17") \
    .config("spark.extraListeners", "datahub.spark.DatahubSparkListener") \
    .config("spark.datahub.rest.server", "http://localhost:8080") \
    .enableHiveSupport() \
    .getOrCreate()
```

### Configuration Instructions: Standalone Java Applications

The configuration for standalone Java apps is very similar.

```java
spark = SparkSession.builder()
        .appName("test-application")
        .config("spark.master", "spark://spark-master:7077")
        .config("spark.jars.packages", "io.acryl:acryl-spark-lineage:0.2.17")
        .config("spark.extraListeners", "datahub.spark.DatahubSparkListener")
        .config("spark.datahub.rest.server", "http://localhost:8080")
        .enableHiveSupport()
        .getOrCreate();
```

### Configuration Instructions: Databricks

The Spark agent can be configured using Databricks
Cluster [Spark configuration](https://docs.databricks.com/clusters/configure.html#spark-configuration)
and [Init script](https://docs.databricks.com/clusters/configure.html#init-scripts).

[Databricks Secrets](https://docs.databricks.com/security/secrets/secrets.html) can be leveraged to store sensitive
information like tokens.

There are two approaches to set up the agent on Databricks:

#### Option 1: Fixed version

- Download `datahub-spark-lineage` jar
  from [the Maven central repository](https://s01.oss.sonatype.org/content/groups/public/io/acryl/acryl-spark-lineage/).
- Create `init.sh` with below content

  ```sh
  #!/bin/bash
  cp /dbfs/datahub/datahub-spark-lineage*.jar /databricks/jars
  ```

- Install and configure [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html).
- Copy jar and init script to Databricks File System(DBFS) using Databricks CLI.

  ```sh
  databricks fs mkdirs dbfs:/datahub
  databricks fs cp --overwrite datahub-spark-lineage*.jar dbfs:/datahub
  databricks fs cp --overwrite init.sh dbfs:/datahub
  ```

#### Option 2: Auto-download latest version (Recommended)

Create an init script that will automatically download the latest version from Maven:

```sh
#!/bin/bash

# Script to download the latest acryl-spark-lineage JAR from Maven Central
MAVEN_URL="https://repo1.maven.io/maven2/io/acryl/acryl-spark-lineage"

# Get the latest version by looking at the maven-metadata.xml
LATEST_VERSION=$(curl -s "${MAVEN_URL}/maven-metadata.xml" | grep -o "<release>.*</release>" | sed -e "s/<release>\(.*\)<\/release>/\1/")

if [ -z "$LATEST_VERSION" ]; then
  echo "Failed to determine latest version, falling back to 0.2.17"
  LATEST_VERSION="0.2.17"
fi

JAR_URL="${MAVEN_URL}/${LATEST_VERSION}/acryl-spark-lineage-${LATEST_VERSION}.jar"
JAR_PATH="/dbfs/datahub/acryl-spark-lineage-${LATEST_VERSION}.jar"
DATABRICKS_JAR_PATH="/databricks/jars/acryl-spark-lineage-${LATEST_VERSION}.jar"

echo "Downloading latest acryl-spark-lineage version ${LATEST_VERSION}"
curl -s -L -o "${JAR_PATH}" "${JAR_URL}"

if [ -f "${JAR_PATH}" ]; then
  echo "Successfully downloaded ${JAR_PATH}"
  cp "${JAR_PATH}" "${DATABRICKS_JAR_PATH}"
  echo "Copied JAR to ${DATABRICKS_JAR_PATH}"
else
  echo "Failed to download JAR from ${JAR_URL}"
  exit 1
fi
```

Save this as `auto_download_init.sh` and upload to DBFS:

```sh
databricks fs mkdirs dbfs:/datahub
databricks fs cp --overwrite auto_download_init.sh dbfs:/datahub/init.sh
```

#### Complete the Databricks setup

- Open Databricks Cluster configuration page. Click the **Advanced Options** toggle. Click the **Spark** tab. Add below
  configurations under `Spark Config`.

  ```text
  spark.extraListeners                    datahub.spark.DatahubSparkListener
  spark.datahub.rest.server               http://localhost:8080
  spark.datahub.stage_metadata_coalescing true
  spark.datahub.databricks.cluster        cluster-name<any preferred cluster identifier>
  ```

- Click the **Init Scripts** tab. Set cluster init script as `dbfs:/datahub/init.sh`.

- Configuring DataHub authentication token

  - Add below config in cluster spark config.

    ```text
    spark.datahub.rest.token <token>
    ```

  - Alternatively, Databricks secrets can be used to secure token.

    - Create secret using Databricks CLI.

      ```sh
      databricks secrets create-scope --scope datahub --initial-manage-principal users
      databricks secrets put --scope datahub --key rest-token
      databricks secrets list --scope datahub <<Enter token value when prompted>>
      ```

    - Add in spark config

      ```text
      spark.datahub.rest.token {{secrets/datahub/rest-token}}
      ```

## Configuration Options

Configuration options are grouped by category for easier reference:

### Core Configuration

| Field                   | Required | Default | Description                                                                        |
| ----------------------- | -------- | ------- | ---------------------------------------------------------------------------------- |
| spark.jars.packages     | ✅       |         | Set with latest version io.acryl:acryl-spark-lineage:0.2.17                        |
| spark.extraListeners    | ✅       |         | datahub.spark.DatahubSparkListener                                                 |
| spark.datahub.emitter   |          | rest    | Specify how to emit metadata. <br /> **Options**: `rest`, `kafka`, `file`, or `s3` |
| spark.datahub.flow_name |          |         | Custom DataFlow name. Default is the Spark app name                                |

### REST Emitter Configuration

| Field                                       | Default               | Description                                                    |
| ------------------------------------------- | --------------------- | -------------------------------------------------------------- |
| spark.datahub.rest.server                   | http://localhost:8080 | DataHub server URL                                             |
| spark.datahub.rest.token                    |                       | Authentication token for DataHub REST API                      |
| spark.datahub.rest.disable_ssl_verification | false                 | Disable SSL certificate validation (use with caution)          |
| spark.datahub.rest.disable_chunked_encoding | false                 | Disable Chunked Transfer Encoding for environments with issues |
| spark.datahub.rest.max_retries              | 0                     | Number of times a request is retried if failed                 |
| spark.datahub.rest.retry_interval           | 10                    | Number of seconds to wait between retries                      |

### Kafka Emitter Configuration

| Field                                       | Default | Description                                                                                        |
| ------------------------------------------- | ------- | -------------------------------------------------------------------------------------------------- |
| spark.datahub.kafka.bootstrap               |         | Kafka bootstrap server URL                                                                         |
| spark.datahub.kafka.schema_registry_url     |         | Schema registry URL                                                                                |
| spark.datahub.kafka.schema_registry_config. |         | Additional config to pass to the Schema Registry Client                                            |
| spark.datahub.kafka.producer_config.        |         | Additional config for Kafka producer (e.g., `spark.datahub.kafka.producer_config.client.id=my_id`) |
| spark.datahub.kafka.mcp_topic               |         | Specify the MCP Kafka topic for the DataHub Kafka sink                                             |

### File/S3 Emitter Configuration

| Field                       | Default | Description                                                |
| --------------------------- | ------- | ---------------------------------------------------------- |
| spark.datahub.file.filename |         | The file where metadata will be written (for file emitter) |
| spark.datahub.s3.bucket     |         | S3 bucket name for metadata (for s3 emitter)               |
| spark.datahub.s3.prefix     |         | S3 prefix for metadata files (for s3 emitter)              |
| spark.datahub.s3.filename   |         | Filename for S3 metadata file (random if not specified)    |
| spark.datahub.log.mcps      | true    | Log MCPs to the application log                            |

### Dataset Configuration

| Field                                                  | Default | Description                                                                             |
| ------------------------------------------------------ | ------- | --------------------------------------------------------------------------------------- |
| spark.datahub.metadata.dataset.platformInstance        |         | Dataset level platform instance (useful with Glue ingestion)                            |
| spark.datahub.metadata.dataset.env                     | PROD    | [Environment type](https://docs.datahub.com/docs/graphql/enums#fabrictype) for datasets |
| spark.datahub.metadata.dataset.hivePlatformAlias       | hive    | Platform alias for Hive-like tables. Set to `glue` if using Glue as Hive metastore      |
| spark.datahub.metadata.include_scheme                  | true    | Include scheme in dataset URN. Recommended: set to false for new deployments            |
| spark.datahub.metadata.remove_partition_pattern        |         | Remove partition pattern (e.g., `/partition=\d+`) from paths                            |
| spark.datahub.metadata.dataset.materialize             | false   | Materialize Datasets in DataHub                                                         |
| spark.datahub.metadata.dataset.include_schema_metadata | false   | Emit dataset schema metadata (not recommended as primary source)                        |
| spark.datahub.metadata.dataset.lowerCaseUrns           | false   | Lowercase dataset URNs                                                                  |
| spark.datahub.disableSymlinkResolution                 | false   | Prefer S3 location over Hive table                                                      |
| spark.datahub.file_partition_regexp                    |         | Regexp to strip partition patterns (e.g., `year=.*/month=.*/day=.*`)                    |

### Lineage and Job Configuration

| Field                                            | Default | Description                                                       |
| ------------------------------------------------ | ------- | ----------------------------------------------------------------- |
| spark.datahub.coalesce_jobs                      | true    | Emit one datajob with all input/output datasets for the Spark app |
| spark.datahub.parent.datajob_urn                 |         | Upstream dataset for datajob (only with coalesce_jobs=true)       |
| spark.datahub.patch.enabled                      | false   | Send lineage as a patch (append not overwrite)                    |
| spark.datahub.stage_metadata_coalescing          | false   | Enable metadata coalescing (required for Databricks/Glue)         |
| spark.datahub.lineage.exclude_empty_jobs         | true    | Exclude jobs with no inputs/outputs                               |
| spark.datahub.lineage.use_legacy_job_names       | false   | Use enhanced job naming with operation type when false            |
| spark.datahub.legacyLineageCleanup.enabled       | false   | Remove legacy lineages from older plugin runs                     |
| spark.datahub.tags                               |         | Comma-separated list of tags for the DataFlow                     |
| spark.datahub.domains                            |         | Comma-separated list of domain URNs for the DataFlow              |
| spark.datahub.metadata.pipeline.platformInstance |         | Pipeline level platform instance                                  |

### Path Specification Configuration

| Field                                    | Default | Description                       |
| ---------------------------------------- | ------- | --------------------------------- |
| spark.datahub.platform.s3.path_spec_list |         | List of pathspecs for S3 platform |

## What to Expect: The Metadata Model

As of current writing, the Spark agent produces metadata related to the Spark job, tasks and lineage edges to datasets.

- A pipeline is created per Spark <master, appName>.
- A task is created per unique Spark query execution within an app.

For Spark on Databricks,

- A pipeline is created per
  - cluster_identifier: specified with spark.datahub.databricks.cluster
  - applicationID: on every restart of the cluster new spark applicationID will be created.
- A task is created per unique Spark query execution.

### Custom properties & relating to Spark UI

The following custom properties in pipelines and tasks relate to the Spark UI:

- appName and appId in a pipeline can be used to determine the Spark application
- Other custom properties of pipelines and tasks capture the start and end times of execution etc.

For Spark on Databricks, pipeline start time is the cluster start time.

### Spark versions supported

Supports Spark 3.x series.

### Environments tested with

This initial release has been tested with the following environments:

- spark-submit of Python/Java applications to local and remote servers
- Standalone Java applications
- Databricks Standalone Cluster
- EMR

Testing with Databricks Standard and High-concurrency Cluster is not done yet.

### Configuring Hdfs based dataset URNs

Spark emits lineage between datasets. It has its own logic for generating URNs. Python sources emit metadata of
datasets. To link these 2 things, URNs generated by both have to match.
This section will help you match URNs to that of other ingestion sources.
By default, URNs are created using
template `urn:li:dataset:(urn:li:dataPlatform:<$platform>,<platformInstance>.<name>,<env>)`. We can configure these 4
components to generate the desired URN.

**Platform**:
Hdfs-based platforms supported explicitly:

- AWS S3 (s3)
- Google Cloud Storage (gcs)
- local (local file system) (local)
  All other platforms will have "hdfs" as a platform.

**Name**:
By default, the name is the complete path. For Hdfs based datasets, tables can be at different levels in the path than
that of the actual file read due to various reasons like partitioning and sharding. 'path_spec' is used to alter the
name.
{table} marker is used to specify the table level. Below are a few examples. One can specify multiple path_specs for
different paths specified in the `path_spec_list`. Each actual path is matched against all path_specs present in the
list. The first one to match will be used to generate URN.

**path_spec Examples**

```
spark.datahub.platform.s3.path_spec_list=s3://my-bucket/foo/{table}/year=*/month=*/day=*/*,s3://my-other-bucket/foo/{table}/year=*/month=*/day=*/*
```

| Absolute path                        | path_spec                        | Urn                                                                          |
| ------------------------------------ | -------------------------------- | ---------------------------------------------------------------------------- |
| s3://my-bucket/foo/tests/bar.avro    | Not provided                     | urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/foo/tests/bar.avro,PROD)    |
| s3://my-bucket/foo/tests/bar.avro    | s3://my-bucket/foo/{table}/\*    | urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/foo/tests,PROD)             |
| s3://my-bucket/foo/tests/bar.avro    | s3://my-bucket/foo/tests/{table} | urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/foo/tests/bar.avro,PROD)    |
| gs://my-bucket/foo/tests/bar.avro    | gs://my-bucket/{table}/_/_       | urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/foo,PROD)                  |
| gs://my-bucket/foo/tests/bar.avro    | gs://my-bucket/{table}           | urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/foo,PROD)                  |
| file:///my-bucket/foo/tests/bar.avro | file:///my-bucket/_/_/{table}    | urn:li:dataset:(urn:li:dataPlatform:local,my-bucket/foo/tests/bar.avro,PROD) |

**Platform instance and env:**

The default value for env is 'PROD' and the platform instance is None. Env and platform instances can be set for all
datasets using configurations 'spark.datahub.metadata.dataset.env' and 'spark.datahub.metadata.dataset.platformInstance'.
If spark is processing data that belongs to a different env or platform instance, then 'path_alias' can be used to
specify `path_spec` specific values of these. 'path_alias' groups the 'path_spec_list', its env, and platform instance
together.

**path_alias_list Example:**

The below example explains the configuration of the case where files from 2 buckets are being processed in a single
spark application and files from my-bucket are supposed to have "instance1" as platform instance and "PROD" as env, and
files from bucket2 should have env "DEV" in their dataset URNs.

```
spark.datahub.platform.s3.path_alias_list: path1,path2
spark.datahub.platform.s3.path1.env: PROD
spark.datahub.platform.s3.path1.path_spec_list: s3://my-bucket/*/*/{table}
spark.datahub.platform.s3.path1.platform_instance: instance-1
spark.datahub.platform.s3.path2.env: DEV
spark.datahub.platform.s3.path2.path_spec_list: s3://bucket2/*/{table}
```

## Troubleshooting

### Common Issues

1. **No lineage appears in DataHub**

   - Verify your DataHub server URL is correct and accessible from the Spark environment
   - Check that the authentication token is valid (if authentication is enabled)
   - Review the Spark logs for any errors (see Debugging section below)

2. **Missing datasets in lineage**

   - Ensure your path_spec configurations match your dataset paths
   - For Hive/Glue tables, make sure the hivePlatformAlias is set correctly
   - Check if the datasets are filtered out by partition patterns

3. **Incorrect dataset URNs**

   - Review the URN generation logic and path_spec settings
   - Ensure platformInstance and env configurations match your DataHub setup
   - Consider using lowerCaseUrns if case sensitivity is causing issues

4. **Performance Issues**
   - For large Spark jobs, enable stage_metadata_coalescing
   - Consider using the patch.enabled=true option to avoid overwriting existing lineage
   - For Databricks, use the auto-download init script to ensure you have the latest optimizations

### Debugging

- Following info logs are generated

On Spark context startup

```text
YY/MM/DD HH:mm:ss INFO DatahubSparkListener: DatahubSparkListener initialised.
YY/MM/DD HH:mm:ss INFO SparkContext: Registered listener datahub.spark.DatahubSparkListener
```

On application start

```text
YY/MM/DD HH:mm:ss INFO DatahubSparkListener: Application started: SparkListenerApplicationStart(AppName,Some(local-1644489736794),1644489735772,user,None,None)
YY/MM/DD HH:mm:ss INFO McpEmitter: REST Emitter Configuration: GMS url <rest.server>
YY/MM/DD HH:mm:ss INFO McpEmitter: REST Emitter Configuration: Token XXXXX
```

On pushing data to server

```text
YY/MM/DD HH:mm:ss INFO McpEmitter: MetadataWriteResponse(success=true, responseContent={"value":"<URN>"}, underlyingResponse=HTTP/1.1 200 OK [Date: day, DD month year HH:mm:ss GMT, Content-Type: application/json, X-RestLi-Protocol-Version: 2.0.0, Content-Length: 97, Server: Jetty(9.4.46.v20220331)] [Content-Length: 97,Chunked: false])
```

On application end

```text
YY/MM/DD HH:mm:ss INFO DatahubSparkListener: Application ended: AppName AppID
```

- To enable debugging logs, add below configuration in log4j.properties file

```properties
log4j.logger.datahub.spark=DEBUG
log4j.logger.datahub.client.rest=DEBUG
```

## How to build

Use Java 8 to build the project. The project uses Gradle as the build tool. To build the project, run the following command:

```shell
./gradlew -PjavaClassVersionDefault=8 :metadata-integration:java:acryl-spark-lineage:shadowJar
```

## Known limitations

-

## Changelog

### Version 0.2.18

- _New features_:
  - Added option to exclude empty jobs (with no inputs or outputs) via `spark.datahub.lineage.exclude_empty_jobs` (default: true)
  - Added option to use enhanced job naming with operation type (MERGE, INSERT, UPDATE, etc.) via `spark.datahub.lineage.use_legacy_job_names` (default: false)
  - Added operation type detection to create more descriptive job names, helping distinguish between different operations on the same dataset
  - Jobs now have unique identifiers in their names to prevent conflicts when multiple operations run with the same name

### Version 0.2.17

- _Major changes_:

  - Finegrained lineage is emitted on the DataJob and not on the emitted Datasets. This is the correct behavior which was not correct earlier. This causes earlier emitted finegrained lineages won't be overwritten by the new ones.
    You can remove the old lineages by setting `spark.datahub.legacyLineageCleanup.enabled=true`. Make sure you have the latest server if you enable with patch support. (this was introduced since 0.2.17-rc5)

- _Changes_:
  - OpenLineage 1.25.0 upgrade
  - Add option to disable chunked encoding in the datahub rest sink -> `spark.datahub.rest.disable_chunked_encoding`
  - Add option to specify the mcp kafka topic for the datahub kafka sink -> `spark.datahub.kafka.mcp_topic`
  - Add option to remove legacy lineages from older Spark Plugin runs. This will remove those lineages from the Datasets which it adds to DataJob -> `spark.datahub.legacyLineageCleanup.enabled`
- _Fixes_:
  - Fix handling map transformation in the lineage. Earlier it generated wrong lineage for map transformation.

### Version 0.2.16

- Remove logging DataHub config into logs

### Version 0.2.15

- Add Kafka emitter to emit lineage to kafka
- Add File emitter to emit lineage to file
- Add S3 emitter to save mcps to s3
- Upgrading OpenLineage to 1.19.0
- Renaming project to acryl-datahub-spark-lineage
- Supporting OpenLineage 1.17+ glue identifier changes
- Fix handling OpenLineage input/output where wasn't any facet attached

### Version 0.2.14

- Fix warning about MeterFilter warning from Micrometer

### Version 0.2.13

- Add kafka emitter to emit lineage to kafka

### Version 0.2.12

- Silencing some chatty warnings in RddPathUtils

### Version 0.2.11

- Add option to lowercase dataset URNs
- Add option to set platform instance and/or env per platform with `spark.datahub.platform.<platform_name>.env` and `spark.datahub.platform.<platform_name>.platform_instance` config parameter
- Fixing platform instance setting for datasets when `spark.datahub.metadata.dataset.platformInstance` is set
- Fixing column level lineage support when patch is enabled
