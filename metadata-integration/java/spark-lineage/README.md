# Spark

To integrate Spark with DataHub, we provide a lightweight Java agent that listens for Spark application and job events and pushes metadata out to DataHub in real-time. The agent listens to events such application start/end, and SQLExecution start/end to create pipelines (i.e. DataJob) and tasks (i.e. DataFlow) in Datahub along with lineage to datasets that are being read from and written to. Read on to learn how to configure this for different Spark scenarios.

## Configuring Spark agent

The Spark agent can be configured using a config file or while creating a Spark Session. If you are using Spark on Databricks, refer [Configuration Instructions for Databricks](#configuration-instructions--databricks).

### Before you begin: Versions and Release Notes

Versioning of the jar artifact will follow the semantic versioning of the main [DataHub repo](https://github.com/datahub-project/datahub) and release notes will be available [here](https://github.com/datahub-project/datahub/releases).
Always check [the Maven central repository](https://search.maven.org/search?q=a:datahub-spark-lineage) for the latest released version.

### Configuration Instructions: spark-submit

When running jobs using spark-submit, the agent needs to be configured in the config file.

```text
#Configuring DataHub spark agent jar
spark.jars.packages                          io.acryl:datahub-spark-lineage:0.8.23
spark.extraListeners                         datahub.spark.DatahubSparkListener
spark.datahub.rest.server                    http://localhost:8080
```

### Configuration Instructions:  Amazon EMR

Set the following spark-defaults configuration properties as it stated [here](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-configure.html)

```text
spark.jars.packages                          io.acryl:datahub-spark-lineage:0.8.23
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
          .config("spark.jars.packages","io.acryl:datahub-spark-lineage:0.8.23") \
          .config("spark.extraListeners","datahub.spark.DatahubSparkListener") \
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
        .config("spark.jars.packages","io.acryl:datahub-spark-lineage:0.8.23")
        .config("spark.extraListeners", "datahub.spark.DatahubSparkListener")
        .config("spark.datahub.rest.server", "http://localhost:8080")
        .enableHiveSupport()
        .getOrCreate();
 ```

### Configuration Instructions:  Databricks

The Spark agent can be configured using Databricks Cluster [Spark configuration](https://docs.databricks.com/clusters/configure.html#spark-configuration) and [Init script](https://docs.databricks.com/clusters/configure.html#init-scripts).

[Databricks Secrets](https://docs.databricks.com/security/secrets/secrets.html) can be leveraged to store sensitive information like tokens.

- Download `datahub-spark-lineage` jar from [the Maven central repository](https://search.maven.org/search?q=a:datahub-spark-lineage).
- Create `init.sh` with below content

    ```sh
    #!/bin/bash
    cp /dbfs/datahub/datahub-spark-lineage*.jar /databricks/jars
    ```

- Install and configure [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html).
- Copy jar and init script to Databricks File System(DBFS) using Databricks CLI.

    ```sh
    databricks fs mkdirs dbfs:/datahub
    databricks fs --overwrite datahub-spark-lineage*.jar dbfs:/datahub
    databricks fs --overwrite init.sh dbfs:/datahub
    ```

- Open Databricks Cluster configuration page. Click the **Advanced Options** toggle. Click the **Spark** tab. Add below configurations under `Spark Config`.

    ```text
    spark.extraListeners                datahub.spark.DatahubSparkListener
    spark.datahub.rest.server           http://localhost:8080
    spark.datahub.databricks.cluster    cluster-name<any preferred cluster identifier>
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
      databricks secrets list --scope datahub &lt;&lt;Edit prompted file with token value&gt;&gt;
      ```
  
    - Add in spark config
  
      ```text
      spark.datahub.rest.token {{secrets/datahub/rest-token}}
      ```

## Configuration Options

| Field                                            | Required | Default | Description                                                                                                                           |
|--------------------------------------------------|----------|---------|---------------------------------------------------------------------------------------------------------------------------------------|
| spark.jars.packages                              | ✅        |         | Set with latest/required version  io.acryl:datahub-spark-lineage:0.8.23                                                               |
| spark.extraListeners                             | ✅        |         | datahub.spark.DatahubSparkListener                                                                                                    |
| spark.datahub.rest.server                        | ✅        |         | Datahub server url  eg:<http://localhost:8080>                                                                                        |
| spark.datahub.rest.token                         |          |         | Authentication token.                                                                                                                 |
| spark.datahub.rest.disable_ssl_verification      |          | false   | Disable SSL certificate validation. Caution: Only use this if you know what you are doing!                                            |
| spark.datahub.metadata.pipeline.platformInstance |          |         | Pipeline level platform instance                                                                                                      |
| spark.datahub.metadata.dataset.platformInstance  |          |         | dataset level platform instance                                                                                                       |
| spark.datahub.metadata.dataset.env               |          | PROD    | [Supported values](https://datahubproject.io/docs/graphql/enums#fabrictype). In all other cases, will fallback to PROD                |
| spark.datahub.metadata.table.hive_platform_alias |          | hive    | By default, datahub assigns Hive-like tables to the Hive platform. If you are using Glue as your Hive metastore, set this config flag to `glue`                                                                                                                   |
| spark.datahub.metadata.include_scheme            |          | true    | Include scheme from the path URI (e.g. hdfs://, s3://) in the dataset URN. We recommend setting this value to false, it is set to true for backwards compatibility with previous versions                                                                             |
| spark.datahub.coalesce_jobs                      |          | false   | Only one datajob(task) will be emitted containing all input and output datasets for the spark application                            |
| spark.datahub.parent.datajob_urn                 |          |         | Specified dataset will be set as upstream dataset for datajob created. Effective only when spark.datahub.coalesce_jobs is set to true |

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
- description and SQLQueryId in a task can be used to determine the Query Execution within the application on the SQL tab of Spark UI
- Other custom properties of pipelines and tasks capture the start and end times of execution etc.
- The query plan is captured in the *queryPlan* property of a task.

For Spark on Databricks, pipeline start time is the cluster start time.

### Spark versions supported

The primary version tested is Spark/Scala version 2.4.8/2_11.
This library has also been tested to work with Spark versions(2.2.0 - 2.4.8) and Scala versions(2.10 - 2.12).
For the Spark 3.x series, this has been tested to work with Spark 3.1.2 and 3.2.0 with Scala 2.12. Other combinations are not guaranteed to work currently.
Support for other Spark versions is planned in the very near future.

### Environments tested with

This initial release has been tested with the following environments:

- spark-submit of Python/Java applications to local and remote servers
- Jupyter notebooks with pyspark code
- Standalone Java applications
- Databricks Standalone Cluster

Testing with Databricks Standard and High-concurrency Cluster is not done yet.

### Spark commands supported

Below is a list of Spark commands that are parsed currently:

- InsertIntoHadoopFsRelationCommand
- SaveIntoDataSourceCommand (jdbc)
- CreateHiveTableAsSelectCommand
- InsertIntoHiveTable

Effectively, these support data sources/sinks corresponding to Hive, HDFS and JDBC.

DataFrame.persist command is supported for below LeafExecNodes:

- FileSourceScanExec
- HiveTableScanExec
- RowDataSourceScanExec
- InMemoryTableScanExec

### Spark commands not yet supported

- View related commands
- Cache commands and implications on lineage
- RDD jobs

### Important notes on usage

- It is advisable to ensure appName is used appropriately to ensure you can trace lineage from a pipeline back to your source code.
- If multiple apps with the same appName run concurrently, dataset-lineage will be captured correctly but the custom-properties e.g. app-id, SQLQueryId would be unreliable. We expect this to be quite rare.
- If spark execution fails, then an empty pipeline would still get created, but it may not have any tasks.
- For HDFS sources, the folder (name) is regarded as the dataset (name) to align with typical storage of parquet/csv formats.

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
YY/MM/DD HH:mm:ss INFO DatahubSparkListener: Application ended : AppName AppID
```

- To enable debugging logs, add below configuration in log4j.properties file

```properties
log4j.logger.datahub.spark=DEBUG
log4j.logger.datahub.client.rest=DEBUG
```

## Known limitations

- Only postgres supported for JDBC sources in this initial release. Support for other driver URL formats will be added in future.
- Behavior with cached datasets is not fully specified/defined in context of lineage.
- There is a possibility that very short-lived jobs that run within a few milliseconds may not be captured by the listener. This should not cause an issue for realistic Spark applications.
