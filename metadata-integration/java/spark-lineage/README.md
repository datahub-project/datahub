# Spark Integration
To integrate Spark with DataHub, we provide a lightweight Java agent that listens for Spark application and job events and pushes metadata out to DataHub in real-time. The agent listens to events such application start/end, and SQLExecution start/end to create pipelines (i.e. DataJob) and tasks (i.e. DataFlow) in Datahub along with lineage to datasets that are being read from and written to. Read on to learn how to configure this for different Spark scenarios.

## Configuring Spark agent
The Spark agent can be configured using a config file or while creating a spark Session.

## Before you begin: Versions and Release Notes
Versioning of the jar artifact will follow the semantic versioning of the main [DataHub repo](https://github.com/datahub-project/datahub) and release notes will be available [here](https://github.com/datahub-project/datahub/releases).
Always check [the Maven central repository](https://search.maven.org/search?q=a:datahub-spark-lineage) for the latest released version.

### Configuration Instructions: spark-submit
When running jobs using spark-submit, the agent needs to be configured in the config file.

```
spark.master                                 spark://spark-master:7077

#Configuring datahub spark agent jar
spark.jars.packages			     io.acryl:datahub-spark-lineage:0.8.23
spark.extraListeners                         datahub.spark.DatahubSparkListener
spark.datahub.rest.server                    http://localhost:8080
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

### Configuration details

| Field                                           | Required | Default | Description                                                             |
|-------------------------------------------------|----------|---------|-------------------------------------------------------------------------|
| spark.jars.packages                              | ✅        |         | Set with latest/required version  io.acryl:datahub-spark-lineage:0.8.23 |
| spark.extraListeners                             | ✅        |         | datahub.spark.DatahubSparkListener                                      |
| spark.datahub.rest.server                        | ✅        |         | Datahub server url  eg:http://localhost:8080                            |
| spark.datahub.rest.token                         |          |         | Authentication token.                         |
| spark.datahub.metadata.pipeline.platformInstance|          |         | Pipeline level platform instance                                        |
| spark.datahub.metadata.dataset.platformInstance|          |         | dataset level platform instance                                        |
| spark.datahub.metadata.dataset.env              |          | PROD    | [Supported values](https://datahubproject.io/docs/graphql/enums#fabrictype). In all other cases, will fallback to PROD           |


## What to Expect: The Metadata Model

As of current writing, the Spark agent produces metadata related to the Spark job, tasks and lineage edges to datasets.

- A pipeline is created per Spark <master, appName>.
- A task is created per unique Spark query execution within an app.

### Custom properties & relating to Spark UI
The following custom properties in pipelines and tasks relate to the Spark UI:
- appName and appId in a pipeline can be used to determine the Spark application
- description and SQLQueryId in a task can be used to determine the Query Execution within the application on the SQL tab of Spark UI

Other custom properties of pipelines and tasks capture the start and end times of execution etc. 
The query plan is captured in the *queryPlan* property of a task.



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

Note that testing for other environments such as Databricks is planned in near future.

### Spark commands supported
Below is a list of Spark commands that are parsed currently:
- InsertIntoHadoopFsRelationCommand
- SaveIntoDataSourceCommand (jdbc)
- CreateHiveTableAsSelectCommand
- InsertIntoHiveTable

Effectively, these support data sources/sinks corresponding to Hive, HDFS and JDBC.

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
```
YY/MM/DD HH:mm:ss INFO DatahubSparkListener: DatahubSparkListener initialised.
YY/MM/DD HH:mm:ss INFO SparkContext: Registered listener datahub.spark.DatahubSparkListener
```
On application start
```
YY/MM/DD HH:mm:ss INFO DatahubSparkListener: Application started: SparkListenerApplicationStart(AppName,Some(local-1644489736794),1644489735772,user,None,None)
YY/MM/DD HH:mm:ss INFO McpEmitter: REST Emitter Configuration: GMS url <rest.server>
YY/MM/DD HH:mm:ss INFO McpEmitter: REST Emitter Configuration: Token XXXXX
```
On pushing data to server
```
YY/MM/DD HH:mm:ss INFO McpEmitter: MetadataWriteResponse(success=true, responseContent={"value":"<URN>"}, underlyingResponse=HTTP/1.1 200 OK [Date: day, DD month year HH:mm:ss GMT, Content-Type: application/json, X-RestLi-Protocol-Version: 2.0.0, Content-Length: 97, Server: Jetty(9.4.20.v20190813)] [Content-Length: 97,Chunked: false])
```
On application end
```
YY/MM/DD HH:mm:ss INFO DatahubSparkListener: Application ended : AppName AppID
```

- To enable debugging logs, add below configuration in log4j.properties file

```
log4j.logger.datahub.spark=DEBUG
log4j.logger.datahub.client.rest=DEBUG
```

## Known limitations
- Only postgres supported for JDBC sources in this initial release. Support for other driver URL formats will be added in future.
- Behavior with cached datasets is not fully specified/defined in context of lineage.
- There is a possibility that very short-lived jobs that run within a few milliseconds may not be captured by the listener. This should not cause an issue for realistic Spark applications.
