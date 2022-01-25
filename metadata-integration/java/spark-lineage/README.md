# Spark lineage emitter
The Spark lineage emitter is a java library that provides a Spark listener implementation "DatahubLineageEmitter". The DatahubLineageEmitter listens to events such application start/end, and SQLExecution start/end to create pipelines (i.e. DataJob) and tasks (i.e. DataFlow) in Datahub along with lineage.

## Configuring Spark emitter
Listener configuration can be done using a config file or while creating a spark Session.

### Config file for spark-submit
When running jobs using spark-submit, the listener is to be configured in the config file.

```
spark.master                                 spark://spark-master:7077

#Configuring datahub spark listener jar
spark.jars.packages			     io.acryl:datahub-spark-lineage:0.0.3
spark.extraListeners                         com.linkedin.datahub.lineage.spark.interceptor.DatahubLineageEmitter
spark.datahub.rest.server                    http://localhost:8080
```

### Configuring with SparkSession Builder for notebooks
When running interactive jobs from a notebook, the listener can be configured while building the Spark Session.

```python
spark = SparkSession.builder \
          .master("spark://spark-master:7077") \
          .appName("test-application") \
          .config("spark.jars.packages","io.acryl:datahub-spark-lineage:0.0.3") \
          .config("spark.extraListeners","com.linkedin.datahub.lineage.interceptor.spark.DatahubLineageEmitter") \
          .config("spark.datahub.rest.server", "http://localhost:8080") \
          .enableHiveSupport() \
          .getOrCreate()
```

## Model mapping
A pipeline is created per Spark <master, appName>.
A task is created per unique Spark query execution within an app.

### Custom properties & relating to Spark UI
The following custom properties in pipelines and tasks relate to the Spark UI:
- appName and appId in a pipeline can be used to determine the Spark application
- description and SQLQueryId in a task can be used to determine the Query Execution within the application on the SQL tab of Spark UI

Other custom properties of pipelines and tasks capture the start and end times of execution etc. 
The query plan is captured in the *queryPlan* property of a task.

## Release notes for v0.0.3
In this version, basic dataset-level lineage is captured using the model mapping as mentioned earlier.

### Spark versions supported
The primary version tested is Spark/Scala version 2.4.8/2_11.
We anticipate this to work well with other Spark 2.4.x versions and Scala 2_11.

Support for other Spark versions is planned in the very near future.

### Environments tested with
This initial release has been tested with the following environments:
- spark-submit of Python/Java applications to local and remote servers
- notebooks

Note that testing for other environments such as Databricks and standalone applications is planned in near future.

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

## Known limitations
- Only postgres supported for JDBC sources in this initial release. Support for other driver URL formats will be added in future.
- Behavior with cached datasets is not fully specified/defined in context of lineage.
- There is a possibility that very short-lived jobs that run within a few milliseconds may not be captured by the listener. This should not cause an issue for realistic Spark applications.
