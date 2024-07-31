DataHub supports integration with Databricks ecosystem using a multitude of connectors, depending on your exact setup. 

## Databricks Unity Catalog (new)

The recently introduced [Unity Catalog](https://www.databricks.com/product/unity-catalog) provides a new way to govern your assets within the Databricks lakehouse. If you have Unity Catalog Enabled Workspace, you can use the `unity-catalog` source (aka `databricks` source, see below for details) to integrate your metadata into DataHub as an alternate to the Hive pathway. This also ingests hive metastore catalog in Databricks and is recommended approach to ingest Databricks ecosystem in DataHub.

## Databricks Hive (old)

The alternative way to integrate is via the Hive connector. The [Hive starter recipe](http://datahubproject.io/docs/generated/ingestion/sources/hive#starter-recipe) has a section describing how to connect to your Databricks workspace.


## Databricks Spark

To complete the picture, we recommend adding push-based ingestion from your Spark jobs to see real-time activity and lineage between your Databricks tables and your Spark jobs. Use the Spark agent to push metadata to DataHub using the instructions [here](../../../../metadata-integration/java/acryl-spark-lineage/README.md#configuration-instructions-databricks).

## Watch the DataHub Talk at the Data and AI Summit 2022

For a deeper look at how to think about DataHub within and across your Databricks ecosystem, watch the recording of our talk at the Data and AI Summit 2022. 
<p align="center">
<a href="https://www.youtube.com/watch?v=SCP0PR3t7dc">
<img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/metadata-ingestion/databricks/data_and_ai_summit_2022.png"/>
</a>
</p>



