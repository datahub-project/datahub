import FeatureAvailability from '@site/src/components/FeatureAvailability';

# About DataHub DataHub Lineage

<FeatureAvailability/>

Lineage in DataHub provides full visibility into the end-to-end data flow across multiple platforms.

Lineage in DataHub is designed to help you with 

* proactive impact analysis and
* reactive data debugging

DataHub extracts lineage from multiple data platforms including cloud warehouses such as BigQuery and Snowflake, transformations like dbt or Airflow, and business intelligence tools including Looker and Tableau.

Types of lineage supported in DataHub are:

* Dataset-to-dataset
* Pipeline lineage (dataset-to-job-to-dataset)
* Dashboard-to-chart lineage
* Chart-to-dataset lineage
* Job-to-dataflow (dbt lineage)

## DataHub Lineage Setup, Prerequisites, and Permissions

The type of lineage supported for different platforms

* Dataset-to-Dataset Lineage: Supported for Snowflake, BigQuery, and between Redshift and external tables
* Pipeline lineage: Supported for Airflow using airflow’s lineage backend
* Dashboard-to-chart lineage: Supported for Looker (automated extraction for models, explores, and views) and Superset
* Chart-to-dataset lineage: Supported for Looker and Superset
* Job-to-dataflow (or dbt lineage): Supported for dbt

## Using DataHub Lineage

:::note

   When you emit any lineage aspect, the existing aspect gets completely overwritten.

:::

### Using Dataset-to-Dataset Lineage

This relationship model uses dataset -> dataset connection through the UpstreamLineage aspect in the Dataset entity.

Here are a few samples for the usage of this type of lineage:

* [lineage_emitter_mcpw_rest.py](../../metadata-ingestion/examples/library/lineage_emitter_mcpw_rest.py) - emits simple bigquery table-to-table (dataset-to-dataset) lineage via REST as MetadataChangeProposalWrapper.
* [lineage_emitter_rest.py](../../metadata-ingestion/examples/library/lineage_emitter_rest.py) - emits simple dataset-to-dataset lineage via REST as MetadataChangeEvent.
* [lineage_emitter_kafka.py](../../metadata-ingestion/examples/library/lineage_emitter_kafka.py) - emits simple dataset-to-dataset lineage via Kafka as MetadataChangeEvent.
* [lineage_emitter_dataset_finegrained.py](../../metadata-ingestion/examples/library/lineage_emitter_dataset_finegrained.py) - emits fine-grained dataset-dataset lineage via REST as MetadataChangeProposalWrapper.
* [Datahub BigQuery Lineage](https://github.com/datahub-project/datahub/blob/a1bf95307b040074c8d65ebb86b5eb177fdcd591/metadata-ingestion/src/datahub/ingestion/source/sql/bigquery.py#L229) - emits Datahub's Bigquery lineage as MetadataChangeProposalWrapper.
* [Datahub Snowflake Lineage](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/sql/snowflake.py#L249) - emits Datahub's Snowflake lineage as MetadataChangeProposalWrapper.

### Using dbt Lineage

This model captures dbt specific nodes (tables, views, etc.) and

* uses datasets as the base entity type and
* extends subclass datasets for each dbt-specific concept, and
* links them together for dataset-to-dataset lineage

Here is a sample usage of this lineage:

* [Datahub dbt Lineage](https://github.com/datahub-project/datahub/blob/a9754ebe83b6b73bc2bfbf49d9ebf5dbd2ca5a8f/metadata-ingestion/src/datahub/ingestion/source/dbt.py#L625,L630) - emits Datahub's dbt lineage as MetadataChangeEvent.

### Using Pipeline Lineage

The relationship model for this is datajob-to-dataset through the dataJobInputOutput aspect in the DataJob entity.

For Airflow, this lineage is supported using Airflow’s lineage backend which allows you to specify the inputs to and output from that task.
 
If you annotate that on your task we can pick up that information and push that as lineage edges into datahub automatically. You can install this package from Airflow’s Astronomer marketplace [here](https://registry.astronomer.io/providers/datahub).

Here are a few samples for the usage of this type of lineage:

* [lineage_dataset_job_dataset.py](../../metadata-ingestion/examples/library/lineage_dataset_job_dataset.py) - emits mysql-to-airflow-to-kafka (dataset-to-job-to-dataset) lineage via REST as MetadataChangeProposalWrapper.
* [lineage_job_dataflow.py](../../metadata-ingestion/examples/library/lineage_job_dataflow.py) - emits the job-to-dataflow lineage via REST as MetadataChangeProposalWrapper.

### Using Dashboard-to-Chart Lineage

This relationship model uses the dashboardInfo aspect of the Dashboard entity and models an explicit edge between a dashboard and a chart (such that charts can be attached to multiple dashboards).

Here is a sample usage of this lineage:

* [lineage_chart_dashboard.py](../../metadata-ingestion/examples/library/lineage_chart_dashboard.py) - emits the chart-to-dashboard lineage via REST as MetadataChangeProposalWrapper.

### Using Chart-to-Dataset Lineage

This relationship model uses the chartInfo aspect of the Chart entity.

Here is a sample usage of this lineage:

* [lineage_dataset_chart.py](../../metadata-ingestion/examples/library/lineage_dataset_chart.py) - emits the dataset-to-chart lineage via REST as MetadataChangeProposalWrapper.

## Additional Resources

### Videos

**DataHub Basics: Lineage 101**

<p align="center">
<iframe width="560" height="315" src="https://www.youtube.com/embed/rONGpsndzRw" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>
</p>

### GraphQL

* [searchAcrossLineage](../../graphql/queries.md#searchacrosslineage)
* [searchAcrossLineageInput](../../graphql/inputObjects.md#searchacrosslineageinput)

### DataHub Blog

* [Acryl Data introduces lineage support and automated propagation of governance information for Snowflake in DataHub](https://blog.datahubproject.io/acryl-data-introduces-lineage-support-and-automated-propagation-of-governance-information-for-339c99536561)
* [Data in Context: Lineage Explorer in DataHub](https://blog.datahubproject.io/data-in-context-lineage-explorer-in-datahub-a53a9a476dc4)
* [Harnessing the Power of Data Lineage with DataHub](https://blog.datahubproject.io/harnessing-the-power-of-data-lineage-with-datahub-ad086358dec4)

## FAQ and Troubleshooting

**The Lineage Tab is greyed out - why can’t I click on it?**

This means you have not yet ingested lineage metadata for that entity. Please ingest lineage to proceed.

**Are there any recommended practices for emitting lineage?**

We recommend emitting aspects as MetadataChangeProposalWrapper over emitting them via the MetadataChangeEvent.

*Need more help? Join the conversation in [Slack](http://slack.datahubproject.io)!*

### Related Features

* [DataHub Lineage Impact Analysis](../act-on-metadata/impact-analysis.md)