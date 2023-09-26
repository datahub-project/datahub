import FeatureAvailability from '@site/src/components/FeatureAvailability';

# About DataHub Lineage

<FeatureAvailability/>

Lineage is used to capture data dependencies within an organization. It allows you to track the inputs from which a data asset is derived, along with the data assets that depend on it downstream.

## Viewing Lineage

<p align="center">  
    <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/lineage/lineage-viz-button.png"/>
</p>

You can view lineage under **Lineage** tab or **Lineage Visualization** screen. 

The UI shows the latest version of the lineage. The time picker can be used to filter out edges within the latest version to exclude those that were last updated outside of the time window. Selecting time windows in the patch will not show you historical lineages. It will only filter the view of the latest version of the lineage.

:::tip The Lineage Tab is greyed out - why can’t I click on it?
This means you have not yet ingested lineage metadata for that entity. Please ingest lineage to proceed.

:::

## Adding Lineage

### Ingestion Source

If you're using an ingestion source that supports extraction of Lineage (e.g. the "Table Lineage Capability"), then lineage information can be extracted automatically. 
For detailed instructions, refer to the [source documentation](https://datahubproject.io/integrations) for the source you are using. 

### UI

As of `v0.9.5`, DataHub supports the manual editing of lineage between entities. Data experts are free to add or remove upstream and downstream lineage edges in both the Lineage Visualization screen as well as the Lineage tab on entity pages. Use this feature to supplement automatic lineage extraction or establish important entity relationships in sources that do not support automatic extraction. Editing lineage by hand is supported for Datasets, Charts, Dashboards, and Data Jobs.
Please refer to our [UI Guides on Lineage]() for more information. 

:::caution Recommendation on UI-based lineage

Lineage added by hand and programmatically may conflict with one another to cause unwanted overwrites. 
It is strongly recommend that lineage is edited manually in cases where lineage information is not also extracted in automated fashion, e.g. by running an ingestion source.

:::

### API

If you are not using a Lineage-support ingestion source, you can programmatically emit lineage edges between entities via API.
Please refer to [API Guides on Lineage](../api/tutorials/lineage.md) for more information. 


## Lineage Support

### Table-level vs Column-level lineages

### Types of Lineage Connections

Types of lineage connections supported in DataHub and the example codes are as follows. 

| Connection          | Examples                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | A.K.A           |
|---------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------|
| Dataset to Dataset  | - [lineage_emitter_mcpw_rest.py](../../metadata-ingestion/examples/library/lineage_emitter_mcpw_rest.py) <br /> - [lineage_emitter_rest.py](../../metadata-ingestion/examples/library/lineage_emitter_rest.py) <br /> - [lineage_emitter_kafka.py](../../metadata-ingestion/examples/library/lineage_emitter_kafka.py) <br /> - [lineage_emitter_dataset_finegrained.py](../../metadata-ingestion/examples/library/lineage_emitter_dataset_finegrained.py) <br /> - [Datahub BigQuery Lineage](https://github.com/datahub-project/datahub/blob/a1bf95307b040074c8d65ebb86b5eb177fdcd591/metadata-ingestion/src/datahub/ingestion/source/sql/bigquery.py#L229) <br /> - [Datahub Snowflake Lineage](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/sql/snowflake.py#L249) |
| DataJob to DataFlow | - [Datahub dbt Lineage](https://github.com/datahub-project/datahub/blob/a9754ebe83b6b73bc2bfbf49d9ebf5dbd2ca5a8f/metadata-ingestion/src/datahub/ingestion/source/dbt.py#L625,L630)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | DBT Lineage     |
| DataJob to Dataset  | - [lineage_dataset_job_dataset.py](../../metadata-ingestion/examples/library/lineage_dataset_job_dataset.py) <br /> - [lineage_job_dataflow.py](../../metadata-ingestion/examples/library/lineage_job_dataflow.py) | Pipeline Lineage |
| Chart to Dashboard  | - [lineage_chart_dashboard.py](../../metadata-ingestion/examples/library/lineage_chart_dashboard.py)  |  |
| Chart to Dataset    | - [lineage_dataset_chart.py](../../metadata-ingestion/examples/library/lineage_dataset_chart.py)  |  |


:::tip Our Roadmap
We're actively working on expanding lineage support for new data sources.
Visit our [Official Roadmap](https://feature-requests.datahubproject.io/roadmap) for upcoming updates!
:::

## References

- [DataHub Basics: Lineage 101](https://www.youtube.com/watch?v=rONGpsndzRw&t=1s)
- [DataHub November 2022 Town Hall](https://www.youtube.com/watch?v=BlCLhG8lGoY&t=1s) - Including Manual Lineage Demo
- [Acryl Data introduces lineage support and automated propagation of governance information for Snowflake in DataHub](https://blog.datahubproject.io/acryl-data-introduces-lineage-support-and-automated-propagation-of-governance-information-for-339c99536561)
- [Data in Context: Lineage Explorer in DataHub](https://blog.datahubproject.io/data-in-context-lineage-explorer-in-datahub-a53a9a476dc4)
- [Harnessing the Power of Data Lineage with DataHub](https://blog.datahubproject.io/harnessing-the-power-of-data-lineage-with-datahub-ad086358dec4)
- [DataHub Lineage Impact Analysis](https://datahubproject.io/docs/next/act-on-metadata/impact-analysis)