# Sources


Sources are **the data systems that we are extracting metadata from.** 

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/sources-sinks.png"/>
</p>

In general, the source will be defined at the top of the [recipe](./recipe_overview.md) like below.


```yaml
#my_recipe.yml
source: 
  type: <source_name>
  config: 
    option_1: <value>
    ...
```

## Types of Source
The `Sources` tab on the left in the sidebar shows you all the sources that are available for you to ingest metadata from. For example, we have sources for [BigQuery](https://datahubproject.io/docs/generated/ingestion/sources/bigquery), [Looker](https://datahubproject.io/docs/generated/ingestion/sources/looker), [Tableau](https://datahubproject.io/docs/generated/ingestion/sources/tableau) and many others.

:::tip Find an Integration Source
See the full **[list of integrations](https://datahubproject.io/integrations)** and filter on their features.
:::

## Metadata Ingestion Source Status

We apply a Support Status to each Metadata Source to help you understand the integration reliability at a glance.

![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen): Certified Sources are well-tested & widely-adopted by the DataHub Community. We expect the integration to be stable with few user-facing issues.

![Incubating](https://img.shields.io/badge/support%20status-incubating-blue): Incubating Sources are ready for DataHub Community adoption but have not been tested for a wide variety of edge-cases. We eagerly solicit feedback from the Community to streghten the connector; minor version changes may arise in future releases.

![Testing](https://img.shields.io/badge/support%20status-testing-lightgrey): Testing Sources are available for experiementation by DataHub Community members, but may change without notice.
