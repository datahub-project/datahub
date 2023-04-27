import FeatureAvailability from '@site/src/components/FeatureAvailability';

# About DataHub Lineage

<FeatureAvailability/>

Lineage is used to capture data dependencies within an organization. It allows you to track the inputs from which a data asset is derived, along with the data assets that depend on it downstream.

If you're using an ingestion source that supports extraction of Lineage (e.g. the "Table Lineage Capability"), then lineage information can be extracted automatically. For detailed instructions, refer to the source documentation for the source you are using. If you are not using a Lineage-support ingestion source, you can programmatically emit lineage edges between entities via API.

Alternatively, as of `v0.9.5`, DataHub supports the manual editing of lineage between entities. Data experts are free to add or remove upstream and downstream lineage edges in both the Lineage Visualization screen as well as the Lineage tab on entity pages. Use this feature to supplement automatic lineage extraction or establish important entity relationships in sources that do not support automatic extraction. Editing lineage by hand is supported for Datasets, Charts, Dashboards, and Data Jobs.

:::note

Lineage added by hand and programmatically may conflict with one another to cause unwanted overwrites. It is strongly recommend that lineage is edited manually in cases where lineage information is not also extracted in automated fashion, e.g. by running an ingestion source.

:::

Types of lineage connections supported in DataHub are:

* Dataset-to-dataset
* Pipeline lineage (dataset-to-job-to-dataset)
* Dashboard-to-chart lineage
* Chart-to-dataset lineage
* Job-to-dataflow (dbt lineage)

## Lineage Setup, Prerequisites, and Permissions

To edit lineage for an entity, you'll need the following [Metadata Privilege](../authorization/policies.md):

* **Edit Lineage** metadata privilege to edit lineage at the entity level

It is important to know that the **Edit Lineage** privilege is required for all entities whose lineage is affected by the changes. For example, in order to add "Dataset B" as an upstream dependency of "Dataset A", you'll need the **Edit Lineage** privilege for both Dataset A and Dataset B.

## Managing Lineage via the DataHub UI

### Editing from Lineage Graph View

The first place that you can edit lineage for entities is from the Lineage Visualization screen. Click on the "Lineage" button on the top right of an entity's profile to get to this view.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/lineage/lineage-viz-button.png"/>
</p>

Once you find the entity that you want to edit the lineage of, click on the three-dot menu dropdown to select whether you want to edit lineage in the upstream direction or the downstream direction.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/lineage/edit-lineage-menu.png"/>
</p>

If you want to edit upstream lineage for entities downstream of the center node or downstream lineage for entities upstream of the center node, you can simply re-center to focus on the node you want to edit. Once focused on the desired node, you can edit lineage in either direction.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/lineage/focus-to-edit.png"/>
</p>

#### Adding Lineage Edges

Once you click "Edit Upstream" or "Edit Downstream," a modal will open that allows you to manage lineage for the selected entity in the chosen direction. In order to add a lineage edge to a new entity, search for it by name in the provided search bar and select it. Once you're satisfied with everything you've added, click "Save Changes." If you change your mind, you can always cancel or exit without saving the changes you've made.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/lineage/add-upstream.png"/>
</p>

#### Removing Lineage Edges

You can remove lineage edges from the same modal used to add lineage edges. Find the edge(s) that you want to remove, and click the "X" on the right side of it. And just like adding, you need to click "Save Changes" to save and if you exit without saving, your changes won't be applied.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/lineage/remove-lineage-edge.png"/>
</p>

#### Reviewing Changes

Any time lineage is edited manually, we keep track of who made the change and when they made it. You can see this information in the modal where you add and remove edges. If an edge was added manually, a user avatar will be in line with the edge that was added. You can hover over this avatar in order to see who added it and when.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/lineage/lineage-edge-audit-stamp.png"/>
</p>

### Editing from Lineage Tab

The other place that you can edit lineage for entities is from the Lineage Tab on an entity's profile. Click on the "Lineage" tab in an entity's profile and then find the "Edit" dropdown that allows you to edit upstream or downstream lineage for the given entity.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/lineage/edit-from-lineage-tab.png"/>
</p>

Using the modal from this view will work the same as described above for editing from the Lineage Visualization screen.

## Managing Lineage via API

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

**DataHub November 2022 Town Hall - Including Manual Lineage Demo**

<p align="center">
<iframe width="560" height="315" src="https://www.youtube.com/embed/BlCLhG8lGoY" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>
</p>

### GraphQL

* [updateLineage](../../graphql/mutations.md#updatelineage)
* [searchAcrossLineage](../../graphql/queries.md#searchacrosslineage)
* [searchAcrossLineageInput](../../graphql/inputObjects.md#searchacrosslineageinput)

#### Examples

**Updating Lineage**

```graphql
mutation updateLineage {
  updateLineage(input: {
    edgesToAdd: [
      {
        downstreamUrn: "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)",
        upstreamUrn: "urn:li:dataset:(urn:li:dataPlatform:datahub,Dataset,PROD)"
      }
    ],
    edgesToRemove: [
      {
        downstreamUrn: "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)",
        upstreamUrn: "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"
      }
    ]
  })
}
```

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