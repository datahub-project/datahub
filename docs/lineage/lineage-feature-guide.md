import FeatureAvailability from '@site/src/components/FeatureAvailability';

# About DataHub Lineage

<FeatureAvailability/>

Lineage is used to capture data dependencies within an organization. It allows you to track the inputs from which a data asset is derived, along with the data assets that depend on it downstream.

If you're using an ingestion source that supports extraction of Lineage (e.g. the "Table Lineage Capability"), then lineage information can be extracted automatically. For detailed instructions, refer to the source documentation for the source you are using.

If you are not using a Lineage-support ingestion source, you can also manage lineage connections by hand inside the DataHub web application. The remainder of this guide will focus on managing Lineage as done within DataHub directly.

Starting in `v0.9.5`, DataHub supports the manual editing of lineage between entities. Data experts are free to add or remove upstream and downstream lineage edges in both the Lineage Visualization screen as well as the Lineage tab on entity pages. Use this feature to supplement automatic lineage extraction or establish important entity relationships in sources that do not support automatic extraction. Editing lineage by hand is supported for Datasets, Charts, Dashboards, and Data Jobs.

:::note

Lineage added by hand and programmatically may conflict with one another to cause unwanted overwrites. It is strongly recommend that lineage is edited manually in cases where lineage information is not also extracted in automated fashion, e.g. by running an ingestion source.

:::

## Lineage Setup, Prerequisites, and Permissions

To edit lineage for an entity, you'll need the following [Metadata Privilege](../authorization/policies.md):

* **Edit Lineage** metadata privilege to edit lineage at the entity level

It is important to know that the **Edit Lineage** privilege is required for all entities whose lineage is affected by the changes. For example, in order to add "Dataset B" as an upstream dependency of "Dataset A", you'll need the **Edit Lineage** privilege for both Dataset A and Dataset B.

## Using Lineage

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

## Additional Resources

### Videos

**DataHub Basics: Lineage 101**

<p align="center">
<iframe width="560" height="315" src="https://www.youtube.com/embed/rONGpsndzRw" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" allowfullscreen></iframe>
</p>

**DataHub November 2022 Town Hall - Including Manual Lineage Demo**

<p align="center">
<iframe width="560" height="315" src="https://www.youtube.com/embed/BlCLhG8lGoY" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>
</p>

### GraphQL

* [updateLineage](../../graphql/mutations.md#updatelineage)
* [searchAcrossLineage](../../graphql/queries.md#searchacrosslineage)

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

*Need more help? Join the conversation in [Slack](http://slack.datahubproject.io)!*

### Related Features

* [Lineage](./intro.md)
