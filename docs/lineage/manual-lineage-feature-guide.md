import FeatureAvailability from '@site/src/components/FeatureAvailability';

# About DataHub Manual Lineage

<FeatureAvailability/>

Starting in version `0.9.5`, DataHub supports the manual editing of lineage between entities in the UI. Add or remove upstream and downstream lineage edges in both the Lineage Visualization screen as well as the Lineage tab on entity pages. Use this feature to supplement automatic lineage extraction in ingestion or establish important entity relationships in sources that we don't support automatic extraction for yet! Manual lineage is only supported for Datasets, Charts, Dashboards, and Data Jobs for now. 

## Manual Lineage Setup, Prerequisites, and Permissions

What you need to edit lineage in the UI:

* **Edit Lineage** metadata privilege to edit lineage at the entity level

You can create these privileges by creating a new [Metadata Policy](../authorization/policies.md).

Keep in mind that you need the **Edit Lineage** metadata privilege on all entity types that you want to edit lineage for. So if you want to add a Chart upstream of a Dataset, you need the **Edit Lineage** privilege for both Charts and Datasets.

## Using Manual Lineage

### Edit from Lineage Visualization

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

Once you click "Edit Upstream" or "Edit Downstream" a modal will open that allows you to manage lineage for the selected entity in the chosen direction. In order to add a lineage edge to a new entity, search for it by name in the provided search bar and select it. Once you're satisfied with everything you've added, click "Save Changes." If you change your mind you can always cancel or exit without saving the changes you've made.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/lineage/add-upstream.png"/>
</p>

#### Removing Lineage Edges

From the same modal that you add new lineage edges, you can remove them as well. Find the edge(s) that you want to remove, and click the "X" on the right side of it. And just like adding, you need to click "Save Changes" to save and if you exit without saving, your changes won't be applied.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/lineage/remove-lineage-edge.png"/>
</p>

#### Reviewing Changes

Any time lineage is edited manually through the UI, we keep track of who made the change and when they made it. You can see this information in the modal where you add and remove edges. If an edge was added manually, a user avatar will be in line with the edge that was added. You can hover over this avatar in order to see who added it and when.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/lineage/lineage-edge-audit-stamp.png"/>
</p>

### Edit from Lineage Tab

The other place that you can edit lineage for entities is from the Lineage Tab on an entity's profile. Click on the "Lineage" tab in an entity's profile and then find the "Edit" dropdown that allows you to edit upstream or downstream lineage for the given entity.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/lineage/edit-from-lineage-tab.png"/>
</p>

Using the modal from this view will work the same as described above for editing from the Lineage Visualization screen.

:::note

If you edit lineage manually through the UI but have lineage automatically extracted in ingestion for the entities you edited, your edits from the UI may be overwritten when ingestion runs again.

:::

## Additional Resources

### Videos

**DataHub November Town Hall - Including Manual Lineage Demo**

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
