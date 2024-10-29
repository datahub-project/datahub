# Managing Data Lineage via UI

## Viewing Data Lineage
The UI shows the latest version of the data lineage. The time picker can be used to filter out edges within the latest version to exclude those that were last updated outside of the time window. Selecting time windows in the patch will not show you historical data lineages. It will only filter the view of the latest version of the lineage. 

## Editing from Lineage Graph View

The first place that you can edit data lineage for entities is from the Lineage Visualization screen. Click on the "Lineage" button on the top right of an entity's profile to get to this view.

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

### Adding Lineage Edges

Once you click "Edit Upstream" or "Edit Downstream," a modal will open that allows you to manage data lineage for the selected entity in the chosen direction. In order to add a lineage edge to a new entity, search for it by name in the provided search bar and select it. Once you're satisfied with everything you've added, click "Save Changes." If you change your mind, you can always cancel or exit without saving the changes you've made.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/lineage/add-upstream.png"/>
</p>

### Removing Lineage Edges

You can remove lineage edges from the same modal used to add lineage edges. Find the edge(s) that you want to remove, and click the "X" on the right side of it. And just like adding, you need to click "Save Changes" to save and if you exit without saving, your changes won't be applied.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/lineage/remove-lineage-edge.png"/>
</p>

### Reviewing Changes

Any time lineage is edited manually, we keep track of who made the change and when they made it. You can see this information in the modal where you add and remove edges. If an edge was added manually, a user avatar will be in line with the edge that was added. You can hover over this avatar in order to see who added it and when.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/lineage/lineage-edge-audit-stamp.png"/>
</p>

## Editing from Lineage Tab

The other place that you can edit lineage for entities is from the Lineage Tab on an entity's profile. Click on the "Lineage" tab in an entity's profile and then find the "Edit" dropdown that allows you to edit upstream or downstream lineage for the given entity.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/lineage/edit-from-lineage-tab.png"/>
</p>

Using the modal from this view will work the same as described above for editing from the Lineage Visualization screen.