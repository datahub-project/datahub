import FeatureAvailability from '@site/src/components/FeatureAvailability';

# About DataHub Lineage

<FeatureAvailability />

Data lineage is a **map that shows how data flows through your organization.** It details where your data originates, how it travels, and where it ultimately ends up.
This can happen within a single system (like data moving between Snowflake tables) or across various platforms.

With data lineage, you can

- Maintain data integrity
- Simplify and refine complex relationships
- Perform [lineage impact analysis](../../act-on-metadata/impact-analysis.md)
- Propagate metadata (e.g. [documentation](../../automations/docs-propagation.md)) across lineage

## Viewing Lineage

You can view lineage under the **Lineage** tab, in an _Explorer_ visualization or the _Impact Analysis_ tool.
You can also survey an asset's impact analysis in a snap on the entity sidebar.

<p align="center">
<img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/lineage/lineage-tab-v3.png" />
</p>

By default, the UI shows the latest version of the lineage. The time picker can be used to filter out edges within the latest version to exclude those that were last updated outside of the time window. Selecting time windows in the patch will not show you historical lineages. It will only filter the view of the latest version of the lineage.

<p align="center">
<img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/lineage/lineage-view-v3.png" />
</p>

In this example, data flows from between different Snowflake tables orchestrated by dbt into Looker views and explores.

### Column Level Lineage

Column-level lineage **tracks changes and movements for each specific data column.** This approach is often contrasted with table-level lineage, which specifies lineage at the table level. Column lineage can be visualized while viewing table-level
lineage by expanding the columns of tables and hovering or clicking on columns with lineage.

<p align="center">
<img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/lineage/column-level-lineage-v3.png" />
</p>

If there is column-level lineage to hidden assets or the table-level view is getting too busy,
you can visualize lineage focused on a single column by clicking on the breadcrumb on a column:

<p align="center">
<img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/lineage/schema-field-level-lineage-link.png" />
</p>

<p align="center">
<img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/lineage/schema-field-level-lineage.png" />
</p>

### Data Pipeline Lineage

DataHub also supports visualizing your data pipelines' task relationships, alongside data lineage. On a data pipeline's
entity page, go to the **Lineage** tab to open the visualization. At its center will be the data pipeline node,
represented as a box containing all of its composite tasks. You can click and drag to move each task within the box,
as well as click and drag the data pipeline box itself.

<p align="center">
<img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/lineage/dataflow-lineage-tab.png" />
</p>

Further, you can leverage DataHub's cross-platform lineage to view the upstreams and downstreams of each task.
The number on the expand lineage button represents how many data-dependence upstreams / downstreams the task has.
After expanding, you can keep expanding lineage further like the standard lineage explorer.

<p align="center">
<img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/lineage/dataflow-lineage-expand.png" />
</p>

Note that you can only expand one task's upstreams and one task's downstreams at a time, to keep the visualization simple.

<p align="center">
<img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/lineage/dataflow-lineage-expand-2.png" />
</p>

## Adding Lineage

### Ingestion Source

If you're using an ingestion source that supports extraction of Lineage (e.g. **Table Lineage Capability**), then lineage information can be extracted automatically. The list of which sources support automatic lineage extraction can be found
[here](../../generated/lineage/automatic-lineage-extraction.md).
For detailed instructions, refer to the [source documentation](https://docs.datahub.com/integrations) for the source you are using.

### UI

As of `v0.9.5`, DataHub supports the manual editing of lineage between entities. Data experts are free to add or remove upstream and downstream lineage edges in both the Lineage Visualization screen as well as the Lineage tab on entity pages. Use this feature to supplement automatic lineage extraction or establish important entity relationships in sources that do not support automatic extraction. Editing lineage by hand is supported for Datasets, Charts, Dashboards, and Data Jobs.
Please refer to our [UI Guides on Lineage](./ui-lineage.md) for more information.

:::caution Recommendation on UI-based lineage

Lineage added by hand and programmatically may conflict with one another to cause unwanted overwrites.
It is strongly recommend that lineage is edited manually in cases where lineage information is not also extracted in automated fashion, e.g. by running an ingestion source. If you are going to manually edit lineage for an entity in which lineage is
automatically ingested, see if the appropriate ingestion source supports `incremental_lineage` and if so, consider enabling
that configuration flag. Note that if you do set this flag, lineage edges that used to exist will not be removed, and so
you may need to use time-based filtering to accurately determine current lineage.

:::

### API

If you are not using a Lineage-support ingestion source, you can programmatically emit lineage edges between entities via API.
Please refer to [API Guides on Lineage](../../api/tutorials/lineage.md) for more information.
