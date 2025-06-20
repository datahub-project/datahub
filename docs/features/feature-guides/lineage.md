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

You can view lineage under the **Lineage** tab, in an *Explorer* visualization or the *Impact Analysis* tool.
You can also survey an asset's impact analysis in a snap on the entity sidebar.

<p align="center">
<img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/lineage/lineage-tab-v3.png" />
</p>

By default, the UI shows the latest version of the lineage. The time picker can be used to filter out edges within the latest version to exclude those that were last updated outside of the time window. Selecting time windows in the patch will not show you historical lineages. It will only filter the view of the latest version of the lineage.

<p align="center">
<img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/lineage/lineage-view-v3.png" />
</p>

In this example, data flows from between different Snowflake tables orchestrated by dbt into Looker views and explores.

## Column Level Lineage Support

Column-level lineage **tracks changes and movements for each specific data column.** This approach is often contrasted with table-level lineage, which specifies lineage at the table level. Column lineage can be visualized while viewing table-level
lineage by expanding the columns of tables and hovering or clicking on columns with lineage.

<p align="center">
<img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/lineage/column-level-lineage-v3.png" />
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
