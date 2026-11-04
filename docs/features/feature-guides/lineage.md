---
description: "Use DataHub data lineage to map how data flows across your organization, from source systems through transformations to consumption."
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';
import ProductTour from '@site/src/components/ProductTour';

# About DataHub Lineage

<FeatureAvailability />

Data lineage is a **map that shows how data flows through your organization.** It details where your data originates, how it travels, and where it ultimately ends up.
This can happen within a single system (like data moving between Snowflake tables) or across various platforms.

Prefer to explore hands-on? Take the interactive tour below, then read on for the details.

<ProductTour name="lineage" title="Lineage" />

With data lineage, you can

- Maintain data integrity
- Simplify and refine complex relationships
- Perform [lineage impact analysis](../../act-on-metadata/impact-analysis.md)
- Propagate metadata (e.g. [documentation](../../automations/docs-propagation.md)) across lineage

## Viewing Lineage

You can view lineage under the **Lineage** tab, in an _Explorer_ visualization or the _Impact Analysis_ tool.
You can also survey an asset's impact analysis in a snap on the entity sidebar.

![](https://raw.githubusercontent.com/datahub-project/static-assets/f19c3f852b9ba370e7db07bc5b90ed620f21125b/imgs/lineage/lineage-tab-v3.png)

By default, the UI shows the latest version of the lineage. The time picker can be used to filter out edges within the latest version to exclude those that were last updated outside of the time window. Selecting time windows in the patch will not show you historical lineages. It will only filter the view of the latest version of the lineage.

![](https://raw.githubusercontent.com/datahub-project/static-assets/f19c3f852b9ba370e7db07bc5b90ed620f21125b/imgs/lineage/lineage-view-v3.png)

In this example, data flows from between different Snowflake tables orchestrated by dbt into Looker views and explores.

### Column Level Lineage

Column-level lineage **tracks changes and movements for each specific data column.** This approach is often contrasted with table-level lineage, which specifies lineage at the table level. Column lineage can be visualized while viewing table-level
lineage by expanding the columns of tables and hovering or clicking on columns with lineage.

![](https://raw.githubusercontent.com/datahub-project/static-assets/f19c3f852b9ba370e7db07bc5b90ed620f21125b/imgs/lineage/column-level-lineage-v3.png)

If there is column-level lineage to hidden assets or the table-level view is getting too busy,
you can visualize lineage focused on a single column by clicking on the breadcrumb on a column:

![](https://raw.githubusercontent.com/datahub-project/static-assets/f19c3f852b9ba370e7db07bc5b90ed620f21125b/imgs/lineage/schema-field-level-lineage-link.png)

![](https://raw.githubusercontent.com/datahub-project/static-assets/f19c3f852b9ba370e7db07bc5b90ed620f21125b/imgs/lineage/schema-field-level-lineage.png)

### Grouping Lineage with Group By

<FeatureAvailability stage="public-beta" />

Large lineage graphs are hard to read when every table is its own node. **Group by** collapses assets that share an attribute into a single group node, so you can see how data flows between platforms, domains, or glossary terms instead of between individual assets.

![](https://raw.githubusercontent.com/datahub-project/static-assets/f19c3f852b9ba370e7db07bc5b90ed620f21125b/imgs/lineage/lineage-groupby.png)

#### Choose a Grouping

Open the lineage view for an asset and choose an option from the **Group by** control. DataHub includes three built-in options, called lenses:

| Lens          | Groups assets by                                                 |
| ------------- | ---------------------------------------------------------------- |
| Platform      | The data platform the asset comes from, such as Snowflake or dbt |
| Domain        | The domain assigned to the asset                                 |
| Glossary Term | The glossary terms applied to the asset                          |

Admins can add their own lenses. See [Managing Lenses](#managing-lenses-admins).

#### Read the Grouped Graph

- **Group nodes** show the name of the value they represent, a color and icon for that value, and the number of member assets.
- **Groups next to the root** (the asset you started from) open automatically, so you can see the first hop of lineage right away.
- **Other groups stay collapsed** until you expand them. Expanding a group reveals the groups that connect to it.
- **Counts can rise while the graph loads.** For a group that is still loading, the count is the number of members found so far. If the walk reaches a server limit, the count becomes a minimum and stops changing.
- **Assets without a value** for the lens are placed in one group that is named after the lens's unmatched name, for example "No domain". Admins set this name when they configure the lens.
- **Schema fields** (columns) are grouped by their parent dataset.

Grouping only changes how the graph is drawn. It does not change the lineage itself.

#### Browse Group Members

![](https://raw.githubusercontent.com/datahub-project/static-assets/f19c3f852b9ba370e7db07bc5b90ed620f21125b/imgs/lineage/lineage-groupby-assetsidebar.png)

Select a group to open the group sidebar. The sidebar lists the assets in that group and lets you:

- Page through the members.
- Search for members by name. Partial-name search needs at least three characters.

Use the sidebar to find a specific asset inside a large group without expanding the whole graph.

#### Filter the Graph

The **Filters** panel narrows the graph to the assets you care about. You can add a condition on any searchable field, such as platform, domain, owner, or tag.

![](https://raw.githubusercontent.com/datahub-project/static-assets/f19c3f852b9ba370e7db07bc5b90ed620f21125b/imgs/lineage/lineage-groupby-filter.png)

Each condition uses one of these operators:

- **is** / **is not**: match or exclude one or more values. Each value in the list shows how many assets it matches.
- **exists** / **does not exist**: match assets that have or lack a value for the field.

Combine conditions with **match all** (every condition must hold) or **match any** (at least one must hold).

Assets that do not match your filters are hopped over. Lineage continues through them, but they are not counted, grouped, or shown as group boundaries. Group counts and members therefore cover only the assets that match your filters.

Filters are saved in the page URL, so you can share a filtered view by copying the link.

##### Pass-Through Filters

A pass-through filter treats matching assets as if they were not there. The graph hops over them, but they are not counted, grouped, or used to split groups. Use pass-through to skip intermediate assets, such as staging tables in a domain, so that the groups show only the assets you want to compare.

Pass-through is available for domain and glossary term filters. Assets with no value for the lens are always passed through.

#### Restricted Groups

If you do not have permission to see the values of a group, the group still appears, but its value and members are hidden. Lineage continues past a restricted group, so you can still see what lies beyond it.

#### Managing Lenses (Admins)

Each **Group by** option is a lens, which you can create, edit, and delete. Only admins can manage lenses. Each lens has these settings:

- **Attribute:** The asset attribute to group by: platform, domain, or glossary term.
- **Values:** Limit the lens to specific values. Use this to group by only the domains or terms you care about.
- **Unmatched name:** The label for the group of assets that have no value for this lens, such as "No domain".
- **Multi-value grouping** (glossary terms only): Decides how an asset with several terms is grouped. See the table below.

##### Grouping Assets with Several Glossary Terms

An asset can have more than one glossary term. Multi-value grouping controls where it appears:

| Option        | Behavior                                                              | Example: an asset tagged `Finance` and `PII`               |
| ------------- | --------------------------------------------------------------------- | ---------------------------------------------------------- |
| `EACH_VALUE`  | The asset appears in one group per term. It is counted in each group. | Counted in the `Finance` group and in the `PII` group.     |
| `COMBINATION` | The asset appears in one group keyed by its full set of terms.        | Counted once, in a `Finance + PII` group.                  |
| `PRIORITY`    | The asset appears only in the group of its highest-ranked term.       | Counted once, in the group of whichever term ranks higher. |

##### Changing a Lens

- **Renaming** a lens is safe. Open views keep working.
- **Changing the attribute, values, or grouping** of a lens affects views that are already open. Those views stop with a "not found" error. Reload the page to start a new view with the updated lens.

#### Use Group By in the API

The grouped lineage view is also available through GraphQL, with the `lineageGroups` query to walk the graph and the `lineageGroupMembers` query to page through a group's members. Both are described in the [GraphQL reference](../../api/graphql/overview.md).

Walks are kept on the server for a short time. If you leave a grouped view idle for a while, start a new walk instead of continuing the old one.

### Data Pipeline Lineage

DataHub also supports visualizing your data pipelines' task relationships, alongside data lineage. On a data pipeline's
entity page, go to the **Lineage** tab to open the visualization. At its center will be the data pipeline node,
represented as a box containing all of its composite tasks. You can click and drag to move each task within the box,
as well as click and drag the data pipeline box itself.

![](https://raw.githubusercontent.com/datahub-project/static-assets/f19c3f852b9ba370e7db07bc5b90ed620f21125b/imgs/lineage/dataflow-lineage-tab.png)

Further, you can leverage DataHub's cross-platform lineage to view the upstreams and downstreams of each task.
The number on the expand lineage button represents how many data-dependence upstreams / downstreams the task has.
After expanding, you can keep expanding lineage further like the standard lineage explorer.

![](https://raw.githubusercontent.com/datahub-project/static-assets/f19c3f852b9ba370e7db07bc5b90ed620f21125b/imgs/lineage/dataflow-lineage-expand.png)

Note that you can only expand one task's upstreams and one task's downstreams at a time, to keep the visualization simple.

![](https://raw.githubusercontent.com/datahub-project/static-assets/f19c3f852b9ba370e7db07bc5b90ed620f21125b/imgs/lineage/dataflow-lineage-expand-2.png)

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
