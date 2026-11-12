---
title: Creating Custom Lenses
description: "Define a custom lineage lens with the Python SDK to group by a specific set of glossary terms, domains, or platforms."
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Creating Custom Lenses

<FeatureAvailability saasOnly stage="public-beta"/>

:::caution Public Beta
Group by is in Public Beta. Features, UI, and configuration options are subject to change.
:::

The built-in lenses group by every platform, domain, or glossary term. Custom lenses are useful when only a subset of values is relevant. For example, if your glossary has hundreds of terms but you want to see how data moves between the Bronze, Silver, and Gold tiers, a custom lens can group by just those three terms.

A **custom lens** appears as an additional option under **Group by** for all users. It defines the attribute to group by, which values to consider, how to group assets with multiple values, and optionally a group for assets with none of the values.

<p align="center">
  <img width="90%" src="/imgs/lineage-group-by/lineage-custom-lens.png" alt="Lineage grouped by a custom Medallion tier lens"/>
</p>

In the example above, a _Medallion tier_ lens shows raw files and tables (Bronze) feeding a Silver model and its Gold outputs. The sources, jobs, and dashboards without a tier are grouped as _Untiered_.

Available in DataHub Cloud **v2.4.0** and later.

## Lens Definition

Every lens, including the three built-in lenses, is a `lineageLens` entity with a `lineageLensInfo` aspect. The built-in lenses are `urn:li:lineageLens:platform`, `urn:li:lineageLens:domain`, and `urn:li:lineageLens:glossary_term`.

| Field                | Required            | Default      | Description                                                                                                                                                                                                 |
| -------------------- | ------------------- | ------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `name`               | Yes                 | —            | The name displayed under **Group by**.                                                                                                                                                                      |
| `description`        | No                  | —            | A description of the lens.                                                                                                                                                                                  |
| `groupBy`            | Yes                 | —            | The attribute to group by: `PLATFORM`, `DOMAIN`, or `GLOSSARY_TERM`.                                                                                                                                        |
| `values`             | Only for `PRIORITY` | All values   | The values the lens considers; all other values are ignored. Each value must be a URN of the `groupBy` type: `urn:li:dataPlatform:…`, `urn:li:domain:…`, or `urn:li:glossaryTerm:…`. Maximum of 100 values. |
| `multiValueGrouping` | No                  | `EACH_VALUE` | How to group assets with multiple considered values. See [Assets with Multiple Values](#assets-with-multiple-values).                                                                                       |
| `unmatchedName`      | No                  | —            | If set, assets with none of the considered values are collected in a group with this name. Otherwise, they are passed through. See [Collecting Unmatched Assets](#collecting-unmatched-assets).             |
| `created`            | Yes                 | —            | The creator of the lens and the creation time.                                                                                                                                                              |
| `lastModified`       | Yes                 | —            | The user who last modified the lens and the modification time.                                                                                                                                              |

The lens ID (the part of the URN after `urn:li:lineageLens:`) appears in shared links as `group_by=<id>`. We recommend a short, readable ID such as `medallion-tier`.

## Permissions

Creating, editing, and deleting lenses requires the **Manage Lineage Lenses** platform privilege, which is granted to the Admin role by default. All lenses are available to every user who can view lineage.

## Creating a Lens

Lenses are created with the Python SDK (`acryl-datahub`). The following example creates a lens that groups lineage by Medallion tier:

```python
import time

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
    AuditStampClass,
    LineageLensGroupByClass,
    LineageLensInfoClass,
    LineageLensMultiValueGroupingClass,
)
from datahub.metadata.urns import LineageLensUrn

graph = DataHubGraph(
    DatahubClientConfig(server="https://<your-instance>.acryl.io/gms", token="<your-access-token>")
)

stamp = AuditStampClass(time=int(time.time() * 1000), actor="urn:li:corpuser:<your-username>")

lens = LineageLensInfoClass(
    name="Medallion tier",
    description="Groups lineage by Bronze, Silver and Gold tier.",
    groupBy=LineageLensGroupByClass.GLOSSARY_TERM,
    values=[
        "urn:li:glossaryTerm:tier.gold",
        "urn:li:glossaryTerm:tier.silver",
        "urn:li:glossaryTerm:tier.bronze",
    ],
    multiValueGrouping=LineageLensMultiValueGroupingClass.PRIORITY,
    unmatchedName="Untiered",
    created=stamp,
    lastModified=stamp,
)

graph.emit_mcp(
    MetadataChangeProposalWrapper(entityUrn=str(LineageLensUrn("medallion-tier")), aspect=lens)
)
```

The lens appears under **Group by** the next time a lineage graph is loaded. Emitting a lens with an existing URN replaces its definition, so the same code can be used to update a lens.

## Assets with Multiple Values

`multiValueGrouping` determines how assets with more than one considered value are grouped. It only applies to the `GLOSSARY_TERM` attribute, since an asset has only one platform and one domain.

| Value         | Asset Is Placed In                                                       | Example: Asset Tagged _Gold_ and _Silver_ |
| ------------- | ------------------------------------------------------------------------ | ----------------------------------------- |
| `EACH_VALUE`  | One group per value, and is counted in each.                             | _Gold_ and _Silver_                       |
| `COMBINATION` | One group for its exact combination of values.                           | _Gold & Silver_                           |
| `PRIORITY`    | Only the group of its highest-ranked value, ranked by order in `values`. | _Gold_, if _Gold_ is listed first         |

## Collecting Unmatched Assets

If `unmatchedName` is not set, assets with none of the lens's values are passed through: they are not displayed or counted, but lineage continues through them. The graph only shows the values defined by the lens.

If `unmatchedName` is set, these assets are collected in a separate group with that name, showing where they sit in the lineage. Click the group to see which values its assets are missing.

## Examples

### Data Quality Tiers (PRIORITY)

Suppose each asset should have one of the _Gold_, _Silver_, or _Bronze_ terms, but some have two (e.g. after being promoted) and some have none. The lens in [Creating a Lens](#creating-a-lens) places each asset in a single group for its highest tier, and collects the rest under _Untiered_.

- An asset tagged _Gold_ and _Silver_ is placed in **Gold**.
- Assets with none of the three terms, including assets with other glossary terms, are placed in **Untiered**.
- Without `unmatchedName`, sources and dashboards without a tier are passed through, and the graph shows **Bronze → Silver → Gold**.

### Conflicting Tiers (COMBINATION)

To find assets that were mistakenly tagged with more than one tier, group by each exact combination of values:

```python
lens = LineageLensInfoClass(
    name="Tier conflicts",
    groupBy=LineageLensGroupByClass.GLOSSARY_TERM,
    values=[
        "urn:li:glossaryTerm:tier.gold",
        "urn:li:glossaryTerm:tier.silver",
        "urn:li:glossaryTerm:tier.bronze",
    ],
    multiValueGrouping=LineageLensMultiValueGroupingClass.COMBINATION,
    unmatchedName="No tier",
    created=stamp,
    lastModified=stamp,
)
```

Correctly tagged assets are grouped into **Gold**, **Silver**, and **Bronze**. Assets with conflicting tiers appear in groups such as **Gold & Silver**, and the group's sidebar lists the affected assets.

### A Subset of Domains (EACH_VALUE)

To view how data moves between three specific domains, e.g. the domains on a critical business process:

```python
lens = LineageLensInfoClass(
    name="Order-to-cash",
    groupBy=LineageLensGroupByClass.DOMAIN,
    values=["urn:li:domain:sales", "urn:li:domain:fulfillment", "urn:li:domain:finance"],
    created=stamp,
    lastModified=stamp,
)
```

Assets in other domains are passed through, so the graph only shows how data moves between these three domains.

## Deleting a Lens

Soft delete a lens to remove it from **Group by**:

```python
graph.soft_delete_entity(str(LineageLensUrn("medallion-tier")))
```

Shared links that specify a deleted lens open the ungrouped graph. The built-in lenses can be deleted the same way, and are not recreated on upgrade.

## Troubleshooting

**A lens does not appear under Group by.** Check the following:

- Group by is enabled. See [Enabling This Feature](overview.md#enabling-this-feature).
- The lens is not soft deleted.
- The lens definition is valid. GMS skips invalid lenses and logs the reason. Common causes include a value that is not a URN of the lens's type (e.g. a domain URN in a `GLOSSARY_TERM` lens), more than 100 values, or `PRIORITY` without `values`.

**A value is displayed as a URN instead of a name.** The glossary term or domain no longer exists, or the viewer does not have permission to view it. Value names are loaded with the viewer's permissions.
