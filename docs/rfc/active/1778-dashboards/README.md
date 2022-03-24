- Start Date: 2020-08-03
- RFC PR: https://github.com/datahub-project/datahub/pull/1778
- Implementation PR(s): https://github.com/datahub-project/datahub/pull/1775

# Dashboards

## Summary

Adding support for dashboards (and charts) metadata cataloging and enabling search & discovery for them.
The design should accommodate for different dashboarding ([Looker](https://looker.com), [Redash](https://redash.io/)) tools used within a company.

## Motivation

Dashboards are a key piece within a data ecosystem of a company. They are used by different groups of employees across different organizations.
They provide a way to visualize some data assets (tracking datasets or metrics) by allowing slice and dicing of the input data source.
When a company scales, data assets including dashboards gets richer and bigger. Therefore, it's important to find and access to the right dashboard.

## Goals

By having dashboards as a top-level entity in DataHub, we achieve below goals:

- Enabling Search & Discovery for dashboard assets by using dashboard metadata
- Link dashboards to underlying data sources and have a more complete picture of data lineage

## Non-goals

DataHub will only serve as a catalog for dashboards where users search dashboards by using keywords. 
Entity page for a dashboard might contain links to the dashboard to direct users to view the dashboard after finding it.
However, DataHub will not try to show the actual dashboard or any charts within that. This is not desired and shouldn't be allowed because:

 - Dashboards or charts within a dashboard might have different ACLs that prevent users without the necessary permission to display the dashboard. 
 Generally, the source of truth for these ACLs are dashboarding tools.
 - Underlying data sources might have some ACLs too. Again, the source of truth for these ACLs are specific data platforms.

## Detailed design

![high level design](high_level_design.png)

As shown in the above diagram, dashboards are composed of a collection of charts at a very high level. These charts
could be shared by different dashboards. In the example sketched above, `Chart_1`, `Chart_2` and `Chart_3` are part of
`Dashboard_A` and `Chart_3` and `Chart_4` are part of `Dashboard_B`.

### Entities
There will be 2 top level GMA [entities](../../../what/entity.md) in the design: dashboards and charts.
It's important to make charts as a top level entity because charts could be shared between different dashboards.
We'll need to build `Contains` relationships between Dashboard and Chart entities.

### URN Representation
We'll define two [URNs](../../../what/urn.md): `DashboardUrn` and `ChartUrn`.
These URNs should allow for unique identification for dashboards and charts even there are multiple dashboarding tools
are used within a company. Most of the time, dashboards & charts are given unique ids by the used dashboarding tool.
An example Dashboard URN for Looker will look like below:
```
urn:li:dashboard:(Looker,<<dashboard_id>>)
```
An example Chart URN for Redash will look like below:
```
urn:li:chart:(Redash,<<chart_id>>)
```

### Chart metadata
Dashboarding tools generally have different jargon to denote a chart.
They are called as [Look](https://docs.looker.com/exploring-data/saving-and-editing-looks) in Looker 
and [Visualization](https://redash.io/help/user-guide/visualizations/visualization-types) in Redash.
But, irrespective of the name, charts are the different tiles which exists in a dashboard.
Charts are mainly used for delivering some information visually to make it easily understandable.
They might be using single or multiple data sources and generally have an associated query running against
the underlying data source to generate the data that it will present.

Below is a list of metadata which can be associated with a chart:

- Title
- Description
- Type (Bar chart, Pie chart, Scatter plot etc.)
- Input sources
- Query (and its type)
- Access level (public, private etc.)
- Ownership
- Status (removed or not)
- Audit info (last modified, last refreshed)

### Dashboard metadata
Aside from containing a set of charts, dashboards carry metadata attached to them.
Below is a list of metadata which can be associated with a dashboard:

- Title
- Description
- List of charts
- Access level (public, private etc.)
- Ownership
- Status (removed or not)
- Audit info (last modified, last refreshed)

### Metadata graph

![dashboards_graph](dashboards_graph.png)

An example metadata graph showing complete data lineage picture is shown above.
In this picture, `Dash_A` and `Dash_B` are dashboards, and they are connected to charts through `Contains` edges.
`C1`, `C2`, `C3` and `C4` are charts, and they are connected to underlying datasets through `DownstreamOf` edges.
`D1`, `D2` and `D3` are datasets.

## How we teach this

We should create/update user guides to educate users for:
 - Search & discovery experience (how to find a dashboard in DataHub)
 - Lineage experience (how to find upstream datasets of a dashboard and how to find dashboards generated from a dataset)

## Rollout / Adoption Strategy

The design is supposed to be generic enough that any user of the DataHub should easily be able
to onboard their dashboard metadata to DataHub irrespective of their dashboarding platform.

Only thing users will need to do is to write an ETL script customized for their 
dashboarding platform (if it's not already provided in DataHub repo). This ETL script will:
 - Extract the metadata for all available dashboards and charts using the APIs of the dashboarding platform
 - Construct and emit this metadata in the form of [MCEs](../../../what/mxe.md)

## Unresolved questions (To-do)
 
1. We'll be adding social features like subscribe and follow later on. However, it's out of scope for this RFC.