---
description: This page provides an overview of the Data Health Dashboard
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Data Health Dashboard

<FeatureAvailability saasOnly />

## What Is the Data Health Dashboard

The Data Health Dashboard aims to solve two critical use cases:

1. Triaging Data Quality Issues
2. Understanding Broader Data Quality Coverage and Trends

You can access it via the Sidebar Nav. It can be found under the _Observe_ section.

<p align="left">
  <img width="80%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/observe/data-health/overview.png"/>
</p>

## How to use it

### Assertions Tab

There are two ways to slice assertions:

1. By Assertion
2. By Table

**When to use `By Assertion`**
This view presents an activity log of assertion runs, sorted by the last time a given assertion ran. This is incredibly valuable for triaging and detecting trends in the data quality checks.
For instance, by applying a time range filter (i.e., `Last 7 Days`), and setting `Results` to `At least one failure`, you can quickly see which assertions have failed at least once in the last 7 days. Furthermore, you'll be able to see how often their failing, relative to how often they're running, enabling you to quickly find and investigate flaky checks.

**When to use `By Table`**
This view presents a list of tables that have at least one assertion that has ran on it. It is sorted by the last time any assertion ran on that table. The health dots indicate the last status of an assertion of that given type on the table.
This view is incredibly useful for understanding monitoring coverage across your team's tables.

### Incidents Tab

The incidents tab presents the tables that have active incidents open against them. It is sorted by the last time an incident activity was reported on the given table.
At a glance, you can grasp how many incidents are open against any given table, see which incident last had updates on that table, and who owns it.

**Coming soon:** In the future we'll be introducing high-level visual cards giving useful statistics on table coverage, time to resolution, and more.
We will also be introducing a timeline view of assertion failures over a given time period. Our hope is to make it even easier to detect trends in data quality failures at a single glance.

## Personalizing

We understand that each team, and perhaps even an individual may care about a different subset of data than others.
For this reason, we have included a broad range of filters to make it easy to drill down the Dashboard to the specific subset of data you care about. You can filter by:

1. Dataset Owner
2. Dataset Domain
3. Dataset Tags
   ...and much more.

In addition, both the `By Tables` tab and the `Incidents` tab will apply your global `View` (managed via the search bar on the very top of DataHub's navigation). So if you already have a view created for your team, these tabs will automatically filter the reports down to the subset of data only you care about.

<p align="left">
  <img width="80%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/observe/data-health/view-applied.png"/>
</p>

**Coming soon:** in the upcoming releases we will be including the filters in the url parameters. This will make it incredibly easy for you to bookmark your specifi c
