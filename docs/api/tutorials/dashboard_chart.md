import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Dashboard & Chart

## Why Would You Use Dashboards and Charts?

The dashboard and chart entities are used to represent visualizations of data, typically in the context of business intelligence or analytics platforms. They allow users to create, manage, and share visual representations of data insights.

### Goal Of This Guide

This guide will show you how to

- Create a dashboard and a chart.
- Link the dashboard to the chart or another dashboard.

## Prerequisites

For this tutorial, you need to deploy DataHub Quickstart and ingest sample data.
For detailed steps, please refer to [Datahub Quickstart Guide](/docs/quickstart.md).

## Create Dashboard

<Tabs>
<TabItem value="python" label="Python" default>

```python
{{ inline /metadata-ingestion/examples/library/dataset_schema.py show_path_as_comment }}
```

</TabItem>
</Tabs>

#### Expected Outcomes of Creating Dataset

You can now see `realestate_db.sales` dataset has been created.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/dataset-created.png"/>
</p>

## Create Chart 
