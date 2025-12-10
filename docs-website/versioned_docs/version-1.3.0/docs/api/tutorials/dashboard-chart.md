---
title: Dashboard & Chart
slug: /api/tutorials/dashboard-chart
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/api/tutorials/dashboard-chart.md
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Dashboard & Chart

## Why Would You Use Dashboards and Charts?

The dashboard and chart entities are used to represent visualizations of data, typically in the context of business intelligence or analytics platforms. They allow users to create, manage, and share visual representations of data insights.

### Goal Of This Guide

This guide will show you how to

- Create a dashboard and a chart.
- Link the dashboard to the chart or another dashboard.
- Read dashboard and chart entities.

## Prerequisites

For this tutorial, you need to deploy DataHub Quickstart and ingest sample data.
For detailed steps, please refer to [Datahub Quickstart Guide](/docs/quickstart.md).

## Create Chart

```python
# Inlined from /metadata-ingestion/examples/library/create_chart.py
from datahub.metadata.urns import TagUrn
from datahub.sdk import Chart, DataHubClient

client = DataHubClient.from_env()

chart = Chart(
    name="example_chart",
    platform="looker",
    description="looker chart for production",
    tags=[TagUrn(name="production"), TagUrn(name="data_engineering")],
)

client.entities.upsert(chart)

```

### Link Chart with Datasets

You can associate datasets with the chart by providing the dataset URN in the `input_datasets` parameter. This will create lineage between the chart and the datasets, so you can track the data sources used by the chart.

```python
# Inlined from /metadata-ingestion/examples/library/create_chart_complex.py
from datahub.metadata.urns import TagUrn
from datahub.sdk import Chart, DataHubClient, Dataset

client = DataHubClient.from_env()

input_datasets = [
    Dataset(
        name="example_dataset",
        platform="snowflake",
        description="looker dataset for production",
        schema=[("id", "string"), ("name", "string")],
    ),
    Dataset(
        name="example_dataset_2",
        platform="snowflake",
        description="looker dataset for production",
        schema=[("id", "string"), ("name", "string")],
    ),
    Dataset(
        name="example_dataset_3",
        platform="snowflake",
        description="looker dataset for production",
        schema=[("id", "string"), ("name", "string")],
    ),
]

# create a chart with two input datasets
chart = Chart(
    name="example_chart",
    platform="looker",
    description="looker chart for production",
    tags=[TagUrn(name="production"), TagUrn(name="data_engineering")],
    input_datasets=[input_datasets[0], input_datasets[1]],
)

for dataset in input_datasets:
    client.entities.upsert(dataset)

# add a new dataset to the chart
chart.add_input_dataset(input_datasets[2])
client.entities.upsert(chart)

```

## Create Dashboard

```python
# Inlined from /metadata-ingestion/examples/library/create_dashboard.py
from datahub.metadata.urns import TagUrn
from datahub.sdk import Dashboard, DataHubClient

client = DataHubClient.from_env()

dashboard = Dashboard(
    name="example_dashboard",
    platform="looker",
    description="looker dashboard for production",
    tags=[TagUrn(name="production"), TagUrn(name="data_engineering")],
)

client.entities.upsert(dashboard)

```

### Link Dashboard with Charts, Dashboards, and Datasets

You can associate charts, dashboards, and datasets with the dashboard by providing their URNs in the `charts`, `dashboards`, and `input_datasets` parameters, respectively. This will create lineage between the dashboard and the associated entities.

```python
# Inlined from /metadata-ingestion/examples/library/create_dashboard_complex.py
from datahub.metadata.urns import TagUrn
from datahub.sdk import Chart, Dashboard, DataHubClient, Dataset

client = DataHubClient.from_env()
dashboard1 = Dashboard(
    name="example_dashboard_2",
    platform="looker",
    description="looker dashboard for production",
)
chart = Chart(
    name="example_chart",
    platform="looker",
    description="looker chart for production",
)

input_dataset = Dataset(
    name="example_dataset5",
    platform="snowflake",
    description="snowflake dataset for production",
)


dashboard2 = Dashboard(
    name="example_dashboard",
    platform="looker",
    description="looker dashboard for production",
    tags=[TagUrn(name="production"), TagUrn(name="data_engineering")],
    input_datasets=[input_dataset.urn],
    charts=[chart.urn],
    dashboards=[dashboard1.urn],
)


client.entities.upsert(dashboard1)
client.entities.upsert(chart)
client.entities.upsert(input_dataset)

client.entities.upsert(dashboard2)

```

## Read Chart

```python
# Inlined from /metadata-ingestion/examples/library/read_chart.py
from datahub.sdk import ChartUrn, DataHubClient

client = DataHubClient.from_env()

# Or get this from the UI (share -> copy urn) and use ChartUrn.from_string(...)
chart_urn = ChartUrn("looker", "example_chart_id")

chart_entity = client.entities.get(chart_urn)
print("Chart name:", chart_entity.name)
print("Chart platform:", chart_entity.platform)
print("Chart description:", chart_entity.description)

```

#### Expected Output

```python
>> Chart name: example_chart
>> Chart platform: urn:li:dataPlatform:looker
>> Chart description: looker chart for production
```

## Read Dashboard

```python
# Inlined from /metadata-ingestion/examples/library/read_dashboard.py
from datahub.sdk import DashboardUrn, DataHubClient

client = DataHubClient.from_env()

# Or get this from the UI (share -> copy urn) and use DashboardUrn.from_string(...)
dashboard_urn = DashboardUrn("looker", "example_dashboard_id")

dashboard_entity = client.entities.get(dashboard_urn)
print("Dashboard name:", dashboard_entity.name)
print("Dashboard platform:", dashboard_entity.platform)
print("Dashboard description:", dashboard_entity.description)

```

#### Expected Output

```python
>> Dashboard name: example_dashboard
>> Dashboard platform: urn:li:dataPlatform:looker
>> Dashboard description: looker dashboard for production
```
