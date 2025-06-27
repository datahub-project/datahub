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
{{ inline /metadata-ingestion/examples/library/create_chart.py show_path_as_comment }}
```

### Link Chart with Datasets

You can associate datasets with the chart by providing the dataset URN in the `input_datasets` parameter. This will create lineage between the chart and the datasets, so you can track the data sources used by the chart.

```python
{{ inline /metadata-ingestion/examples/library/create_chart_complex.py show_path_as_comment }}
```

## Create Dashboard

```python
{{ inline /metadata-ingestion/examples/library/create_dashboard.py show_path_as_comment }}
```

### Link Dashboard with Charts, Dashboards, and Datasets

You can associate charts, dashboards, and datasets with the dashboard by providing their URNs in the `charts`, `dashboards`, and `input_datasets` parameters, respectively. This will create lineage between the dashboard and the associated entities.

```python
{{ inline /metadata-ingestion/examples/library/create_dashboard_complex.py show_path_as_comment }}
```

## Read Chart

```python
{{ inline /metadata-ingestion/examples/library/read_chart.py show_path_as_comment }}
```

#### Expected Output

```python
>> Chart name: example_chart
>> Chart platform: urn:li:dataPlatform:looker
>> Chart description: looker chart for production
```

## Read Dashboard

```python
{{ inline /metadata-ingestion/examples/library/read_dashboard.py show_path_as_comment }}
```

#### Expected Output

```python
>> Dashboard name: example_dashboard
>> Dashboard platform: urn:li:dataPlatform:looker
>> Dashboard description: looker dashboard for production
```
