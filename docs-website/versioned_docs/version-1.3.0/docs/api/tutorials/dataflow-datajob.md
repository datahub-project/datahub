---
title: DataFlow & DataJob
slug: /api/tutorials/dataflow-datajob
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/api/tutorials/dataflow-datajob.md
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# DataFlow & DataJob

## Why Would You Use DataFlow and DataJob?

The DataFlow and DataJob entities are used to represent data processing pipelines and jobs within a data ecosystem. They allow users to define, manage, and monitor the flow of data through various stages of processing, from ingestion to transformation and storage.

### Goal Of This Guide

This guide will show you how to

- Create a DataFlow.
- Create a Datajob with a DataFlow.

## Prerequisites

For this tutorial, you need to deploy DataHub Quickstart and ingest sample data.
For detailed steps, please refer to [Datahub Quickstart Guide](/docs/quickstart.md).

## Create DataFlow

<Tabs>
<TabItem value="python" label="Python" default>

```python
# Inlined from /metadata-ingestion/examples/library/create_dataflow.py
from datahub.metadata.urns import TagUrn
from datahub.sdk import DataFlow, DataHubClient

client = DataHubClient.from_env()

dataflow = DataFlow(
    name="example_dataflow",
    platform="airflow",
    description="airflow pipeline for production",
    tags=[TagUrn(name="production"), TagUrn(name="data_engineering")],
)

client.entities.upsert(dataflow)

```

</TabItem>
</Tabs>

## Create DataJob

DataJob must be associated with a DataFlow. You can create a DataJob by providing the DataFlow object or the DataFlow URN and its platform instance.

<Tabs>
<TabItem value="python" label="Create DataJob with a DataFlow Object" default>
```python
# Inlined from /metadata-ingestion/examples/library/create_datajob.py
from datahub.metadata.urns import DatasetUrn, TagUrn
from datahub.sdk import DataFlow, DataHubClient, DataJob

client = DataHubClient.from_env()

# datajob will inherit the platform and platform instance from the flow

dataflow = DataFlow(
    platform="airflow",
    name="example_dag",
    platform_instance="PROD",
    description="example dataflow",
    tags=[TagUrn(name="tag1"), TagUrn(name="tag2")],
)

datajob = DataJob(
    name="example_datajob",
    flow=dataflow,
    inlets=[
        DatasetUrn(platform="hdfs", name="dataset1", env="PROD"),
    ],
    outlets=[
        DatasetUrn(platform="hdfs", name="dataset2", env="PROD"),
    ],
)

client.entities.upsert(dataflow)
client.entities.upsert(datajob)

```

</TabItem>
<TabItem value="python" label="Create DataJob with DataFlow URN">
```python
# Inlined from /metadata-ingestion/examples/library/create_datajob_with_flow_urn.py
from datahub.metadata.urns import DataFlowUrn, DatasetUrn
from datahub.sdk import DataHubClient, DataJob

client = DataHubClient.from_env()

# datajob will inherit the platform and platform instance from the flow

datajob = DataJob(
    name="example_datajob",
    flow_urn=DataFlowUrn(
        orchestrator="airflow",
        flow_id="example_dag",
        cluster="PROD",
    ),
    platform_instance="PROD",
    inlets=[
        DatasetUrn(platform="hdfs", name="dataset1", env="PROD"),
    ],
    outlets=[
        DatasetUrn(platform="hdfs", name="dataset2", env="PROD"),
    ],
)

client.entities.upsert(datajob)

```
</TabItem>
</Tabs>

## Read DataFlow

```python
# Inlined from /metadata-ingestion/examples/library/read_dataflow.py
from datahub.sdk import DataFlowUrn, DataHubClient

client = DataHubClient.from_env()

# Or get this from the UI (share -> copy urn) and use DataFlowUrn.from_string(...)
dataflow_urn = DataFlowUrn("airflow", "example_dataflow_id")

dataflow_entity = client.entities.get(dataflow_urn)
print("DataFlow name:", dataflow_entity.name)
print("DataFlow platform:", dataflow_entity.platform)
print("DataFlow description:", dataflow_entity.description)

```

#### Example Output

```python
>> DataFlow name: example_dataflow
>> DataFlow platform: urn:li:dataPlatform:airflow
>> DataFlow description: airflow pipeline for production
```

## Read DataJob

```python
# Inlined from /metadata-ingestion/examples/library/read_datajob.py
from datahub.sdk import DataHubClient, DataJobUrn

client = DataHubClient.from_env()

# Or get this from the UI (share -> copy urn) and use DataJobUrn.from_string(...)
datajob_urn = DataJobUrn("airflow", "example_dag", "example_datajob_id")

datajob_entity = client.entities.get(datajob_urn)
print("DataJob name:", datajob_entity.name)
print("DataJob Flow URN:", datajob_entity.flow_urn)
print("DataJob description:", datajob_entity.description)

```

#### Example Output

```python
>> DataJob name: example_datajob
>> DataJob Flow URN: urn:li:dataFlow:(airflow,PROD.example_dag,PROD)
>> DataJob description: example datajob
```
