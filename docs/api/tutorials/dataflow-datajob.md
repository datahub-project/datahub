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
{{ inline /metadata-ingestion/examples/library/create_dataflow.py show_path_as_comment }}
```

</TabItem>
</Tabs>

## Create DataJob

DataJob must be associated with a DataFlow. You can create a DataJob by providing the DataFlow object or the DataFlow URN and its platform instance.

<Tabs>
<TabItem value="python" label="Create DataJob with a DataFlow Object" default>
```python
{{ inline /metadata-ingestion/examples/library/create_datajob.py show_path_as_comment }}
```

</TabItem>
<TabItem value="python" label="Create DataJob with DataFlow URN">
```python
{{ inline /metadata-ingestion/examples/library/create_datajob_with_flow_urn.py show_path_as_comment }}
```
</TabItem>
</Tabs>

## Read DataFlow

```python
{{ inline /metadata-ingestion/examples/library/read_dataflow.py show_path_as_comment }}
```

#### Example Output

```python
>> DataFlow name: example_dataflow
>> DataFlow platform: urn:li:dataPlatform:airflow
>> DataFlow description: airflow pipeline for production
```

## Read DataJob

```python
{{ inline /metadata-ingestion/examples/library/read_datajob.py show_path_as_comment }}
```

#### Example Output

```python
>> DataJob name: example_datajob
>> DataJob Flow URN: urn:li:dataFlow:(airflow,PROD.example_dag,PROD)
>> DataJob description: example datajob
```
