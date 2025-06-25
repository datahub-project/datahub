import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Dataset

## Why Would You Use Containers?

The Container entity is used to represent a logical grouping of datasets, such as a database, schema, or folder. It allows users to organize and manage datasets in a hierarchical structure, making it easier to navigate and understand the relationships between different datasets.

### Goal Of This Guide

This guide will show you how to

- Create a container 

## Prerequisites

For this tutorial, you need to deploy DataHub Quickstart and ingest sample data.
For detailed steps, please refer to [Datahub Quickstart Guide](/docs/quickstart.md).

## Create Container

<Tabs>
<TabItem value="python" label="Python" default>

```python
{{ inline /metadata-ingestion/examples/library/dataset_schema.py show_path_as_comment }}
```

</TabItem>
</Tabs>

### Expected Outcomes of Creating Container

You can now see `realestate_db.sales` dataset has been created.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/dataset-created.png"/>
</p>
