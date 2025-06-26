import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Container

## Why Would You Use Containers?

The Container entity represents a logical grouping of entities, such as datasets, data processing instances, or even other containers. It helps users organize and manage metadata in a hierarchical structure, making it easier to navigate and understand relationships between different entities.

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
{{ inline /metadata-ingestion/examples/library/create_container.py show_path_as_comment }}
```

</TabItem>
</Tabs>
