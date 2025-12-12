---
title: Container
slug: /api/tutorials/container
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/api/tutorials/container.md
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Container

## Why Would You Use Containers?

The Container entity represents a logical grouping of entities, such as datasets, data processing instances, or even other containers. It helps users organize and manage metadata in a hierarchical structure, making it easier to navigate and understand relationships between different entities.

#### How is a Container related to other entities?

1. **Parent-Child Relationship** : Containers can contain other entities such as datasets, charts, dashboards, data jobs, ML models, and even other containers (nested containers). For example, a dataset can have a container aspect that links it to the schema or folder (container) it belongs to.

2. **Hierarchical Organization** : Containers can be nested, forming a hierarchy (e.g., a database container contains schema containers, which contain table containers). This enables a folder-like browsing experience in the DataHub UI.

3. **Relationships in the Metadata Model** : Many entities (datasets, data jobs, ML models, etc.) have a container aspect that links them to their parent container. Containers themselves can have a parentContainer aspect for nesting.

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
# Inlined from /metadata-ingestion/examples/library/create_container.py
from datahub.emitter.mcp_builder import DatabaseKey
from datahub.sdk import Container, DataHubClient

client = DataHubClient.from_env()

container = Container(
    container_key=DatabaseKey(platform="snowflake", database="my_database"),
    display_name="MY_DATABASE",
)

client.entities.upsert(container)

```

</TabItem>
</Tabs>
