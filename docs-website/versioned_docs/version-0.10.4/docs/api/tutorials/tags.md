---
title: Tags
slug: /api/tutorials/tags
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/api/tutorials/tags.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Tags

## Why Would You Use Tags on Datasets?

Tags are informal, loosely controlled labels that help in search & discovery. They can be added to datasets, dataset schemas, or containers, for an easy way to label or categorize entities – without having to associate them to a broader business glossary or vocabulary.
For more information about tags, refer to [About DataHub Tags](/docs/tags.md).

### Goal Of This Guide

This guide will show you how to

- Create: create a tag.
- Read : read tags attached to a dataset.
- Add: add a tag to a column of a dataset or a dataset itself.
- Remove: remove a tag from a dataset.

## Prerequisites

For this tutorial, you need to deploy DataHub Quickstart and ingest sample data.
For detailed information, please refer to [Datahub Quickstart Guide](/docs/quickstart.md).

:::note
Before modifying tags, you need to ensure the target dataset is already present in your DataHub instance.
If you attempt to manipulate entities that do not exist, your operation will fail.
In this guide, we will be using data from sample ingestion.
:::

For more information on how to set up for GraphQL, please refer to [How To Set Up GraphQL](/docs/api/graphql/how-to-set-up-graphql.md).

## Create Tags

The following code creates a tag `Deprecated`.

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```json
mutation createTag {
    createTag(input:
    {
      name: "Deprecated",
      id: "deprecated",
      description: "Having this tag means this column or table is deprecated."
    })
}
```

If you see the following response, the operation was successful:

```python
{
  "data": {
    "createTag": "urn:li:tag:deprecated"
  },
  "extensions": {}
}
```

</TabItem>

<TabItem value="curl" label="Curl">

```shell
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{ "query": "mutation createTag { createTag(input: { name: \"Deprecated\", id: \"deprecated\",description: \"Having this tag means this column or table is deprecated.\" }) }", "variables":{}}'
```

Expected Response:

```json
{ "data": { "createTag": "urn:li:tag:deprecated" }, "extensions": {} }
```

</TabItem>

<TabItem value="python" label="Python">

```python
# Inlined from /metadata-ingestion/examples/library/create_tag.py
import logging

from datahub.emitter.mce_builder import make_tag_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

# Imports for metadata model classes
from datahub.metadata.schema_classes import TagPropertiesClass

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

tag_urn = make_tag_urn("deprecated")
tag_properties_aspect = TagPropertiesClass(
    name="Deprecated",
    description="Having this tag means this column or table is deprecated.",
)

event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
    entityUrn=tag_urn,
    aspect=tag_properties_aspect,
)

# Create rest emitter
rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
rest_emitter.emit(event)
log.info(f"Created tag {tag_urn}")

```

</TabItem>
</Tabs>

### Expected Outcome of Creating Tags

You can now see the new tag `Deprecated` has been created.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main//imgs/apis/tutorials/tag-created.png"/>
</p>

We can also verify this operation by programmatically searching `Deprecated` tag after running this code using the `datahub` cli.

```shell
datahub get --urn "urn:li:tag:deprecated" --aspect tagProperties

{
  "tagProperties": {
    "description": "Having this tag means this column or table is deprecated.",
    "name": "Deprecated"
  }
}
```

## Read Tags

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```json
query {
  dataset(urn: "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)") {
    tags {
      tags {
        tag {
          name
          urn
        	properties {
        	  description
        	  colorHex
        	}
        }
      }
    }
  }
}
```

If you see the following response, the operation was successful:

```python
{
  "data": {
    "dataset": {
      "tags": {
        "tags": [
          {
            "tag": {
              "name": "Legacy",
              "urn": "urn:li:tag:Legacy",
              "properties": {
                "description": "Indicates the dataset is no longer supported",
                "colorHex": null,
                "name": "Legacy"
              }
            }
          }
        ]
      }
    }
  },
  "extensions": {}
}
```

</TabItem>
<TabItem value="curl" label="Curl">

```shell
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{ "query": "{dataset(urn: \"urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)\") {tags {tags {tag {name urn properties { description colorHex } } } } } }", "variables":{}}'
```

Expected Response:

```json
{
  "data": {
    "dataset": {
      "tags": {
        "tags": [
          {
            "tag": {
              "name": "Legacy",
              "urn": "urn:li:tag:Legacy",
              "properties": {
                "description": "Indicates the dataset is no longer supported",
                "colorHex": null
              }
            }
          }
        ]
      }
    }
  },
  "extensions": {}
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
# Inlined from /metadata-ingestion/examples/library/dataset_query_tags.py
from datahub.emitter.mce_builder import make_dataset_urn

# read-modify-write requires access to the DataHubGraph (RestEmitter is not enough)
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

# Imports for metadata model classes
from datahub.metadata.schema_classes import GlobalTagsClass

dataset_urn = make_dataset_urn(platform="hive", name="SampleHiveDataset", env="PROD")

gms_endpoint = "http://localhost:8080"
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))

# Query multiple aspects from entity
result = graph.get_aspects_for_entity(
    entity_urn=dataset_urn,
    aspects=["globalTags"],
    aspect_types=[GlobalTagsClass],
)

print(result)

```

</TabItem>
</Tabs>

## Add Tags

### Add Tags to a dataset

The following code shows you how can add tags to a dataset.
In the following code, we add a tag `Deprecated` to a dataset named `fct_users_created`.

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```json
mutation addTags {
    addTags(
      input: {
        tagUrns: ["urn:li:tag:deprecated"],
        resourceUrn: "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)",
      }
    )
}
```

If you see the following response, the operation was successful:

```python
{
  "data": {
    "addTags": true
  },
  "extensions": {}
}
```

</TabItem>
<TabItem value="curl" label="Curl">

```shell
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{ "query": "mutation addTags { addTags(input: { tagUrns: [\"urn:li:tag:deprecated\"], resourceUrn: \"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)\" }) }", "variables":{}}'
```

Expected Response:

```json
{ "data": { "addTags": true }, "extensions": {} }
```

</TabItem>
<TabItem value="python" label="Python">

```python
# Inlined from /metadata-ingestion/examples/library/dataset_add_tag.py
import logging
from typing import Optional

from datahub.emitter.mce_builder import make_dataset_urn, make_tag_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper

# read-modify-write requires access to the DataHubGraph (RestEmitter is not enough)
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

# Imports for metadata model classes
from datahub.metadata.schema_classes import GlobalTagsClass, TagAssociationClass

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# First we get the current tags
gms_endpoint = "http://localhost:8080"
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))

dataset_urn = make_dataset_urn(platform="hive", name="realestate_db.sales", env="PROD")

current_tags: Optional[GlobalTagsClass] = graph.get_aspect(
    entity_urn=dataset_urn,
    aspect_type=GlobalTagsClass,
)

tag_to_add = make_tag_urn("purchase")
tag_association_to_add = TagAssociationClass(tag=tag_to_add)

need_write = False
if current_tags:
    if tag_to_add not in [x.tag for x in current_tags.tags]:
        # tags exist, but this tag is not present in the current tags
        current_tags.tags.append(TagAssociationClass(tag_to_add))
        need_write = True
else:
    # create a brand new tags aspect
    current_tags = GlobalTagsClass(tags=[tag_association_to_add])
    need_write = True

if need_write:
    event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=current_tags,
    )
    graph.emit(event)
    log.info(f"Tag {tag_to_add} added to dataset {dataset_urn}")

else:
    log.info(f"Tag {tag_to_add} already exists, omitting write")

```

</TabItem>
</Tabs>

### Add Tags to a Column of a dataset

<Tabs>
<TabItem value="graphql" label="GraphQL">

```json
mutation addTags {
    addTags(
      input: {
        tagUrns: ["urn:li:tag:deprecated"],
        resourceUrn: "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)",
        subResourceType:DATASET_FIELD,
        subResource:"user_name"})
}
```

</TabItem>
<TabItem value="curl" label="Curl">

```shell
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{ "query": "mutation addTags { addTags(input: { tagUrns: [\"urn:li:tag:deprecated\"], resourceUrn: \"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)\", subResourceType: DATASET_FIELD, subResource: \"user_name\" }) }", "variables":{}}'
```

Expected Response:

```json
{ "data": { "addTags": true }, "extensions": {} }
```

</TabItem>
<TabItem value="python" label="Python">

```python
# Inlined from /metadata-ingestion/examples/library/dataset_add_column_tag.py
import logging
import time

from datahub.emitter.mce_builder import make_dataset_urn, make_tag_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper

# read-modify-write requires access to the DataHubGraph (RestEmitter is not enough)
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

# Imports for metadata model classes
from datahub.metadata.schema_classes import (
    AuditStampClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    GlobalTagsClass,
    TagAssociationClass,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def get_simple_field_path_from_v2_field_path(field_path: str) -> str:
    """A helper function to extract simple . path notation from the v2 field path"""
    if not field_path.startswith("[version=2.0]"):
        # not a v2, we assume this is a simple path
        return field_path
        # this is a v2 field path
    tokens = [
        t for t in field_path.split(".") if not (t.startswith("[") or t.endswith("]"))
    ]

    return ".".join(tokens)


# Inputs -> the column, dataset and the tag to set
column = "user_name"
dataset_urn = make_dataset_urn(platform="hive", name="fct_users_created", env="PROD")
tag_to_add = make_tag_urn("deprecated")


# First we get the current editable schema metadata
gms_endpoint = "http://localhost:8080"
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))


current_editable_schema_metadata = graph.get_aspect(
    entity_urn=dataset_urn,
    aspect_type=EditableSchemaMetadataClass,
)


# Some pre-built objects to help all the conditional pathways
tag_association_to_add = TagAssociationClass(tag=tag_to_add)
tags_aspect_to_set = GlobalTagsClass(tags=[tag_association_to_add])
field_info_to_set = EditableSchemaFieldInfoClass(
    fieldPath=column, globalTags=tags_aspect_to_set
)


need_write = False
field_match = False
if current_editable_schema_metadata:
    for fieldInfo in current_editable_schema_metadata.editableSchemaFieldInfo:
        if get_simple_field_path_from_v2_field_path(fieldInfo.fieldPath) == column:
            # we have some editable schema metadata for this field
            field_match = True
            if fieldInfo.globalTags:
                if tag_to_add not in [x.tag for x in fieldInfo.globalTags.tags]:
                    # this tag is not present
                    fieldInfo.globalTags.tags.append(tag_association_to_add)
                    need_write = True
            else:
                fieldInfo.globalTags = tags_aspect_to_set
                need_write = True

    if not field_match:
        # this field isn't present in the editable schema metadata aspect, add it
        field_info = field_info_to_set
        current_editable_schema_metadata.editableSchemaFieldInfo.append(field_info)
        need_write = True

else:
    # create a brand new editable schema metadata aspect
    now = int(time.time() * 1000)  # milliseconds since epoch
    current_timestamp = AuditStampClass(time=now, actor="urn:li:corpuser:ingestion")
    current_editable_schema_metadata = EditableSchemaMetadataClass(
        editableSchemaFieldInfo=[field_info_to_set],
        created=current_timestamp,
    )
    need_write = True

if need_write:
    event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=current_editable_schema_metadata,
    )
    graph.emit(event)
    log.info(f"Tag {tag_to_add} added to column {column} of dataset {dataset_urn}")

else:
    log.info(f"Tag {tag_to_add} already attached to column {column}, omitting write")

```

</TabItem>
</Tabs>

### Expected Outcome of Adding Tags

You can now see `Deprecated` tag has been added to `user_name` column.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main//imgs/apis/tutorials/tag-added.png"/>
</p>

We can also verify this operation programmatically by checking the `globalTags` aspect using the `datahub` cli.

```shell
datahub get --urn "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)" --aspect globalTags

```

## Remove Tags

The following code remove a tag from a dataset.
After running this code, `Deprecated` tag will be removed from a `user_name` column.

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```json
mutation removeTag {
    removeTag(
      input: {
        tagUrn: "urn:li:tag:deprecated",
        resourceUrn: "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)",
        subResourceType:DATASET_FIELD,
        subResource:"user_name"})
}
```

</TabItem>
<TabItem value="curl" label="Curl">

```shell
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{ "query": "mutation removeTag { removeTag(input: { tagUrn: \"urn:li:tag:deprecated\", resourceUrn: \"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)\" }) }", "variables":{}}'
```

</TabItem>
<TabItem value="python" label="Python">

```python
# Inlined from /metadata-ingestion/examples/library/dataset_remove_tag_execute_graphql.py
# read-modify-write requires access to the DataHubGraph (RestEmitter is not enough)
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

gms_endpoint = "http://localhost:8080"
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))

# Query multiple aspects from entity
query = """
mutation removeTag {
    removeTag(
      input: {
        tagUrn: "urn:li:tag:deprecated",
        resourceUrn: "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)",
        subResourceType:DATASET_FIELD,
        subResource:"user_name"})
}
"""
result = graph.execute_graphql(query=query)

print(result)

```

</TabItem>
</Tabs>

### Expected Outcome of Removing Tags

You can now see `Deprecated` tag has been removed to `user_name` column.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main//imgs/apis/tutorials/tag-removed.png"/>
</p>

We can also verify this operation programmatically by checking the `gloablTags` aspect using the `datahub` cli.

```shell
datahub get --urn "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)" --aspect globalTags

{
  "globalTags": {
    "tags": []
  }
}
```
