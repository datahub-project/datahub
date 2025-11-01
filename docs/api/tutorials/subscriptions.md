import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Subscriptions

<FeatureAvailability saasOnly />

## Why Would You Use Subscriptions on Datasets?

Subscriptions are a way to receive notifications when entity changes occur (e.g. deprecations, schema changes, ownership changes, etc.) or when assertions change state (pass, fail, or error). Subscriptions can be created at the dataset level (affecting any changes on the dataset, as well as all assertions on the dataset) or at the assertion level (affecting only specific assertions).

### Goal Of This Guide

This guide specifically covers how to use the [DataHub Cloud Python SDK](https://pypi.org/project/acryl-datahub-cloud/) for managing Subscriptions:

- Create: create a subscription to a dataset or assertion.
- Remove: remove a subscription.

# Prerequisites

- DataHub Cloud Python SDK installed (`pip install acryl-datahub-cloud`)
- The actor making API calls must have the `Manage User Subscriptions` privilege for the datasets at hand.
- If subscribing to a group, the actor should also be a member of the group.

:::note
Before creating subscriptions, you need to ensure the target datasets and groups are already present in your DataHub instance.
If you attempt to create subscriptions for entities that do not exist, GMS will continuously report errors to the logs.
:::

## Create Subscription

You can create subscriptions to receive notifications when assertions change state (pass, fail, or error) or when other entity changes occur. Subscriptions can be created at the dataset level (affecting any changes on the dataset, as well as all assertions on the dataset) or at the assertion level (affecting only specific assertions).

<Tabs>
<TabItem value="python" label="Python">

```python
{{ inline /metadata-ingestion/examples/library/subscription_create.py show_path_as_comment }}
```

</TabItem>
</Tabs>

## Remove Subscription

You can remove existing subscriptions to stop receiving notifications. The unsubscribe method supports selective removal of specific change types or complete removal of subscriptions.

<Tabs>
<TabItem value="python" label="Python">

```python
{{ inline /metadata-ingestion/examples/library/subscription_remove.py show_path_as_comment }}
```

</TabItem>
</Tabs>

# Available Change Types

The following change types are available for subscriptions:

#### Schema Changes

- `OPERATION_COLUMN_ADDED` - When a new column is added to a dataset
- `OPERATION_COLUMN_REMOVED` - When a column is removed from a dataset
- `OPERATION_COLUMN_MODIFIED` - When an existing column is modified

#### Operational Metadata Changes

- `OPERATION_ROWS_INSERTED` - When rows are inserted into a dataset
- `OPERATION_ROWS_UPDATED` - When rows are updated in a dataset
- `OPERATION_ROWS_REMOVED` - When rows are removed from a dataset

#### Assertion Events

- `ASSERTION_PASSED` - When an assertion run passes
- `ASSERTION_FAILED` - When an assertion run fails
- `ASSERTION_ERROR` - When an assertion run encounters an error

#### Incident Status Changes

- `INCIDENT_RAISED` - When a new incident is raised
- `INCIDENT_RESOLVED` - When an incident is resolved

#### Test Status Changes

- `TEST_PASSED` - When a test passes
- `TEST_FAILED` - When a test fails

#### Deprecation Status Changes

- `DEPRECATED` - When an entity is marked as deprecated
- `UNDEPRECATED` - When an entity's deprecation status is removed

#### Ingestion Status Changes

- `INGESTION_SUCCEEDED` - When ingestion completes successfully
- `INGESTION_FAILED` - When ingestion fails

#### Documentation Changes

- `DOCUMENTATION_CHANGE` - When documentation is modified

#### Ownership Changes

- `OWNER_ADDED` - When an owner is added to an entity
- `OWNER_REMOVED` - When an owner is removed from an entity

#### Glossary Term Changes

- `GLOSSARY_TERM_ADDED` - When a glossary term is added to an entity
- `GLOSSARY_TERM_REMOVED` - When a glossary term is removed from an entity
- `GLOSSARY_TERM_PROPOSED` - When a glossary term is proposed for an entity

#### Tag Changes

- `TAG_ADDED` - When a tag is added to an entity
- `TAG_REMOVED` - When a tag is removed from an entity
- `TAG_PROPOSED` - When a tag is proposed for an entity
