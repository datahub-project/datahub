import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Operational Metadata

## Why Would You Push Operational Metadata?

Operational metadata like (usage, profiling, freshness) is used to enrich the search results and add context to the Entity page which the users looking at the page can use to make decisions.

### Goal Of This Guide

This guide will show you how to

- Push profiling information

## Prerequisites

For this tutorial, you need to deploy DataHub Quickstart and ingest sample data.
For detailed steps, please refer to [Datahub Quickstart Guide](/docs/quickstart.md).

## Add Profiling

<Tabs>
<TabItem value="python" label="Python">

{{ inline /metadata-ingestion/examples/library/dataset_usage.py show_path_as_comment }}

</TabItem>
</Tabs>