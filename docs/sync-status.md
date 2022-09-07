import FeatureAvailability from '@site/src/components/FeatureAvailability';

# About DataHub Sync Status

<FeatureAvailability/>

When looking at metadata in DataHub, it's useful to know if the information you're looking at is relevant.
Specifically, if metadata is stale, or hasn't been updated in a while, then you should consider refreshing that metadata
using [metadata ingestion](./../metadata-ingestion/README.md) or [deleting](TODO) it if it no longer exists.

## Sync Status Setup, Prerequisites, and Permissions

The sync status feature is enabled by default and does not require any special setup.

## Using Sync Status

The DataHub UI will display the sync status in the top right corner of the page.

![sync status](https://raw.githubusercontent.com/datahub-project/static-assets/master/imgs/sync-status.png)

The last synchronized date is computed as the most recent edit to the entity, excluding edits done through the UI.
We'll automatically assign a color based on the sync status recency, with green indicating a fresh entity and red indicating a stale one.
You can hover over the sync status message in the UI to view the exact timestamp of the most recent sync.

![hover card](https://raw.githubusercontent.com/datahub-project/static-assets/master/imgs/sync-status-hover-card.png)

_Need more help? Join the conversation in [Slack](http://slack.datahubproject.io)!_
