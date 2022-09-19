import FeatureAvailability from '@site/src/components/FeatureAvailability';

# About DataHub Sync Status

<FeatureAvailability/>

When looking at metadata in DataHub, it's useful to know if the information you're looking at is relevant.
Specifically, if metadata is stale, or hasn't been updated in a while, then you should consider refreshing that metadata
using [metadata ingestion](./../metadata-ingestion/README.md) or [deleting](./how/delete-metadata.md) it if it no longer exists.

## Sync Status Setup, Prerequisites, and Permissions

The sync status feature is enabled by default and does not require any special setup.

## Using Sync Status

The DataHub UI will display the sync status in the top right corner of the page.

The last synchronized date is basically the last time an ingestion run saw an entity. It is computed as the most recent update to the entity, excluding changes done through the UI. If an ingestion run restates an entity but doesn't actually cause any changes, we still count that as an update for the purposes of sync status.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/master/imgs/sync-status-normal.png"/>
</p>

We'll automatically assign a color based on the sync status recency:
- Green: last synchronized in the past week
- Yellow: last synchronized in the past month
- Red: last synchronized more than a month ago

You can hover over the sync status message in the UI to view the exact timestamp of the most recent sync.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/master/imgs/sync-status-hover-card.png"/>
</p>

_Need more help? Join the conversation in [Slack](http://slack.datahubproject.io)!_
