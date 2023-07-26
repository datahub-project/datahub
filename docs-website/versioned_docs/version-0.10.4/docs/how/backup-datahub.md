---
title: Taking backup of DataHub
slug: /how/backup-datahub
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/how/backup-datahub.md
---

# Taking backup of DataHub

## Production

The recommended backup strategy is to periodically dump the database `datahub.metadata_aspect_v2` so it can be recreated from the dump which most managed DB services will support (e.g. AWS RDS). Then run [restore indices](./restore-indices.md) to recreate the indices.

In order to back up Time Series Aspects (which power usage and dataset profiles), you'd have to do a backup of Elasticsearch, which is possible via AWS OpenSearch. Otherwise, you'd have to reingest dataset profiles from your sources in the event of a disaster scenario!

## Quickstart

To take a backup of your quickstart, take a look at this [document](../quickstart.md#backing-up-your-datahub-quickstart-experimental) on how to accomplish it.
