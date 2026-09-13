---
description: Share catalog entities, including data products, from one DataHub Cloud instance to another.
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Metadata Sharing

<FeatureAvailability saasOnly />

Metadata Share lets you share a catalog entity — including a data product — from one DataHub Cloud instance into another, so it appears in both catalogs. This is useful when two teams or organizations each run their own DataHub Cloud and want the same asset to show up on both sides.

Metadata Share is **DataHub Cloud → DataHub Cloud only**. Both the source and the destination must be DataHub Cloud instances.

If you're running self-hosted or open-source DataHub, Metadata Share isn't available. See [Open source and self-hosted DataHub](#open-source-and-self-hosted-datahub) at the end of this page for a one-way alternative.

## What you can share

Sharing works across entity types, including data products.

One thing to know up front: sharing a data product shares the data product entity itself — its member datasets are **not** automatically included. To bring related entities along, enable lineage sharing when you share (see the steps below).

Most semantic metadata comes along with the asset: properties, documentation, tags, glossary terms, ownership, schema, domains, and so on.

Some things are never shared: secrets, access tokens, policies, ingestion sources, and connections. Sharing copies catalog metadata only — never credentials or platform configuration.

There's one nuance for related entities pulled in via lineage. For a related entity that you don't own, only a limited, skeleton set of metadata is shared, and column-level lineage is not included. You share full metadata only for entities you own.

## Prerequisites

- Metadata Share is enabled for your DataHub Cloud instance. It's turned on through configuration; if you don't see the sharing option, contact your DataHub Cloud representative.
- An admin has set up a connection to the destination instance (see the next section).
- You have permission to share the entity — the "Share Entity" privilege, or edit access on the asset. Note that setting up connections is a separate, admin-level permission ("Manage Connections").

## Set up a connection to another instance

This is a one-time, admin task for each destination instance, and it requires the "Manage Connections" privilege.

1. Go to Settings → Platform → Integrations → "DataHub".
2. Click "Add a connection".
3. Enter a Name, the destination instance's URL, and an access token for it.
4. Save.

The token is stored encrypted. Once a connection exists, anyone with permission to share can share entities to that destination — you don't need to repeat this setup.

## Share an entity

1. Open the entity you want to share (for example, a data product).
2. From its menu, choose "Share with another instance".
3. In the dialog, select one or more connected instances ("Select Instances").
4. Optionally, check "Share assets upstream and downstream of [entity]" to also share lineage-connected entities.
5. Click "Share".

For destinations you've already shared to, you can Unshare to remove the entity from the other instance, or Resync to push the latest metadata again.

## How updates propagate

After the initial share, shared entities are re-shared automatically on a recurring schedule, so the destination stays up to date — you don't have to re-share manually every time the source changes.

On the destination instance, shared entities are marked as originating externally: their origin reflects that they came from another instance via sharing.

## Open source and self-hosted DataHub

Metadata Share is a DataHub Cloud feature and is not available in open-source or self-hosted DataHub.

The closest option for open-source DataHub is the [DataHub-to-DataHub ingestion source](../generated/ingestion/sources/datahub.md), which copies metadata from one DataHub instance into another. It's a one-way replication — source to destination — not live two-way sharing. To keep both catalogs in sync you'd run it in a single direction, from the source instance to the destination.
