# Deleting Ingested Metadata

## Introduction

In DataHub, there are two ways to remove ingested assets from the search and discovery experience:&#x20;

1. **Soft Delete:** In a soft-delete, the underlying information about an asset will be preserved. However, the entity will not be discoverable itself in search, browse, or lineage experiences. Soft deletes are used when you want to cosmetically remove an entity, but would like to preserve the information that may exist about it (e.g. tags, owners, and more.&#x20;
2. **Hard Delete:** In a hard-delete, the underlying information about an entity is completely purged and becomes unrecoverable. This is used when you want to totally remove all information about an entity, for example when you want to start from scratch.&#x20;

There are a few ways to delete metadata that has been ingested into DataHub:

1. Delete via the UI (soft)&#x20;
2. Delete urns via the `datahub` CLI (soft / hard)&#x20;
3. Rollback all ingested metadata for a specific ingestion run via the `datahub` CLI (soft / hard)

We'll cover each briefly in the subsequent sections.&#x20;

### Deleting via the UI

When you delete via the UI, entities will be marked as "soft" deleted, as described above.&#x20;

Steps:

1\. Navigate to the **Ingestion** tab&#x20;

2\. View an ingestion run summary (click on `DETAILS)`&#x20;

<figure><img src="../../imgs/saas/assets/Screen Shot 2022-08-22 at 11.21.42 AM.png" alt=""><figcaption></figcaption></figure>

3\. Click **View All** &#x20;

4\. Click the **more** (3 dots) button&#x20;

5\. Click **Edit**

<figure><img src="../../imgs/saas/assets/Screen Shot 2022-08-22 at 11.22.23 AM.png" alt=""><figcaption></figcaption></figure>

6\. Select the entities you'd like to delete

7\. Click **Delete** > **Mark as deleted**

<figure><img src="../../imgs/saas/assets/Screen Shot 2022-08-22 at 11.23.08 AM.png" alt=""><figcaption></figcaption></figure>

Now the entities will be "soft" deleted. This means that they will no longer appear in normal search results. This may take up to 10 minutes to be fully applied to your instance.&#x20;

However, if you have dangling pointers to the entities, they will remain visible. You will also be able to continue to see the entity in the ingestion run summary.&#x20;

Note that when they are re-ingested, soft deleted entities will be reinstated. &#x20;

### Deleting via the datahub CLI

By using the CLI directly, you can soft or hard-delete assets ingested into DataHub with more control. [This doc ](https://datahubproject.io/docs/how/delete-metadata/#delete-by-urn)is the best place to start. It will instruct you on how to remove assets by urn or those matching specific filters.&#x20;

Soft deleting may take up to 10 minutes to be fully applied to your instance.

### Rolling back an ingestion run

Using the CLI, you can rollback a specific ingestion run. This will rollback the changes that were made during a given ingestion run. In cases where new assets were ingested during a run, it will also soft or hard delete the entity (depending on the flags).&#x20;

To rollback an ingestion run that was initiated via the the UI, follow these steps:

_UI_

1\. Navigate to the **Ingestion** tab

2\. Find the ingestion run you want to rollback

3\. Click **Copy Execution Request URN** &#x20;

<figure><img src="../../imgs/saas/assets/Screen Shot 2022-08-22 at 11.47.57 AM.png" alt=""><figcaption></figcaption></figure>

4\. Extract the **run id**: The run id will be inside the URN you just copied. If the copied urn is&#x20;

```
urn:li:dataHubExecutionRequest:abcdefg # Urns are urn:li:dataHubExecutionRequest:run-id
```

Then the **run id** will be&#x20;

```
abcdefg
```

Hold onto this identifier, it will be needed in the next step to rollback the run.&#x20;



_CLI_&#x20;

5\. Navigate to your Terminal&#x20;

6\. Confirm that you can view the ingestion stats for the given run by executing

```
datahub ingest show --run-id <run-id> 
```

Using the run id extracted in step 4. This should show a summary of all the things that were changed during this run.

7\. Execute the rollback command to rollback the changes from the run:

```
datahub ingest rollback --run-id <run-id>
```

> **Pro-Tip:** Before running this command, you can run a dry-run to confirm that the command will delete what you expect:
>
> ```
> datahub ingest rollback --dry-run --run-id <run-id>
> ```

For more information, check out the [Removing Metadata from DataHub](https://datahubproject.io/docs/how/delete-metadata/#delete-by-urn) guide.&#x20;

Rollbacks may take up to 10 minutes to be fully applied to your instance.
