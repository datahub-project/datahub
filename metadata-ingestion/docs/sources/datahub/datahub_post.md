### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Stateful Ingestion

The source checkpoints by database `createdon` and Kafka offsets so interrupted runs can resume without restarting from scratch. Use `stateful_ingestion.ignore_old_state` or a distinct `pipeline_name` when you want a full replay.

On first run, the source will read from the earliest data in the database and the earliest kafka offsets. Every `commit_state_interval` (default 1000) records, the source will store a checkpoint to remember its place, i.e. the last `createdon` timestamp and kafka offsets. This allows you to stop and restart the source without losing much progress, but note that you will re-ingest some data at the start of the new run.

If any errors are encountered in the ingestion process, e.g. we are unable to emit an aspect due to network errors, the source will keep running, but will stop committing checkpoints, unless `commit_with_parse_errors` (default false) is set. Thus, if you re-run the ingestion, you can re-ingest the data that was missed, but note it will all re-ingest all subsequent data.

If you want to re-ingest all data, you can set a different `pipeline_name` in your recipe, or set `stateful_ingestion.ignore_old_state: true`

##### Limitations

- Can only pull timeseries aspects retained by Kafka, which by default lasts 90 days.
- Does not detect hard timeseries deletions, e.g. if via a datahub delete command using the CLI. Therefore, if you deleted data in this way, it will still exist in the destination instance.
- If you have a significant amount of aspects with the exact same createdon timestamp, stateful ingestion will not be able to save checkpoints partially through that timestamp. On a subsequent run, all aspects for that timestamp will be ingested.

#### Performance

For large migrations, ensure `metadata_aspect_v2.createdon` is indexed (`timeIndex`), enable async ingestion on the destination, and scale consumers/GMS/Elasticsearch workers as needed.

- Enable [async ingestion](https://docs.datahub.com/docs/deploy/environment-vars#ingestion)
- Use standalone consumers ([mae-consumer](https://docs.datahub.com/docs/metadata-jobs/mae-consumer-job) and [mce-consumer](https://docs.datahub.com/docs/metadata-jobs/mce-consumer-job))
  - If you are migrating large amounts of data, consider scaling consumer replicas.
- Increase the number of gms pods to add redundancy and increase resilience to node evictions
  - If you are migrating large amounts of data, consider increasing elasticsearch's thread count via the `ELASTICSEARCH_THREAD_COUNT` environment variable.

#### Exclusions

You will likely want to exclude some urn types from your ingestion, as they contain instance-specific metadata, such as settings, roles, policies, ingestion sources, and ingestion runs. For example, you will likely want to start with this:

```yaml
source:
  config:
    urn_pattern: # URN pattern to ignore/include in the ingestion
      deny:
        # Ignores all datahub metadata where the urn matches the regex
        - ^urn:li:role.* # Only exclude if you do not want to ingest roles
        - ^urn:li:dataHubRole.* # Only exclude if you do not want to ingest roles
        - ^urn:li:dataHubPolicy.* # Only exclude if you do not want to ingest policies
        - ^urn:li:dataHubIngestionSource.* # Only exclude if you do not want to ingest ingestion sources
        - ^urn:li:dataHubSecret.*
        - ^urn:li:dataHubExecutionRequest.*
        - ^urn:li:dataHubAccessToken.*
        - ^urn:li:dataHubUpgrade.*
        - ^urn:li:inviteToken.*
        - ^urn:li:globalSettings.*
        - ^urn:li:dataHubStepState.*
```

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
