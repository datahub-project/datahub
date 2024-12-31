### Overview

This source pulls data from two locations:
- The DataHub database, containing a single table holding all versioned aspects
- The DataHub Kafka cluster, reading from the [MCL Log](../../../../docs/what/mxe.md#metadata-change-log-mcl)
topic for timeseries aspects.

All data is first read from the database, before timeseries data is ingested from kafka.
To prevent this source from potentially running forever, it will not ingest data produced after the
datahub_source ingestion job is started. This `stop_time` is reflected in the report.

Data from the database and kafka are read in chronological order, specifically by the
createdon timestamp in the database and by kafka offset per partition. In order to
properly read from the database, please ensure that the `createdon` column is indexed.
Newly created databases should have this index, named `timeIndex`, by default, but older
ones you may have to create yourself, with the statement:

```
CREATE INDEX timeIndex ON metadata_aspect_v2 (createdon);
```

*If you do not have this index, the source may run incredibly slowly and produce
significant database load.*

#### Stateful Ingestion
On first run, the source will read from the earliest data in the database and the earliest
kafka offsets. Every `commit_state_interval` (default 1000) records, the source will store
a checkpoint to remember its place, i.e. the last createdon timestamp and kafka offsets.
This allows you to stop and restart the source without losing much progress, but note that
you will re-ingest some data at the start of the new run.

If any errors are encountered in the ingestion process, e.g. we are unable to emit an aspect
due to network errors, the source will keep running, but will stop committing checkpoints,
unless `commit_with_parse_errors` (default `false`) is set. Thus, if you re-run the ingestion,
you can re-ingest the data that was missed, but note it will all re-ingest all subsequent data.

If you want to re-ingest all data, you can set a different `pipeline_name` in your recipe,
or set `stateful_ingestion.ignore_old_state`:

```yaml
source:
  config:
    # ... connection config, etc.
    stateful_ingestion:
      enabled: true
      ignore_old_state: true
    urn_pattern: # URN pattern to ignore/include in the ingestion
      deny:
        # Ignores all datahub metadata where the urn matches the regex
        - ^denied.urn.*
      allow:
        # Ingests all datahub metadata where the urn matches the regex.
        - ^allowed.urn.*
```

#### Limitations
- Can only pull timeseries aspects retained by Kafka, which by default lasts 90 days.
- Does not detect hard timeseries deletions, e.g. if via a `datahub delete` command using the CLI.
Therefore, if you deleted data in this way, it will still exist in the destination instance.
- If you have a significant amount of aspects with the exact same `createdon` timestamp,
stateful ingestion will not be able to save checkpoints partially through that timestamp.
On a subsequent run, all aspects for that timestamp will be ingested.

#### Performance
On your destination DataHub instance, we suggest the following settings:
- Enable [async ingestion](../../../../docs/deploy/environment-vars.md#ingestion)
- Use standalone consumers
([mae-consumer](../../../../metadata-jobs/mae-consumer-job/README.md)
and [mce-consumer](../../../../metadata-jobs/mce-consumer-job/README.md))
  * If you are migrating large amounts of data, consider scaling consumer replicas.
- Increase the number of gms pods to add redundancy and increase resilience to node evictions
  * If you are migrating large amounts of data, consider increasing elasticsearch's
  thread count via the `ELASTICSEARCH_THREAD_COUNT` environment variable.

#### Exclusions
You will likely want to exclude some urn types from your ingestion, as they contain instance-specific
metadata, such as settings, roles, policies, ingestion sources, and ingestion runs. For example, you 
will likely want to start with this:

```yaml
source:
  config:
    urn_pattern: # URN pattern to ignore/include in the ingestion
      deny:
        # Ignores all datahub metadata where the urn matches the regex
        - ^urn:li:role.*                    # Only exclude if you do not want to ingest roles
        - ^urn:li:dataHubRole.*             # Only exclude if you do not want to ingest roles
        - ^urn:li:dataHubPolicy.*           # Only exclude if you do not want to ingest policies
        - ^urn:li:dataHubIngestionSource.*  # Only exclude if you do not want to ingest ingestion sources
        - ^urn:li:dataHubSecret.*
        - ^urn:li:dataHubExecutionRequest.*
        - ^urn:li:dataHubAccessToken.*
        - ^urn:li:dataHubUpgrade.*
        - ^urn:li:inviteToken.*
        - ^urn:li:globalSettings.*
        - ^urn:li:dataHubStepState.*
```
