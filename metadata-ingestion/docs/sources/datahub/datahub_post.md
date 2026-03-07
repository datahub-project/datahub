### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Stateful Ingestion

The source checkpoints by database `createdon` and Kafka offsets so interrupted runs can resume without restarting from scratch. Use `stateful_ingestion.ignore_old_state` or a distinct `pipeline_name` when you want a full replay.

#### Performance

For large migrations, ensure `metadata_aspect_v2.createdon` is indexed (`timeIndex`), enable async ingestion on the destination, and scale consumers/GMS/Elasticsearch workers as needed.

#### Exclusions

Use `urn_pattern` allow/deny filters to exclude instance-specific entities (for example roles, policies, ingestion runs, secrets, and global settings) when migrating between environments.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
