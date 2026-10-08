### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Probe support

`datahub recipe probe` checks a recipe against NiFi without running ingestion. It offers `flow`
(the root process group) and `process_groups` (every group below the root, nested ones included):

```shell
datahub recipe probe methods --recipe nifi_recipe.yml
datahub recipe probe run process_groups --recipe nifi_recipe.yml --report-to groups.json
datahub recipe probe filter --recipe nifi_recipe.yml --from-run groups.json
datahub recipe probe filter --recipe nifi_recipe.yml --kind "Process Group" \
  --parent "NiFi Flow" --parent "Ingest" --name "Load"
```

The probe signs in the way ingestion does, with every `auth` mode and `ca_file`. It reads only the
process group tree, never provenance events or processor properties. `process_group_pattern` is
matched on a group's name, and ingestion stops walking at a group the pattern refuses, so every
group inside it is excluded too, whatever its own name. The root group's name is matched as well:
an allow list that leaves it out excludes everything. Each listed group carries its `ancestors`,
root first, so `probe filter --from-run` judges them. For a bare `--name`, pass each enclosing group
as `--parent`, root first. A group ingestion walks is emitted as a container only with
`emit_process_group_as_container`, and only when it, or a group inside it, holds a supported
ingress or egress processor.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

- Lineage extraction analyzes provenance events. Verify your NiFi provenance retention period and run ingestion frequently enough to capture events before they expire.

- Limited ingress/egress processors are supported
  - S3: `ListS3`, `FetchS3Object`, `PutS3Object`
  - SFTP: `ListSFTP`, `FetchSFTP`, `GetSFTP`, `PutSFTP`

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
