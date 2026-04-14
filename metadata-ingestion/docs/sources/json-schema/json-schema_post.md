#### Configuration Notes

- You must provide a `platform` field. Most organizations have custom project names for their schema repositories, so you can pick whatever name makes sense. For example, you might want to call your schema platform **schemaregistry**. After picking a custom platform, you can use the [put platform](../../../../docs/cli.md#put-platform) command to register your custom platform into DataHub.

### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
