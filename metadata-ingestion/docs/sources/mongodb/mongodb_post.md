### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Emitting as the DocumentDB platform

By default the connector emits all ingested entities under the `mongodb` data platform, regardless of whether the underlying source is MongoDB or AWS DocumentDB. To surface DocumentDB clusters as their own platform, set `emit_as_documentdb: true`. This requires `hostingEnvironment` to be `AWS_DOCUMENTDB`.

Enabling this for an existing recipe will generate new `documentdb` URNs; previously emitted `mongodb` URNs will need to be cleaned up via stateful ingestion (if enabled) or manually soft-deleted.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
