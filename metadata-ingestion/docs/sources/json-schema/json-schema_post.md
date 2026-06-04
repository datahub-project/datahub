#### Configuration Notes

- You must provide a `platform` field. Most organizations have custom project names for their schema repositories, so you can pick whatever name makes sense. For example, you might want to call your schema platform **schemaregistry**. After picking a custom platform, you can use the [put platform](../../../../docs/cli.md#put-platform) command to register your custom platform into DataHub.

### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Dataset Display Name Control

By default, the display name for each ingested schema is derived from the last path segment of the schema identifier (e.g. a schema with `$id: "https://example.com/schemas/domain/1.0"` gets the display name `1.0`). This can be insufficient when many schemas share similar version-based names.

The `dataset_name_strategy` config controls how display names are computed:

- `BASENAME` (default): Last path segment of the `$id` or filename. Current behavior, no breaking change.
- `SCHEMA_ID`: Uses the full `$id` URI as the display name. Falls back to BASENAME if no `$id` is present.
- `TITLE`: Uses the `title` field from the JSON schema. Falls back to BASENAME if no title is present.

Additionally, `dataset_name_replace_pattern` applies a simple string match-replace on the computed display name, regardless of which strategy is used.

These settings only affect the display name shown in the DataHub UI (`DatasetProperties.name`). The dataset URN is not affected.

Example: use the full schema `$id` as the display name, stripping the host prefix:

```yaml
source:
  type: json-schema
  config:
    path: /schemas
    platform: schemaregistry
    dataset_name_strategy: SCHEMA_ID
    dataset_name_replace_pattern:
      match: "https://dev.schema.example.com/"
      replace: ""
```

Example: use the schema `title` field as the display name:

```yaml
source:
  type: json-schema
  config:
    path: /schemas
    platform: schemaregistry
    dataset_name_strategy: TITLE
```

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
