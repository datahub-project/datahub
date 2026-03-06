As a prerequisite, you should create a DataHub Application within the Okta Developer Console with full permissions to read your organization's Users and Groups.

### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.

### Compatibility

Validated against Okta API Versions:

- `2021.07.2`

Validated against load:

- User Count: `1000`
- Group Count: `100`
- Group Membership Edges: `1000` (1 per User)
- Run Time (Wall Clock): `2min 7sec`
