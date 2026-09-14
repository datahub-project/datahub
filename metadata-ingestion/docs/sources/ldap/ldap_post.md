### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

LDAP ingestion emits both `corpUserInfo.active` and the `corpUserStatus` aspect for each user (`ACTIVE` or `SUSPENDED`). For Active Directory deployments, disabled accounts are detected via the `userAccountControl` attribute (bit `0x2`). Override the LDAP attribute used for status mapping with `user_attrs_map.accountStatus` (default: `userAccountControl`).

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
