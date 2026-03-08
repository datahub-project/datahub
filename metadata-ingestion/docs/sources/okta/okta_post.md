As a prerequisite, you should create a DataHub Application within the Okta Developer Console with full permissions to read your organization's Users and Groups.

### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Compatibility

Validated against Okta API Versions:

- `2021.07.2`

Validated against load:

- User Count: `1000`
- Group Count: `100`
- Group Membership Edges: `1000` (1 per User)
- Run Time (Wall Clock): `2min 7sec`

#### Extracting DataHub Users

User entities are extracted from Okta users APIs and mapped to DataHub `CorpUser` entities.

#### Usernames

By default, usernames are derived from Okta user profile attributes. You can customize attribute and regex mapping using connector configuration options.

#### Profiles

The connector also maps selected Okta profile fields (for example display name, email, title, and other standard user metadata) to DataHub user info aspects.

#### Extracting DataHub Groups

Group entities are extracted from Okta groups APIs and mapped to DataHub `CorpGroup` entities.

#### Group Names

Group identifiers and display names are derived from Okta group attributes. Mapping behavior can be customized via connector configuration.

#### Extracting Group Membership

User-to-group membership edges are extracted and emitted as DataHub group membership relationships.

#### Filtering and Searching

Use connector filter/search configuration to scope user and group extraction to relevant identities and reduce ingestion load.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
