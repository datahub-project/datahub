### Overview

The `omni` module ingests metadata from the [Omni](https://omni.co/) BI platform into DataHub. It is intended for production ingestion workflows and supports the following:

- Folders (as Containers), Dashboards, and Chart tiles
- Semantic layer: Models, Topics, and Views with schema fields (dimensions and measures)
- Physical warehouse tables with upstream lineage stitched to existing DataHub entities
- Column-level (fine-grained) lineage from semantic view fields back to warehouse columns
- Ownership propagated from the Omni document API, with owners resolved to email addresses

Lineage is emitted as a five-hop chain:

```
Folder → Dashboard → Chart (tile) → Topic → Semantic View → Physical Table
```

### Prerequisites

Before running ingestion, ensure you have the following:

1. **An Omni Organization API key** with read access to models, documents, and connections. Generate API keys in Omni Admin → API Keys. Personal access tokens do not work: owner email resolution calls the Omni SCIM users API (`/api/scim/v2/users`), which accepts only Organization API keys.

2. **Connection mapping configuration** if you want physical table lineage to stitch with existing warehouse entities in DataHub. You will need to map each Omni connection ID to the corresponding DataHub platform name, platform instance, and database name:

```yaml
connection_to_platform:
  "conn_abc123": "snowflake"
connection_to_platform_instance:
  "conn_abc123": "my_snowflake_account"
connection_to_database:
  "conn_abc123": "ANALYTICS_PROD"
```

Connection IDs can be found by calling the Omni `/v1/connections` API or from the Omni Admin UI.

:::note
If the Omni API key does not have permission to list connections (`403 Forbidden`), the connector will fall back to the `connection_to_platform` config overrides and continue ingestion without failing.
:::

#### Owner resolution

Omni identifies document owners by an internal user ID. By default (`resolve_owner_emails: true`), the connector lists users from the Omni SCIM users API once per run and uses each owner's lowercased email address as the DataHub user ID, for example `urn:li:corpuser:jane.doe@example.com`. This matches DataHub users provisioned through SSO with email-based usernames.

If the users API cannot be read, or an owner is not in the user list, that owner falls back to the Omni user ID and the ingestion report records a warning (or lists the ID under `owners_unresolved`).
