### Overview

The `sac` module ingests metadata from SAP Analytics Cloud (SAC) into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

### Configuration Notes

1. Refer to [Manage OAuth Clients](https://help.sap.com/docs/SAP_ANALYTICS_CLOUD/00f68c2e08b941f081002fd3691d86a7/4f43b54398fc4acaa5efa32badfe3df6.html) to create an OAuth client in SAP Analytics Cloud. The OAuth client is required to have the following properties:

   - Purpose: API Access
   - Access:
     - Story Listing
     - Data Import Service
   - Authorization Grant: Client Credentials

2. Maintain connection mappings (optional):

To map individual connections in SAP Analytics Cloud to platforms, platform instances and environments, the `connection_mapping` configuration can be used within the recipe:

```yaml
connection_mapping:
  MY_BW_CONNECTION:
    platform: bw
    platform_instance: PROD_BW
    env: PROD
  MY_HANA_CONNECTION:
    platform: hana
    platform_instance: PROD_HANA
    env: PROD
```

The key in the connection mapping dictionary represents the name of the connection created in SAP Analytics Cloud.

### Concept mapping

| SAP Analytics Cloud | DataHub     |
| ------------------- | ----------- |
| `Story`             | `Dashboard` |
| `Application`       | `Dashboard` |
| `Live Data Model`   | `Dataset`   |
| `Import Data Model` | `Dataset`   |
| `Model`             | `Dataset`   |
