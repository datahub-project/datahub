### Overview

The `azure-data-factory` module ingests metadata from Azure Data Factory into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

#### Required Permissions

The connector only performs **read operations**. Grant one of the following:

**Option 1: Built-in Reader Role** (recommended)

Assign the [Reader](https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#reader) role at subscription, resource group, or Data Factory level.

**Option 2: Custom Role with Minimal Permissions**

Download [`datahub-adf-reader-role.json`](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/docs/sources/azure-data-factory/datahub-adf-reader-role.json), update the `{subscription-id}`, then:

```bash
# Create custom role
az role definition create --role-definition datahub-adf-reader-role.json

# Assign to service principal
az role assignment create \
  --assignee <service-principal-id> \
  --role "DataHub ADF Reader" \
  --scope /subscriptions/{subscription-id}
```

For detailed instructions, see [Azure custom roles](https://learn.microsoft.com/en-us/azure/role-based-access-control/custom-roles).

#### Setup

1. Configure authentication for the connector runtime.
2. Grant read permissions on the target Data Factory resources.
3. Provide a subscription scope and optional pattern filters in the ingestion recipe.

This section intentionally complements (and does not duplicate) the generated **Starter Recipe** section.
