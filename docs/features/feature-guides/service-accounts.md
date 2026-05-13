import FeatureAvailability from '@site/src/components/FeatureAvailability';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Service Accounts

<FeatureAvailability/>

Available starting in DataHub Cloud v0.3.17, DataHub Core v1.4.0.

## Introduction

Service Accounts provide a secure way to enable programmatic access to DataHub APIs without using personal user credentials. They are designed for automated workflows, CI/CD pipelines, data ingestion processes, and any other use case where a non-human identity needs to interact with DataHub.

Key benefits of using service accounts:

1. **Separation of Concerns** - Decouple automation credentials from personal user accounts
2. **Auditable Access** - Track API usage by specific automation workflows
3. **Granular Permissions** - Assign specific roles and policies to each service account
4. **Token Management** - Generate, rotate, and revoke tokens independently for each service account
5. **Security** - Revoke access instantly without affecting human users

## Prerequisites and Permissions

To manage service accounts, a user must have the **Manage Service Accounts** platform privilege. This privilege allows users to:

- Create new service accounts
- View and list existing service accounts
- Delete service accounts
- Generate access tokens for service accounts

By default, users with the **Admin** role have this privilege. You can grant this privilege to other users or groups through [Policies](../../authorization/policies.md).

## Using Service Accounts

### Accessing Service Accounts

Service accounts can be managed from the **Settings** page in DataHub:

1. Navigate to **Settings** (gear icon in the top navigation)
2. Click **Users & Groups** in the left sidebar
3. Select the **Service Accounts** tab

<!-- Screenshot placeholder: Settings > Users & Groups > Service Accounts tab -->
<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/service-accounts/service-accounts-list.png"/>
</p>

### Creating a Service Account

To create a new service account:

1. Click the **Create Service Account** button
2. Enter a **Name** for the service account (e.g., "Ingestion Pipeline", "CI/CD Bot")
3. Optionally, add a **Description** to explain the service account's purpose
4. Click **Create**

<!-- Screenshot placeholder: Create Service Account modal -->
<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/service-accounts/create-service-account-modal.png"/>
</p>

The service account will be assigned a unique identifier (URN) automatically. This URN follows the format:

```
urn:li:corpuser:service_<uuid>
```

### Generating Access Tokens

After creating a service account, you need to generate an access token for it to authenticate with DataHub APIs:

1. Navigate to **Settings** > **Access Tokens**
2. Click **Generate new token**
3. Select **Service Account** as the token type
4. Choose the service account from the dropdown
5. Provide a **Name** for the token (e.g., "production-ingestion-token")
6. Select a **Duration** for the token validity
7. Click **Generate**

<!-- Screenshot placeholder: Generate token modal with service account selected -->
<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/service-accounts/generate-token-modal.png"/>
</p>

:::caution Important
Copy and store the generated token securely. It will only be displayed once and cannot be retrieved later.
:::

### Assigning Roles to Service Accounts

Service accounts can be assigned DataHub roles to control their permissions. To assign a role:

1. Navigate to **Settings** > **Users & Groups** > **Service Accounts**
2. Find the service account in the list
3. Use the **Role** dropdown to select a role (e.g., Admin, Editor, Reader)
4. Confirm the role assignment

<!-- Screenshot placeholder: Service account with role dropdown -->
<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/service-accounts/assign-role.png"/>
</p>

Alternatively, you can add service accounts to [Policies](../../authorization/policies.md) for more granular permission control.

### Deleting a Service Account

To delete a service account:

1. Navigate to **Settings** > **Users & Groups** > **Service Accounts**
2. Find the service account you want to delete
3. Click the 3-dot more menu on the right side
4. Click the **Delete** button (trash icon)

:::warning
Deleting a service account will immediately invalidate all access tokens associated with it. Any automated workflows using those tokens will stop working.
:::

### Revoking Access Tokens

To revoke a specific token without deleting the entire service account:

1. Navigate to **Settings** > **Access Tokens**
2. Find the token you want to revoke
3. Click **Revoke**
4. Confirm the revocation

## Using Service Account Tokens

Once you have generated an access token for a service account, you can use it to authenticate with DataHub APIs.

### With the DataHub CLI

```bash
# Set the token as an environment variable
export DATAHUB_GMS_TOKEN="<your-service-account-token>"

# Or pass it directly
datahub get --urn "urn:li:dataset:..."
```

### With the Python SDK

```python
from datahub.emitter.rest_emitter import DatahubRestEmitter

# Create an emitter with service account token
emitter = DatahubRestEmitter(
    gms_server="http://localhost:8080",
    token="<your-service-account-token>"
)

# Use the emitter for API calls
emitter.emit(...)
```

### With GraphQL API

```bash
curl -X POST 'https://your-datahub-instance/api/graphql' \
  -H 'Authorization: Bearer <your-service-account-token>' \
  -H 'Content-Type: application/json' \
  -d '{"query": "{ me { corpUser { urn } } }"}'
```

### With REST API

```bash
curl 'https://your-datahub-instance/openapi/v3/entity/dataset/...' \
  -H 'Authorization: Bearer <your-service-account-token>'
```

## GraphQL API Reference

Service accounts can also be managed programmatically using the GraphQL API.

### Create a Service Account

```graphql
mutation createServiceAccount($input: CreateServiceAccountInput!) {
  createServiceAccount(input: $input) {
    urn
    type
    name
    displayName
    description
    createdBy
    createdAt
  }
}
```

Variables:

```json
{
  "input": {
    "displayName": "My Ingestion Pipeline",
    "description": "Service account for automated data ingestion"
  }
}
```

### List Service Accounts

```graphql
query listServiceAccounts($input: ListServiceAccountsInput!) {
  listServiceAccounts(input: $input) {
    start
    count
    total
    serviceAccounts {
      urn
      name
      displayName
      description
      createdBy
      createdAt
      updatedAt
    }
  }
}
```

Variables:

```json
{
  "input": {
    "start": 0,
    "count": 20,
    "query": ""
  }
}
```

### Get a Service Account

```graphql
query getServiceAccount($urn: String!) {
  getServiceAccount(urn: $urn) {
    urn
    name
    displayName
    description
    createdBy
    createdAt
    updatedAt
  }
}
```

### Delete a Service Account

```graphql
mutation deleteServiceAccount($urn: String!) {
  deleteServiceAccount(urn: $urn)
}
```

### Create Access Token for Service Account

```graphql
mutation createAccessToken($input: CreateAccessTokenInput!) {
  createAccessToken(input: $input) {
    accessToken
    metadata {
      id
      actorUrn
      ownerUrn
      name
      description
    }
  }
}
```

Variables:

```json
{
  "input": {
    "type": "SERVICE_ACCOUNT",
    "actorUrn": "urn:li:corpuser:service_<uuid>",
    "name": "my-token-name",
    "duration": "ONE_YEAR"
  }
}
```

Valid duration options: `ONE_HOUR`, `ONE_DAY`, `ONE_MONTH`, `THREE_MONTHS`, `SIX_MONTHS`, `ONE_YEAR`, `NO_EXPIRY`

## Best Practices

### Naming Conventions

Use descriptive names that indicate the purpose of each service account:

- `ingestion-snowflake-prod` - For Snowflake production ingestion
- `cicd-github-actions` - For GitHub Actions CI/CD pipelines
- `airflow-metadata-sync` - For Airflow metadata synchronization

### Token Rotation

Regularly rotate service account tokens to maintain security:

1. Generate a new token for the service account
2. Update your automation to use the new token
3. Revoke the old token

### Least Privilege Access

Assign the minimum permissions required for each service account:

- For ingestion pipelines: Assign a custom policy that only allows creating/updating specific entity types
- For read-only integrations: Use a Reader role or custom read-only policy
- For admin automation: Use the Admin role only when absolutely necessary

## FAQ and Troubleshooting

### Why can't I see the Service Accounts tab?

The Service Accounts tab is only visible to users with the **Manage Service Accounts** privilege. Contact your DataHub administrator to request access.

### My service account token stopped working

Check the following:

1. **Token expiration**: The token may have expired. Generate a new token.
2. **Service account deleted**: The service account may have been deleted. Create a new one.
3. **Token revoked**: Someone may have revoked the token. Generate a new one.
4. **Permission changes**: The service account's permissions may have been modified.

### Can I see which tokens exist for a service account?

Yes, navigate to **Settings** > **Access Tokens** and filter by the service account name to see all tokens associated with it. Note that YOU must filter for the owner of the service account to see it. Whoever created the service account will be the token owner.

### What happens to a service account's tokens when I delete it?

All tokens associated with a service account are invalidated when the service account is deleted.

### Can I use service accounts in policies?

Yes, service accounts can be added to policies just like regular users. Use the service account's URN (e.g., `urn:li:corpuser:service_<uuid>`) when configuring policy actors.

### What's the difference between a personal access token and a service account token?

| Feature           | Personal Access Token         | Service Account Token                    |
| ----------------- | ----------------------------- | ---------------------------------------- |
| Associated with   | A human user                  | A service account                        |
| Permissions       | Inherits user's permissions   | Based on service account's role/policies |
| Revocation impact | Only affects the token holder | Only affects the specific automation     |
| Use case          | Individual user API access    | Automated workflows and integrations     |
