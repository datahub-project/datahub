### Prerequisites

#### Obtain API Credentials

The connector uses OAuth2 client credentials and automatically handles token generation and refresh.

1. Log into Matillion Data Productivity Cloud as a **Super Admin**
2. Navigate to **Profile & Account** → **API credentials**
3. Click **Set an API Credential**
4. Provide a descriptive name (e.g., "DataHub Integration")
5. Assign an **Account Role** with read permissions to required APIs
6. Click **Save** and immediately copy the **Client Secret** (not shown again)
7. Note the **Client ID** (remains visible)

For detailed instructions, see [Matillion API Authentication](https://docs.matillion.com/data-productivity-cloud/api/docs/authentication/).

#### Required Permissions

The API credentials must have an **Account Role** with **Read** permissions to:

- **Projects** (`/v1/projects`)
- **Environments** (`/v1/environments`)
- **Pipelines** (`/v1/pipelines`)
- **Schedules** (`/v1/schedules`)
- **Lineage Events** (`/v1/lineage/events`)
- **Pipeline Executions** (`/v1/pipeline-executions`) - optional
- **Streaming Pipelines** (`/v1/streaming-pipelines`) - optional

If using an account role other than **Super Admin**, grant project and environment-level roles as needed.

See [Matillion RBAC documentation](https://docs.matillion.com/data-productivity-cloud/hub/docs/role-based-access-control-overview/) for details.

#### Lineage Data Sources

Lineage is extracted from:

1. **OpenLineage Events API** (`/v1/lineage/events`) - Primary source for table and column-level lineage ([docs](https://docs.matillion.com/data-productivity-cloud/api/docs/endpoint-reference/?fullpage=true#/Data%20Lineage/get-lineage-events))
2. **Pipeline Executions API** (`/v1/pipeline-executions`) - Operational metadata emitted as DataProcessInstance entities ([docs](https://docs.matillion.com/data-productivity-cloud/api/docs/endpoint-reference/?fullpage=true#/Pipeline%20Execution/getPipelineExecutions))

#### OpenLineage Namespace Mapping (Optional)

**Optional**: Map OpenLineage namespace URIs to DataHub platform instances for lineage connections. If not configured, the connector extracts platform type from URIs (e.g., `postgresql://...` → `postgres`) with default environment (`PROD`).

**When to use**: Configure this when you need lineage to connect to existing datasets with platform instances.

Example namespaces: `postgresql://host:5432`, `snowflake://account.snowflakecomputing.com`, `bigquery://project`

```yaml
namespace_to_platform_instance:
  "postgresql://prod-db.us-east-1.rds.amazonaws.com:5432":
    platform_instance: postgres_prod
    env: PROD
    database: analytics
    schema: public

  "snowflake://prod-account.snowflakecomputing.com":
    platform_instance: snowflake_prod
    env: PROD
    convert_urns_to_lowercase: true
```

Platform instances must match those used when ingesting the source data platforms.
