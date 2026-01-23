### Prerequisites

#### Obtain API Credentials

To connect DataHub to Matillion Data Productivity Cloud, you need an API token with sufficient permissions to read project and pipeline metadata.

**For Matillion DPC Cloud:**

1. Log into your Matillion Data Productivity Cloud account
2. Navigate to **Settings** → **API Tokens**
3. Click **Create New Token**
4. Name your token (e.g., "DataHub Integration")
5. Copy the generated token - you'll need it for the configuration

**For Self-Hosted Matillion:**

If you're using a self-hosted Matillion instance behind an API gateway or proxy, you may need to configure HTTP basic authentication in addition to the API token.

#### Required Permissions

The API token needs the following permissions:

- **Read Projects**: View project names, environments, and metadata
- **Read Pipelines**: Access pipeline definitions and metadata
- **Read Lineage Events**: Access OpenLineage events for lineage extraction
- **Read Executions**: View pipeline execution history (optional, for DataProcessInstance extraction)

Refer to the [Matillion API documentation](https://docs.matillion.com/data-productivity-cloud/api/) for the latest information on API permissions and authentication.

#### Setup OpenLineage Namespace Mapping

The connector extracts lineage from OpenLineage events provided by the Matillion API. To correctly link Matillion pipelines to your existing datasets in DataHub, you must configure namespace mapping.

OpenLineage events include namespace URIs like:

- `postgresql://prod-db.us-east-1.rds.amazonaws.com:5432`
- `snowflake://prod-account.snowflakecomputing.com`
- `bigquery://my-gcp-project`

You need to map these to your DataHub platform instances:

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

**Why this is important:**

- Without proper mapping, lineage will not connect to your existing datasets in DataHub
- Platform instances must match what you used when ingesting data from those sources
- Database and schema defaults help normalize incomplete table names from OpenLineage events

See the configuration section below for complete details on namespace mapping options.
