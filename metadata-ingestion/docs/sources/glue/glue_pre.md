### Overview

The `glue` module ingests metadata from Glue into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

This plugin extracts the following:

- Tables in the Glue catalog
- Column types associated with each table
- Table metadata, such as owner, description and parameters
- Jobs and their component transformations, data sources, and data sinks
- Upstream lineage from JDBC sources (e.g. PostgreSQL, MySQL, Redshift) referenced by Glue jobs

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

#### IAM permissions

For ingesting datasets, the following IAM permissions are required:

```
{
    "Effect": "Allow",
    "Action": [
        "glue:GetDatabases",
        "glue:GetTables"
    ],
    "Resource": [
        "arn:aws:glue:$region-id:$account-id:catalog",
        "arn:aws:glue:$region-id:$account-id:database/*",
        "arn:aws:glue:$region-id:$account-id:table/*"
    ]
}
```

For ingesting jobs (extract_transforms: True), the following additional permissions are required:

```
{
    "Effect": "Allow",
    "Action": [
        "glue:GetDataflowGraph",
        "glue:GetJobs",
        "glue:GetConnection",
        "s3:GetObject",
    ],
    "Resource": "*"
}
```

The `glue:GetConnection` permission is required when Glue jobs reference named connections (e.g. JDBC connections configured in the Glue console). If your jobs only use inline connection parameters, this permission is not needed.

For profiling datasets, the following additional permissions are required:

```
    {
    "Effect": "Allow",
    "Action": [
        "glue:GetPartitions",
    ],
    "Resource": "*"
}
```

#### Cross-Account Access

The Glue connector supports cross-account access via AWS STS AssumeRole. This allows DataHub running in one AWS account to ingest Glue metadata from a catalog in a different AWS account.

**Setup steps:**

1. **In the target account** (where the Glue catalog lives), create an IAM role with:
   - The Glue permissions policy shown above
   - A trust policy allowing the source account to assume the role:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::SOURCE-ACCOUNT-ID:role/DataHubExecutionRole"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "your-unique-external-id"
        }
      }
    }
  ]
}
```

2. **In the ingestion recipe**, configure `aws_config.aws_role` with the target role ARN:

**Simple ARN format:**

```yaml
source:
  type: glue
  config:
    aws_config:
      aws_role: "arn:aws:iam::TARGET-ACCOUNT-ID:role/DataHubGlueReadRole"
```

**With External ID** (recommended for security):

```yaml
source:
  type: glue
  config:
    aws_config:
      aws_role:
        RoleArn: "arn:aws:iam::TARGET-ACCOUNT-ID:role/DataHubGlueReadRole"
        ExternalId: "your-unique-external-id"
```

**Role chaining** (assume multiple roles in sequence):

```yaml
source:
  type: glue
  config:
    aws_config:
      aws_role:
        - "arn:aws:iam::INTERMEDIARY-ACCOUNT-ID:role/IntermediateRole"
        - RoleArn: "arn:aws:iam::TARGET-ACCOUNT-ID:role/DataHubGlueReadRole"
          ExternalId: "your-unique-external-id"
```

The connector uses [boto3's assume_role](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts.html#STS.Client.assume_role), so additional parameters like `RoleSessionName`, `DurationSeconds`, and `Policy` are also supported.

**Cross-account catalog access:**

For accessing a specific Glue catalog in another account (without assuming a role), use the `catalog_id` parameter:

```yaml
source:
  type: glue
  config:
    catalog_id: "123456789012" # Target account's AWS account ID
```

This is useful when Account A has shared its Glue catalog with Account B. If you're running ingestion from Account B and want to access Account A's catalog, specify Account A's ID in `catalog_id`.

**Platform instance considerations:**

- **Without platform instance**: If you ingest the same Glue catalog from different accounts without setting `platform_instance`, DataHub recognizes them as the same entities and creates a single dataset.
- **With platform instance**: Using different `platform_instance` values creates separate dataset entities with distinct URNs, useful for tracking the same data through different access paths.
