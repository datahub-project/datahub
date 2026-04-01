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

#### Glue Cross-account Access

Glue ingestion supports cross-account access and lineage by allowing you to specify the target AWS account's Glue catalog using the `catalog_id` parameter in the ingestion recipe. This enables ingestion of Glue metadata from different AWS accounts, supporting cross-account lineage scenarios. You must ensure the correct IAM roles and permissions are set up for cross-account access.

Example: There are 2 AWS accounts A and B, A has shared metadata with B. Account A has Glue table - tableA. If you ingest account A using Glue it will create dataset tableA in DataHub. If you want to ingest tableA via account B you can pass `catalog_id` parameter in recipe with A's catalog id.

Ingestion without platform instance parameter

- If both catalogs are ingested without platform instance parameter, DataHub should be able to understand that the database and tables are same
- DataHub will create single entity for table tableA
- It should show lineage between Glue and S3. You have to ingest S3 as separate source (https://docs.datahub.com/docs/generated/ingestion/sources/s3)

- Ingestion with platform instance parameter

- It will create separate entities for tableA as it will have different URN path
- It should show lineage between Glue and S3
