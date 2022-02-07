# Glue

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[glue]'`.

Note: if you also have files in S3 that you'd like to ingest, we recommend you use Glue's built-in data catalog. See [here](./s3.md) for a quick guide on how to set up a crawler on Glue and ingest the outputs with DataHub.

## Capabilities

This plugin extracts the following:

- Tables in the Glue catalog
- Column types associated with each table
- Table metadata, such as owner, description and parameters
- Jobs and their component transformations, data sources, and data sinks

| Capability | Status | Details | 
| -----------| ------ | ---- |
| Platform Instance | 🛑 | [link](../../docs/platform-instances.md) |

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: glue
  config:
    # Coordinates
    aws_region: "my-aws-region"

sink:
  # sink configs
```

## IAM permissions
For ingesting datasets, the following IAM permissions are required:
```json
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

For ingesting jobs (`extract_transforms: True`), the following additional permissions are required:
```json
{
    "Effect": "Allow",
    "Action": [
        "glue:GetDataflowGraph",
        "glue:GetJobs",
    ],
    "Resource": "*"
}
```

plus `s3:GetObject` for the job script locations.

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                           | Required | Default      | Description                                                                        |
| ------------------------------- | -------- | ------------ | ---------------------------------------------------------------------------------- |
| `aws_region`                    | ✅       |              | AWS region code.                                                                   |
| `env`                           |          | `"PROD"`     | Environment to use in namespace when constructing URNs.                            |
| `aws_access_key_id`             |          | Autodetected | See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html |
| `aws_secret_access_key`         |          | Autodetected | See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html |
| `aws_session_token`             |          | Autodetected | See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html |
| `aws_role`                      |          | Autodetected | See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html |
| `extract_transforms`            |          | `True`       | Whether to extract Glue transform jobs.                                            |
| `database_pattern.allow`        |          |              | List of regex patterns for databases to include in ingestion.                      |
| `database_pattern.deny`         |          |              | List of regex patterns for databases to exclude from ingestion.                    |
| `database_pattern.ignoreCase`   |          | `True`       | Whether to ignore case sensitivity during pattern matching.                        |
| `table_pattern.allow`           |          |              | List of regex patterns for tables to include in ingestion.                         |
| `table_pattern.deny`            |          |              | List of regex patterns for tables to exclude from ingestion.                       |
| `table_pattern.ignoreCase`      |          | `True`       | Whether to ignore case sensitivity during pattern matching.                        |
| `underlying_platform`           |          | `glue`       | Override for platform name. Allowed values - `glue`, `athena`                      |
| `ignore_unsupported_connectors` |          | `True`       | Whether to ignore unsupported connectors. If disabled, an error will be raised.    |
| `emit_s3_lineage`               |          | `True`       | Whether to emit S3-to-Glue lineage.                                                |
| `glue_s3_lineage_direction`     |          | `upstream`   | If `upstream`, S3 is upstream to Glue. If `downstream` S3 is downstream to Glue.   |

## Compatibility

To capture lineage across Glue jobs and databases, a requirements must be met – otherwise the AWS API is unable to report any lineage. The job must be created in Glue Studio with the "Generate classic script" option turned on (this option can be accessed in the "Script" tab). Any custom scripts that do not have the proper annotations will not have reported lineage.

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
