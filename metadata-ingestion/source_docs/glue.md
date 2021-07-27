
# AWS Glue `glue`

Note: if you also have files in S3 that you'd like to ingest, we recommend you use Glue's built-in data catalog. See [here](./s3-ingestion.md) for a quick guide on how to set up a crawler on Glue and ingest the outputs with DataHub.

Extracts:

- List of tables
- Column types associated with each table
- Table metadata, such as owner, description and parameters
- Jobs and their component transformations, data sources, and data sinks

```yml
source:
  type: glue
  config:
    aws_region: # aws_region_name, i.e. "eu-west-1"
    extract_transforms: True # whether to ingest Glue jobs, defaults to True
    env: # environment for the DatasetSnapshot URN, one of "DEV", "EI", "PROD" or "CORP". Defaults to "PROD".

    # Filtering patterns for databases and tables to scan
    database_pattern: # Optional, to filter databases scanned, same as schema_pattern above.
    table_pattern: # Optional, to filter tables scanned, same as table_pattern above.

    # Credentials. If not specified here, these are picked up according to boto3 rules.
    # (see https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html)
    aws_access_key_id: # Optional.
    aws_secret_access_key: # Optional.
    aws_session_token: # Optional.
    aws_role: # Optional (Role chaining supported by using a sorted list).
```
