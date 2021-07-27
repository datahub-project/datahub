# AWS Glue

To install this plugin, run `pip install 'acryl-datahub[glue]'`.

Note: if you also have files in S3 that you'd like to ingest, we recommend you use Glue's built-in data catalog. See [here](../s3-ingestion.md) for a quick guide on how to set up a crawler on Glue and ingest the outputs with DataHub.

This plugin extracts the following:

- List of tables
- Column types associated with each table
- Table metadata, such as owner, description and parameters
- Jobs and their component transformations, data sources, and data sinks

```yml
source:
  type: glue
  config:
    aws_region: # aws_region_name, i.e. "eu-west-1"
    env: # environment for the DatasetSnapshot URN, one of "DEV", "EI", "PROD" or "CORP". Defaults to "PROD".

    # Credentials. If not specified here, these are picked up according to boto3 rules.
    # (see https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html)
    aws_access_key_id: # Optional.
    aws_secret_access_key: # Optional.
    aws_session_token: # Optional.
    aws_role: # Optional (Role chaining supported by using a sorted list).

    extract_transforms: True # whether to ingest Glue jobs, defaults to True

    # Regex filters for databases to scan
    database_pattern:
      deny:
        # Note that the deny patterns take precedence over the allow patterns.
        - "bad_database"
        - "junk_database"
        # Can also be a regular expression
        - "(old|used|deprecated)_database"
      allow:
        - "good_database"
        - "excellent_database"
    table_pattern: # Optional, to filter tables scanned, same as table_pattern above.
      deny:
        # ...
      allow:
        # ...
```
