# AWS Athena

To install this plugin, run `pip install 'acryl-datahub[athena]'`.

This plugin extracts the following:

- List of databases and tables
- Column types associated with each table

```yml
source:
  type: athena
  config:
    username: aws_access_key_id # Optional. If not specified, credentials are picked up according to boto3 rules.
    # See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
    password: aws_secret_access_key # Optional.
    database: database # Optional, defaults to "default"
    aws_region: aws_region_name # i.e. "eu-west-1"
    s3_staging_dir: s3_location # "s3://<bucket-name>/prefix/"
    # The s3_staging_dir parameter is needed because Athena always writes query results to S3.
    # See https://docs.aws.amazon.com/athena/latest/ug/querying.html
    # However, the athena driver will transparently fetch these results as you would expect from any other sql client.
    work_group: athena_workgroup # "primary"
    # table_pattern/schema_pattern is same as above
```
