# This is a pretty limited list, and is not really complete yet. Right now it's only used to allow
# automatic platform mapping when generating lineage and we have a manual override, so
# it being incomplete is ok. This should not be used for urn validation.
KNOWN_VALID_PLATFORM_NAMES = [
    "bigquery",
    "cassandra",
    "databricks",
    "delta-lake",
    "dbt",
    "feast",
    "file",
    "gcs",
    "hdfs",
    "hive",
    "mssql",
    "mysql",
    "oracle",
    "postgres",
    "redshift",
    "s3",
    "sagemaker",
    "snowflake",
]
