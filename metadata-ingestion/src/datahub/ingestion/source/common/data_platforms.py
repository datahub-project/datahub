# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
    "streamlit",
]
