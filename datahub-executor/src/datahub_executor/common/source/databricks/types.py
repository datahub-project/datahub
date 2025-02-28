DEFAULT_OPERATION_TYPES_FILTER = "'INSERT', 'UPDATE', 'CREATE', 'CREATE_TABLE', 'CREATE_TABLE_AS_SELECT', 'COPY', 'MERGE'"  # Note that Alter is not included :)

SUPPORTED_LAST_MODIFIED_COLUMN_TYPES = [
    "DATE",
    "TIMESTAMP",
    "TIMESTAMP_NTZ",
]

SUPPORTED_HIGH_WATERMARK_COLUMN_TYPES = [
    "TINYINT",
    "SMALLINT",
    "INT",
    "BIGINT",
    "LONG",
    "SHORT",
    "FLOAT",
    "DOUBLE",
    "DECIMAL",
    "DATE",
    "TIMESTAMP",
    "TIMESTAMP_NTZ",
]

HIGH_WATERMARK_DATE_AND_TIME_TYPES = [
    "DATE",
    "TIMESTAMP",
    "TIMESTAMP_NTZ",
]

# https://docs.databricks.com/en/delta/history.html#operation-metrics-keys
OPERATION_TYPES_ALL = [
    "WRITE",
    "CREATE TABLE AS SELECT",
    "REPLACE TABLE AS SELECT",
    "COPY INTO",
    "STREAMING UPDATE",
    "DELETE",
    "TRUNCATE",
    "MERGE",
    "UPDATE",
    "FSCK",
    "CONVERT",
    "OPTIMIZE",
    "CLONE",
    "RESTORE",
    "VACUUM",
]
