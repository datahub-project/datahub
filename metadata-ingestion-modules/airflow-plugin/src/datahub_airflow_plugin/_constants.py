"""
Shared constants for the DataHub Airflow plugin.
"""

# Key under which the SQLParser patch stashes DataHub's enhanced parsing result
# (with column-level lineage) on the OpenLineage run-facets dict, so the
# DataHub listener can read it downstream.
DATAHUB_SQL_PARSING_RESULT_KEY = "datahub_sql_parsing_result"

FILE_PLATFORM = "file"

# Filesystem/object-store schemes and the DataHub platform each maps to. Mirrors
# HdfsPlatform in the Java openlineage-converter (used by the Spark agent and the
# GMS OpenLineage endpoint) so Airflow and Spark produce the same URN for a path,
# and that URN matches DataHub's S3/GCS/ABS sources (`<bucket>/<key>`). Keep the
# two tables in sync.
OL_FS_SCHEME_TO_PLATFORM = {
    "s3": "s3",
    "s3a": "s3",
    "s3n": "s3",
    "gs": "gcs",
    "gcs": "gcs",
    "abfs": "abs",
    "abfss": "abs",
    "wasb": "abs",
    "wasbs": "abs",
    "dbfs": "dbfs",
    "file": FILE_PLATFORM,
}
