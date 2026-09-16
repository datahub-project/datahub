"""
Shared constants for the DataHub Airflow plugin.
"""

# Key under which the SQLParser patch stashes DataHub's enhanced parsing result
# (with column-level lineage) on the OpenLineage run-facets dict, so the
# DataHub listener can read it downstream.
DATAHUB_SQL_PARSING_RESULT_KEY = "datahub_sql_parsing_result"
