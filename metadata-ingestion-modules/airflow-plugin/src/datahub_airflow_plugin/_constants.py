"""
Shared constants for the DataHub Airflow plugin.

This module centralizes constant values used across multiple modules
to avoid duplication and ensure consistency.
"""

# SQL parsing result keys for storing SQL lineage in OpenLineage facets

# Key for DataHub's enhanced SQL parsing result (with column-level lineage)
# Used in Airflow 3.x to pass results from SQLParser patch to DataHub listener
DATAHUB_SQL_PARSING_RESULT_KEY = "datahub_sql_parsing_result"

# Key for DataHub's SQL parsing result in Airflow 2.x extractors
# Used to pass results from extractors to DataHub listener
SQL_PARSING_RESULT_KEY = "datahub_sql"
