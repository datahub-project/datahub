#!/usr/bin/env python3
"""
Debug script to test the Snowflake database filter condition with multiple patterns.
"""

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.snowflake.snowflake_queries import (
    _build_access_history_database_filter_condition,
    _build_enriched_query_log_query
)
from datetime import datetime, timezone

# Test multiple database patterns like the user mentioned
database_pattern = AllowDenyPattern(allow=[r"^SMOKE_TEST_DB$", r"^SMOKE_TEST_DB_2$"])

# Test the filter condition
filter_condition = _build_access_history_database_filter_condition(database_pattern)
print("Filter condition:")
print(filter_condition)
print()

# Test with additional database names
additional_dbs = ["SALES_FORECASTING"]
filter_condition_with_additional = _build_access_history_database_filter_condition(
    database_pattern, additional_dbs
)
print("Filter condition with additional databases:")
print(filter_condition_with_additional)
print()

# Test a complete query
start_time = datetime.now(timezone.utc)
end_time = datetime.now(timezone.utc)
from datahub.configuration.time_window_config import BucketDuration

full_query = _build_enriched_query_log_query(
    start_time=start_time,
    end_time=end_time,
    bucket_duration=BucketDuration.HOUR,
    deny_usernames=None,
    database_pattern=database_pattern,
    additional_database_names=additional_dbs
)

print("Full query (showing database filter section):")
# Extract the relevant section
lines = full_query.split('\n')
for i, line in enumerate(lines):
    if 'root_query_id IS NOT NULL' in line:
        # Print the context around the stored procedure exemption
        for j in range(max(0, i-5), min(len(lines), i+10)):
            print(f"{j+1:3d}: {lines[j]}")
        break