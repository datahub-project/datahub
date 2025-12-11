# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

class SnowflakeDMFHandler:
    def create_dmf(
        self, dmf_name: str, dmf_args: str, dmf_comment: str, dmf_sql: str
    ) -> str:
        return f"""
            CREATE or REPLACE DATA METRIC FUNCTION
            {dmf_name} ({dmf_args})
            RETURNS NUMBER
            COMMENT = '{dmf_comment}'
            AS
            $$
            {dmf_sql}
            $$;
            """

    def add_dmf_to_table(
        self, dmf_name: str, dmf_col_args: str, dmf_schedule: str, table_identifier: str
    ) -> str:
        return f"""
            ALTER TABLE {table_identifier} SET DATA_METRIC_SCHEDULE = '{dmf_schedule}';
            ALTER TABLE {table_identifier} ADD DATA METRIC FUNCTION {dmf_name} ON ({dmf_col_args});
            """
