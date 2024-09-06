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
