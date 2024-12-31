class DaxQuery:
    @staticmethod
    def data_sample_query(table_name: str) -> str:
        return f"EVALUATE TOPN(3, '{table_name}')"

    @staticmethod
    def column_data_query(table_name: str, column_name: str) -> str:
        return f"""
        EVALUATE ROW(
            "min", MIN('{table_name}'[{column_name}]),
            "max", MAX('{table_name}'[{column_name}]),
            "unique_count", COUNTROWS ( DISTINCT ( '{table_name}'[{column_name}] ) )
        )"""

    @staticmethod
    def row_count_query(table_name: str) -> str:
        return f"""EVALUATE ROW("count", COUNTROWS ( '{table_name}' ))"""
