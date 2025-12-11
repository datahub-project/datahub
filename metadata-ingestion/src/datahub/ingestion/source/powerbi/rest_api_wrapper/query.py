# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
