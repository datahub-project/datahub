from datahub.ingestion.source.powerbi import m_parser

def test_parse_m_query():
    expression: str = "let\n    Source = Snowflake.Databases(\"bu20658.ap-southeast-2.snowflakecomputing.com\",\"PBI_TEST_WAREHOUSE_PROD\",[Role=\"PBI_TEST_MEMBER\"]),\n    PBI_TEST_Database = Source{[Name=\"PBI_TEST\",Kind=\"Database\"]}[Data],\n    TEST_Schema = PBI_TEST_Database{[Name=\"TEST\",Kind=\"Schema\"]}[Data],\n    TESTTABLE_Table = TEST_Schema{[Name=\"TESTTABLE\",Kind=\"Table\"]}[Data]\nin\n    TESTTABLE_Table"
    m_parser.parse_expression(expression)
