import logging
import sys
from typing import List

import pytest
from lark import Tree

import datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes as powerbi_data_classes
from datahub.ingestion.source.powerbi.config import PowerBiDashboardSourceReport
from datahub.ingestion.source.powerbi.m_query import parser, tree_function
from datahub.ingestion.source.powerbi.m_query.resolver import (
    DataPlatformTable,
    SupportedDataPlatform,
)

M_QUERIES = [
    'let\n    Source = Snowflake.Databases("bu20658.ap-southeast-2.snowflakecomputing.com","PBI_TEST_WAREHOUSE_PROD",[Role="PBI_TEST_MEMBER"]),\n    PBI_TEST_Database = Source{[Name="PBI_TEST",Kind="Database"]}[Data],\n    TEST_Schema = PBI_TEST_Database{[Name="TEST",Kind="Schema"]}[Data],\n    TESTTABLE_Table = TEST_Schema{[Name="TESTTABLE",Kind="Table"]}[Data]\nin\n    TESTTABLE_Table',
    'let\n    Source = Value.NativeQuery(Snowflake.Databases("bu20658.ap-southeast-2.snowflakecomputing.com","operations_analytics_warehouse_prod",[Role="OPERATIONS_ANALYTICS_MEMBER"]){[Name="OPERATIONS_ANALYTICS"]}[Data], "SELECT#(lf)concat((UPPER(REPLACE(SELLER,\'-\',\'\'))), MONTHID) as AGENT_KEY,#(lf)concat((UPPER(REPLACE(CLIENT_DIRECTOR,\'-\',\'\'))), MONTHID) as CD_AGENT_KEY,#(lf) *#(lf)FROM#(lf)OPERATIONS_ANALYTICS.TRANSFORMED_PROD.V_APS_SME_UNITS_V4", null, [EnableFolding=true]),\n    #"ADDed Conditional Column" = Table.AddColumn(Source, "SME Units ENT", each if [DEAL_TYPE] = "SME Unit" then [UNIT] else 0),\n    #"Added Conditional Column1" = Table.AddColumn(#"Added Conditional Column", "Banklink Units", each if [DEAL_TYPE] = "Banklink" then [UNIT] else 0),\n    #"Removed Columns" = Table.RemoveColumns(#"Added Conditional Column1",{"Banklink Units"}),\n    #"Added Custom" = Table.AddColumn(#"Removed Columns", "Banklink Units", each if [DEAL_TYPE] = "Banklink" and [SALES_TYPE] = "3 - Upsell"\nthen [UNIT]\n\nelse if [SALES_TYPE] = "Adjusted BL Migration"\nthen [UNIT]\n\nelse 0),\n    #"Added Custom1" = Table.AddColumn(#"Added Custom", "SME Units in $ (*$361)", each if [DEAL_TYPE] = "SME Unit" \nand [SALES_TYPE] <> "4 - Renewal"\n    then [UNIT] * 361\nelse 0),\n    #"Added Custom2" = Table.AddColumn(#"Added Custom1", "Banklink in $ (*$148)", each [Banklink Units] * 148)\nin\n    #"Added Custom2"',
    'let\n    Source = Value.NativeQuery(Snowflake.Databases("bu20  658.ap-southeast-2.snowflakecomputing.com","operations_analytics_warehouse_prod",[Role="OPERATIONS_ANALYTICS_MEMBER"]){[Name="OPERATIONS_ANALYTICS"]}[Data], "select #(lf)UPPER(REPLACE(AGENT_NAME,\'-\',\'\')) AS Agent,#(lf)TIER,#(lf)UPPER(MANAGER),#(lf)TEAM_TYPE,#(lf)DATE_TARGET,#(lf)MONTHID,#(lf)TARGET_TEAM,#(lf)SELLER_EMAIL,#(lf)concat((UPPER(REPLACE(AGENT_NAME,\'-\',\'\'))), MONTHID) as AGENT_KEY,#(lf)UNIT_TARGET AS SME_Quota,#(lf)AMV_TARGET AS Revenue_Quota,#(lf)SERVICE_QUOTA,#(lf)BL_TARGET,#(lf)SOFTWARE_QUOTA as Software_Quota#(lf)#(lf)from OPERATIONS_ANALYTICS.TRANSFORMED_PROD.V_SME_UNIT_TARGETS#(lf)#(lf)where YEAR_TARGET >= 2022#(lf)and TEAM_TYPE = \'Accounting\'#(lf)and TARGET_TEAM = \'Enterprise\'", null, [EnableFolding=true]),\n    #"Added Conditional Column" = Table.AddColumn(Source, "Has PS Software Quota?", each if [TIER] = "Expansion (Medium)" then "Yes" else if [TIER] = "Acquisition" then "Yes" else "No")\nin\n    #"Added Conditional Column"',
    'let\n    Source = Sql.Database("AUPRDWHDB", "COMMOPSDB", [Query="select *#(lf),concat((UPPER(REPLACE(CLIENT_MANAGER_QUOTED,\'-\',\'\'))), MONTHID) as AGENT_KEY#(lf),concat((UPPER(REPLACE(CLIENT_DIRECTOR,\'-\',\'\'))), MONTHID) as CD_AGENT_KEY#(lf)#(lf)from V_OIP_ENT_2022"]),\n    #"Added Custom" = Table.AddColumn(Source, "OIP in $(*$350)", each [SALES_INVOICE_AMOUNT] * 350),\n    #"Changed Type" = Table.TransformColumnTypes(#"Added Custom",{{"OIP in $(*$350)", type number}})\nin\n    #"Changed Type"',
    'let\n    Source = Sql.Database("AUPRDWHDB", "COMMOPSDB", [Query="Select *,#(lf)#(lf)concat((UPPER(REPLACE(CLIENT_MANAGER_QUOTED,\'-\',\'\'))), #(lf)LEFT(CAST(DTE AS DATE),4)+LEFT(RIGHT(CAST(DTE AS DATE),5),2)) AS AGENT_KEY,#(lf)concat((UPPER(REPLACE(CLIENT_DIRECTOR,\'-\',\'\'))), #(lf)LEFT(CAST(DTE AS DATE),4)+LEFT(RIGHT(CAST(DTE AS DATE),5),2)) AS CD_AGENT_KEY#(lf)#(lf)from V_INVOICE_BOOKING_2022"]),\n    #"Changed Type" = Table.TransformColumnTypes(Source,{{"CLIENT_ID", Int64.Type}}),\n    #"Added Conditional Column" = Table.AddColumn(#"Changed Type", "PS Software (One-Off)", each if Text.Contains([REVENUE_TYPE], "Software") then [Inv_Amt] else if Text.Contains([REVENUE_TYPE], "Tax Seminar") then [Inv_Amt] else 0),\n    #"Filtered Rows" = Table.SelectRows(#"Added Conditional Column", each true),\n    #"Duplicated Column" = Table.DuplicateColumn(#"Filtered Rows", "CLIENT_ID", "CLIENT_ID - Copy"),\n    #"Changed Type1" = Table.TransformColumnTypes(#"Duplicated Column",{{"CLIENT_ID - Copy", type text}}),\n    #"Renamed Columns" = Table.RenameColumns(#"Changed Type1",{{"CLIENT_ID - Copy", "CLIENT_ID for Filter"}})\nin\n    #"Renamed Columns"',
    'let\n    Source = Sql.Database("AUPRDWHDB", "COMMOPSDB", [Query="SELECT *,#(lf)concat((UPPER(REPLACE(CLIENT_MANAGER_CLOSING_MONTH,\'-\',\'\'))), #(lf)LEFT(CAST(MONTH_DATE AS DATE),4)+LEFT(RIGHT(CAST(MONTH_DATE AS DATE),5),2)) AS AGENT_KEY#(lf)#(lf)FROM dbo.V_ARR_ADDS"]),\n    #"Changed Type" = Table.TransformColumnTypes(Source,{{"MONTH_DATE", type date}}),\n    #"Added Custom" = Table.AddColumn(#"Changed Type", "Month", each Date.Month([MONTH_DATE]))\nin\n    #"Added Custom"',
    "let\n    Source = Value.NativeQuery(Snowflake.Databases(\"bu20658.ap-southeast-2.snowflakecomputing.com\",\"operations_analytics_warehouse_prod\",[Role=\"OPERATIONS_ANALYTICS_MEMBER\"]){[Name=\"OPERATIONS_ANALYTICS\"]}[Data], \"select #(lf)UPPER(REPLACE(AGENT_NAME,'-','')) AS CLIENT_DIRECTOR,#(lf)TIER,#(lf)UPPER(MANAGER),#(lf)TEAM_TYPE,#(lf)DATE_TARGET,#(lf)MONTHID,#(lf)TARGET_TEAM,#(lf)SELLER_EMAIL,#(lf)concat((UPPER(REPLACE(AGENT_NAME,'-',''))), MONTHID) as AGENT_KEY,#(lf)UNIT_TARGET AS SME_Quota,#(lf)AMV_TARGET AS Revenue_Quota,#(lf)SERVICE_QUOTA,#(lf)BL_TARGET,#(lf)SOFTWARE_QUOTA as Software_Quota#(lf)#(lf)from OPERATIONS_ANALYTICS.TRANSFORMED_PROD.V_SME_UNIT_TARGETS#(lf)#(lf)where YEAR_TARGET >= 2022#(lf)and TEAM_TYPE = 'Accounting'#(lf)and TARGET_TEAM = 'Enterprise'#(lf)AND TIER = 'Client Director'\", null, [EnableFolding=true])\nin\n    Source",
    'let\n    Source = Sql.Database("AUPRDWHDB", "COMMOPSDB", [Query="select *,#(lf)concat((UPPER(REPLACE(CLIENT_DIRECTOR,\'-\',\'\'))), MONTH_WID) as CD_AGENT_KEY,#(lf)concat((UPPER(REPLACE(CLIENT_MANAGER_CLOSING_MONTH,\'-\',\'\'))), MONTH_WID) as AGENT_KEY#(lf)#(lf)from V_PS_CD_RETENTION", CommandTimeout=#duration(0, 1, 30, 0)]),\n    #"Changed Type" = Table.TransformColumnTypes(Source,{{"mth_date", type date}}),\n    #"Added Custom" = Table.AddColumn(#"Changed Type", "Month", each Date.Month([mth_date])),\n    #"Added Custom1" = Table.AddColumn(#"Added Custom", "TPV Opening", each if [Month] = 1 then [TPV_AMV_OPENING]\nelse if [Month] = 2 then 0\nelse if [Month] = 3 then 0\nelse if [Month] = 4 then [TPV_AMV_OPENING]\nelse if [Month] = 5 then 0\nelse if [Month] = 6 then 0\nelse if [Month] = 7 then [TPV_AMV_OPENING]\nelse if [Month] = 8 then 0\nelse if [Month] = 9 then 0\nelse if [Month] = 10 then [TPV_AMV_OPENING]\nelse if [Month] = 11 then 0\nelse if [Month] = 12 then 0\n\nelse 0)\nin\n    #"Added Custom1"',
    'let\n    Source = Sql.Database("AUPRDWHDB", "COMMOPSDB", [Query="select#(lf)CLIENT_ID,#(lf)PARTNER_ACCOUNT_NAME,#(lf)CM_CLOSING_MNTH_COUNTRY,#(lf)MONTH_WID,#(lf)PS_DELETES,#(lf)CLIENT_MANAGER_CLOSING_MONTH,#(lf)SME_DELETES,#(lf)TPV_AMV_OPENING,#(lf)concat((UPPER(REPLACE(CLIENT_MANAGER_CLOSING_MONTH,\'-\',\'\'))), MONTH_WID) as AGENT_KEY#(lf)#(lf)from V_TPV_LEADERBOARD", CommandTimeout=#duration(0, 1, 30, 0)]),\n    #"Changed Type" = Table.TransformColumnTypes(Source,{{"MONTH_WID", type text}}),\n    #"Added Custom" = Table.AddColumn(#"Changed Type", "MONTH_DATE", each Date.FromText(\nText.Range([MONTH_WID], 0,4) & "-"  &\nText.Range([MONTH_WID], 4,2)\n)),\n    #"Added Custom2" = Table.AddColumn(#"Added Custom", "Month", each Date.Month([MONTH_DATE])),\n    #"Added Custom1" = Table.AddColumn(#"Added Custom2", "TPV Opening", each if [Month] = 1 then [TPV_AMV_OPENING]\nelse if [Month] = 2 then 0\nelse if [Month] = 3 then 0\nelse if [Month] = 4 then [TPV_AMV_OPENING]\nelse if [Month] = 5 then 0\nelse if [Month] = 6 then 0\nelse if [Month] = 7 then [TPV_AMV_OPENING]\nelse if [Month] = 8 then 0\nelse if [Month] = 9 then 0\nelse if [Month] = 10 then [TPV_AMV_OPENING]\nelse if [Month] = 11 then 0\nelse if [Month] = 12 then 0\n\nelse 0)\nin\n    #"Added Custom1"',
    'let\n    Source = Snowflake.Databases("bu20658.ap-southeast-2.snowflakecomputing.com","OPERATIONS_ANALYTICS_WAREHOUSE_PROD",[Role="OPERATIONS_ANALYTICS_MEMBER_AD"]),\n    OPERATIONS_ANALYTICS_Database = Source{[Name="OPERATIONS_ANALYTICS",Kind="Database"]}[Data],\n    TEST_Schema = OPERATIONS_ANALYTICS_Database{[Name="TEST",Kind="Schema"]}[Data],\n    LZ_MIGRATION_DOWNLOAD_View = TEST_Schema{[Name="LZ_MIGRATION_DOWNLOAD",Kind="View"]}[Data],\n    #"Changed Type" = Table.TransformColumnTypes(LZ_MIGRATION_DOWNLOAD_View,{{"MIGRATION_MONTH_ID", type text}}),\n    #"Added Custom" = Table.AddColumn(#"Changed Type", "Migration Month", each Date.FromText(\nText.Range([MIGRATION_MONTH_ID], 0,4) & "-" & \nText.Range([MIGRATION_MONTH_ID], 4,2) \n)),\n    #"Changed Type1" = Table.TransformColumnTypes(#"Added Custom",{{"Migration Month", type date}})\nin\n    #"Changed Type1"',
    "let\n    Source = Value.NativeQuery(Snowflake.Databases(\"bu20658.ap-southeast-2.snowflakecomputing.com\",\"operations_analytics_warehouse_prod\",[Role=\"OPERATIONS_ANALYTICS_MEMBER\"]){[Name=\"OPERATIONS_ANALYTICS\"]}[Data], \"select *,#(lf)UPPER(REPLACE(AGENT_NAME,'-','')) AS Agent,#(lf)concat((UPPER(REPLACE(AGENT_NAME,'-',''))), MONTHID) as AGENT_KEY#(lf)#(lf)from OPERATIONS_ANALYTICS.TRANSFORMED_PROD.V_SME_UNIT_TARGETS#(lf)#(lf)where YEAR_TARGET >= 2022#(lf)and TEAM_TYPE = 'Industries'#(lf)and TARGET_TEAM = 'Enterprise'\", null, [EnableFolding=true])\nin\n    Source",
    'let\n    Source = Sql.Database("AUPRDWHDB", "COMMOPSDB", [Query="Select#(lf)*,#(lf)concat((UPPER(REPLACE(SALES_SPECIALIST,\'-\',\'\'))),#(lf)LEFT(CAST(INVOICE_DATE AS DATE),4)+LEFT(RIGHT(CAST(INVOICE_DATE AS DATE),5),2)) AS AGENT_KEY,#(lf)CASE#(lf)    WHEN CLASS = \'Software\' and (NOT(PRODUCT in (\'ADV\', \'Adv\') and left(ACCOUNT_ID,2)=\'10\') #(lf)    or V_ENTERPRISE_INVOICED_REVENUE.TYPE = \'Manual Adjustment\') THEN INVOICE_AMOUNT#(lf)    WHEN V_ENTERPRISE_INVOICED_REVENUE.TYPE IN (\'Recurring\',\'0\') THEN INVOICE_AMOUNT#(lf)    ELSE 0#(lf)END as SOFTWARE_INV#(lf)#(lf)from V_ENTERPRISE_INVOICED_REVENUE", CommandTimeout=#duration(0, 1, 30, 0)]),\n    #"Added Conditional Column" = Table.AddColumn(Source, "Services", each if [CLASS] = "Services" then [INVOICE_AMOUNT] else 0),\n    #"Added Custom" = Table.AddColumn(#"Added Conditional Column", "Advanced New Sites", each if [PRODUCT] = "ADV"\nor [PRODUCT] = "Adv"\nthen [NEW_SITE]\nelse 0)\nin\n    #"Added Custom"',
    'let\n    Source = Snowflake.Databases("xaa48144.snowflakecomputing.com","GSL_TEST_WH",[Role="ACCOUNTADMIN"]),\n Source2 = PostgreSQL.Database("localhost", "mics"),\n  public_order_date = Source2{[Schema="public",Item="order_date"]}[Data],\n    GSL_TEST_DB_Database = Source{[Name="GSL_TEST_DB",Kind="Database"]}[Data],\n  PUBLIC_Schema = GSL_TEST_DB_Database{[Name="PUBLIC",Kind="Schema"]}[Data],\n   SALES_ANALYST_VIEW_View = PUBLIC_Schema{[Name="SALES_ANALYST_VIEW",Kind="View"]}[Data],\n  two_source_table  = Table.Combine({public_order_date, SALES_ANALYST_VIEW_View})\n in\n    two_source_table',
    'let\n    Source = PostgreSQL.Database("localhost"  ,   "mics"      ),\n  public_order_date =    Source{[Schema="public",Item="order_date"]}[Data] \n in \n public_order_date',
    'let\n    Source = Oracle.Database("localhost:1521/salesdb.GSLAB.COM", [HierarchicalNavigation=true]), HR = Source{[Schema="HR"]}[Data], EMPLOYEES1 = HR{[Name="EMPLOYEES"]}[Data] \n in EMPLOYEES1',
    'let\n    Source = Sql.Database("localhost", "library"),\n dbo_book_issue = Source{[Schema="dbo",Item="book_issue"]}[Data]\n in dbo_book_issue',
    'let\n    Source = Snowflake.Databases("xaa48144.snowflakecomputing.com","GSL_TEST_WH",[Role="ACCOUNTADMIN"]),\n    GSL_TEST_DB_Database = Source{[Name="GSL_TEST_DB",Kind="Database"]}[Data],\n    PUBLIC_Schema = GSL_TEST_DB_Database{[Name="PUBLIC",Kind="Schema"]}[Data],\n    SALES_FORECAST_Table = PUBLIC_Schema{[Name="SALES_FORECAST",Kind="Table"]}[Data],\n    SALES_ANALYST_Table = PUBLIC_Schema{[Name="SALES_ANALYST",Kind="Table"]}[Data],\n    RESULT = Table.Combine({SALES_FORECAST_Table, SALES_ANALYST_Table})\n\nin\n    RESULT',
]


@pytest.mark.integration
def test_parse_m_query1():
    expression: str = M_QUERIES[0]
    parse_tree: Tree = parser._parse_expression(expression)
    assert tree_function.get_output_variable(parse_tree) == "TESTTABLE_Table"


@pytest.mark.integration
def test_parse_m_query2():
    expression: str = M_QUERIES[1]
    parse_tree: Tree = parser._parse_expression(expression)
    assert tree_function.get_output_variable(parse_tree) == '"Added Custom2"'


@pytest.mark.integration
def test_parse_m_query3():
    expression: str = M_QUERIES[2]
    parse_tree: Tree = parser._parse_expression(expression)
    assert tree_function.get_output_variable(parse_tree) == '"Added Conditional Column"'


@pytest.mark.integration
def test_parse_m_query4():
    expression: str = M_QUERIES[3]
    parse_tree: Tree = parser._parse_expression(expression)
    assert tree_function.get_output_variable(parse_tree) == '"Changed Type"'


@pytest.mark.integration
def test_parse_m_query5():
    expression: str = M_QUERIES[4]
    parse_tree: Tree = parser._parse_expression(expression)
    assert tree_function.get_output_variable(parse_tree) == '"Renamed Columns"'


@pytest.mark.integration
def test_parse_m_query6():
    expression: str = M_QUERIES[5]
    parse_tree: Tree = parser._parse_expression(expression)
    assert tree_function.get_output_variable(parse_tree) == '"Added Custom"'


@pytest.mark.integration
def test_parse_m_query7():
    expression: str = M_QUERIES[6]
    parse_tree: Tree = parser._parse_expression(expression)
    assert tree_function.get_output_variable(parse_tree) == "Source"


@pytest.mark.integration
def test_parse_m_query8():
    expression: str = M_QUERIES[7]
    parse_tree: Tree = parser._parse_expression(expression)
    assert tree_function.get_output_variable(parse_tree) == '"Added Custom1"'


@pytest.mark.integration
def test_parse_m_query9():
    expression: str = M_QUERIES[8]
    parse_tree: Tree = parser._parse_expression(expression)
    assert tree_function.get_output_variable(parse_tree) == '"Added Custom1"'


@pytest.mark.integration
def test_parse_m_query10():
    expression: str = M_QUERIES[9]
    parse_tree: Tree = parser._parse_expression(expression)
    assert tree_function.get_output_variable(parse_tree) == '"Changed Type1"'


@pytest.mark.integration
def test_parse_m_query11():
    expression: str = M_QUERIES[10]
    parse_tree: Tree = parser._parse_expression(expression)
    assert tree_function.get_output_variable(parse_tree) == "Source"


@pytest.mark.integration
def test_parse_m_query12():
    expression: str = M_QUERIES[11]
    parse_tree: Tree = parser._parse_expression(expression)
    assert tree_function.get_output_variable(parse_tree) == '"Added Custom"'


@pytest.mark.integration
def test_parse_m_query13():
    expression: str = M_QUERIES[12]
    parse_tree: Tree = parser._parse_expression(expression)
    assert tree_function.get_output_variable(parse_tree) == "two_source_table"


@pytest.mark.integration
def test_snowflake_regular_case():
    q: str = M_QUERIES[0]
    table: powerbi_data_classes.Table = powerbi_data_classes.Table(
        expression=q,
        name="virtual_order_table",
        full_name="OrderDataSet.virtual_order_table",
    )

    reporter = PowerBiDashboardSourceReport()

    data_platform_tables: List[DataPlatformTable] = parser.get_upstream_tables(
        table, reporter
    )

    assert len(data_platform_tables) == 1
    assert data_platform_tables[0].name == "TESTTABLE"
    assert data_platform_tables[0].full_name == "PBI_TEST.TEST.TESTTABLE"
    assert (
        data_platform_tables[0].data_platform_pair.powerbi_data_platform_name
        == SupportedDataPlatform.SNOWFLAKE.value.powerbi_data_platform_name
    )


@pytest.mark.integration
def test_postgres_regular_case():
    q: str = M_QUERIES[13]
    table: powerbi_data_classes.Table = powerbi_data_classes.Table(
        expression=q,
        name="virtual_order_table",
        full_name="OrderDataSet.virtual_order_table",
    )

    reporter = PowerBiDashboardSourceReport()
    data_platform_tables: List[DataPlatformTable] = parser.get_upstream_tables(
        table, reporter
    )

    assert len(data_platform_tables) == 1
    assert data_platform_tables[0].name == "order_date"
    assert data_platform_tables[0].full_name == "mics.public.order_date"
    assert (
        data_platform_tables[0].data_platform_pair.powerbi_data_platform_name
        == SupportedDataPlatform.POSTGRES_SQL.value.powerbi_data_platform_name
    )


@pytest.mark.integration
def test_oracle_regular_case():
    q: str = M_QUERIES[14]
    table: powerbi_data_classes.Table = powerbi_data_classes.Table(
        expression=q,
        name="virtual_order_table",
        full_name="OrderDataSet.virtual_order_table",
    )

    reporter = PowerBiDashboardSourceReport()
    data_platform_tables: List[DataPlatformTable] = parser.get_upstream_tables(
        table, reporter
    )

    assert len(data_platform_tables) == 1
    assert data_platform_tables[0].name == "EMPLOYEES"
    assert data_platform_tables[0].full_name == "salesdb.HR.EMPLOYEES"
    assert (
        data_platform_tables[0].data_platform_pair.powerbi_data_platform_name
        == SupportedDataPlatform.ORACLE.value.powerbi_data_platform_name
    )


@pytest.mark.integration
def test_mssql_regular_case():
    q: str = M_QUERIES[15]
    table: powerbi_data_classes.Table = powerbi_data_classes.Table(
        expression=q,
        name="virtual_order_table",
        full_name="OrderDataSet.virtual_order_table",
    )

    reporter = PowerBiDashboardSourceReport()

    data_platform_tables: List[DataPlatformTable] = parser.get_upstream_tables(
        table, reporter
    )

    assert len(data_platform_tables) == 1
    assert data_platform_tables[0].name == "book_issue"
    assert data_platform_tables[0].full_name == "library.dbo.book_issue"
    assert (
        data_platform_tables[0].data_platform_pair.powerbi_data_platform_name
        == SupportedDataPlatform.MS_SQL.value.powerbi_data_platform_name
    )


@pytest.mark.integration
def test_mssql_with_query():
    mssql_queries: List[str] = [
        M_QUERIES[3],
        M_QUERIES[4],
        M_QUERIES[5],
        M_QUERIES[7],
        M_QUERIES[8],
        M_QUERIES[11],
    ]
    expected_tables = [
        "COMMOPSDB.dbo.V_OIP_ENT_2022",
        "COMMOPSDB.dbo.V_INVOICE_BOOKING_2022",
        "COMMOPSDB.dbo.V_ARR_ADDS",
        "COMMOPSDB.dbo.V_PS_CD_RETENTION",
        "COMMOPSDB.dbo.V_TPV_LEADERBOARD",
        "COMMOPSDB.dbo.V_ENTERPRISE_INVOICED_REVENUE",
    ]

    for index, query in enumerate(mssql_queries):
        table: powerbi_data_classes.Table = powerbi_data_classes.Table(
            expression=query,
            name="virtual_order_table",
            full_name="OrderDataSet.virtual_order_table",
        )
        reporter = PowerBiDashboardSourceReport()

        data_platform_tables: List[DataPlatformTable] = parser.get_upstream_tables(
            table, reporter, native_query_enabled=False
        )

        assert len(data_platform_tables) == 1
        assert data_platform_tables[0].name == expected_tables[index].split(".")[2]
        assert data_platform_tables[0].full_name == expected_tables[index]
        assert (
            data_platform_tables[0].data_platform_pair.powerbi_data_platform_name
            == SupportedDataPlatform.MS_SQL.value.powerbi_data_platform_name
        )


@pytest.mark.integration
def test_snowflake_native_query():
    snowflake_queries: List[str] = [
        M_QUERIES[1],
        M_QUERIES[2],
        M_QUERIES[6],
        M_QUERIES[10],
    ]

    expected_tables = [
        "OPERATIONS_ANALYTICS.TRANSFORMED_PROD.V_APS_SME_UNITS_V4",
        "OPERATIONS_ANALYTICS.TRANSFORMED_PROD.V_SME_UNIT_TARGETS",
        "OPERATIONS_ANALYTICS.TRANSFORMED_PROD.V_SME_UNIT_TARGETS",
        "OPERATIONS_ANALYTICS.TRANSFORMED_PROD.V_SME_UNIT_TARGETS",
    ]

    for index, query in enumerate(snowflake_queries):
        table: powerbi_data_classes.Table = powerbi_data_classes.Table(
            expression=query,
            name="virtual_order_table",
            full_name="OrderDataSet.virtual_order_table",
        )
        reporter = PowerBiDashboardSourceReport()

        data_platform_tables: List[DataPlatformTable] = parser.get_upstream_tables(
            table, reporter
        )

        assert len(data_platform_tables) == 1
        assert data_platform_tables[0].name == expected_tables[index].split(".")[2]
        assert data_platform_tables[0].full_name == expected_tables[index]
        assert (
            data_platform_tables[0].data_platform_pair.powerbi_data_platform_name
            == SupportedDataPlatform.SNOWFLAKE.value.powerbi_data_platform_name
        )


@pytest.mark.integration
def test_native_query_disabled():
    table: powerbi_data_classes.Table = powerbi_data_classes.Table(
        expression=M_QUERIES[1],  # 1st index has the native query
        name="virtual_order_table",
        full_name="OrderDataSet.virtual_order_table",
    )

    reporter = PowerBiDashboardSourceReport()

    data_platform_tables: List[DataPlatformTable] = parser.get_upstream_tables(
        table, reporter, native_query_enabled=False
    )
    assert len(data_platform_tables) == 0


@pytest.mark.integration
def test_multi_source_table():

    table: powerbi_data_classes.Table = powerbi_data_classes.Table(
        expression=M_QUERIES[12],  # 1st index has the native query
        name="virtual_order_table",
        full_name="OrderDataSet.virtual_order_table",
    )

    reporter = PowerBiDashboardSourceReport()
    data_platform_tables: List[DataPlatformTable] = parser.get_upstream_tables(
        table, reporter, native_query_enabled=False
    )

    assert len(data_platform_tables) == 2
    assert data_platform_tables[0].full_name == "mics.public.order_date"
    assert (
        data_platform_tables[0].data_platform_pair.powerbi_data_platform_name
        == SupportedDataPlatform.POSTGRES_SQL.value.powerbi_data_platform_name
    )

    assert data_platform_tables[1].full_name == "GSL_TEST_DB.PUBLIC.SALES_ANALYST_VIEW"
    assert (
        data_platform_tables[1].data_platform_pair.powerbi_data_platform_name
        == SupportedDataPlatform.SNOWFLAKE.value.powerbi_data_platform_name
    )


@pytest.mark.integration
def test_table_combine():
    table: powerbi_data_classes.Table = powerbi_data_classes.Table(
        expression=M_QUERIES[16],  # 1st index has the native query
        name="virtual_order_table",
        full_name="OrderDataSet.virtual_order_table",
    )

    reporter = PowerBiDashboardSourceReport()

    data_platform_tables: List[DataPlatformTable] = parser.get_upstream_tables(
        table, reporter
    )

    assert len(data_platform_tables) == 2
    assert data_platform_tables[0].full_name == "GSL_TEST_DB.PUBLIC.SALES_FORECAST"
    assert (
        data_platform_tables[0].data_platform_pair.powerbi_data_platform_name
        == SupportedDataPlatform.SNOWFLAKE.value.powerbi_data_platform_name
    )

    assert data_platform_tables[1].full_name == "GSL_TEST_DB.PUBLIC.SALES_ANALYST"
    assert (
        data_platform_tables[1].data_platform_pair.powerbi_data_platform_name
        == SupportedDataPlatform.SNOWFLAKE.value.powerbi_data_platform_name
    )


@pytest.mark.integration
def test_expression_is_none():
    """
    This test verifies the logging.debug should work if expression is None
    :return:
    """
    # set logging to console
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
    logging.getLogger().setLevel(logging.DEBUG)

    table: powerbi_data_classes.Table = powerbi_data_classes.Table(
        expression=None,  # 1st index has the native query
        name="virtual_order_table",
        full_name="OrderDataSet.virtual_order_table",
    )

    reporter = PowerBiDashboardSourceReport()

    data_platform_tables: List[DataPlatformTable] = parser.get_upstream_tables(
        table, reporter
    )

    assert len(data_platform_tables) == 0
