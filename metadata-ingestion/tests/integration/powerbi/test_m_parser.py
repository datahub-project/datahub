import logging
import sys
import time
from typing import List, Tuple
from unittest.mock import MagicMock, patch

import pytest
from lark import Tree

import datahub.ingestion.source.powerbi.m_query.data_classes
import datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes as powerbi_data_classes
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import StructuredLogLevel
from datahub.ingestion.source.powerbi.config import (
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
    AbstractDataPlatformInstanceResolver,
    create_dataplatform_instance_resolver,
)
from datahub.ingestion.source.powerbi.m_query import parser, tree_function
from datahub.ingestion.source.powerbi.m_query.data_classes import (
    DataPlatformTable,
    Lineage,
)

pytestmark = pytest.mark.integration_batch_2

M_QUERIES = [
    'let\n    Source = Snowflake.Databases("bu10758.ap-unknown-2.fakecomputing.com","PBI_TEST_WAREHOUSE_PROD",[Role="PBI_TEST_MEMBER"]),\n    PBI_TEST_Database = Source{[Name="PBI_TEST",Kind="Database"]}[Data],\n    TEST_Schema = PBI_TEST_Database{[Name="TEST",Kind="Schema"]}[Data],\n    TESTTABLE_Table = TEST_Schema{[Name="TESTTABLE",Kind="Table"]}[Data]\nin\n    TESTTABLE_Table',
    'let\n    Source = Value.NativeQuery(Snowflake.Databases("bu10758.ap-unknown-2.fakecomputing.com","operations_analytics_warehouse_prod",[Role="OPERATIONS_ANALYTICS_MEMBER"]){[Name="OPERATIONS_ANALYTICS"]}[Data], "SELECT#(lf)concat((UPPER(REPLACE(SELLER,\'-\',\'\'))), MONTHID) as AGENT_KEY,#(lf)concat((UPPER(REPLACE(CLIENT_DIRECTOR,\'-\',\'\'))), MONTHID) as CD_AGENT_KEY,#(lf) *#(lf)FROM#(lf)OPERATIONS_ANALYTICS.TRANSFORMED_PROD.V_APS_SME_UNITS_V4", null, [EnableFolding=true]),\n    #"ADDed Conditional Column" = Table.AddColumn(Source, "SME Units ENT", each if [DEAL_TYPE] = "SME Unit" then [UNIT] else 0),\n    #"Added Conditional Column1" = Table.AddColumn(#"Added Conditional Column", "Banklink Units", each if [DEAL_TYPE] = "Banklink" then [UNIT] else 0),\n    #"Removed Columns" = Table.RemoveColumns(#"Added Conditional Column1",{"Banklink Units"}),\n    #"Added Custom" = Table.AddColumn(#"Removed Columns", "Banklink Units", each if [DEAL_TYPE] = "Banklink" and [SALES_TYPE] = "3 - Upsell"\nthen [UNIT]\n\nelse if [SALES_TYPE] = "Adjusted BL Migration"\nthen [UNIT]\n\nelse 0),\n    #"Added Custom1" = Table.AddColumn(#"Added Custom", "SME Units in $ (*$361)", each if [DEAL_TYPE] = "SME Unit" \nand [SALES_TYPE] <> "4 - Renewal"\n    then [UNIT] * 361\nelse 0),\n    #"Added Custom2" = Table.AddColumn(#"Added Custom1", "Banklink in $ (*$148)", each [Banklink Units] * 148)\nin\n    #"Added Custom2"',
    'let\n    Source = Value.NativeQuery(Snowflake.Databases("bu10758.ap-unknown-2.fakecomputing.com","operations_analytics_warehouse_prod",[Role="OPERATIONS_ANALYTICS_MEMBER"]){[Name="OPERATIONS_ANALYTICS"]}[Data], "select #(lf)UPPER(REPLACE(AGENT_NAME,\'-\',\'\')) AS Agent,#(lf)TIER,#(lf)UPPER(MANAGER),#(lf)TEAM_TYPE,#(lf)DATE_TARGET,#(lf)MONTHID,#(lf)TARGET_TEAM,#(lf)SELLER_EMAIL,#(lf)concat((UPPER(REPLACE(AGENT_NAME,\'-\',\'\'))), MONTHID) as AGENT_KEY,#(lf)UNIT_TARGET AS SME_Quota,#(lf)AMV_TARGET AS Revenue_Quota,#(lf)SERVICE_QUOTA,#(lf)BL_TARGET,#(lf)SOFTWARE_QUOTA as Software_Quota#(lf)#(lf)from OPERATIONS_ANALYTICS.TRANSFORMED_PROD.V_SME_UNIT_TARGETS#(lf)#(lf)where YEAR_TARGET >= 2022#(lf)and TEAM_TYPE = \'Accounting\'#(lf)and TARGET_TEAM = \'Enterprise\'", null, [EnableFolding=true]),\n    #"Added Conditional Column" = Table.AddColumn(Source, "Has PS Software Quota?", each if [TIER] = "Expansion (Medium)" then "Yes" else if [TIER] = "Acquisition" then "Yes" else "No")\nin\n    #"Added Conditional Column"',
    'let\n    Source = Sql.Database("AUPRDWHDB", "COMMOPSDB", [Query="select *#(lf),concat((UPPER(REPLACE(CLIENT_MANAGER_QUOTED,\'-\',\'\'))), MONTHID) as AGENT_KEY#(lf),concat((UPPER(REPLACE(CLIENT_DIRECTOR,\'-\',\'\'))), MONTHID) as CD_AGENT_KEY#(lf)#(lf)from V_OIP_ENT_2022"]),\n    #"Added Custom" = Table.AddColumn(Source, "OIP in $(*$350)", each [SALES_INVOICE_AMOUNT] * 350),\n    #"Changed Type" = Table.TransformColumnTypes(#"Added Custom",{{"OIP in $(*$350)", type number}})\nin\n    #"Changed Type"',
    'let\n    Source = Sql.Database("AUPRDWHDB", "COMMOPSDB", [Query="Select *,#(lf)#(lf)concat((UPPER(REPLACE(CLIENT_MANAGER_QUOTED,\'-\',\'\'))), #(lf)LEFT(CAST(DTE AS DATE),4)+LEFT(RIGHT(CAST(DTE AS DATE),5),2)) AS AGENT_KEY,#(lf)concat((UPPER(REPLACE(CLIENT_DIRECTOR,\'-\',\'\'))), #(lf)LEFT(CAST(DTE AS DATE),4)+LEFT(RIGHT(CAST(DTE AS DATE),5),2)) AS CD_AGENT_KEY#(lf)#(lf)from V_INVOICE_BOOKING_2022"]),\n    #"Changed Type" = Table.TransformColumnTypes(Source,{{"CLIENT_ID", Int64.Type}}),\n    #"Added Conditional Column" = Table.AddColumn(#"Changed Type", "PS Software (One-Off)", each if Text.Contains([REVENUE_TYPE], "Software") then [Inv_Amt] else if Text.Contains([REVENUE_TYPE], "Tax Seminar") then [Inv_Amt] else 0),\n    #"Filtered Rows" = Table.SelectRows(#"Added Conditional Column", each true),\n    #"Duplicated Column" = Table.DuplicateColumn(#"Filtered Rows", "CLIENT_ID", "CLIENT_ID - Copy"),\n    #"Changed Type1" = Table.TransformColumnTypes(#"Duplicated Column",{{"CLIENT_ID - Copy", type text}}),\n    #"Renamed Columns" = Table.RenameColumns(#"Changed Type1",{{"CLIENT_ID - Copy", "CLIENT_ID for Filter"}})\nin\n    #"Renamed Columns"',
    'let\n    Source = Sql.Database("AUPRDWHDB", "COMMOPSDB", [Query="SELECT *,#(lf)concat((UPPER(REPLACE(CLIENT_MANAGER_CLOSING_MONTH,\'-\',\'\'))), #(lf)LEFT(CAST(MONTH_DATE AS DATE),4)+LEFT(RIGHT(CAST(MONTH_DATE AS DATE),5),2)) AS AGENT_KEY#(lf)#(lf)FROM dbo.V_ARR_ADDS"]),\n    #"Changed Type" = Table.TransformColumnTypes(Source,{{"MONTH_DATE", type date}}),\n    #"Added Custom" = Table.AddColumn(#"Changed Type", "Month", each Date.Month([MONTH_DATE]))\nin\n    #"Added Custom"',
    "let\n    Source = Value.NativeQuery(Snowflake.Databases(\"bu10758.ap-unknown-2.fakecomputing.com\",\"operations_analytics_warehouse_prod\",[Role=\"OPERATIONS_ANALYTICS_MEMBER\"]){[Name=\"OPERATIONS_ANALYTICS\"]}[Data], \"select #(lf)UPPER(REPLACE(AGENT_NAME,'-','')) AS CLIENT_DIRECTOR,#(lf)TIER,#(lf)UPPER(MANAGER),#(lf)TEAM_TYPE,#(lf)DATE_TARGET,#(lf)MONTHID,#(lf)TARGET_TEAM,#(lf)SELLER_EMAIL,#(lf)concat((UPPER(REPLACE(AGENT_NAME,'-',''))), MONTHID) as AGENT_KEY,#(lf)UNIT_TARGET AS SME_Quota,#(lf)AMV_TARGET AS Revenue_Quota,#(lf)SERVICE_QUOTA,#(lf)BL_TARGET,#(lf)SOFTWARE_QUOTA as Software_Quota#(lf)#(lf)from OPERATIONS_ANALYTICS.TRANSFORMED_PROD.V_SME_UNIT_TARGETS#(lf)#(lf)where YEAR_TARGET >= 2022#(lf)and TEAM_TYPE = 'Accounting'#(lf)and TARGET_TEAM = 'Enterprise'#(lf)AND TIER = 'Client Director'\", null, [EnableFolding=true])\nin\n    Source",
    'let\n    Source = Sql.Database("AUPRDWHDB", "COMMOPSDB", [Query="select *,#(lf)concat((UPPER(REPLACE(CLIENT_DIRECTOR,\'-\',\'\'))), MONTH_WID) as CD_AGENT_KEY,#(lf)concat((UPPER(REPLACE(CLIENT_MANAGER_CLOSING_MONTH,\'-\',\'\'))), MONTH_WID) as AGENT_KEY#(lf)#(lf)from V_PS_CD_RETENTION", CommandTimeout=#duration(0, 1, 30, 0)]),\n    #"Changed Type" = Table.TransformColumnTypes(Source,{{"mth_date", type date}}),\n    #"Added Custom" = Table.AddColumn(#"Changed Type", "Month", each Date.Month([mth_date])),\n    #"Added Custom1" = Table.AddColumn(#"Added Custom", "TPV Opening", each if [Month] = 1 then [TPV_AMV_OPENING]\nelse if [Month] = 2 then 0\nelse if [Month] = 3 then 0\nelse if [Month] = 4 then [TPV_AMV_OPENING]\nelse if [Month] = 5 then 0\nelse if [Month] = 6 then 0\nelse if [Month] = 7 then [TPV_AMV_OPENING]\nelse if [Month] = 8 then 0\nelse if [Month] = 9 then 0\nelse if [Month] = 10 then [TPV_AMV_OPENING]\nelse if [Month] = 11 then 0\nelse if [Month] = 12 then 0\n\nelse 0)\nin\n    #"Added Custom1"',
    'let\n    Source = Sql.Database("AUPRDWHDB", "COMMOPSDB", [Query="select#(lf)CLIENT_ID,#(lf)PARTNER_ACCOUNT_NAME,#(lf)CM_CLOSING_MNTH_COUNTRY,#(lf)MONTH_WID,#(lf)PS_DELETES,#(lf)CLIENT_MANAGER_CLOSING_MONTH,#(lf)SME_DELETES,#(lf)TPV_AMV_OPENING,#(lf)concat((UPPER(REPLACE(CLIENT_MANAGER_CLOSING_MONTH,\'-\',\'\'))), MONTH_WID) as AGENT_KEY#(lf)#(lf)from V_TPV_LEADERBOARD", CommandTimeout=#duration(0, 1, 30, 0)]),\n    #"Changed Type" = Table.TransformColumnTypes(Source,{{"MONTH_WID", type text}}),\n    #"Added Custom" = Table.AddColumn(#"Changed Type", "MONTH_DATE", each Date.FromText(\nText.Range([MONTH_WID], 0,4) & "-"  &\nText.Range([MONTH_WID], 4,2)\n)),\n    #"Added Custom2" = Table.AddColumn(#"Added Custom", "Month", each Date.Month([MONTH_DATE])),\n    #"Added Custom1" = Table.AddColumn(#"Added Custom2", "TPV Opening", each if [Month] = 1 then [TPV_AMV_OPENING]\nelse if [Month] = 2 then 0\nelse if [Month] = 3 then 0\nelse if [Month] = 4 then [TPV_AMV_OPENING]\nelse if [Month] = 5 then 0\nelse if [Month] = 6 then 0\nelse if [Month] = 7 then [TPV_AMV_OPENING]\nelse if [Month] = 8 then 0\nelse if [Month] = 9 then 0\nelse if [Month] = 10 then [TPV_AMV_OPENING]\nelse if [Month] = 11 then 0\nelse if [Month] = 12 then 0\n\nelse 0)\nin\n    #"Added Custom1"',
    'let\n    Source = Snowflake.Databases("bu10758.ap-unknown-2.fakecomputing.com","OPERATIONS_ANALYTICS_WAREHOUSE_PROD",[Role="OPERATIONS_ANALYTICS_MEMBER_AD"]),\n    OPERATIONS_ANALYTICS_Database = Source{[Name="OPERATIONS_ANALYTICS",Kind="Database"]}[Data],\n    TEST_Schema = OPERATIONS_ANALYTICS_Database{[Name="TEST",Kind="Schema"]}[Data],\n    LZ_MIGRATION_DOWNLOAD_View = TEST_Schema{[Name="LZ_MIGRATION_DOWNLOAD",Kind="View"]}[Data],\n    #"Changed Type" = Table.TransformColumnTypes(LZ_MIGRATION_DOWNLOAD_View,{{"MIGRATION_MONTH_ID", type text}}),\n    #"Added Custom" = Table.AddColumn(#"Changed Type", "Migration Month", each Date.FromText(\nText.Range([MIGRATION_MONTH_ID], 0,4) & "-" & \nText.Range([MIGRATION_MONTH_ID], 4,2) \n)),\n    #"Changed Type1" = Table.TransformColumnTypes(#"Added Custom",{{"Migration Month", type date}})\nin\n    #"Changed Type1"',
    "let\n    Source = Value.NativeQuery(Snowflake.Databases(\"bu10758.ap-unknown-2.fakecomputing.com\",\"operations_analytics_warehouse_prod\",[Role=\"OPERATIONS_ANALYTICS_MEMBER\"]){[Name=\"OPERATIONS_ANALYTICS\"]}[Data], \"select *,#(lf)UPPER(REPLACE(AGENT_NAME,'-','')) AS Agent,#(lf)concat((UPPER(REPLACE(AGENT_NAME,'-',''))), MONTHID) as AGENT_KEY#(lf)#(lf)from OPERATIONS_ANALYTICS.TRANSFORMED_PROD.V_SME_UNIT_TARGETS#(lf)#(lf)where YEAR_TARGET >= 2022#(lf)and TEAM_TYPE = 'Industries'#(lf)and TARGET_TEAM = 'Enterprise'\", null, [EnableFolding=true])\nin\n    Source",
    'let\n    Source = Sql.Database("AUPRDWHDB", "COMMOPSDB", [Query="Select#(lf)*,#(lf)concat((UPPER(REPLACE(SALES_SPECIALIST,\'-\',\'\'))),#(lf)LEFT(CAST(INVOICE_DATE AS DATE),4)+LEFT(RIGHT(CAST(INVOICE_DATE AS DATE),5),2)) AS AGENT_KEY,#(lf)CASE#(lf)    WHEN CLASS = \'Software\' and (NOT(PRODUCT in (\'ADV\', \'Adv\') and left(ACCOUNT_ID,2)=\'10\') #(lf)    or V_ENTERPRISE_INVOICED_REVENUE.TYPE = \'Manual Adjustment\') THEN INVOICE_AMOUNT#(lf)    WHEN V_ENTERPRISE_INVOICED_REVENUE.TYPE IN (\'Recurring\',\'0\') THEN INVOICE_AMOUNT#(lf)    ELSE 0#(lf)END as SOFTWARE_INV#(lf)#(lf)from V_ENTERPRISE_INVOICED_REVENUE", CommandTimeout=#duration(0, 1, 30, 0)]),\n    #"Added Conditional Column" = Table.AddColumn(Source, "Services", each if [CLASS] = "Services" then [INVOICE_AMOUNT] else 0),\n    #"Added Custom" = Table.AddColumn(#"Added Conditional Column", "Advanced New Sites", each if [PRODUCT] = "ADV"\nor [PRODUCT] = "Adv"\nthen [NEW_SITE]\nelse 0)\nin\n    #"Added Custom"',
    'let\n    Source = Snowflake.Databases("ghh48144.snowflakefakecomputing.com","GSL_TEST_WH",[Role="ACCOUNTADMIN"]),\n Source2 = PostgreSQL.Database("localhost", "mics"),\n  public_order_date = Source2{[Schema="public",Item="order_date"]}[Data],\n    GSL_TEST_DB_Database = Source{[Name="GSL_TEST_DB",Kind="Database"]}[Data],\n  PUBLIC_Schema = GSL_TEST_DB_Database{[Name="PUBLIC",Kind="Schema"]}[Data],\n   SALES_ANALYST_VIEW_View = PUBLIC_Schema{[Name="SALES_ANALYST_VIEW",Kind="View"]}[Data],\n  two_source_table  = Table.Combine({public_order_date, SALES_ANALYST_VIEW_View})\n in\n    two_source_table',
    'let\n    Source = PostgreSQL.Database("localhost"  ,   "mics"      ),\n  public_order_date =    Source{[Schema="public",Item="order_date"]}[Data] \n in \n public_order_date',
    'let\n    Source = Oracle.Database("localhost:1521/salesdb.domain.com", [HierarchicalNavigation=true]), HR = Source{[Schema="HR"]}[Data], EMPLOYEES1 = HR{[Name="EMPLOYEES"]}[Data] \n in EMPLOYEES1',
    'let\n    Source = Sql.Database("localhost", "library"),\n dbo_book_issue = Source{[Schema="dbo",Item="book_issue"]}[Data]\n in dbo_book_issue',
    'let\n    Source = Snowflake.Databases("ghh48144.snowflakefakecomputing.com","GSL_TEST_WH",[Role="ACCOUNTADMIN"]),\n    GSL_TEST_DB_Database = Source{[Name="GSL_TEST_DB",Kind="Database"]}[Data],\n    PUBLIC_Schema = GSL_TEST_DB_Database{[Name="PUBLIC",Kind="Schema"]}[Data],\n    SALES_FORECAST_Table = PUBLIC_Schema{[Name="SALES_FORECAST",Kind="Table"]}[Data],\n    SALES_ANALYST_Table = PUBLIC_Schema{[Name="SALES_ANALYST",Kind="Table"]}[Data],\n    RESULT = Table.Combine({SALES_FORECAST_Table, SALES_ANALYST_Table})\n\nin\n    RESULT',
    'let\n    Source = GoogleBigQuery.Database(),\n    #"seraphic-music-344307" = Source{[Name="seraphic-music-344307"]}[Data],\n    school_dataset_Schema = #"seraphic-music-344307"{[Name="school_dataset",Kind="Schema"]}[Data],\n    first_Table = school_dataset_Schema{[Name="first",Kind="Table"]}[Data]\nin\n    first_Table',
    'let    \nSource = GoogleBigQuery.Database([BillingProject = #"Parameter - Source"]),\n#"gcp-project" = Source{[Name=#"Parameter - Source"]}[Data],\ngcp_billing_Schema = #"gcp-project"{[Name=#"My bq project",Kind="Schema"]}[Data],\nF_GCP_COST_Table = gcp_billing_Schema{[Name="GCP_TABLE",Kind="Table"]}[Data]\nin\nF_GCP_COST_Table',
    'let\n Source = GoogleBigQuery.Database([BillingProject = #"Parameter - Source"]),\n#"gcp-project" = Source{[Name=#"Parameter - Source"]}[Data],\nuniversal_Schema = #"gcp-project"{[Name="universal",Kind="Schema"]}[Data],\nD_WH_DATE_Table = universal_Schema{[Name="D_WH_DATE",Kind="Table"]}[Data],\n#"Filtered Rows" = Table.SelectRows(D_WH_DATE_Table, each [D_DATE] > #datetime(2019, 9, 10, 0, 0, 0)),\n#"Filtered Rows1" = Table.SelectRows(#"Filtered Rows", each DateTime.IsInPreviousNHours([D_DATE], 87600))\n in \n#"Filtered Rows1"',
    'let\n Source = GoogleBigQuery.Database([BillingProject="dwh-prod"]),\ngcp_project = Source{[Name="dwh-prod"]}[Data],\ngcp_billing_Schema = gcp_project {[Name="gcp_billing",Kind="Schema"]}[Data],\nD_GCP_CUSTOM_LABEL_Table = gcp_billing_Schema{[Name="D_GCP_CUSTOM_LABEL",Kind="Table"]}[Data] \n in \n D_GCP_CUSTOM_LABEL_Table',
    'let\n    Source = AmazonRedshift.Database("redshift-url","dev"),\n    public = Source{[Name="public"]}[Data],\n    category1 = public{[Name="category"]}[Data]\nin\n    category1',
    'let\n Source = Value.NativeQuery(AmazonRedshift.Database("redshift-url","dev"), "select * from dev.public.category", null, [EnableFolding=true]) \n in Source',
    'let\n    Source = Databricks.Catalogs("adb-123.azuredatabricks.net", "/sql/1.0/endpoints/12345dc91aa25844", [Database=null, Catalog="abc"]),\n    hive_metastore_Database = Source{[Name="hive_metastore",Kind="Database"]}[Data],\n    sandbox_revenue_Schema = hive_metastore_Database{[Name="sandbox_revenue",Kind="Schema"]}[Data],\n    public_consumer_price_index_Table = sandbox_revenue_Schema{[Name="public_consumer_price_index",Kind="Table"]}[Data],\n    #"Renamed Columns" = Table.RenameColumns(public_consumer_price_index_Table,{{"Country", "country"}, {"Metric", "metric"}}),\n #"Inserted Year" = Table.AddColumn(#"Renamed Columns", "ID", each Date.Year([date_id]) + Date.Month([date_id]), Text.Type),\n #"Added Custom" = Table.AddColumn(#"Inserted Year", "Custom", each Text.Combine({Number.ToText(Date.Year([date_id])), Number.ToText(Date.Month([date_id])), [country]})),\n    #"Removed Columns" = Table.RemoveColumns(#"Added Custom",{"ID"}),\n    #"Renamed Columns1" = Table.RenameColumns(#"Removed Columns",{{"Custom", "ID"}}),\n #"Filtered Rows" = Table.SelectRows(#"Renamed Columns1", each ([metric] = "Consumer Price Index") and (not Number.IsNaN([value])))\nin\n    #"Filtered Rows"',
    "let\n    Source = Value.NativeQuery(Snowflake.Databases(\"bu10758.ap-unknown-2.fakecomputing.com\",\"operations_analytics_warehouse_prod\",[Role=\"OPERATIONS_ANALYTICS_MEMBER\"]){[Name=\"OPERATIONS_ANALYTICS\"]}[Data], \"select #(lf)UPPER(REPLACE(AGENT_NAME,'-','')) AS CLIENT_DIRECTOR,#(lf)TIER,#(lf)UPPER(MANAGER),#(lf)TEAM_TYPE,#(lf)DATE_TARGET,#(lf)MONTHID,#(lf)TARGET_TEAM,#(lf)SELLER_EMAIL,#(lf)concat((UPPER(REPLACE(AGENT_NAME,'-',''))), MONTHID) as AGENT_KEY,#(lf)UNIT_TARGET AS SME_Quota,#(lf)AMV_TARGET AS Revenue_Quota,#(lf)SERVICE_QUOTA,#(lf)BL_TARGET,#(lf)SOFTWARE_QUOTA as Software_Quota#(lf)#(lf)from OPERATIONS_ANALYTICS.TRANSFORMED_PROD.V_SME_UNIT_TARGETS inner join OPERATIONS_ANALYTICS.TRANSFORMED_PROD.V_SME_UNIT #(lf)#(lf)where YEAR_TARGET >= 2022#(lf)and TEAM_TYPE = 'Accounting'#(lf)and TARGET_TEAM = 'Enterprise'#(lf)AND TIER = 'Client Director'\", null, [EnableFolding=true])\nin\n    Source",
    'let\n Source = DatabricksMultiCloud.Catalogs("abc.cloud.databricks.com", "/sql/gh2cfe3fe1d4c7cd", [Catalog=null, Database=null, EnableAutomaticProxyDiscovery=null]),\n ml_prod_Database = Source{[Name="ml_prod",Kind="Database"]}[Data],\n membership_corn_Schema = ml_prod_Database{[Name="membership_corn",Kind="Schema"]}[Data],\n time_exp_data_v3_Table = membership_corn_Schema{[Name="time_exp_data_v3",Kind="Table"]}[Data],\n #"Renamed Columns" = Table.RenameColumns(time_exp_data_v3_Table)\nin\n #"Renamed Columns"',
    'let\n Source = DatabricksMultiCloud.Catalogs("abc.cloud.databricks.com", "/sql/gh2cfe3fe1d4c7cd", [Catalog="data_analysis", Database="summary", EnableAutomaticProxyDiscovery=null]),\n committee_data_summary_dev = Source{[Item="committee_data",Schema="summary",Catalog="data_analysis"]}[Data],\n #"Added Index" = Table.AddIndexColumn(committee_data_summary_dev, "Index", 1, 1, Int64.Type),\n #"Renamed Columns" = Table.RenameColumns(#"Added Index",)\nin\n #"Renamed Columns"',
    'let\n Source = DatabricksMultiCloud.Catalogs("abc.cloud.databricks.com", "/sql/gh2cfe3fe1d4c7cd", [Catalog="data_analysis", Database="summary", EnableAutomaticProxyDiscovery=null]),\n vips_data_summary_dev = Source{[Item="vips_data",Schema="summary",Catalog="data_analysis"]}[Data],\n #"Changed Type" = Table.TransformColumnTypes(vips_data_summary_dev,{{"vipstartDate", type date}, {"enteredDate", type datetime}, {"estDraftDate", type datetime}, {"estPublDate", type datetime}})\nin\n #"Changed Type"',
    'let\n Source = Value.NativeQuery(Snowflake.Databases("0DD93C6BD5A6.snowflakecomputing.com","sales_analytics_warehouse_prod",[Role="sales_analytics_member_ad"]){[Name="ORDERING"]}[Data], "SELECT#(lf) DISTINCT#(lf) T5.PRESENTMENT_START_DATE#(lf),T5.PRESENTMENT_END_DATE#(lf),T5.DISPLAY_NAME#(lf),T5.NAME#(tab)#(lf),T5.PROMO_DISPLAY_NAME#(lf),T5.REGION#(lf),T5.ID#(lf),T5.WALKOUT#(lf),T6.DEAL_ID#(lf),T6.TYPE#(lf),T5.FREE_PERIOD#(lf),T6.PRICE_MODIFICATION#(lf)#(lf)FROM#(lf)#(lf)(#(lf)    SELECT #(lf) T1.NAME#(lf),DATE(T1.CREATED_AT) as CREATED_AT#(lf),T1.PROMO_CODE#(lf),T1.STATUS#(lf),DATE(T1.UPDATED_AT) as UPDATED_AT#(lf),T1.ID#(lf),T1.DISPLAY_NAME as PROMO_DISPLAY_NAME#(lf),T4.*#(lf)FROM#(lf)(SELECT#(lf) DISTINCT#(lf) NAME#(lf),CREATED_AT#(lf),PROMO_CODE#(lf),STATUS#(lf),UPDATED_AT#(lf),ID#(lf),DISPLAY_NAME#(lf) FROM RAW.PROMOTIONS#(lf)#(lf)) T1#(lf)INNER JOIN#(lf)#(lf) (#(lf)    SELECT #(lf) T3.PRODUCT_STATUS#(lf),T3.CODE#(lf),T3.REGION#(lf),T3.DISPLAY_ORDER_SEQUENCE#(lf),T3.PRODUCT_LINE_ID#(lf),T3.DISPLAY_NAME#(lf),T3.PRODUCT_TYPE#(lf),T3.ID as PROD_TBL_ID#(lf),T3.NAME as PROD_TBL_NAME#(lf),DATE(T2.PRESENTMENT_END_DATE) as PRESENTMENT_END_DATE#(lf),T2.PRICE_COMMITMENT_PERIOD#(lf),T2.NAME as SEAL_TBL_NAME#(lf),DATE(T2.CREATED_AT) as SEAL_TBL_CREATED_AT#(lf),T2.DESCRIPTION#(lf),T2.FREE_PERIOD#(lf),T2.WALKOUT#(lf),T2.PRODUCT_CAT_ID#(lf),T2.PROMOTION_ID#(lf),DATE(T2.PRESENTMENT_START_DATE) as PRESENTMENT_START_DATE#(lf),YEAR(T2.PRESENTMENT_START_DATE) as DEAL_YEAR_START#(lf),MONTH(T2.PRESENTMENT_START_DATE) as DEAL_MONTH_START#(lf),T2.DEAL_TYPE#(lf),DATE(T2.UPDATED_AT) as SEAL_TBL_UPDATED_AT#(lf),T2.ID as SEAL_TBL_ID#(lf),T2.STATUS as SEAL_TBL_STATUS#(lf)FROM#(lf)(SELECT#(lf) DISTINCT#(lf) PRODUCT_STATUS#(lf),CODE#(lf),REGION#(lf),DISPLAY_ORDER_SEQUENCE#(lf),PRODUCT_LINE_ID#(lf),DISPLAY_NAME#(lf),PRODUCT_TYPE#(lf),ID #(lf),NAME #(lf) FROM#(lf) RAW.PRODUCTS#(lf)#(lf)) T3#(lf)INNER JOIN#(lf)(#(lf)    SELECT#(lf)    DISTINCT#(lf)    PRESENTMENT_END_DATE#(lf),PRICE_COMMITMENT_PERIOD#(lf),NAME#(lf),CREATED_AT#(lf),DESCRIPTION#(lf),FREE_PERIOD#(lf),WALKOUT#(lf),PRODUCT_CAT_ID#(lf),PROMOTION_ID#(lf),PRESENTMENT_START_DATE#(lf),DEAL_TYPE#(lf),UPDATED_AT#(lf),ID#(lf),STATUS#(lf)    FROM#(lf)    RAW.DEALS#(lf)#(lf)) T2#(lf)ON#(lf)T3.ID   =   T2.PRODUCT_CAT_ID   #(lf)WHERE#(lf)T2.PRESENTMENT_START_DATE >= \'2015-01-01\'#(lf)AND#(lf)T2.STATUS = \'active\'#(lf)#(lf))T4#(lf)ON#(lf)T1.ID   =   T4.PROMOTION_ID#(lf))T5#(lf)INNER JOIN#(lf)RAW.PRICE_MODIFICATIONS T6#(lf)ON#(lf)T5.SEAL_TBL_ID  =   T6.DEAL_ID", null, [EnableFolding=true]) \n in \n Source',
    'let\n Source = Databricks.Catalogs(#"hostname",#"http_path", null),\n edp_prod_Database = Source{[Name=#"catalog",Kind="Database"]}[Data],\n gold_Schema = edp_prod_Database{[Name=#"schema",Kind="Schema"]}[Data],\n pet_view = gold_Schema{[Name="pet_list",Kind="View"]}[Data],\n #"Filtered Rows" = Table.SelectRows(pet_view, each true),\n #"Removed Columns" = Table.RemoveColumns(#"Filtered Rows",{"created_timestmp"})\nin\n #"Removed Columns"',
    'let\n    Source = Value.NativeQuery(Snowflake.Databases("0DD93C6BD5A6.snowflakecomputing.com","sales_analytics_warehouse_prod",[Role="sales_analytics_member_ad"]){[Name="SL_OPERATIONS"]}[Data], "select SALE_NO AS ""\x1b[4mSaleNo\x1b[0m""#(lf)        ,CODE AS ""Code""#(lf)        ,ENDDATE AS ""end_date""#(lf) from SL_OPERATIONS.SALE.REPORTS#(lf)  where ENDDATE > \'2024-02-03\'", null, [EnableFolding=true]),\n    #"selected Row" = Table.SelectRows(Source)\nin\n    #"selected Row"',
    'let\n    Source = Value.NativeQuery(DatabricksMultiCloud.Catalogs("foo.com", "/sql/1.0/warehouses/423423ew", [Catalog="sales_db", Database=null, EnableAutomaticProxyDiscovery=null]){[Name="sales_db",Kind="Database"]}[Data], "select * from public.slae_history#(lf)where creation_timestamp  >= getDate(-3)", null, [EnableFolding=true]),\n    #"NewTable" = Table.TransformColumn(Source,{{"creation_timestamp", type date}})\nin\n    #"NewTable"',
    'let Source = Snowflake.Databases("example.snowflakecomputing.com","WAREHOUSE_NAME",[Role="CUSTOM_ROLE"]), DB_Source = Source{[Name="DATABASE_NAME",Kind="Database"]}[Data], SCHEMA_Source = DB_Source{[Name="SCHEMA_NAME",Kind="Schema"]}[Data], TABLE_Source = SCHEMA_Source{[Name="TABLE_NAME",Kind="View"]}[Data], #"Split Column by Time" = Table.SplitColumn(Table.TransformColumnTypes(TABLE_Source, {{"TIMESTAMP_COLUMN", type text}}, "en-GB"), "TIMESTAMP_COLUMN", Splitter.SplitTextByDelimiter(" ", QuoteStyle.Csv), {"TIMESTAMP_COLUMN.1", "TIMESTAMP_COLUMN.2"}), #"Added Custom" = Table.AddColumn(#"Split Column by Time", "SOB", each ([ENDTIME] - [STARTTIME]) * 60 * 60 * 24) in #"Added Custom"',
    'let\n    Source = Sql.Database("AUPRDWHDB", "COMMOPSDB", [Query="DROP TABLE IF EXISTS #KKR;#(lf)Select#(lf)*,#(lf)concat((UPPER(REPLACE(SALES_SPECIALIST,\'-\',\'\'))),#(lf)LEFT(CAST(INVOICE_DATE AS DATE),4)+LEFT(RIGHT(CAST(INVOICE_DATE AS DATE),5),2)) AS AGENT_KEY,#(lf)CASE#(lf)    WHEN CLASS = \'Software\' and (NOT(PRODUCT in (\'ADV\', \'Adv\') and left(ACCOUNT_ID,2)=\'10\') #(lf)    or V_ENTERPRISE_INVOICED_REVENUE.TYPE = \'Manual Adjustment\') THEN INVOICE_AMOUNT#(lf)    WHEN V_ENTERPRISE_INVOICED_REVENUE.TYPE IN (\'Recurring\',\'0\') THEN INVOICE_AMOUNT#(lf)    ELSE 0#(lf)END as SOFTWARE_INV#(lf)#(lf)from V_ENTERPRISE_INVOICED_REVENUE", CommandTimeout=#duration(0, 1, 30, 0)]),\n    #"Added Conditional Column" = Table.AddColumn(Source, "Services", each if [CLASS] = "Services" then [INVOICE_AMOUNT] else 0),\n    #"Added Custom" = Table.AddColumn(#"Added Conditional Column", "Advanced New Sites", each if [PRODUCT] = "ADV"\nor [PRODUCT] = "Adv"\nthen [NEW_SITE]\nelse 0)\nin\n    #"Added Custom"',
    "LOAD_DATA(SOURCE)",
]


def get_data_platform_tables_with_dummy_table(
    q: str,
) -> List[datahub.ingestion.source.powerbi.m_query.data_classes.Lineage]:
    table: powerbi_data_classes.Table = powerbi_data_classes.Table(
        columns=[],
        measures=[],
        expression=q,
        name="virtual_order_table",
        full_name="OrderDataSet.virtual_order_table",
    )

    reporter = PowerBiDashboardSourceReport()

    ctx, config, platform_instance_resolver = get_default_instances()

    config.enable_advance_lineage_sql_construct = True

    return parser.get_upstream_tables(
        table,
        reporter,
        ctx=ctx,
        config=config,
        platform_instance_resolver=platform_instance_resolver,
    )


def get_default_instances(
    override_config: dict = {},
) -> Tuple[
    PipelineContext, PowerBiDashboardSourceConfig, AbstractDataPlatformInstanceResolver
]:
    config: PowerBiDashboardSourceConfig = PowerBiDashboardSourceConfig.parse_obj(
        {
            "tenant_id": "fake",
            "client_id": "foo",
            "client_secret": "bar",
            "enable_advance_lineage_sql_construct": False,
            **override_config,
        }
    )

    platform_instance_resolver: AbstractDataPlatformInstanceResolver = (
        create_dataplatform_instance_resolver(config)
    )

    return PipelineContext(run_id="fake"), config, platform_instance_resolver


def combine_upstreams_from_lineage(lineage: List[Lineage]) -> List[DataPlatformTable]:
    data_platforms: List[DataPlatformTable] = []

    for item in lineage:
        data_platforms.extend(item.upstreams)

    return data_platforms


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
        columns=[],
        measures=[],
        expression=q,
        name="virtual_order_table",
        full_name="OrderDataSet.virtual_order_table",
    )

    reporter = PowerBiDashboardSourceReport()

    ctx, config, platform_instance_resolver = get_default_instances()

    data_platform_tables: List[DataPlatformTable] = parser.get_upstream_tables(
        table,
        reporter,
        ctx=ctx,
        config=config,
        platform_instance_resolver=platform_instance_resolver,
    )[0].upstreams

    assert len(data_platform_tables) == 1
    assert (
        data_platform_tables[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,pbi_test.test.testtable,PROD)"
    )


@pytest.mark.integration
def test_postgres_regular_case():
    q: str = M_QUERIES[13]
    table: powerbi_data_classes.Table = powerbi_data_classes.Table(
        columns=[],
        measures=[],
        expression=q,
        name="virtual_order_table",
        full_name="OrderDataSet.virtual_order_table",
    )

    reporter = PowerBiDashboardSourceReport()

    ctx, config, platform_instance_resolver = get_default_instances()

    data_platform_tables: List[DataPlatformTable] = parser.get_upstream_tables(
        table,
        reporter,
        ctx=ctx,
        config=config,
        platform_instance_resolver=platform_instance_resolver,
    )[0].upstreams

    assert len(data_platform_tables) == 1
    assert (
        data_platform_tables[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:postgres,mics.public.order_date,PROD)"
    )


@pytest.mark.integration
def test_databricks_regular_case():
    q: str = M_QUERIES[23]
    table: powerbi_data_classes.Table = powerbi_data_classes.Table(
        columns=[],
        measures=[],
        expression=q,
        name="public_consumer_price_index",
        full_name="hive_metastore.sandbox_revenue.public_consumer_price_index",
    )

    reporter = PowerBiDashboardSourceReport()

    ctx, config, platform_instance_resolver = get_default_instances()

    data_platform_tables: List[DataPlatformTable] = parser.get_upstream_tables(
        table,
        reporter,
        ctx=ctx,
        config=config,
        platform_instance_resolver=platform_instance_resolver,
    )[0].upstreams

    assert len(data_platform_tables) == 1
    assert (
        data_platform_tables[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:databricks,hive_metastore.sandbox_revenue.public_consumer_price_index,PROD)"
    )


@pytest.mark.integration
def test_oracle_regular_case():
    q: str = M_QUERIES[14]
    table: powerbi_data_classes.Table = powerbi_data_classes.Table(
        columns=[],
        measures=[],
        expression=q,
        name="virtual_order_table",
        full_name="OrderDataSet.virtual_order_table",
    )

    reporter = PowerBiDashboardSourceReport()

    ctx, config, platform_instance_resolver = get_default_instances()

    data_platform_tables: List[DataPlatformTable] = parser.get_upstream_tables(
        table,
        reporter,
        ctx=ctx,
        config=config,
        platform_instance_resolver=platform_instance_resolver,
    )[0].upstreams

    assert len(data_platform_tables) == 1
    assert (
        data_platform_tables[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:oracle,salesdb.hr.employees,PROD)"
    )


@pytest.mark.integration
def test_mssql_regular_case():
    q: str = M_QUERIES[15]
    table: powerbi_data_classes.Table = powerbi_data_classes.Table(
        columns=[],
        measures=[],
        expression=q,
        name="virtual_order_table",
        full_name="OrderDataSet.virtual_order_table",
    )

    reporter = PowerBiDashboardSourceReport()

    ctx, config, platform_instance_resolver = get_default_instances()

    data_platform_tables: List[DataPlatformTable] = parser.get_upstream_tables(
        table,
        reporter,
        ctx=ctx,
        config=config,
        platform_instance_resolver=platform_instance_resolver,
    )[0].upstreams

    assert len(data_platform_tables) == 1
    assert (
        data_platform_tables[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:mssql,library.dbo.book_issue,PROD)"
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
        "urn:li:dataset:(urn:li:dataPlatform:mssql,commopsdb.dbo.v_oip_ent_2022,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:mssql,commopsdb.dbo.v_invoice_booking_2022,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:mssql,commopsdb.dbo.v_arr_adds,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:mssql,commopsdb.dbo.v_ps_cd_retention,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:mssql,commopsdb.dbo.v_tpv_leaderboard,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:mssql,commopsdb.dbo.v_enterprise_invoiced_revenue,PROD)",
    ]

    ctx, config, platform_instance_resolver = get_default_instances()

    for index, query in enumerate(mssql_queries):
        table: powerbi_data_classes.Table = powerbi_data_classes.Table(
            columns=[],
            measures=[],
            expression=query,
            name="virtual_order_table",
            full_name="OrderDataSet.virtual_order_table",
        )
        reporter = PowerBiDashboardSourceReport()

        data_platform_tables: List[DataPlatformTable] = parser.get_upstream_tables(
            table,
            reporter,
            ctx=ctx,
            config=config,
            platform_instance_resolver=platform_instance_resolver,
        )[0].upstreams

        assert len(data_platform_tables) == 1
        assert data_platform_tables[0].urn == expected_tables[index]


@pytest.mark.integration
def test_snowflake_native_query():
    snowflake_queries: List[str] = [
        M_QUERIES[1],
        M_QUERIES[2],
        M_QUERIES[6],
        M_QUERIES[10],
    ]

    expected_tables = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,operations_analytics.transformed_prod.v_aps_sme_units_v4,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,operations_analytics.transformed_prod.v_sme_unit_targets,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,operations_analytics.transformed_prod.v_sme_unit_targets,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,operations_analytics.transformed_prod.v_sme_unit_targets,PROD)",
    ]

    ctx, config, platform_instance_resolver = get_default_instances()

    for index, query in enumerate(snowflake_queries):
        table: powerbi_data_classes.Table = powerbi_data_classes.Table(
            columns=[],
            measures=[],
            expression=query,
            name="virtual_order_table",
            full_name="OrderDataSet.virtual_order_table",
        )
        reporter = PowerBiDashboardSourceReport()

        data_platform_tables: List[DataPlatformTable] = parser.get_upstream_tables(
            table,
            reporter,
            ctx=ctx,
            config=config,
            platform_instance_resolver=platform_instance_resolver,
        )[0].upstreams

        assert len(data_platform_tables) == 1
        assert data_platform_tables[0].urn == expected_tables[index]


def test_google_bigquery_1():
    table: powerbi_data_classes.Table = powerbi_data_classes.Table(
        expression=M_QUERIES[17],
        name="first",
        full_name="seraphic-music-344307.school_dataset.first",
    )
    reporter = PowerBiDashboardSourceReport()

    ctx, config, platform_instance_resolver = get_default_instances()

    data_platform_tables: List[DataPlatformTable] = parser.get_upstream_tables(
        table,
        reporter,
        ctx=ctx,
        config=config,
        platform_instance_resolver=platform_instance_resolver,
    )[0].upstreams

    assert len(data_platform_tables) == 1
    assert (
        data_platform_tables[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:bigquery,seraphic-music-344307.school_dataset.first,PROD)"
    )


def test_google_bigquery_2():
    # The main purpose of this test is actually to validate that we're handling parameter
    # references correctly.

    table: powerbi_data_classes.Table = powerbi_data_classes.Table(
        expression=M_QUERIES[18],
        name="gcp_table",
        full_name="my-test-project.gcp_billing.GCP_TABLE",
    )
    reporter = PowerBiDashboardSourceReport()

    ctx, config, platform_instance_resolver = get_default_instances()

    data_platform_tables: List[DataPlatformTable] = parser.get_upstream_tables(
        table,
        reporter,
        parameters={
            "Parameter - Source": "my-test-project",
            "My bq project": "gcp_billing",
        },
        ctx=ctx,
        config=config,
        platform_instance_resolver=platform_instance_resolver,
    )[0].upstreams

    assert len(data_platform_tables) == 1
    assert (
        data_platform_tables[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-test-project.gcp_billing.gcp_table,PROD)"
    )


def test_for_each_expression_1():
    table: powerbi_data_classes.Table = powerbi_data_classes.Table(
        expression=M_QUERIES[19],
        name="D_WH_DATE",
        full_name="my-test-project.universal.D_WH_DATE",
    )

    reporter = PowerBiDashboardSourceReport()

    ctx, config, platform_instance_resolver = get_default_instances()

    data_platform_tables: List[DataPlatformTable] = parser.get_upstream_tables(
        table,
        reporter,
        parameters={
            "Parameter - Source": "my-test-project",
            "My bq project": "gcp_billing",
        },
        ctx=ctx,
        config=config,
        platform_instance_resolver=platform_instance_resolver,
    )[0].upstreams

    assert len(data_platform_tables) == 1
    assert (
        data_platform_tables[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-test-project.universal.d_wh_date,PROD)"
    )


def test_for_each_expression_2():
    table: powerbi_data_classes.Table = powerbi_data_classes.Table(
        expression=M_QUERIES[20],
        name="D_GCP_CUSTOM_LABEL",
        full_name="dwh-prod.gcp_billing.D_GCP_CUSTOM_LABEL",
    )

    reporter = PowerBiDashboardSourceReport()

    ctx, config, platform_instance_resolver = get_default_instances()

    data_platform_tables: List[DataPlatformTable] = parser.get_upstream_tables(
        table,
        reporter,
        parameters={
            "dwh-prod": "originally-not-a-variable-ref-and-not-resolved",
        },
        ctx=ctx,
        config=config,
        platform_instance_resolver=platform_instance_resolver,
    )[0].upstreams

    assert len(data_platform_tables) == 1
    assert (
        data_platform_tables[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:bigquery,dwh-prod.gcp_billing.d_gcp_custom_label,PROD)"
    )


@pytest.mark.integration
def test_native_query_disabled():
    table: powerbi_data_classes.Table = powerbi_data_classes.Table(
        columns=[],
        measures=[],
        expression=M_QUERIES[1],  # 1st index has the native query
        name="virtual_order_table",
        full_name="OrderDataSet.virtual_order_table",
    )

    reporter = PowerBiDashboardSourceReport()

    ctx, config, platform_instance_resolver = get_default_instances()
    config.native_query_parsing = False  # Disable native query parsing
    lineage: List[Lineage] = parser.get_upstream_tables(
        table,
        reporter,
        ctx=ctx,
        config=config,
        platform_instance_resolver=platform_instance_resolver,
    )
    assert len(lineage) == 0


@pytest.mark.integration
def test_multi_source_table():
    table: powerbi_data_classes.Table = powerbi_data_classes.Table(
        columns=[],
        measures=[],
        expression=M_QUERIES[12],  # 1st index has the native query
        name="virtual_order_table",
        full_name="OrderDataSet.virtual_order_table",
    )

    reporter = PowerBiDashboardSourceReport()

    ctx, config, platform_instance_resolver = get_default_instances()

    data_platform_tables: List[DataPlatformTable] = combine_upstreams_from_lineage(
        parser.get_upstream_tables(
            table,
            reporter,
            ctx=ctx,
            config=config,
            platform_instance_resolver=platform_instance_resolver,
        )
    )

    assert len(data_platform_tables) == 2
    assert (
        data_platform_tables[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:postgres,mics.public.order_date,PROD)"
    )
    assert (
        data_platform_tables[1].urn
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,gsl_test_db.public.sales_analyst_view,PROD)"
    )


@pytest.mark.integration
def test_table_combine():
    table: powerbi_data_classes.Table = powerbi_data_classes.Table(
        columns=[],
        measures=[],
        expression=M_QUERIES[16],
        name="virtual_order_table",
        full_name="OrderDataSet.virtual_order_table",
    )

    reporter = PowerBiDashboardSourceReport()

    ctx, config, platform_instance_resolver = get_default_instances()

    data_platform_tables: List[DataPlatformTable] = combine_upstreams_from_lineage(
        parser.get_upstream_tables(
            table,
            reporter,
            ctx=ctx,
            config=config,
            platform_instance_resolver=platform_instance_resolver,
        )
    )

    assert len(data_platform_tables) == 2

    assert (
        data_platform_tables[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,gsl_test_db.public.sales_forecast,PROD)"
    )

    assert (
        data_platform_tables[1].urn
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,gsl_test_db.public.sales_analyst,PROD)"
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
        columns=[],
        measures=[],
        expression=None,  # 1st index has the native query
        name="virtual_order_table",
        full_name="OrderDataSet.virtual_order_table",
    )

    reporter = PowerBiDashboardSourceReport()

    ctx, config, platform_instance_resolver = get_default_instances()

    lineage: List[Lineage] = parser.get_upstream_tables(
        table,
        reporter,
        ctx=ctx,
        config=config,
        platform_instance_resolver=platform_instance_resolver,
    )

    assert len(lineage) == 0


def test_redshift_regular_case():
    table: powerbi_data_classes.Table = powerbi_data_classes.Table(
        expression=M_QUERIES[21],
        name="category",
        full_name="dev.public.category",
    )
    reporter = PowerBiDashboardSourceReport()

    ctx, config, platform_instance_resolver = get_default_instances()

    data_platform_tables: List[DataPlatformTable] = parser.get_upstream_tables(
        table,
        reporter,
        ctx=ctx,
        config=config,
        platform_instance_resolver=platform_instance_resolver,
    )[0].upstreams

    assert len(data_platform_tables) == 1
    assert (
        data_platform_tables[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.category,PROD)"
    )


def test_redshift_native_query():
    table: powerbi_data_classes.Table = powerbi_data_classes.Table(
        expression=M_QUERIES[22],
        name="category",
        full_name="dev.public.category",
    )
    reporter = PowerBiDashboardSourceReport()

    ctx, config, platform_instance_resolver = get_default_instances()

    config.native_query_parsing = True

    data_platform_tables: List[DataPlatformTable] = parser.get_upstream_tables(
        table,
        reporter,
        ctx=ctx,
        config=config,
        platform_instance_resolver=platform_instance_resolver,
    )[0].upstreams

    assert len(data_platform_tables) == 1
    assert (
        data_platform_tables[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.category,PROD)"
    )


def test_sqlglot_parser():
    table: powerbi_data_classes.Table = powerbi_data_classes.Table(
        expression=M_QUERIES[24],
        name="SALES_TARGET",
        full_name="dev.public.sales",
    )
    reporter = PowerBiDashboardSourceReport()

    ctx, config, platform_instance_resolver = get_default_instances(
        override_config={
            "server_to_platform_instance": {
                "bu10758.ap-unknown-2.fakecomputing.com": {
                    "platform_instance": "sales_deployment",
                    "env": "PROD",
                }
            },
            "native_query_parsing": True,
            "enable_advance_lineage_sql_construct": True,
        }
    )

    lineage: List[datahub.ingestion.source.powerbi.m_query.data_classes.Lineage] = (
        parser.get_upstream_tables(
            table,
            reporter,
            ctx=ctx,
            config=config,
            platform_instance_resolver=platform_instance_resolver,
        )
    )

    data_platform_tables: List[DataPlatformTable] = lineage[0].upstreams

    assert len(data_platform_tables) == 2
    assert (
        data_platform_tables[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_deployment.operations_analytics.transformed_prod.v_sme_unit,PROD)"
    )
    assert (
        data_platform_tables[1].urn
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_deployment.operations_analytics.transformed_prod.v_sme_unit_targets,PROD)"
    )

    # TODO: None of these columns have upstreams?
    # That doesn't seem right - we probably need to add fake schemas for the two tables above.
    cols = [
        "client_director",
        "tier",
        'upper("manager")',
        "team_type",
        "date_target",
        "monthid",
        "target_team",
        "seller_email",
        "agent_key",
        "sme_quota",
        "revenue_quota",
        "service_quota",
        "bl_target",
        "software_quota",
    ]
    for i, column in enumerate(cols):
        assert lineage[0].column_lineage[i].downstream.table is None
        assert lineage[0].column_lineage[i].downstream.column == column
        assert lineage[0].column_lineage[i].upstreams == []


def test_databricks_multi_cloud():
    q = M_QUERIES[25]

    lineage: List[datahub.ingestion.source.powerbi.m_query.data_classes.Lineage] = (
        get_data_platform_tables_with_dummy_table(q=q)
    )

    assert len(lineage) == 1

    data_platform_tables = lineage[0].upstreams

    assert len(data_platform_tables) == 1

    assert (
        data_platform_tables[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:databricks,ml_prod.membership_corn.time_exp_data_v3,PROD)"
    )


def test_databricks_catalog_pattern_1():
    q = M_QUERIES[26]

    lineage: List[datahub.ingestion.source.powerbi.m_query.data_classes.Lineage] = (
        get_data_platform_tables_with_dummy_table(q=q)
    )

    assert len(lineage) == 1

    data_platform_tables = lineage[0].upstreams

    assert len(data_platform_tables) == 1

    assert (
        data_platform_tables[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:databricks,data_analysis.summary.committee_data,PROD)"
    )


def test_databricks_catalog_pattern_2():
    table: powerbi_data_classes.Table = powerbi_data_classes.Table(
        expression=M_QUERIES[27],
        name="category",
        full_name="dev.public.category",
    )
    reporter = PowerBiDashboardSourceReport()

    ctx, config, platform_instance_resolver = get_default_instances(
        override_config={
            "server_to_platform_instance": {
                "abc.cloud.databricks.com": {
                    "metastore": "central_metastore",
                    "platform_instance": "abc",
                }
            }
        }
    )
    data_platform_tables: List[DataPlatformTable] = parser.get_upstream_tables(
        table,
        reporter,
        ctx=ctx,
        config=config,
        platform_instance_resolver=platform_instance_resolver,
    )[0].upstreams

    assert len(data_platform_tables) == 1

    assert (
        data_platform_tables[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:databricks,abc.central_metastore.data_analysis.summary.vips_data,PROD)"
    )


def test_sqlglot_parser_2():
    table: powerbi_data_classes.Table = powerbi_data_classes.Table(
        expression=M_QUERIES[28],
        name="SALES_TARGET",
        full_name="dev.public.sales",
    )
    reporter = PowerBiDashboardSourceReport()

    ctx, config, platform_instance_resolver = get_default_instances(
        override_config={
            "server_to_platform_instance": {
                "0DD93C6BD5A6.snowflakecomputing.com": {
                    "platform_instance": "sales_deployment",
                    "env": "PROD",
                }
            },
            "native_query_parsing": True,
            "enable_advance_lineage_sql_construct": True,
        }
    )

    lineage: List[datahub.ingestion.source.powerbi.m_query.data_classes.Lineage] = (
        parser.get_upstream_tables(
            table,
            reporter,
            ctx=ctx,
            config=config,
            platform_instance_resolver=platform_instance_resolver,
        )
    )

    data_platform_tables: List[DataPlatformTable] = lineage[0].upstreams

    assert len(data_platform_tables) == 4
    assert [dpt.urn for dpt in data_platform_tables] == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_deployment.raw.deals,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_deployment.raw.price_modifications,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_deployment.raw.products,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_deployment.raw.promotions,PROD)",
    ]


def test_databricks_regular_case_with_view():
    q: str = M_QUERIES[29]

    table: powerbi_data_classes.Table = powerbi_data_classes.Table(
        columns=[],
        measures=[],
        expression=q,
        name="pet_price_index",
        full_name="datalake.sandbox_pet.pet_price_index",
    )

    reporter = PowerBiDashboardSourceReport()

    ctx, config, platform_instance_resolver = get_default_instances()

    data_platform_tables: List[DataPlatformTable] = parser.get_upstream_tables(
        table,
        reporter,
        ctx=ctx,
        config=config,
        platform_instance_resolver=platform_instance_resolver,
        parameters={
            "hostname": "xyz.databricks.com",
            "http_path": "/sql/1.0/warehouses/abc",
            "catalog": "cat",
            "schema": "public",
        },
    )[0].upstreams

    assert len(data_platform_tables) == 1
    assert (
        data_platform_tables[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:databricks,cat.public.pet_list,PROD)"
    )


@pytest.mark.integration
def test_snowflake_double_double_quotes():
    q = M_QUERIES[30]

    lineage: List[datahub.ingestion.source.powerbi.m_query.data_classes.Lineage] = (
        get_data_platform_tables_with_dummy_table(q=q)
    )

    assert len(lineage) == 1

    data_platform_tables = lineage[0].upstreams

    assert len(data_platform_tables) == 1

    assert (
        data_platform_tables[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,sl_operations.sale.reports,PROD)"
    )


def test_databricks_multicloud():
    q = M_QUERIES[31]

    lineage: List[datahub.ingestion.source.powerbi.m_query.data_classes.Lineage] = (
        get_data_platform_tables_with_dummy_table(q=q)
    )

    assert len(lineage) == 1

    data_platform_tables = lineage[0].upstreams

    assert len(data_platform_tables) == 1

    assert (
        data_platform_tables[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:databricks,sales_db.public.slae_history,PROD)"
    )


def test_snowflake_multi_function_call():
    q = M_QUERIES[32]

    lineage: List[datahub.ingestion.source.powerbi.m_query.data_classes.Lineage] = (
        get_data_platform_tables_with_dummy_table(q=q)
    )

    assert len(lineage) == 1

    data_platform_tables = lineage[0].upstreams

    assert len(data_platform_tables) == 1

    assert (
        data_platform_tables[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,database_name.schema_name.table_name,PROD)"
    )


def test_mssql_drop_with_select():
    q = M_QUERIES[33]

    lineage: List[datahub.ingestion.source.powerbi.m_query.data_classes.Lineage] = (
        get_data_platform_tables_with_dummy_table(q=q)
    )

    assert len(lineage) == 1

    data_platform_tables = lineage[0].upstreams

    assert len(data_platform_tables) == 1

    assert (
        data_platform_tables[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:mssql,commopsdb.dbo.v_enterprise_invoiced_revenue,PROD)"
    )


def test_unsupported_data_platform():
    q = M_QUERIES[34]
    table: powerbi_data_classes.Table = powerbi_data_classes.Table(
        columns=[],
        measures=[],
        expression=q,
        name="virtual_order_table",
        full_name="OrderDataSet.virtual_order_table",
    )

    reporter = PowerBiDashboardSourceReport()

    ctx, config, platform_instance_resolver = get_default_instances()

    config.enable_advance_lineage_sql_construct = True

    assert (
        parser.get_upstream_tables(
            table,
            reporter,
            ctx=ctx,
            config=config,
            platform_instance_resolver=platform_instance_resolver,
        )
        == []
    )

    info_entries: dict = reporter._structured_logs._entries.get(
        StructuredLogLevel.INFO, {}
    )  # type :ignore

    is_entry_present: bool = False
    for entry in info_entries.values():
        if entry.title == "Non-Data Platform Expression":
            is_entry_present = True
            break

    assert is_entry_present, (
        'Info message "Non-Data Platform Expression" should be present in reporter'
    )


def test_empty_string_in_m_query():
    # TRIM(TRIM(TRIM(AGENT_NAME, '\"\"'), '+'), '\\'') is in Query
    q = "let\n  Source = Value.NativeQuery(Snowflake.Databases(\"bu10758.ap-unknown-2.fakecomputing.com\",\"operations_analytics_warehouse_prod\",[Role=\"OPERATIONS_ANALYTICS_MEMBER\"]){[Name=\"OPERATIONS_ANALYTICS\"]}[Data], \"select #(lf)UPPER(REPLACE(AGENT_NAME,'-','')) AS CLIENT_DIRECTOR,#(lf)TRIM(TRIM(TRIM(AGENT_NAME, '\"\"'), '+'), '\\'') AS TRIM_AGENT_NAME,#(lf)TIER,#(lf)UPPER(MANAGER),#(lf)TEAM_TYPE,#(lf)DATE_TARGET,#(lf)MONTHID,#(lf)TARGET_TEAM,#(lf)SELLER_EMAIL,#(lf)concat((UPPER(REPLACE(AGENT_NAME,'-',''))), MONTHID) as AGENT_KEY,#(lf)UNIT_TARGET AS SME_Quota,#(lf)AMV_TARGET AS Revenue_Quota,#(lf)SERVICE_QUOTA,#(lf)BL_TARGET,#(lf)SOFTWARE_QUOTA as Software_Quota#(lf)#(lf)from OPERATIONS_ANALYTICS.TRANSFORMED_PROD.V_SME_UNIT_TARGETS inner join OPERATIONS_ANALYTICS.TRANSFORMED_PROD.V_SME_UNIT #(lf)#(lf)where YEAR_TARGET >= 2022#(lf)and TEAM_TYPE = 'Accounting'#(lf)and TARGET_TEAM = 'Enterprise'#(lf)AND TIER = 'Client Director'\", null, [EnableFolding=true])\nin\n    Source"

    lineage: List[datahub.ingestion.source.powerbi.m_query.data_classes.Lineage] = (
        get_data_platform_tables_with_dummy_table(q=q)
    )

    assert len(lineage) == 1

    data_platform_tables = lineage[0].upstreams

    assert len(data_platform_tables) == 2

    assert (
        data_platform_tables[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,operations_analytics.transformed_prod.v_sme_unit,PROD)"
    )
    assert (
        data_platform_tables[1].urn
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,operations_analytics.transformed_prod.v_sme_unit_targets,PROD)"
    )


def test_double_quotes_in_alias():
    # SELECT CAST(sales_date AS DATE) AS \"\"Date\"\" in query
    q = 'let \n Source = Sql.Database("abc.com", "DB", [Query="SELECT CAST(sales_date AS DATE) AS ""Date"",#(lf) SUM(cshintrpret) / 60.0      AS ""Total Order All Items"",#(lf)#(tab)#(tab)#(tab)  SUM(cshintrpret) / 60.0 - LAG(SUM(cshintrpret) / 60.0, 1) OVER (ORDER BY CAST(sales_date AS DATE)) AS ""Total minute difference"",#(lf)#(tab)#(tab)#(tab)  SUM(sale_price)  / 60.0 - LAG(SUM(sale_price)  / 60.0, 1) OVER (ORDER BY CAST(sales_date AS DATE)) AS ""Normal minute difference""#(lf)        FROM   [DB].[dbo].[sales_t]#(lf)        WHERE  sales_date >= GETDATE() - 365#(lf)        GROUP  BY CAST(sales_date AS DATE),#(lf)#(tab)#(tab)CAST(sales_date AS TIME);"]) \n in \n Source'

    lineage: List[datahub.ingestion.source.powerbi.m_query.data_classes.Lineage] = (
        get_data_platform_tables_with_dummy_table(q=q)
    )

    assert len(lineage) == 1

    data_platform_tables = lineage[0].upstreams

    assert len(data_platform_tables) == 1

    assert (
        data_platform_tables[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:mssql,db.dbo.sales_t,PROD)"
    )


@patch("datahub.ingestion.source.powerbi.m_query.parser.get_lark_parser")
def test_m_query_timeout(mock_get_lark_parser):
    q = 'let\n    Source = Value.NativeQuery(Snowflake.Databases("0DD93C6BD5A6.snowflakecomputing.com","sales_analytics_warehouse_prod",[Role="sales_analytics_member_ad"]){[Name="SL_OPERATIONS"]}[Data], "select SALE_NO AS ""\x1b[4mSaleNo\x1b[0m""#(lf)        ,CODE AS ""Code""#(lf)        ,ENDDATE AS ""end_date""#(lf) from SL_OPERATIONS.SALE.REPORTS#(lf)  where ENDDATE > \'2024-02-03\'", null, [EnableFolding=true]),\n    #"selected Row" = Table.SelectRows(Source)\nin\n    #"selected Row"'

    table: powerbi_data_classes.Table = powerbi_data_classes.Table(
        columns=[],
        measures=[],
        expression=q,
        name="virtual_order_table",
        full_name="OrderDataSet.virtual_order_table",
    )

    reporter = PowerBiDashboardSourceReport()

    ctx, config, platform_instance_resolver = get_default_instances()

    config.enable_advance_lineage_sql_construct = True

    config.m_query_parse_timeout = 1

    mock_lark_instance = MagicMock()

    mock_get_lark_parser.return_value = mock_lark_instance
    # sleep for 5 seconds to trigger timeout
    mock_lark_instance.parse.side_effect = lambda expression: time.sleep(5)

    parser.get_upstream_tables(
        table,
        reporter,
        ctx=ctx,
        config=config,
        platform_instance_resolver=platform_instance_resolver,
    )

    warn_entries: dict = reporter._structured_logs._entries.get(
        StructuredLogLevel.WARN, {}
    )  # type :ignore

    is_entry_present: bool = False
    for entry in warn_entries.values():
        if entry.title == "M-Query Parsing Timeout":
            is_entry_present = True
            break

    assert is_entry_present, (
        'Warning message "M-Query Parsing Timeout" should be present in reporter'
    )


def test_comments_in_m_query():
    q: str = 'let\n    Source = Snowflake.Databases("xaa48144.snowflakecomputing.com", "COMPUTE_WH", [Role="ACCOUNTADMIN"]),\n    SNOWFLAKE_SAMPLE_DATA_Database = Source{[Name="SNOWFLAKE_SAMPLE_DATA", Kind="Database"]}[Data],\n    TPCDS_SF100TCL_Schema = SNOWFLAKE_SAMPLE_DATA_Database{[Name="TPCDS_SF100TCL", Kind="Schema"]}[Data],\n    ITEM_Table = TPCDS_SF100TCL_Schema{[Name="ITEM", Kind="Table"]}[Data],\n    \n    // Group by I_BRAND and calculate the count\n    BrandCountsTable = Table.Group(ITEM_Table, {"I_BRAND"}, {{"BrandCount", each Table.RowCount(_), Int64.Type}})\nin\n    BrandCountsTable'

    table: powerbi_data_classes.Table = powerbi_data_classes.Table(
        columns=[],
        measures=[],
        expression=q,
        name="pet_price_index",
        full_name="datalake.sandbox_pet.pet_price_index",
    )

    reporter = PowerBiDashboardSourceReport()

    ctx, config, platform_instance_resolver = get_default_instances()

    data_platform_tables: List[DataPlatformTable] = parser.get_upstream_tables(
        table,
        reporter,
        ctx=ctx,
        config=config,
        platform_instance_resolver=platform_instance_resolver,
        parameters={
            "hostname": "xyz.databricks.com",
            "http_path": "/sql/1.0/warehouses/abc",
            "catalog": "cat",
            "schema": "public",
        },
    )[0].upstreams

    assert len(data_platform_tables) == 1
    assert (
        data_platform_tables[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,snowflake_sample_data.tpcds_sf100tcl.item,PROD)"
    )
