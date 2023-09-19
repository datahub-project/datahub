import pathlib

import pytest

from datahub.testing.check_sql_parser_result import assert_sql_result

RESOURCE_DIR = pathlib.Path(__file__).parent / "goldens"


def test_select_max():
    # The COL2 should get normalized to col2.
    assert_sql_result(
        """
SELECT max(col1, COL2) as max_col
FROM mytable
""",
        dialect="mysql",
        expected_file=RESOURCE_DIR / "test_select_max.json",
    )


def test_select_max_with_schema():
    # Note that `this_will_not_resolve` will be dropped from the result because it's not in the schema.
    assert_sql_result(
        """
SELECT max(`col1`, COL2, `this_will_not_resolve`) as max_col
FROM mytable
""",
        dialect="mysql",
        schemas={
            "urn:li:dataset:(urn:li:dataPlatform:mysql,mytable,PROD)": {
                "col1": "NUMBER",
                "col2": "NUMBER",
            },
        },
        # Shared with the test above.
        expected_file=RESOURCE_DIR / "test_select_max.json",
    )


def test_select_count():
    assert_sql_result(
        """
SELECT
  COUNT(etl_data_dt_id)
FROM something_prd.fact_complaint_snapshot
WHERE
  etl_data_dt_id = 20230317
""",
        dialect="mysql",
        expected_file=RESOURCE_DIR / "test_select_count.json",
    )


def test_select_with_ctes():
    assert_sql_result(
        """
WITH cte1 AS (
    SELECT col1, col2
    FROM table1
    WHERE col1 = 'value1'
), cte2 AS (
    SELECT col3, col4
    FROM table2
    WHERE col2 = 'value2'
)
SELECT cte1.col1, cte2.col3
FROM cte1
JOIN cte2 ON cte1.col2 = cte2.col4
""",
        dialect="oracle",
        expected_file=RESOURCE_DIR / "test_select_with_ctes.json",
    )


def test_create_view_as_select():
    assert_sql_result(
        """
CREATE VIEW vsal
AS
  SELECT a.deptno                  "Department",
         a.num_emp / b.total_count "Employees",
         a.sal_sum / b.total_sal   "Salary"
  FROM   (SELECT deptno,
                 Count()  num_emp,
                 SUM(sal) sal_sum
          FROM   scott.emp
          WHERE  city = 'NYC'
          GROUP  BY deptno) a,
         (SELECT Count()  total_count,
                 SUM(sal) total_sal
          FROM   scott.emp
          WHERE  city = 'NYC') b
;
""",
        dialect="oracle",
        expected_file=RESOURCE_DIR / "test_create_view_as_select.json",
    )


def test_insert_as_select():
    # Note: this also tests lineage with case statements.

    assert_sql_result(
        """
insert into query72
select i_item_desc
     , w_warehouse_name
     , d1.d_week_seq
     , sum(case when promotion.p_promo_sk is null then 1 else 0 end)     no_promo
     , sum(case when promotion.p_promo_sk is not null then 1 else 0 end) promo
     , count(*)                                                          total_cnt
from catalog_sales
         join inventory on (cs_item_sk = inv_item_sk)
         join warehouse on (w_warehouse_sk = inv_warehouse_sk)
         join item on (i_item_sk = cs_item_sk)
         join customer_demographics on (cs_bill_cdemo_sk = cd_demo_sk)
         join household_demographics on (cs_bill_hdemo_sk = hd_demo_sk)
         join date_dim d1 on (cs_sold_date_sk = d1.d_date_sk)
         join date_dim d2 on (inv_date_sk = d2.d_date_sk)
         join date_dim d3 on (cs_ship_date_sk = d3.d_date_sk)
         left outer join promotion on (cs_promo_sk = p_promo_sk)
         left outer join catalog_returns on (cr_item_sk = cs_item_sk and cr_order_number = cs_order_number)
where d1.d_week_seq = d2.d_week_seq
  and inv_quantity_on_hand < cs_quantity
  and hd_buy_potential = '>10000'
  and cd_marital_status = 'D'
group by i_item_desc, w_warehouse_name, d1.d_week_seq
order by total_cnt desc, i_item_desc, w_warehouse_name, d_week_seq
limit 100;
""",
        dialect="hive",
        expected_file=RESOURCE_DIR / "test_insert_as_select.json",
    )


def test_select_with_full_col_name():
    # In this case, `widget` is a struct column.
    # This also tests the `default_db` functionality.
    assert_sql_result(
        """
SELECT distinct post_id , widget.asset.id
FROM data_reporting.abcde_transformed
WHERE post_id LIKE '%268662%'
""",
        dialect="bigquery",
        default_db="my-bq-ProjectName",
        schemas={
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-bq-ProjectName.data_reporting.abcde_transformed,PROD)": {
                "post_id": "NUMBER",
                "widget": "struct",
            },
        },
        expected_file=RESOURCE_DIR / "test_select_with_full_col_name.json",
    )


def test_select_from_struct_subfields():
    # In this case, `widget` is a column name.
    assert_sql_result(
        """
SELECT distinct post_id ,
    widget.asset.id,
    min(widget.metric.metricA, widget.metric.metric_b) as min_metric
FROM data_reporting.abcde_transformed
WHERE post_id LIKE '%12345%'
""",
        dialect="bigquery",
        default_db="my-bq-proj",
        schemas={
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-bq-proj.data_reporting.abcde_transformed,PROD)": {
                "post_id": "NUMBER",
                "widget": "struct",
                "widget.asset.id": "int",
                "widget.metric.metricA": "int",
                "widget.metric.metric_b": "int",
            },
        },
        expected_file=RESOURCE_DIR / "test_select_from_struct_subfields.json",
    )


def test_select_from_union():
    assert_sql_result(
        """
SELECT
    'orders_10' as label,
    SUM(totalprice) as total_agg
FROM snowflake_sample_data.tpch_sf10.orders
UNION ALL
SELECT
    'orders_100' as label,
    SUM(totalprice) as total_agg,
FROM snowflake_sample_data.tpch_sf100.orders
""",
        dialect="snowflake",
        schemas={
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,snowflake_sample_data.tpch_sf10.orders,PROD)": {
                "orderkey": "NUMBER",
                "totalprice": "FLOAT",
            },
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,snowflake_sample_data.tpch_sf100.orders,PROD)": {
                "orderkey": "NUMBER",
                "totalprice": "FLOAT",
            },
        },
        expected_file=RESOURCE_DIR / "test_select_from_union.json",
    )


def test_merge_from_union():
    # TODO: We don't support merge statements yet, but the union should still get handled.

    assert_sql_result(
        """
    merge into `demo-pipelines-stg`.`referrer`.`base_union` as DBT_INTERNAL_DEST
        using (
SELECT * FROM `demo-pipelines-stg`.`referrer`.`prep_from_ios` WHERE partition_time = "2018-03-03"
UNION ALL
SELECT * FROM `demo-pipelines-stg`.`referrer`.`prep_from_web` WHERE partition_time = "2018-03-03"
        ) as DBT_INTERNAL_SOURCE
        on FALSE

    when not matched by source
         and timestamp_trunc(DBT_INTERNAL_DEST.partition_time, day) in (
              timestamp('2018-03-03')
          )
        then delete

    when not matched then insert
        (`platform`, `pageview_id`, `query`, `referrer`, `partition_time`)
    values
        (`platform`, `pageview_id`, `query`, `referrer`, `partition_time`)
""",
        dialect="bigquery",
        expected_file=RESOURCE_DIR / "test_merge_from_union.json",
    )


def test_expand_select_star_basic():
    assert_sql_result(
        """
SELECT
    SUM(totalprice) as total_agg,
    *
FROM snowflake_sample_data.tpch_sf1.orders
WHERE orderdate = '1992-01-01'
""",
        dialect="snowflake",
        schemas={
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,snowflake_sample_data.tpch_sf1.orders,PROD)": {
                "orderkey": "NUMBER",
                "custkey": "NUMBER",
                "orderstatus": "TEXT",
                "totalprice": "FLOAT",
                "orderdate": "DATE",
                "orderpriority": "TEXT",
                "clerk": "TEXT",
                "shippriority": "NUMBER",
                "comment": "TEXT",
            },
        },
        expected_file=RESOURCE_DIR / "test_expand_select_star_basic.json",
    )


def test_snowflake_column_normalization():
    # Technically speaking this is incorrect since the column names are different and both quoted.

    assert_sql_result(
        """
SELECT
    SUM(o."totalprice") as total_agg,
    AVG("TotalPrice") as total_avg,
    MIN("TOTALPRICE") as total_min,
    MAX(TotalPrice)   as total_max
FROM snowflake_sample_data.tpch_sf1.orders o
""",
        dialect="snowflake",
        schemas={
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,snowflake_sample_data.tpch_sf1.orders,PROD)": {
                "orderkey": "NUMBER",
                "TotalPrice": "FLOAT",
            },
        },
        expected_file=RESOURCE_DIR / "test_snowflake_column_normalization.json",
    )


def test_snowflake_ctas_column_normalization():
    # For CTAS statements, we also should try to match the output table's
    # column name casing. This is technically incorrect since we have the
    # exact column names from the query, but necessary to match our column
    # name normalization behavior in the Snowflake source.

    assert_sql_result(
        """
CREATE TABLE snowflake_sample_data.tpch_sf1.orders_normalized
AS
SELECT
    SUM(o."totalprice") as Total_Agg,
    AVG("TotalPrice") as TOTAL_AVG,
    MIN("TOTALPRICE") as TOTAL_MIN,
    MAX(TotalPrice)   as Total_Max
FROM snowflake_sample_data.tpch_sf1.orders o
""",
        dialect="snowflake",
        schemas={
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,snowflake_sample_data.tpch_sf1.orders,PROD)": {
                "orderkey": "NUMBER",
                "TotalPrice": "FLOAT",
            },
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,snowflake_sample_data.tpch_sf1.orders_normalized,PROD)": {
                "Total_Agg": "FLOAT",
                "total_avg": "FLOAT",
                "TOTAL_MIN": "FLOAT",
                # Purposely excluding total_max to test out the fallback behavior.
            },
        },
        expected_file=RESOURCE_DIR / "test_snowflake_ctas_column_normalization.json",
    )


def test_snowflake_case_statement():
    assert_sql_result(
        """
SELECT
    CASE
        WHEN o."totalprice" > 1000 THEN 'high'
        WHEN o."totalprice" > 100 THEN 'medium'
        ELSE 'low'
    END as total_price_category,
    -- Also add a case where the column is in the THEN clause.
    CASE
        WHEN o."is_payment_successful" THEN o."totalprice"
        ELSE 0
    END as total_price_success
FROM snowflake_sample_data.tpch_sf1.orders o
""",
        dialect="snowflake",
        schemas={
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,snowflake_sample_data.tpch_sf1.orders,PROD)": {
                "orderkey": "NUMBER",
                "totalprice": "FLOAT",
                "is_payment_successful": "BOOLEAN",
            },
        },
        expected_file=RESOURCE_DIR / "test_snowflake_case_statement.json",
    )


@pytest.mark.skip(reason="We don't handle the unnest lineage correctly")
def test_bigquery_unnest_columns():
    assert_sql_result(
        """
SELECT
  DATE(reporting_day) AS day,
  CASE
    WHEN p.product_code IN ('A', 'B', 'C')
    THEN pr.other_field
    ELSE 'Other'
  END AS product,
  pr.other_field AS other_field,
  SUM(p.product_code_dau) AS daily_active_users
FROM `bq-proj`.dataset.table1
LEFT JOIN UNNEST(by_product) AS p
LEFT JOIN (
  SELECT DISTINCT
    product_code,
    other_field
  FROM `bq-proj`.dataset.table2
) AS pr
--  USING (product_code)
ON p.product_code = pr.product_code
""",
        dialect="bigquery",
        schemas={
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,bq-proj.dataset.table1,PROD)": {
                "reporting_day": "DATE",
                "by_product": "ARRAY<STRUCT<product_code STRING, product_code_dau INT64>>",
            },
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,bq-proj.dataset.table2,PROD)": {
                "product_code": "STRING",
                "other_field": "STRING",
            },
        },
        expected_file=RESOURCE_DIR / "test_bigquery_unnest_columns.json",
    )


def test_bigquery_create_view_with_cte():
    assert_sql_result(
        """
CREATE VIEW `my-proj-2`.dataset.my_view AS
WITH cte1 AS (
    SELECT *
    FROM dataset.table1
    WHERE col1 = 'value1'
), cte2 AS (
    SELECT col3, col4 as join_key
    FROM dataset.table2
    WHERE col3 = 'value2'
)
SELECT col5, cte1.*, col3
FROM dataset.table3
JOIN cte1 ON table3.col5 = cte1.col2
JOIN cte2 USING (join_key)
""",
        dialect="bigquery",
        default_db="my-proj-1",
        schemas={
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-proj-1.dataset.table1,PROD)": {
                "col1": "STRING",
                "col2": "STRING",
            },
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-proj-1.dataset.table2,PROD)": {
                "col3": "STRING",
                "col4": "STRING",
            },
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-proj-1.dataset.table3,PROD)": {
                "col5": "STRING",
                "join_key": "STRING",
            },
        },
        expected_file=RESOURCE_DIR / "test_bigquery_create_view_with_cte.json",
    )


def test_bigquery_nested_subqueries():
    assert_sql_result(
        """
SELECT *
FROM (
    SELECT *
    FROM (
        SELECT *
        FROM `bq-proj`.dataset.table1
    )
)
""",
        dialect="bigquery",
        schemas={
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,bq-proj.dataset.table1,PROD)": {
                "col1": "STRING",
                "col2": "STRING",
            },
        },
        expected_file=RESOURCE_DIR / "test_bigquery_nested_subqueries.json",
    )


def test_bigquery_sharded_table_normalization():
    assert_sql_result(
        """
SELECT *
FROM `bq-proj.dataset.table_20230101`
""",
        dialect="bigquery",
        schemas={
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,bq-proj.dataset.table_yyyymmdd,PROD)": {
                "col1": "STRING",
                "col2": "STRING",
            },
        },
        expected_file=RESOURCE_DIR / "test_bigquery_sharded_table_normalization.json",
    )


def test_bigquery_from_sharded_table_wildcard():
    assert_sql_result(
        """
SELECT *
FROM `bq-proj.dataset.table_2023*`
""",
        dialect="bigquery",
        schemas={
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,bq-proj.dataset.table_yyyymmdd,PROD)": {
                "col1": "STRING",
                "col2": "STRING",
            },
        },
        expected_file=RESOURCE_DIR / "test_bigquery_from_sharded_table_wildcard.json",
    )


def test_bigquery_star_with_replace():
    assert_sql_result(
        """
CREATE VIEW `my-project.my-dataset.test_table` AS
SELECT
  * REPLACE(
    LOWER(something) AS something)
FROM
  `my-project2.my-dataset2.test_physical_table`;
""",
        dialect="bigquery",
        schemas={
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project2.my-dataset2.test_physical_table,PROD)": {
                "col1": "STRING",
                "col2": "STRING",
                "something": "STRING",
            },
        },
        expected_file=RESOURCE_DIR / "test_bigquery_star_with_replace.json",
    )


def test_bigquery_view_from_union():
    assert_sql_result(
        """
CREATE VIEW my_view as
select * from my_project_2.my_dataset_2.sometable
union
select * from my_project_2.my_dataset_2.sometable2 as a
""",
        dialect="bigquery",
        schemas={
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,my_project_2.my_dataset_2.sometable,PROD)": {
                "col1": "STRING",
                "col2": "STRING",
            },
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,my_project_2.my_dataset_2.sometable2,PROD)": {
                "col1": "STRING",
                "col2": "STRING",
            },
        },
        expected_file=RESOURCE_DIR / "test_bigquery_view_from_union.json",
    )


def test_snowflake_default_normalization():
    assert_sql_result(
        """
create table active_customer_ltv as (

with active_customers as (
  select * from customer_last_purchase_date
  where
    last_purchase_date >= current_date - interval '90 days'
)

, purchases as (
  select * from ecommerce.purchases
)

select
  active_customers.user_fk
  , active_customers.email
  , active_customers.last_purchase_date
  , sum(purchases.purchase_amount) as lifetime_purchase_amount
  , count(distinct(purchases.pk)) as lifetime_purchase_count
  , sum(purchases.purchase_amount) / count(distinct(purchases.pk)) as average_purchase_amount
from
  active_customers
join
  purchases
  on active_customers.user_fk = purchases.user_fk
group by 1,2,3

)
""",
        dialect="snowflake",
        default_db="long_tail_companions",
        default_schema="analytics",
        schemas={
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.ecommerce.purchases,PROD)": {
                "pk": "NUMBER(38,0)",
                "USER_FK": "NUMBER(38,0)",
                "status": "VARCHAR(16777216)",
                "purchase_amount": "NUMBER(10,2)",
                "tax_AMOUNT": "NUMBER(10,2)",
                "TOTAL_AMOUNT": "NUMBER(10,2)",
                "CREATED_AT": "TIMESTAMP_NTZ",
                "UPDATED_AT": "TIMESTAMP_NTZ",
            },
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.customer_last_purchase_date,PROD)": {
                "USER_FK": "NUMBER(38,0)",
                "EMAIL": "VARCHAR(16777216)",
                "LAST_PURCHASE_DATE": "DATE",
            },
        },
        expected_file=RESOURCE_DIR / "test_snowflake_default_normalization.json",
    )


# TODO: Add a test for setting platform_instance or env
