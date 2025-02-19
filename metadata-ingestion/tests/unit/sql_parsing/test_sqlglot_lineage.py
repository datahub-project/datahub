import pathlib

import pytest

from datahub.testing.check_sql_parser_result import assert_sql_result

RESOURCE_DIR = pathlib.Path(__file__).parent / "goldens"


def test_invalid_sql():
    assert_sql_result(
        """
SELECT as '
FROM snowflake_sample_data.tpch_sf1.orders o
""",
        dialect="snowflake",
        expected_file=RESOURCE_DIR / "test_invalid_sql.json",
        allow_table_error=True,
    )


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
        expected_file=RESOURCE_DIR / "test_select_max_with_schema.json",
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


def test_select_with_complex_ctes():
    # This one has group bys in the CTEs, which means they can't be collapsed into the main query.
    assert_sql_result(
        """
WITH cte1 AS (
    SELECT col1, col2
    FROM table1
    WHERE col1 = 'value1'
    GROUP BY 1, 2
), cte2 AS (
    SELECT col3, col4
    FROM table2
    WHERE col2 = 'value2'
    GROUP BY col3, col4
)
SELECT cte1.col1, cte2.col3
FROM cte1
JOIN cte2 ON cte1.col2 = cte2.col4
""",
        dialect="oracle",
        expected_file=RESOURCE_DIR / "test_select_with_complex_ctes.json",
    )


def test_multiple_select_subqueries():
    assert_sql_result(
        """
SELECT SUM((SELECT max(a) a from x) + (SELECT min(b) b from x) + c) AS y FROM x
""",
        dialect="mysql",
        expected_file=RESOURCE_DIR / "test_multiple_select_subqueries.json",
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


def test_insert_with_column_list():
    assert_sql_result(
        """\
insert into downstream (a, c) select a, c from upstream2
""",
        dialect="redshift",
        expected_file=RESOURCE_DIR / "test_insert_with_column_list.json",
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


def test_select_ambiguous_column_no_schema():
    assert_sql_result(
        """
        select A, B, C from t1 inner join t2 on t1.id = t2.id
        """,
        dialect="hive",
        expected_file=RESOURCE_DIR / "test_select_ambiguous_column_no_schema.json",
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


def test_create_table_ddl():
    assert_sql_result(
        """
CREATE TABLE IF NOT EXISTS costs (
    id INTEGER PRIMARY KEY,
    month TEXT NOT NULL,
    total_cost REAL NOT NULL,
    area REAL NOT NULL
)
""",
        dialect="sqlite",
        expected_file=RESOURCE_DIR / "test_create_table_ddl.json",
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


def test_bigquery_partitioned_table_insert():
    assert_sql_result(
        """
SELECT *
FROM `bq-proj.dataset.my-table$__UNPARTITIONED__`
""",
        dialect="bigquery",
        schemas={
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,bq-proj.dataset.my-table,PROD)": {
                "col1": "STRING",
                "col2": "STRING",
            },
        },
        expected_file=RESOURCE_DIR / "test_bigquery_partitioned_table_insert.json",
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
union all
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


def test_snowflake_column_cast():
    assert_sql_result(
        """
SELECT
    o.o_orderkey::NUMBER(20,0) as orderkey,
    CAST(o.o_totalprice AS INT) as total_cast_int,
    CAST(o.o_totalprice AS NUMBER(16,4)) as total_cast_float
FROM snowflake_sample_data.tpch_sf1.orders o
LIMIT 10
""",
        dialect="snowflake",
        schemas={
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,snowflake_sample_data.tpch_sf1.orders,PROD)": {
                "orderkey": "NUMBER(38,0)",
                "totalprice": "NUMBER(12,2)",
            },
        },
        expected_file=RESOURCE_DIR / "test_snowflake_column_cast.json",
    )


def test_snowflake_unused_cte():
    # For this, we expect table level lineage to include table1, but CLL should not.
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
SELECT cte1.col1, table3.col6
FROM cte1
JOIN table3 ON table3.col5 = cte1.col2
""",
        dialect="snowflake",
        expected_file=RESOURCE_DIR / "test_snowflake_unused_cte.json",
    )


def test_snowflake_cte_name_collision():
    # In this example, output col1 should come from table3 and not table1, since the cte is unused.
    # We'll still generate table-level lineage that includes table1.
    assert_sql_result(
        """
WITH cte_alias AS (
    SELECT col1, col2
    FROM table1
)
SELECT table2.col2, cte_alias.col1
FROM table2
JOIN table3 AS cte_alias ON cte_alias.col2 = cte_alias.col2
""",
        dialect="snowflake",
        default_db="my_db",
        default_schema="my_schema",
        schemas={
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.table1,PROD)": {
                "col1": "NUMBER(38,0)",
                "col2": "VARCHAR(16777216)",
            },
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.table2,PROD)": {
                "col2": "VARCHAR(16777216)",
            },
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.table3,PROD)": {
                "col1": "VARCHAR(16777216)",
                "col2": "VARCHAR(16777216)",
            },
        },
        expected_file=RESOURCE_DIR / "test_snowflake_cte_name_collision.json",
    )


def test_snowflake_full_table_name_col_reference():
    assert_sql_result(
        """
SELECT
    my_db.my_schema.my_table.id,
    case when my_db.my_schema.my_table.id > 100 then 1 else 0 end as id_gt_100,
    my_db.my_schema.my_table.struct_field.field1 as struct_field1,
FROM my_db.my_schema.my_table
""",
        dialect="snowflake",
        default_db="my_db",
        default_schema="my_schema",
        schemas={
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.my_db.my_schema.my_table,PROD)": {
                "id": "NUMBER(38,0)",
                "struct_field": "struct",
            },
        },
        expected_file=RESOURCE_DIR
        / "test_snowflake_full_table_name_col_reference.json",
    )


# TODO: Add a test for setting platform_instance or env


def test_teradata_default_normalization():
    assert_sql_result(
        """
create table demo_user.test_lineage2 as
 (
    select
        ppd.PatientId,
        ppf.bmi
    from
        demo_user.pima_patient_features ppf
    join demo_user.pima_patient_diagnoses ppd on
        ppd.PatientId = ppf.PatientId
 ) with data;
""",
        dialect="teradata",
        default_schema="dbc",
        platform_instance="myteradata",
        schemas={
            "urn:li:dataset:(urn:li:dataPlatform:teradata,myteradata.demo_user.pima_patient_diagnoses,PROD)": {
                "HasDiabetes": "INTEGER()",
                "PatientId": "INTEGER()",
            },
            "urn:li:dataset:(urn:li:dataPlatform:teradata,myteradata.demo_user.pima_patient_features,PROD)": {
                "Age": "INTEGER()",
                "BMI": "FLOAT()",
                "BloodP": "INTEGER()",
                "DiPedFunc": "FLOAT()",
                "NumTimesPrg": "INTEGER()",
                "PatientId": "INTEGER()",
                "PlGlcConc": "INTEGER()",
                "SkinThick": "INTEGER()",
                "TwoHourSerIns": "INTEGER()",
            },
            "urn:li:dataset:(urn:li:dataPlatform:teradata,myteradata.demo_user.test_lineage2,PROD)": {
                "BMI": "FLOAT()",
                "PatientId": "INTEGER()",
            },
        },
        expected_file=RESOURCE_DIR / "test_teradata_default_normalization.json",
    )


def test_teradata_strange_operators():
    # This is a test for the following operators:
    # - `SEL` (select)
    # - `EQ` (equals)
    # - `MINUS` (except)
    assert_sql_result(
        """
sel col1, col2 from dbc.table1
where col1 eq 'value1'
minus
select col1, col2 from dbc.table2
""",
        dialect="teradata",
        default_schema="dbc",
        expected_file=RESOURCE_DIR / "test_teradata_strange_operators.json",
    )


@pytest.mark.skip("sqlglot doesn't support this cast syntax yet")
def test_teradata_cast_syntax():
    assert_sql_result(
        """
SELECT my_table.date_col MONTH(4) AS month_col
FROM my_table
""",
        dialect="teradata",
        default_schema="dbc",
        expected_file=RESOURCE_DIR / "test_teradata_cast_syntax.json",
    )


def test_snowflake_update_hardcoded():
    assert_sql_result(
        """
UPDATE snowflake_sample_data.tpch_sf1.orders
SET orderkey = 1, totalprice = 2
WHERE orderkey = 3
""",
        dialect="snowflake",
        schemas={
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,snowflake_sample_data.tpch_sf1.orders,PROD)": {
                "orderkey": "NUMBER(38,0)",
                "totalprice": "NUMBER(12,2)",
            },
        },
        expected_file=RESOURCE_DIR / "test_snowflake_update_hardcoded.json",
    )


def test_snowflake_update_from_table():
    # Can create these tables with the following SQL:
    """
    -- Create or replace my_table
    CREATE OR REPLACE TABLE my_table (
        id INT IDENTITY PRIMARY KEY,
        col1 VARCHAR(50),
        col2 VARCHAR(50)
    );

    -- Create or replace table1
    CREATE OR REPLACE TABLE table1 (
        id INT IDENTITY PRIMARY KEY,
        col1 VARCHAR(50),
        col2 VARCHAR(50)
    );

    -- Create or replace table2
    CREATE OR REPLACE TABLE table2 (
        id INT IDENTITY PRIMARY KEY,
        col2 VARCHAR(50)
    );

    -- Insert data into my_table
    INSERT INTO my_table (col1, col2)
    VALUES ('foo', 'bar'),
           ('baz', 'qux');

    -- Insert data into table1
    INSERT INTO table1 (col1, col2)
    VALUES ('foo', 'bar'),
           ('baz', 'qux');

    -- Insert data into table2
    INSERT INTO table2 (col2)
    VALUES ('bar'),
           ('qux');
    """

    assert_sql_result(
        """
UPDATE my_table
SET
    col1 = t1.col1 || t1.col2,
    col2 = t1.col1 || t2.col2
FROM table1 t1
JOIN table2 t2 ON t1.id = t2.id
WHERE my_table.id = t1.id;
""",
        dialect="snowflake",
        default_db="my_db",
        default_schema="my_schema",
        schemas={
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.my_table,PROD)": {
                "id": "NUMBER(38,0)",
                "col1": "VARCHAR(16777216)",
                "col2": "VARCHAR(16777216)",
            },
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.table1,PROD)": {
                "id": "NUMBER(38,0)",
                "col1": "VARCHAR(16777216)",
                "col2": "VARCHAR(16777216)",
            },
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.table2,PROD)": {
                "id": "NUMBER(38,0)",
                "col1": "VARCHAR(16777216)",
                "col2": "VARCHAR(16777216)",
            },
        },
        expected_file=RESOURCE_DIR / "test_snowflake_update_from_table.json",
    )


def test_snowflake_update_self():
    assert_sql_result(
        """
UPDATE snowflake_sample_data.tpch_sf1.orders
SET orderkey = orderkey + 1
""",
        dialect="snowflake",
        schemas={
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,snowflake_sample_data.tpch_sf1.orders,PROD)": {
                "orderkey": "NUMBER(38,0)",
                "totalprice": "NUMBER(12,2)",
            },
        },
        expected_file=RESOURCE_DIR / "test_snowflake_update_self.json",
    )


def test_postgres_select_subquery():
    assert_sql_result(
        """
SELECT
    a,
    b,
    (SELECT c FROM table2 WHERE table2.id = table1.id) as c
FROM table1
""",
        dialect="postgres",
        default_db="my_db",
        default_schema="my_schema",
        schemas={
            "urn:li:dataset:(urn:li:dataPlatform:postgres,my_db.my_schema.table1,PROD)": {
                "id": "INTEGER",
                "a": "INTEGER",
                "b": "INTEGER",
            },
            "urn:li:dataset:(urn:li:dataPlatform:postgres,my_db.my_schema.table2,PROD)": {
                "id": "INTEGER",
                "c": "INTEGER",
            },
        },
        expected_file=RESOURCE_DIR / "test_postgres_select_subquery.json",
    )


def test_postgres_update_subselect():
    assert_sql_result(
        """
UPDATE accounts SET sales_person_name =
    (SELECT name FROM employees
     WHERE employees.id = accounts.sales_person_id)
""",
        dialect="postgres",
        default_db="my_db",
        default_schema="my_schema",
        schemas={
            "urn:li:dataset:(urn:li:dataPlatform:postgres,my_db.my_schema.accounts,PROD)": {
                "id": "INTEGER",
                "sales_person_id": "INTEGER",
                "sales_person_name": "VARCHAR(16777216)",
            },
            "urn:li:dataset:(urn:li:dataPlatform:postgres,my_db.my_schema.employees,PROD)": {
                "id": "INTEGER",
                "name": "VARCHAR(16777216)",
            },
        },
        expected_file=RESOURCE_DIR / "test_postgres_update_subselect.json",
    )


@pytest.mark.skip(reason="We can't parse column-list syntax with sub-selects yet")
def test_postgres_complex_update():
    # Example query from the postgres docs:
    # https://www.postgresql.org/docs/current/sql-update.html
    assert_sql_result(
        """
UPDATE accounts SET (contact_first_name, contact_last_name) =
    (SELECT first_name, last_name FROM employees
     WHERE employees.id = accounts.sales_person);
""",
        dialect="postgres",
        schemas={
            "urn:li:dataset:(urn:li:dataPlatform:postgres,my_db.my_schema.accounts,PROD)": {
                "id": "INTEGER",
                "contact_first_name": "VARCHAR(16777216)",
                "contact_last_name": "VARCHAR(16777216)",
                "sales_person": "INTEGER",
            },
            "urn:li:dataset:(urn:li:dataPlatform:postgres,my_db.my_schema.employees,PROD)": {
                "id": "INTEGER",
                "first_name": "VARCHAR(16777216)",
                "last_name": "VARCHAR(16777216)",
            },
        },
        expected_file=RESOURCE_DIR / "test_postgres_complex_update.json",
    )


def test_redshift_materialized_view_auto_refresh():
    # Example query from the redshift docs: https://docs.aws.amazon.com/prescriptive-guidance/latest/materialized-views-redshift/refreshing-materialized-views.html
    assert_sql_result(
        """
CREATE MATERIALIZED VIEW mv_total_orders
AUTO REFRESH YES -- Add this clause to auto refresh the MV
AS
 SELECT c.cust_id,
        c.first_name,
        sum(o.amount) as total_amount
 FROM orders o
 JOIN customer c
    ON c.cust_id = o.customer_id
 GROUP BY c.cust_id,
          c.first_name;
""",
        dialect="redshift",
        expected_file=RESOURCE_DIR
        / "test_redshift_materialized_view_auto_refresh.json",
    )


def test_redshift_temp_table_shortcut():
    # On redshift, tables starting with # are temporary tables.
    assert_sql_result(
        """
CREATE TABLE #my_custom_name
distkey (1)
sortkey (1,2)
AS
WITH cte AS (
SELECT *
FROM other_schema.table1
)
SELECT * FROM cte
""",
        dialect="redshift",
        default_db="my_db",
        default_schema="my_schema",
        schemas={
            "urn:li:dataset:(urn:li:dataPlatform:redshift,my_db.other_schema.table1,PROD)": {
                "col1": "INTEGER",
                "col2": "INTEGER",
            },
        },
        expected_file=RESOURCE_DIR / "test_redshift_temp_table_shortcut.json",
    )


def test_redshift_union_view():
    # TODO: This currently fails to generate CLL. Need to debug further.
    assert_sql_result(
        """
CREATE VIEW sales_vw AS SELECT * FROM public.sales UNION ALL SELECT * FROM spectrum.sales WITH NO SCHEMA BINDING
""",
        dialect="redshift",
        default_db="my_db",
        schemas={
            "urn:li:dataset:(urn:li:dataPlatform:redshift,my_db.public.sales,PROD)": {
                "col1": "INTEGER",
                "col2": "INTEGER",
            },
            # Testing a case where we only have one schema available.
        },
        expected_file=RESOURCE_DIR / "test_redshift_union_view.json",
    )


def test_redshift_system_automove() -> None:
    # Came across this in the Redshift query log, but it seems to be a system-generated query.
    assert_sql_result(
        """
CREATE TABLE "pg_automv"."mv_tbl__auto_mv_12708107__0_recomputed"
BACKUP YES
DISTSTYLE KEY
DISTKEY(2)
AS (
    SELECT
        COUNT(CAST(1 AS INT4)) AS "aggvar_3",
        COUNT(CAST(1 AS INT4)) AS "num_rec"
    FROM
        "public"."permanent_1" AS "permanent_1"
    WHERE (
        (CAST("permanent_1"."insertxid" AS INT8) <= 41990135)
        AND (CAST("permanent_1"."deletexid" AS INT8) > 41990135)
    )
    OR (
        CAST(FALSE AS BOOL)
        AND (CAST("permanent_1"."insertxid" AS INT8) = 0)
        AND (CAST("permanent_1"."deletexid" AS INT8) <> 0)
    )
)
""",
        dialect="redshift",
        default_db="my_db",
        expected_file=RESOURCE_DIR / "test_redshift_system_automove.json",
    )


def test_snowflake_with_unnamed_column_from_udf_call() -> None:
    assert_sql_result(
        """SELECT
  A.ID,
  B.NAME,
  PARSE_JSON(B.MY_JSON) AS :userInfo,
  B.ADDRESS
FROM my_db.my_schema.my_table AS A
LEFT JOIN my_db.my_schema.my_table_B AS B
  ON A.ID = B.ID
""",
        dialect="snowflake",
        default_db="my_db",
        expected_file=RESOURCE_DIR / "test_snowflake_unnamed_column_udf.json",
    )


def test_sqlite_insert_into_values() -> None:
    assert_sql_result(
        """\
INSERT INTO my_table (id, month, total_cost, area)
    VALUES
        (1, '2021-01', 100, 10),
        (2, '2021-02', 200, 20),
        (3, '2021-03', 300, 30)
""",
        dialect="sqlite",
        expected_file=RESOURCE_DIR / "test_sqlite_insert_into_values.json",
    )


def test_bigquery_information_schema_query() -> None:
    # Special case - the BigQuery INFORMATION_SCHEMA views are prefixed with a
    # project + possibly a dataset/region, so sometimes are 4 parts instead of 3.
    # https://cloud.google.com/bigquery/docs/information-schema-intro#syntax

    assert_sql_result(
        """\
select
  c.table_catalog as table_catalog,
  c.table_schema as table_schema,
  c.table_name as table_name,
  c.column_name as column_name,
  c.ordinal_position as ordinal_position,
  cfp.field_path as field_path,
  c.is_nullable as is_nullable,
  CASE WHEN CONTAINS_SUBSTR(cfp.field_path, ".") THEN NULL ELSE c.data_type END as data_type,
  description as comment,
  c.is_hidden as is_hidden,
  c.is_partitioning_column as is_partitioning_column,
  c.clustering_ordinal_position as clustering_ordinal_position,
from
  `acryl-staging-2`.`smoke_test_db_4`.INFORMATION_SCHEMA.COLUMNS c
  join `acryl-staging-2`.`smoke_test_db_4`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS as cfp on cfp.table_name = c.table_name
  and cfp.column_name = c.column_name
ORDER BY
  table_catalog, table_schema, table_name, ordinal_position ASC, data_type DESC""",
        dialect="bigquery",
        expected_file=RESOURCE_DIR / "test_bigquery_information_schema_query.json",
    )


def test_bigquery_alter_table_column() -> None:
    assert_sql_result(
        """\
ALTER TABLE `my-bq-project.covid_data.covid_deaths` drop COLUMN patient_name
    """,
        dialect="bigquery",
        expected_file=RESOURCE_DIR / "test_bigquery_alter_table_column.json",
    )


def test_sqlite_drop_table() -> None:
    assert_sql_result(
        """\
DROP TABLE my_schema.my_table
""",
        dialect="sqlite",
        expected_file=RESOURCE_DIR / "test_sqlite_drop_table.json",
    )


def test_sqlite_drop_view() -> None:
    assert_sql_result(
        """\
DROP VIEW my_schema.my_view
""",
        dialect="sqlite",
        expected_file=RESOURCE_DIR / "test_sqlite_drop_view.json",
    )


def test_snowflake_drop_schema() -> None:
    assert_sql_result(
        """\
DROP SCHEMA my_schema
""",
        dialect="snowflake",
        expected_file=RESOURCE_DIR / "test_snowflake_drop_schema.json",
    )


def test_bigquery_subquery_column_inference() -> None:
    assert_sql_result(
        """\
SELECT user_id, source, user_source
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY __partition_day DESC) AS rank_
    FROM invent_dw.UserDetail
) source_user
WHERE rank_ = 1
""",
        dialect="bigquery",
        expected_file=RESOURCE_DIR / "test_bigquery_subquery_column_inference.json",
    )


def test_sqlite_attach_database() -> None:
    assert_sql_result(
        """\
ATTACH DATABASE ':memory:' AS aux1
""",
        dialect="sqlite",
        expected_file=RESOURCE_DIR / "test_sqlite_attach_database.json",
        allow_table_error=True,
    )


def test_mssql_casing_resolver() -> None:
    assert_sql_result(
        """\
SELECT Age, name, UPPERCASED_COL, COUNT(*) as Count
FROM Foo.Persons
GROUP BY Age
""",
        dialect="mssql",
        default_db="NewData",
        schemas={
            "urn:li:dataset:(urn:li:dataPlatform:mssql,newdata.foo.persons,PROD)": {
                "Age": "INTEGER",
                "Name": "VARCHAR(16777216)",
                "Uppercased_Col": "VARCHAR(16777216)",
            },
        },
        expected_file=RESOURCE_DIR / "test_mssql_casing_resolver.json",
    )


def test_mssql_select_into() -> None:
    assert_sql_result(
        """\
SELECT age as AGE, COUNT(*) as Count
INTO Foo.age_dist
FROM Foo.Persons
GROUP BY Age
""",
        dialect="mssql",
        default_db="NewData",
        schemas={
            "urn:li:dataset:(urn:li:dataPlatform:mssql,newdata.foo.persons,PROD)": {
                "Age": "INTEGER",
                "Name": "VARCHAR(16777216)",
            },
            "urn:li:dataset:(urn:li:dataPlatform:mssql,newdata.foo.age_dist,PROD)": {
                "AGE": "INTEGER",
                "Count": "INTEGER",
            },
        },
        expected_file=RESOURCE_DIR / "test_mssql_select_into.json",
    )
