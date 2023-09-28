# This file is copied from https://docs.astronomer.io/learn/airflow-snowflake.

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import (
    SQLColumnCheckOperator,
    SQLTableCheckOperator,
)
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.task_group import TaskGroup
from pendulum import datetime

SNOWFLAKE_FORESTFIRE_TABLE = "forestfires"
SNOWFLAKE_COST_TABLE = "costs"
SNOWFLAKE_FORESTFIRE_COST_TABLE = "forestfire_costs"

SNOWFLAKE_CONN_ID = "snowflake_default"

ROW_COUNT_CHECK = "COUNT(*) = 9"


class sql_stmts:
    create_forestfire_table = """
        CREATE OR REPLACE TABLE {{ params.table_name }}
            (
                id INT,
                y INT,
                month VARCHAR(25),
                day VARCHAR(25),
                ffmc FLOAT,
                dmc FLOAT,
                dc FLOAT,
                isi FLOAT,
                temp FLOAT,
                rh FLOAT,
                wind FLOAT,
                rain FLOAT,
                area FLOAT
            );
    """

    create_cost_table = """
        CREATE OR REPLACE TABLE {{ params.table_name }}
            (
                id INT,
                land_damage_cost INT,
                property_damage_cost INT,
                lost_profits_cost INT
            );
    """

    create_forestfire_cost_table = """
        CREATE OR REPLACE TABLE {{ params.table_name }}
            (
                id INT,
                land_damage_cost INT,
                property_damage_cost INT,
                lost_profits_cost INT,
                total_cost INT,
                y INT,
                month VARCHAR(25),
                day VARCHAR(25),
                area FLOAT
            );
    """

    load_forestfire_data = """
        INSERT INTO {{ params.table_name }} VALUES
            (1,2,'aug','fri',91,166.9,752.6,7.1,25.9,41,3.6,0,100),
            (2,2,'feb','mon',84,9.3,34,2.1,13.9,40,5.4,0,57.8),
            (3,4,'mar','sat',69,2.4,15.5,0.7,17.4,24,5.4,0,92.9),
            (4,4,'mar','mon',87.2,23.9,64.7,4.1,11.8,35,1.8,0,1300),
            (5,5,'mar','sat',91.7,35.8,80.8,7.8,15.1,27,5.4,0,4857),
            (6,5,'sep','wed',92.9,133.3,699.6,9.2,26.4,21,4.5,0,9800),
            (7,5,'mar','fri',86.2,26.2,94.3,5.1,8.2,51,6.7,0,14),
            (8,6,'mar','fri',91.7,33.3,77.5,9,8.3,97,4,0.2,74.5),
            (9,9,'feb','thu',84.2,6.8,26.6,7.7,6.7,79,3.1,0,8880.7);
    """

    load_cost_data = """
        INSERT INTO {{ params.table_name }} VALUES
            (1,150000,32000,10000),
            (2,200000,50000,50000),
            (3,90000,120000,300000),
            (4,230000,14000,7000),
            (5,98000,27000,48000),
            (6,72000,800000,0),
            (7,50000,2500000,0),
            (8,8000000,33000000,0),
            (9,6325000,450000,76000);
    """

    load_forestfire_cost_data = """
        INSERT INTO forestfire_costs (
                id, land_damage_cost, property_damage_cost, lost_profits_cost,
                total_cost, y, month, day, area
            )
            SELECT
                c.id,
                c.land_damage_cost,
                c.property_damage_cost,
                c.lost_profits_cost,
                c.land_damage_cost + c.property_damage_cost + c.lost_profits_cost,
                ff.y,
                ff.month,
                ff.day,
                ff.area
            FROM costs c
            LEFT JOIN forestfires ff
                ON c.id = ff.id
    """

    transform_forestfire_cost_table = """
        SELECT
            id,
            month,
            day,
            total_cost,
            area,
            total_cost / area as cost_per_area
        FROM {{ params.table_name }}
    """

    delete_table = """
        DROP TABLE {{ params.table_name }} ;
    """


with DAG(
    "complex_snowflake_example",
    description="""
        Example DAG showcasing loading, transforming,
        and data quality checking with multiple datasets in Snowflake.
    """,
    doc_md=__doc__,
    start_date=datetime(2022, 12, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    """
    #### Snowflake table creation
    Create the tables to store sample data.
    """
    create_forestfire_table = SnowflakeOperator(
        task_id="create_forestfire_table",
        sql=sql_stmts.create_forestfire_table,
        params={"table_name": SNOWFLAKE_FORESTFIRE_TABLE},
    )

    create_cost_table = SnowflakeOperator(
        task_id="create_cost_table",
        sql=sql_stmts.create_cost_table,
        params={"table_name": SNOWFLAKE_COST_TABLE},
    )

    create_forestfire_cost_table = SnowflakeOperator(
        task_id="create_forestfire_cost_table",
        sql=sql_stmts.create_forestfire_cost_table,
        params={"table_name": SNOWFLAKE_FORESTFIRE_COST_TABLE},
    )

    """
    #### Insert data
    Insert data into the Snowflake tables using existing SQL queries
    stored in the include/sql/snowflake_examples/ directory.
    """
    load_forestfire_data = SnowflakeOperator(
        task_id="load_forestfire_data",
        sql=sql_stmts.load_forestfire_data,
        params={"table_name": SNOWFLAKE_FORESTFIRE_TABLE},
    )

    load_cost_data = SnowflakeOperator(
        task_id="load_cost_data",
        sql=sql_stmts.load_cost_data,
        params={"table_name": SNOWFLAKE_COST_TABLE},
    )

    load_forestfire_cost_data = SnowflakeOperator(
        task_id="load_forestfire_cost_data",
        sql=sql_stmts.load_forestfire_cost_data,
        params={"table_name": SNOWFLAKE_FORESTFIRE_COST_TABLE},
    )

    """
    #### Transform
    Transform the forestfire_costs table to perform
    sample logic.
    """
    transform_forestfire_cost_table = SnowflakeOperator(
        task_id="transform_forestfire_cost_table",
        sql=sql_stmts.transform_forestfire_cost_table,
        params={"table_name": SNOWFLAKE_FORESTFIRE_COST_TABLE},
    )

    """
    #### Quality checks
    Perform data quality checks on the various tables.
    """
    with TaskGroup(
        group_id="quality_check_group_forestfire",
        default_args={
            "conn_id": SNOWFLAKE_CONN_ID,
        },
    ) as quality_check_group_forestfire:
        """
        #### Column-level data quality check
        Run data quality checks on columns of the forestfire table
        """
        forestfire_column_checks = SQLColumnCheckOperator(
            task_id="forestfire_column_checks",
            table=SNOWFLAKE_FORESTFIRE_TABLE,
            column_mapping={
                "ID": {"null_check": {"equal_to": 0}},
                "RH": {"max": {"leq_to": 100}},
            },
        )

        """
        #### Table-level data quality check
        Run data quality checks on the forestfire table
        """
        forestfire_table_checks = SQLTableCheckOperator(
            task_id="forestfire_table_checks",
            table=SNOWFLAKE_FORESTFIRE_TABLE,
            checks={"row_count_check": {"check_statement": ROW_COUNT_CHECK}},
        )

    with TaskGroup(
        group_id="quality_check_group_cost",
        default_args={
            "conn_id": SNOWFLAKE_CONN_ID,
        },
    ) as quality_check_group_cost:
        """
        #### Column-level data quality check
        Run data quality checks on columns of the forestfire table
        """
        cost_column_checks = SQLColumnCheckOperator(
            task_id="cost_column_checks",
            table=SNOWFLAKE_COST_TABLE,
            column_mapping={
                "ID": {"null_check": {"equal_to": 0}},
                "LAND_DAMAGE_COST": {"min": {"geq_to": 0}},
                "PROPERTY_DAMAGE_COST": {"min": {"geq_to": 0}},
                "LOST_PROFITS_COST": {"min": {"geq_to": 0}},
            },
        )

        """
        #### Table-level data quality check
        Run data quality checks on the forestfire table
        """
        cost_table_checks = SQLTableCheckOperator(
            task_id="cost_table_checks",
            table=SNOWFLAKE_COST_TABLE,
            checks={"row_count_check": {"check_statement": ROW_COUNT_CHECK}},
        )

    with TaskGroup(
        group_id="quality_check_group_forestfire_costs",
        default_args={
            "conn_id": SNOWFLAKE_CONN_ID,
        },
    ) as quality_check_group_forestfire_costs:
        """
        #### Column-level data quality check
        Run data quality checks on columns of the forestfire table
        """
        forestfire_costs_column_checks = SQLColumnCheckOperator(
            task_id="forestfire_costs_column_checks",
            table=SNOWFLAKE_FORESTFIRE_COST_TABLE,
            column_mapping={"AREA": {"min": {"geq_to": 0}}},
        )

        """
        #### Table-level data quality check
        Run data quality checks on the forestfire table
        """
        forestfire_costs_table_checks = SQLTableCheckOperator(
            task_id="forestfire_costs_table_checks",
            table=SNOWFLAKE_FORESTFIRE_COST_TABLE,
            checks={
                "row_count_check": {"check_statement": ROW_COUNT_CHECK},
                "total_cost_check": {
                    "check_statement": "land_damage_cost + \
                    property_damage_cost + lost_profits_cost = total_cost"
                },
            },
        )

    """
    #### Delete tables
    Clean up the tables created for the example.
    """
    delete_forestfire_table = SnowflakeOperator(
        task_id="delete_forestfire_table",
        sql=sql_stmts.delete_table,
        params={"table_name": SNOWFLAKE_FORESTFIRE_TABLE},
    )

    delete_cost_table = SnowflakeOperator(
        task_id="delete_costs_table",
        sql=sql_stmts.delete_table,
        params={"table_name": SNOWFLAKE_COST_TABLE},
    )

    delete_forestfire_cost_table = SnowflakeOperator(
        task_id="delete_forestfire_cost_table",
        sql=sql_stmts.delete_table,
        params={"table_name": SNOWFLAKE_FORESTFIRE_COST_TABLE},
    )

    begin = EmptyOperator(task_id="begin")
    create_done = EmptyOperator(task_id="create_done")
    load_done = EmptyOperator(task_id="load_done")
    end = EmptyOperator(task_id="end")

    chain(
        begin,
        [create_forestfire_table, create_cost_table, create_forestfire_cost_table],
        create_done,
        [load_forestfire_data, load_cost_data],
        load_done,
        [quality_check_group_forestfire, quality_check_group_cost],
        load_forestfire_cost_data,
        quality_check_group_forestfire_costs,
        transform_forestfire_cost_table,
        [delete_forestfire_table, delete_cost_table, delete_forestfire_cost_table],
        end,
    )
