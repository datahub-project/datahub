from datetime import datetime

from airflow import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

CONN_ID = "my_sqlite"

COST_TABLE = "costs"
PROCESSED_TABLE = "processed_costs"

with DAG(
    "sqlite_operator",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    create_cost_table = SqliteOperator(
        sqlite_conn_id=CONN_ID,
        task_id="create_cost_table",
        sql="""
        CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
            id INTEGER PRIMARY KEY,
            month TEXT NOT NULL,
            total_cost REAL NOT NULL,
            area REAL NOT NULL
        )
        """,
        params={"table_name": COST_TABLE},
    )

    populate_cost_table = SqliteOperator(
        sqlite_conn_id=CONN_ID,
        task_id="populate_cost_table",
        sql="""
        INSERT INTO {{ params.table_name }} (id, month, total_cost, area)
        VALUES
            (1, '2021-01', 100, 10),
            (2, '2021-02', 200, 20),
            (3, '2021-03', 300, 30)
        """,
        params={"table_name": COST_TABLE},
    )

    transform_cost_table = SqliteOperator(
        sqlite_conn_id=CONN_ID,
        task_id="transform_cost_table",
        sql="""
        CREATE TABLE IF NOT EXISTS {{ params.out_table_name }} AS
        SELECT
            id,
            month,
            total_cost,
            area,
            total_cost / area as cost_per_area
        FROM {{ params.in_table_name }}
        """,
        params={
            "in_table_name": COST_TABLE,
            "out_table_name": PROCESSED_TABLE,
        },
    )

    cleanup_tables = []
    for table_name in [COST_TABLE, PROCESSED_TABLE]:
        cleanup_table = SqliteOperator(
            sqlite_conn_id=CONN_ID,
            task_id=f"cleanup_{table_name}",
            sql="""
            DROP TABLE {{ params.table_name }}
            """,
            params={"table_name": table_name},
        )
        cleanup_tables.append(cleanup_table)

    create_cost_table >> populate_cost_table >> transform_cost_table >> cleanup_tables
