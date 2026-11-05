from datahub.ingestion.source.qlik_sense.script_lineage import (
    connection_lookup_names,
    infer_data_connection,
    parse_load_script,
)

SCRIPT = """
// Load from files, not a database
Sales: LOAD * FROM [lib://Files/sales.qvd] (qvd);

/* SQL Server
   source */
LIB CONNECT TO 'Finance:Sales SQL';
Orders:
NoConcatenate
SQL SELECT o.order_id, o.amount FROM dbo.orders o WHERE note <> 'a;b';

Customers:
LOAD customer_id, upper(name) AS name;
SELECT customer_id, name FROM dbo.customers;

DISCONNECT;
ODBC CONNECT TO [Lakehouse];
SELECT * FROM main.sales.events;
"""


def test_parse_load_script_tracks_connections_and_labels() -> None:
    tables = parse_load_script(SCRIPT)

    assert [(t.label, t.connection) for t in tables] == [
        ("Orders", "Finance:Sales SQL"),
        ("Customers", "Finance:Sales SQL"),
        ("table_3", "Lakehouse"),
    ]
    assert tables[0].sql.startswith("SELECT o.order_id")
    assert "'a;b'" in tables[0].sql


def test_infer_data_connection_reads_driver_and_database() -> None:
    sql_server = infer_data_connection(
        "Sales SQL",
        "OLEDB CONNECT TO [Provider=MSOLEDBSQL;Data Source=db;Initial Catalog=sales_db]",
    )
    assert (sql_server.platform, sql_server.database) == ("mssql", "sales_db")

    databricks = infer_data_connection("Lakehouse", "Simba Spark ODBC Databricks")
    assert databricks.platform == "databricks"

    assert infer_data_connection("Files", "folder").platform is None


def test_connection_lookup_names_strips_cloud_space() -> None:
    assert connection_lookup_names("Finance:Sales SQL") == [
        "Finance:Sales SQL",
        "Sales SQL",
    ]
    assert connection_lookup_names("Sales SQL") == ["Sales SQL"]
