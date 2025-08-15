from constants import namespace, table_name, warehouse
from pyiceberg.catalog import load_catalog

# Load the catalog
from datahub.ingestion.graph.client import get_default_graph

graph = get_default_graph()

catalog = load_catalog("local_datahub", warehouse=warehouse, token=graph.config.token)
# Append the data to the Iceberg table
table = catalog.load_table(f"{namespace}.{table_name}")
con = table.scan().to_duckdb(table_name=f"{table_name}")

for row in con.execute(f"SELECT * FROM {table_name}").fetchall():
    print(row)
