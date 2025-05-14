from constants import namespace, table_name, warehouse
from pyiceberg.catalog import load_catalog

# Load the catalog
from datahub.ingestion.graph.client import get_default_graph

graph = get_default_graph()
catalog = load_catalog("local_datahub", warehouse=warehouse, token=graph.config.token)
# Append the data to the Iceberg table
catalog.drop_table(f"{namespace}.{table_name}")
