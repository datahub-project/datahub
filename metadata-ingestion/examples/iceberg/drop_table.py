# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from constants import namespace, table_name, warehouse
from pyiceberg.catalog import load_catalog

# Load the catalog
from datahub.ingestion.graph.client import get_default_graph

graph = get_default_graph()
catalog = load_catalog("local_datahub", warehouse=warehouse, token=graph.config.token)
# Append the data to the Iceberg table
catalog.drop_table(f"{namespace}.{table_name}")
