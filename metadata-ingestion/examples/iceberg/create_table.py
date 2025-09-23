from datetime import datetime

import pyarrow as pa
import pyiceberg
from constants import namespace, table_name, warehouse
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType, TimestampType

from datahub.ingestion.graph.client import get_default_graph

# Define a more comprehensive schema for ski resort data
schema = Schema(
    NestedField(
        field_id=1,
        name="resort_id",
        field_type=LongType(),
        required=True,
        doc="Unique identifier for each ski resort",
        initial_default=None,
    ),
    NestedField(
        field_id=2,
        name="resort_name",
        field_type=StringType(),
        required=True,
        doc="Official name of the ski resort",
        initial_default=None,
    ),
    NestedField(
        field_id=3,
        name="daily_snowfall",
        field_type=LongType(),
        required=False,
        doc="Amount of new snow in inches during the last 24 hours. Null if no measurement available",
        initial_default=0,
    ),
    NestedField(
        field_id=4,
        name="conditions",
        field_type=StringType(),
        required=False,
        doc="Current snow conditions description (e.g., 'Powder', 'Packed Powder', 'Groomed'). Null if not reported",
        initial_default=None,
    ),
    NestedField(
        field_id=5,
        name="last_updated",
        field_type=TimestampType(),
        required=False,
        doc="Timestamp of when the snow report was last updated",
        initial_default=None,
    ),
)

# Load the catalog with new warehouse name
graph = get_default_graph()
catalog = load_catalog("local_datahub", warehouse=warehouse, token=graph.config.token)

# Create namespace (database)
try:
    catalog.create_namespace(namespace)
except Exception as e:
    print(f"Namespace creation error (might already exist): {e}")

full_table_name = f"{namespace}.{table_name}"
try:
    catalog.create_table(full_table_name, schema)
except pyiceberg.exceptions.TableAlreadyExistsError:
    print(f"Table {full_table_name} already exists")

# Create sample data with explicit PyArrow schema to match required fields
pa_schema = pa.schema(
    [
        ("resort_id", pa.int64(), False),  # False means not nullable
        ("resort_name", pa.string(), False),  # False means not nullable
        ("daily_snowfall", pa.int64(), True),
        ("conditions", pa.string(), True),
        ("last_updated", pa.timestamp("us"), True),
    ]
)
# Create sample data
sample_data = pa.Table.from_pydict(
    {
        "resort_id": [1, 2, 3],
        "resort_name": ["Snowpeak Resort", "Alpine Valley", "Glacier Heights"],
        "daily_snowfall": [12, 8, 15],
        "conditions": ["Powder", "Packed", "Fresh Powder"],
        "last_updated": [
            pa.scalar(datetime.now()),
            pa.scalar(datetime.now()),
            pa.scalar(datetime.now()),
        ],
    },
    schema=pa_schema,
)

# Write data to table
table = catalog.load_table(full_table_name)
table.overwrite(sample_data)

table.refresh()
# Read and verify data
con = table.scan().to_duckdb(table_name=f"{table_name}")
print("\nResort Metrics Data:")
print("-" * 50)
for row in con.execute(f"SELECT * FROM {table_name}").fetchall():
    print(row)
