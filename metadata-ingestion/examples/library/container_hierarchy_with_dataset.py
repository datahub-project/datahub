from datahub.emitter.mcp_builder import DatabaseKey, SchemaKey
from datahub.metadata.urns import CorpUserUrn, TagUrn
from datahub.sdk import Container, DataHubClient, Dataset

client = DataHubClient.from_env()

# Create a complete container hierarchy for a Snowflake database structure
# Hierarchy: Platform -> Database -> Schema -> Dataset

# Step 1: Create the database container (top-level)
database_key = DatabaseKey(
    platform="snowflake",
    instance="production",
    database="analytics_db",
)

database_container = Container(
    database_key,
    display_name="Analytics Database",
    description="Main analytics database for business intelligence",
    subtype="Database",
    parent_container=None,
)

# Step 2: Create schema containers within the database
reporting_schema_key = SchemaKey(
    platform="snowflake",
    instance="production",
    database="analytics_db",
    schema="reporting",
)

reporting_schema_container = Container(
    reporting_schema_key,
    display_name="Reporting Schema",
    description="Schema for business reporting tables",
    subtype="Schema",
    parent_container=database_key,
    tags=[TagUrn("reporting")],
)

metrics_schema_key = SchemaKey(
    platform="snowflake",
    instance="production",
    database="analytics_db",
    schema="metrics",
)

metrics_schema_container = Container(
    metrics_schema_key,
    display_name="Metrics Schema",
    description="Schema for aggregated metrics and KPIs",
    subtype="Schema",
    parent_container=database_key,
    tags=[TagUrn("metrics"), TagUrn("kpi")],
)

# Step 3: Create datasets within the schema containers
sales_dataset = Dataset(
    platform="snowflake",
    name="analytics_db.reporting.sales_summary",
    env="PROD",
    description="Daily sales summary aggregations",
    parent_container=reporting_schema_key,
    schema=[
        ("date", "DATE", "Transaction date"),
        ("region", "VARCHAR", "Sales region"),
        ("revenue", "DECIMAL", "Total revenue in USD"),
        ("order_count", "INTEGER", "Number of orders"),
    ],
    owners=[(CorpUserUrn("sales-team"), "DATAOWNER")],
    tags=[TagUrn("sales"), TagUrn("daily")],
)

kpi_dataset = Dataset(
    platform="snowflake",
    name="analytics_db.metrics.monthly_kpis",
    env="PROD",
    description="Monthly key performance indicators",
    parent_container=metrics_schema_key,
    schema=[
        ("month", "DATE", "Month start date"),
        ("metric_name", "VARCHAR", "KPI metric name"),
        ("metric_value", "DECIMAL", "Metric value"),
        ("target_value", "DECIMAL", "Target value for the metric"),
    ],
    owners=[(CorpUserUrn("analytics-team"), "DATAOWNER")],
    tags=[TagUrn("kpi"), TagUrn("monthly")],
)

# Emit all entities to DataHub in hierarchical order
print("Creating container hierarchy...")
print()

# Emit database (top level)
client.entities.upsert(database_container)
print(f"1. Database: {database_container.display_name}")
print(f"   URN: {database_container.urn}")
print()

# Emit schemas (second level)
client.entities.upsert(reporting_schema_container)
print(f"2. Schema: {reporting_schema_container.display_name}")
print(f"   URN: {reporting_schema_container.urn}")
print(f"   Parent: {database_container.display_name}")
print()

client.entities.upsert(metrics_schema_container)
print(f"3. Schema: {metrics_schema_container.display_name}")
print(f"   URN: {metrics_schema_container.urn}")
print(f"   Parent: {database_container.display_name}")
print()

# Emit datasets (leaf level)
client.entities.upsert(sales_dataset)
print("4. Dataset: sales_summary")
print(f"   URN: {sales_dataset.urn}")
print(f"   Parent: {reporting_schema_container.display_name}")
print()

client.entities.upsert(kpi_dataset)
print("5. Dataset: monthly_kpis")
print(f"   URN: {kpi_dataset.urn}")
print(f"   Parent: {metrics_schema_container.display_name}")
print()

print("Complete hierarchy created:")
print("  analytics_db (Database)")
print("    ├── reporting (Schema)")
print("    │   └── sales_summary (Dataset)")
print("    └── metrics (Schema)")
print("        └── monthly_kpis (Dataset)")
