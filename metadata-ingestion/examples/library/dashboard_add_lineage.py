from datahub.sdk import Dashboard, DataHubClient, Dataset

client = DataHubClient.from_env()

# Define the source datasets
sales_dataset = Dataset(platform="bigquery", name="project.dataset.sales")
customer_dataset = Dataset(platform="bigquery", name="project.dataset.customers")
products_dataset = Dataset(platform="bigquery", name="project.dataset.products")

# Create dashboard with lineage to upstream datasets
dashboard = Dashboard(
    platform="looker",
    name="sales_dashboard",
    display_name="Sales Overview Dashboard",
    description="Dashboard showing sales metrics across regions and products",
)

# Add dataset dependencies
dashboard.add_input_dataset(sales_dataset)
dashboard.add_input_dataset(customer_dataset)
dashboard.add_input_dataset(products_dataset)

# Upsert the dashboard (this will create the Consumes relationships)
client.entities.upsert(dashboard)
