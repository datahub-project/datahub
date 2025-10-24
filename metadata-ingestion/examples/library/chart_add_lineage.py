from datahub.sdk import Chart, DataHubClient, Dataset

client = DataHubClient.from_env()

# Define the source datasets
upstream_dataset1 = Dataset(platform="bigquery", name="project.dataset.sales_table")
upstream_dataset2 = Dataset(platform="bigquery", name="project.dataset.customer_table")

# Create a chart with lineage to upstream datasets
chart = Chart(
    name="sales_by_customer_chart",
    platform="looker",
    display_name="Sales by Customer",
    description="Bar chart showing total sales aggregated by customer",
    input_datasets=[upstream_dataset1, upstream_dataset2],
)

client.entities.upsert(chart)
