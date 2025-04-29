import logging

from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.sdk.dataset import Dataset
from datahub.sdk.lineage_client import LineageClient
from datahub.sdk.main_client import DataHubClient

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_assets(client: DataHubClient) -> tuple:
    # Create input dataset
    input_dataset = Dataset(
        platform="snowflake",
        name="input_sales_data",
        display_name="Sales Data",
        env="PROD",
        description="Source sales data",
        schema=[
            ("order_id", "string"),
            ("customer_id", "string"),
            ("product_id", "string"),
            ("amount", "decimal"),
        ],
    )
    client.entities.upsert(input_dataset)
    logger.info(f"Created input dataset: {input_dataset.display_name}")

    # Create output dataset
    output_dataset = Dataset(
        platform="snowflake",
        name="output_sales_summary",
        display_name="Sales Summary",
        env="PROD",
        description="Processed sales summary",
        schema=[
            ("customer_id", "string"),
            ("total_orders", "integer"),
            ("total_amount", "decimal"),
        ],
    )
    client.entities.upsert(output_dataset)
    logger.info(f"Created output dataset: {output_dataset.display_name}")

    # Create dataflow
    processing_flow = DataFlow(
        id="sales_data_processing",
        name="Sales Data Processing",
        description="Data flow for processing sales data",
        platform="airflow",
    )

    client.entities.upsert(processing_flow)

    # Create datajob
    processing_job = DataJob(
        id="process_sales_data",
        flow_urn=processing_flow.urn,
        name="Process Sales Data",
        description="Transform sales data into summary statistics",
        platform="airflow",
    )
    client.entities.upsert(processing_job)
    logger.info(f"Created datajob: {processing_job.name}")

    return input_dataset, output_dataset, processing_job


if __name__ == "__main__":
    client = DataHubClient.from_env()
    lineage_client = LineageClient(client=client)
    input_dataset, output_dataset, processing_job = create_assets(client)
    # Create lineage connections
    logger.info("\n=== Creating lineage connections ===")

    # Input dataset to job
    lineage_client.add_datajob_lineage(
        upstream=input_dataset.urn,
        downstream=processing_job.urn,
    )
    logger.info(f"Added lineage: {input_dataset.display_name} → {processing_job.name}")

    # Job to output dataset
    lineage_client.add_datajob_lineage(
        upstream=processing_job.urn,
        downstream=output_dataset.urn,
    )
    logger.info(f"Added lineage: {processing_job.name} → {output_dataset.display_name}")
